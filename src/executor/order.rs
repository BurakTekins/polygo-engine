use std::{str::FromStr, sync::Arc};

use polymarket_client_sdk_v2::types::U256;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use crate::config::ExecutionMode;
use crate::emitter::audit::{AuditEmitter, AuditEvent, AuditExitRetryAttempt};
use crate::engine::state::{round_5, EngineState};
use crate::engine::strategy::BtcPriceHistory;
use crate::health::HealthState;
use crate::market::{now_ms, ActiveMarket, MarketClock};
use crate::runtime_config::StrategyStore;
use crate::shadow::{ShadowExitEngine, ShadowExitRequest};
use crate::ws::polymarket::BookState;

use super::audit_context::audit_context;
use super::errors::{is_balance_not_settled_error, is_no_match_error};
use super::exit_decision::wait_for_exit_decision;
use super::math::{
    buy_notional_spent, buy_shares_filled, decimal_to_f64, floor_to_2_decimals, monotonic_ns,
    sell_price, sell_shares_filled, ElapsedToNowMs,
};
use super::rejection::{reject, reject_with_context};
pub use super::revalidation::revalidate_entry;
use super::revalidation::{executable_ask_shares, outcome_book};
pub use super::{client::LiveExecutor, types::OrderSide, types::OrderSignal};

const LIVE_EXIT_MAX_ATTEMPTS: u32 = 40;
const LIVE_EXIT_RETRY_MS: u64 = 250;
const ENTRY_DEPTH_MULTIPLIER: f64 = 1.25;
const LIVE_EXIT_GTC_REPRICE_ATTEMPTS: u32 = 3;
const LIVE_EXIT_GTC_POLLS_PER_PRICE: u32 = 8;
const LIVE_EXIT_GTC_FINAL_POLLS: u32 = 1_200;
const LIVE_EXIT_GTC_POLL_MS: u64 = 250;
const LIVE_EXIT_GTC_FLOOR_PRICE: f64 = 0.01;
const LIVE_ENTRY_GTC_POLLS: u32 = 8;
const LIVE_ENTRY_GTC_FAST_POLLS: u32 = 2;
const LIVE_ENTRY_GTC_POLL_MS: u64 = 250;
const LIVE_EXIT_BALANCE_RETRY_DELAYS_MS: [u64; 4] = [250, 500, 1_000, 2_000];

struct GtcExitResult {
    sold_shares: f64,
    sale_proceeds: f64,
    order_id: Option<String>,
    order_status: Option<String>,
    error: Option<String>,
    pending: bool,
}

struct GtcEntryResult {
    filled_shares: f64,
    notional_usd: f64,
    order_id: Option<String>,
    order_status: Option<String>,
    error: Option<String>,
    pending: bool,
    submit_latency_ms: Option<u64>,
    build_latency_ms: Option<u64>,
    sign_latency_ms: Option<u64>,
    post_latency_ms: Option<u64>,
    post_success: Option<bool>,
    post_status: Option<String>,
    post_making_amount: Option<f64>,
    post_taking_amount: Option<f64>,
    poll_count: u32,
    poll_status: Option<String>,
    poll_size_matched: Option<f64>,
    cancel_confirmed: Option<bool>,
    final_status: Option<String>,
    final_size_matched: Option<f64>,
}

pub async fn run(
    mut order_rx: mpsc::Receiver<OrderSignal>,
    market_rx: watch::Receiver<Option<ActiveMarket>>,
    book_state: Arc<BookState>,
    btc_price_history: Arc<BtcPriceHistory>,
    engine_state: Arc<EngineState>,
    health: Arc<HealthState>,
    market_clock: Arc<MarketClock>,
    strategy_store: Arc<StrategyStore>,
    execution_mode: ExecutionMode,
    live_executor: Option<LiveExecutor>,
    audit: AuditEmitter,
    shadow_exit: ShadowExitEngine,
) {
    info!(mode = execution_mode.as_str(), "Executor started");
    if let Some(live_executor) = live_executor.clone() {
        tokio::spawn(warm_live_market_cache(market_rx.clone(), live_executor));
    }
    while let Some(signal) = order_rx.recv().await {
        let signal_market = market_rx.borrow().clone();
        let signal_context = audit_context(
            book_state.load(),
            now_ms(),
            signal.side,
            signal_market.as_ref(),
            None,
            None,
            None,
        );
        let _ = audit.emit(AuditEvent::signal(signal, Some(signal_context)));
        let task_market_rx = market_rx.clone();
        let task_book_state = Arc::clone(&book_state);
        let task_btc_price_history = Arc::clone(&btc_price_history);
        let task_state = Arc::clone(&engine_state);
        let task_health = Arc::clone(&health);
        let task_clock = Arc::clone(&market_clock);
        let task_store = Arc::clone(&strategy_store);
        let task_audit = audit.clone();
        let task_live_executor = live_executor.clone();
        let task_shadow_exit = shadow_exit.clone();
        tokio::spawn(async move {
            match execution_mode {
                ExecutionMode::DryRun => {
                    execute_dry_run(
                        signal,
                        task_book_state,
                        task_btc_price_history,
                        task_state,
                        task_health,
                        task_clock,
                        task_store,
                        task_audit,
                        task_shadow_exit,
                    )
                    .await;
                }
                ExecutionMode::Live => {
                    let Some(live_executor) = task_live_executor else {
                        reject(signal, "live_executor_missing", &task_state, &task_audit);
                        return;
                    };
                    execute_live(
                        signal,
                        task_market_rx,
                        task_book_state,
                        task_btc_price_history,
                        task_state,
                        task_health,
                        task_clock,
                        task_store,
                        live_executor,
                        task_audit,
                        task_shadow_exit,
                    )
                    .await;
                }
            }
        });
    }
    warn!("Execution channel closed");
}

async fn warm_live_market_cache(
    mut market_rx: watch::Receiver<Option<ActiveMarket>>,
    live_executor: LiveExecutor,
) {
    let mut last_slug = String::new();
    loop {
        if market_rx.changed().await.is_err() {
            return;
        }
        let Some(market) = market_rx.borrow().clone() else {
            continue;
        };
        if market.slug == last_slug {
            continue;
        }
        let started_ms = now_ms();
        match live_executor.warm_market_cache(&market).await {
            Ok(()) => {
                last_slug = market.slug.clone();
                info!(
                    market_slug = %market.slug,
                    elapsed_ms = now_ms().saturating_sub(started_ms),
                    "Polymarket live order cache warmed"
                );
            }
            Err(error) => {
                warn!(%error, market_slug = %market.slug, "Polymarket live order cache warm-up failed");
            }
        }
    }
}

async fn execute_dry_run(
    signal: OrderSignal,
    book_state: Arc<BookState>,
    btc_price_history: Arc<BtcPriceHistory>,
    engine_state: Arc<EngineState>,
    health: Arc<HealthState>,
    market_clock: Arc<MarketClock>,
    strategy_store: Arc<StrategyStore>,
    audit: AuditEmitter,
    shadow_exit: ShadowExitEngine,
) {
    info!(
        side = signal.side.as_str(),
        signal_ts_ms = signal.signal_ts_ms,
        "Execution task started"
    );
    tokio::time::sleep_until(signal.execute_at).await;
    let executed_at_ms = now_ms();
    if !health.is_running() {
        reject(signal, "engine_stopped", &engine_state, &audit);
        return;
    }
    let Some(config) = strategy_store.load() else {
        reject(signal, "config_missing", &engine_state, &audit);
        return;
    };
    if config.generation != signal.config_generation {
        reject(signal, "config_changed", &engine_state, &audit);
        return;
    }
    let book = book_state.load();
    let Some((ask, shares)) =
        revalidate_entry(book, executed_at_ms, &market_clock, &config, signal.side)
    else {
        reject_with_context(
            signal,
            "entry_revalidation",
            &engine_state,
            &audit,
            Some(audit_context(
                book,
                executed_at_ms,
                signal.side,
                None,
                Some(config.entry_slippage),
                None,
                None,
            )),
        );
        return;
    };

    let position_id = engine_state.open_reserved();
    let _ = audit.emit(AuditEvent::DryRunEntry {
        position_id,
        signal_ts_ms: signal.signal_ts_ms,
        executed_at_ms,
        latency_ms: executed_at_ms.saturating_sub(signal.signal_ts_ms),
        side: signal.side.as_str(),
        price: round_5(ask),
        shares,
        notional_usd: round_5(ask * shares as f64),
    });

    let exit_decision = wait_for_exit_decision(
        &signal,
        executed_at_ms,
        ask,
        shares as f64,
        &config,
        &book_state,
        &btc_price_history,
    )
    .await;
    let exit_at_ms = exit_decision.decided_at_ms;
    let book = book_state.load();
    if exit_at_ms.saturating_sub(book.received_at_ms) > config.max_book_age_ms {
        engine_state.release_reservation();
        let _ = audit.emit(AuditEvent::ExecutionRejected {
            signal_ts_ms: signal.signal_ts_ms,
            side: signal.side.as_str(),
            reason: "stale_exit_book_position_released",
            context: None,
        });
        return;
    }
    let Some(exit_price) = outcome_book(book, signal.side).bid else {
        engine_state.release_reservation();
        let _ = audit.emit(AuditEvent::ExecutionRejected {
            signal_ts_ms: signal.signal_ts_ms,
            side: signal.side.as_str(),
            reason: "missing_exit_bid_position_released",
            context: None,
        });
        return;
    };
    if shadow_exit.enabled() {
        let processing_ns = monotonic_ns();
        shadow_exit.observe_exit(ShadowExitRequest {
            position_id,
            side: signal.side,
            entry_ms: executed_at_ms,
            decision_ms: exit_at_ms,
            exit_reason: exit_decision.reason,
            entry_price: ask,
            shares: shares as f64,
            decision_processing_started_ns: processing_ns,
            decision_processing_finished_ns: monotonic_ns(),
            event_receive_ns: processing_ns,
            event_exchange_timestamp_ms: book.source_ts_ms,
            book_age_ms: exit_at_ms.saturating_sub(book.received_at_ms),
        });
    }
    let closed = engine_state.close(ask, exit_price, shares);
    let _ = audit.emit(AuditEvent::exit(
        position_id,
        signal.side.as_str(),
        closed,
        exit_decision.reason,
        exit_decision.held_ms,
        exit_decision.exit_momentum_usd.map(round_5),
    ));
}

async fn execute_live(
    signal: OrderSignal,
    market_rx: watch::Receiver<Option<ActiveMarket>>,
    book_state: Arc<BookState>,
    btc_price_history: Arc<BtcPriceHistory>,
    engine_state: Arc<EngineState>,
    health: Arc<HealthState>,
    market_clock: Arc<MarketClock>,
    strategy_store: Arc<StrategyStore>,
    live_executor: LiveExecutor,
    audit: AuditEmitter,
    shadow_exit: ShadowExitEngine,
) {
    tokio::time::sleep_until(signal.execute_at).await;
    let executed_at_ms = now_ms();
    if !health.is_running() {
        reject(signal, "engine_stopped", &engine_state, &audit);
        return;
    }
    let Some(config) = strategy_store.load() else {
        reject(signal, "config_missing", &engine_state, &audit);
        return;
    };
    if config.generation != signal.config_generation {
        reject(signal, "config_changed", &engine_state, &audit);
        return;
    }
    let book = book_state.load();
    let Some(_) = revalidate_entry(book, executed_at_ms, &market_clock, &config, signal.side)
    else {
        let revalidation_market = market_rx.borrow().clone();
        reject_with_context(
            signal,
            "entry_revalidation",
            &engine_state,
            &audit,
            Some(audit_context(
                book,
                executed_at_ms,
                signal.side,
                revalidation_market.as_ref(),
                Some(config.entry_slippage),
                None,
                None,
            )),
        );
        return;
    };
    let Some(initial_bid) = outcome_book(book, signal.side).bid else {
        reject(
            signal,
            "entry_confirmation_missing_bid",
            &engine_state,
            &audit,
        );
        return;
    };

    tokio::time::sleep(tokio::time::Duration::from_millis(
        config.entry_confirmation_ms,
    ))
    .await;
    let confirmed_at_ms = now_ms();
    if !health.is_running() {
        reject(signal, "engine_stopped", &engine_state, &audit);
        return;
    }
    let Some(config) = strategy_store.load() else {
        reject(signal, "config_missing", &engine_state, &audit);
        return;
    };
    if config.generation != signal.config_generation {
        reject(signal, "config_changed", &engine_state, &audit);
        return;
    }
    let confirmed_book = book_state.load();
    let Some((ask, shares)) = revalidate_entry(
        confirmed_book,
        confirmed_at_ms,
        &market_clock,
        &config,
        signal.side,
    ) else {
        let revalidation_market = market_rx.borrow().clone();
        reject_with_context(
            signal,
            "entry_confirmation_revalidation",
            &engine_state,
            &audit,
            Some(audit_context(
                confirmed_book,
                confirmed_at_ms,
                signal.side,
                revalidation_market.as_ref(),
                Some(config.entry_slippage),
                None,
                None,
            )),
        );
        return;
    };
    let Some(confirmed_bid) = outcome_book(confirmed_book, signal.side).bid else {
        reject(
            signal,
            "entry_confirmation_missing_bid",
            &engine_state,
            &audit,
        );
        return;
    };
    if confirmed_bid + 0.00001 < initial_bid + config.min_entry_bid_improvement {
        let confirmation_market = market_rx.borrow().clone();
        reject_with_context(
            signal,
            "entry_confirmation",
            &engine_state,
            &audit,
            Some(audit_context(
                confirmed_book,
                confirmed_at_ms,
                signal.side,
                confirmation_market.as_ref(),
                Some(config.entry_slippage),
                None,
                None,
            )),
        );
        return;
    }
    let Some(active_market) = market_rx.borrow().clone() else {
        reject(signal, "market_missing", &engine_state, &audit);
        return;
    };
    let token_id_text = match signal.side {
        OrderSide::BuyYes => active_market.yes_asset_id.clone(),
        OrderSide::BuyNo => active_market.no_asset_id.clone(),
    };
    let token_id = match U256::from_str(&token_id_text) {
        Ok(token_id) => token_id,
        Err(error) => {
            warn!(%error, side = signal.side.as_str(), "Invalid Polymarket token id");
            reject(signal, "invalid_token_id", &engine_state, &audit);
            return;
        }
    };

    let entry_limit_price = (ask + config.entry_slippage).min(0.95);
    let mut entry_context = audit_context(
        confirmed_book,
        confirmed_at_ms,
        signal.side,
        Some(&active_market),
        Some(config.entry_slippage),
        Some(entry_limit_price),
        Some(shares),
    );
    if shares < config.min_exchange_shares {
        reject_with_context(
            signal,
            "entry_below_exchange_min_size",
            &engine_state,
            &audit,
            Some(entry_context),
        );
        return;
    }
    let depth = book_state.load_depth();
    let available_shares = executable_ask_shares(&depth, signal.side, entry_limit_price);
    let required_shares = shares as f64 * ENTRY_DEPTH_MULTIPLIER;
    entry_context.available_shares = Some(round_5(available_shares));
    entry_context.required_shares = Some(round_5(required_shares));
    if confirmed_at_ms.saturating_sub(depth.received_at_ms) > config.max_book_age_ms
        || available_shares + 0.00001 < required_shares
    {
        reject_with_context(
            signal,
            "insufficient_entry_depth",
            &engine_state,
            &audit,
            Some(entry_context),
        );
        return;
    }
    let entry = buy_with_gtc_limit(&live_executor, token_id, shares, entry_limit_price).await;
    entry_context.fallback_order_id = entry.order_id.clone();
    entry_context.exchange_error = entry.error.clone();
    entry_context.filled_shares = Some(round_5(entry.filled_shares));
    entry_context.entry_submit_latency_ms = entry.submit_latency_ms;
    entry_context.entry_build_latency_ms = entry.build_latency_ms;
    entry_context.entry_sign_latency_ms = entry.sign_latency_ms;
    entry_context.entry_post_latency_ms = entry.post_latency_ms;
    entry_context.gtc_post_success = entry.post_success;
    entry_context.gtc_post_status = entry.post_status.clone();
    entry_context.gtc_post_making_amount = entry.post_making_amount.map(round_5);
    entry_context.gtc_post_taking_amount = entry.post_taking_amount.map(round_5);
    entry_context.gtc_poll_count = Some(entry.poll_count);
    entry_context.gtc_poll_status = entry.poll_status.clone();
    entry_context.gtc_poll_size_matched = entry.poll_size_matched.map(round_5);
    entry_context.gtc_cancel_confirmed = entry.cancel_confirmed;
    entry_context.gtc_final_status = entry.final_status.clone();
    entry_context.gtc_final_size_matched = entry.final_size_matched.map(round_5);
    entry_context.entry_matched_funder_address = live_executor.funder_address.clone();
    entry_context.signature_type = Some(live_executor.signature_type.clone());
    if entry.filled_shares <= 0.0 {
        warn!(side = signal.side.as_str(), shares, entry_limit_price, error = ?entry.error, pending = entry.pending, "Live GTC entry produced zero fill");
        reject_with_context(
            signal,
            if entry.pending {
                "live_entry_gtc_pending"
            } else {
                "live_entry_zero_fill"
            },
            &engine_state,
            &audit,
            Some(entry_context),
        );
        return;
    }
    if entry.pending {
        warn!(side = signal.side.as_str(), filled_shares = entry.filled_shares, shares, entry_limit_price, order_id = ?entry.order_id, "Live GTC entry has an unconfirmed open order");
        health.trip(5);
    }

    let filled_shares = entry.filled_shares;
    let entry_notional_usd = entry.notional_usd;
    let entry_order_id = entry.order_id.unwrap_or_default();
    let entry_order_status = entry.order_status.unwrap_or_else(|| "UNKNOWN".to_owned());
    let entry_price = entry_notional_usd / filled_shares;
    let position_id = engine_state.open_reserved();
    info!(
        position_id,
        side = signal.side.as_str(),
        entry_price = round_5(entry_price),
        shares = round_5(filled_shares),
        order_status = %entry_order_status,
        "Live entry opened"
    );
    let _ = audit.emit(AuditEvent::LiveEntry {
        position_id,
        signal_ts_ms: signal.signal_ts_ms,
        executed_at_ms: confirmed_at_ms,
        latency_ms: confirmed_at_ms.saturating_sub(signal.signal_ts_ms),
        side: signal.side.as_str(),
        token_id: token_id_text.clone(),
        order_id: entry_order_id,
        order_status: entry_order_status,
        price: round_5(entry_price),
        shares: round_5(filled_shares),
        notional_usd: round_5(entry_notional_usd),
        context: Some(entry_context),
    });

    let exit_decision = wait_for_exit_decision(
        &signal,
        confirmed_at_ms,
        entry_price,
        filled_shares,
        &config,
        &book_state,
        &btc_price_history,
    )
    .await;
    let exit_at_ms = exit_decision.decided_at_ms;
    let book = book_state.load();
    let mut exit_context = audit_context(
        book,
        exit_at_ms,
        signal.side,
        Some(&active_market),
        None,
        None,
        None,
    );
    exit_context.entry_matched_funder_address = live_executor.funder_address.clone();
    exit_context.exit_attempt_funder_address = live_executor.funder_address.clone();
    exit_context.signature_type = Some(live_executor.signature_type.clone());
    if exit_at_ms.saturating_sub(book.received_at_ms) > config.max_book_age_ms {
        health.trip(5);
        let _ = audit.emit(AuditEvent::ExecutionRejected {
            signal_ts_ms: signal.signal_ts_ms,
            side: signal.side.as_str(),
            reason: "live_exit_stale_book_position_open",
            context: Some(exit_context.clone()),
        });
        return;
    }
    let Some(exit_bid) = outcome_book(book, signal.side).bid else {
        health.trip(5);
        let _ = audit.emit(AuditEvent::ExecutionRejected {
            signal_ts_ms: signal.signal_ts_ms,
            side: signal.side.as_str(),
            reason: "live_exit_missing_bid_position_open",
            context: Some(exit_context.clone()),
        });
        return;
    };

    let sellable_shares = floor_to_2_decimals(filled_shares);
    if sellable_shares <= 0.0 {
        health.trip(5);
        let _ = audit.emit(AuditEvent::ExecutionRejected {
            signal_ts_ms: signal.signal_ts_ms,
            side: signal.side.as_str(),
            reason: "live_exit_unsellable_amount_position_open",
            context: Some(exit_context.clone()),
        });
        return;
    }

    if shadow_exit.enabled() {
        let processing_ns = monotonic_ns();
        shadow_exit.observe_exit(ShadowExitRequest {
            position_id,
            side: signal.side,
            entry_ms: confirmed_at_ms,
            decision_ms: exit_at_ms,
            exit_reason: exit_decision.reason,
            entry_price,
            shares: sellable_shares,
            decision_processing_started_ns: processing_ns,
            decision_processing_finished_ns: monotonic_ns(),
            event_receive_ns: processing_ns,
            event_exchange_timestamp_ms: book.source_ts_ms,
            book_age_ms: exit_at_ms.saturating_sub(book.received_at_ms),
        });
    }

    let mut sold_shares = 0.0;
    let mut sale_proceeds = 0.0;
    let mut last_response = None;
    let mut last_error = None;
    let mut attempts = 0;
    let mut exit_retry_attempts = Vec::new();
    let mut balance_retry_exhausted = false;

    while attempts < LIVE_EXIT_MAX_ATTEMPTS {
        let remaining_shares = floor_to_2_decimals(sellable_shares - sold_shares);
        if remaining_shares <= 0.0 {
            break;
        }
        attempts += 1;
        match live_executor.sell_shares(token_id, remaining_shares).await {
            Ok(response) if response.success => {
                let filled = sell_shares_filled(&response)
                    .unwrap_or(0.0)
                    .min(remaining_shares);
                if filled > 0.0 {
                    let fill_price = sell_price(&response).unwrap_or(exit_bid);
                    sold_shares += filled;
                    sale_proceeds += filled * fill_price;
                    last_response = Some(response);
                }
            }
            Ok(response) => {
                let error = response
                    .error_msg
                    .clone()
                    .unwrap_or_else(|| response.status.to_string());
                if is_balance_not_settled_error(&error) {
                    last_error = Some(error.clone());
                    if let Some(delay_ms) = LIVE_EXIT_BALANCE_RETRY_DELAYS_MS
                        .get(exit_retry_attempts.len())
                        .copied()
                    {
                        exit_retry_attempts.push(AuditExitRetryAttempt {
                            attempt: attempts,
                            error_class: "balance_not_settled",
                            error: Some(error),
                            delay_ms,
                        });
                        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                        continue;
                    }
                    balance_retry_exhausted = true;
                    exit_retry_attempts.push(AuditExitRetryAttempt {
                        attempt: attempts,
                        error_class: "balance_not_settled_exhausted",
                        error: Some(error),
                        delay_ms: 0,
                    });
                    break;
                }
                if !is_no_match_error(&error) {
                    last_error = Some(error);
                    break;
                }
                last_error = Some(error);
            }
            Err(error) => {
                let error_chain = format!("{error:#}");
                if is_balance_not_settled_error(&error_chain) {
                    last_error = Some(error_chain.clone());
                    if let Some(delay_ms) = LIVE_EXIT_BALANCE_RETRY_DELAYS_MS
                        .get(exit_retry_attempts.len())
                        .copied()
                    {
                        exit_retry_attempts.push(AuditExitRetryAttempt {
                            attempt: attempts,
                            error_class: "balance_not_settled",
                            error: Some(error_chain),
                            delay_ms,
                        });
                        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                        continue;
                    }
                    balance_retry_exhausted = true;
                    exit_retry_attempts.push(AuditExitRetryAttempt {
                        attempt: attempts,
                        error_class: "balance_not_settled_exhausted",
                        error: Some(error_chain),
                        delay_ms: 0,
                    });
                    break;
                }
                if !is_no_match_error(&error_chain) {
                    last_error = Some(error_chain);
                    break;
                }
                last_error = Some(error_chain);
            }
        }
        if floor_to_2_decimals(sellable_shares - sold_shares) > 0.0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(LIVE_EXIT_RETRY_MS)).await;
        }
    }

    let mut fallback_order_id = None;
    let mut fallback_order_status = None;
    let mut fallback_pending = false;
    if !balance_retry_exhausted && sold_shares + 0.00001 < sellable_shares {
        let fallback = sell_with_gtc_fallback(
            &live_executor,
            token_id,
            sellable_shares - sold_shares,
            signal.side,
            &book_state,
        )
        .await;
        sold_shares += fallback.sold_shares;
        sale_proceeds += fallback.sale_proceeds;
        fallback_order_id = fallback.order_id;
        fallback_order_status = fallback.order_status;
        fallback_pending = fallback.pending;
        if fallback.error.is_some() {
            last_error = fallback.error;
        }
    }

    if sold_shares + 0.00001 < sellable_shares {
        warn!(side = signal.side.as_str(), sold_shares, sellable_shares, attempts, error = ?last_error, fallback_pending, "Live exit fallback incomplete; position remains open");
        health.trip(5);
        let mut rejected_context = exit_context;
        rejected_context.exchange_error = last_error;
        rejected_context.filled_shares = Some(round_5(sold_shares));
        rejected_context.exit_attempts = Some(attempts);
        if !exit_retry_attempts.is_empty() {
            rejected_context.exit_retry_attempts = Some(exit_retry_attempts);
        }
        rejected_context.fallback_order_id = fallback_order_id;
        let _ = audit.emit(AuditEvent::ExecutionRejected {
            signal_ts_ms: signal.signal_ts_ms,
            side: signal.side.as_str(),
            reason: if balance_retry_exhausted {
                "manual_exit_required_balance_unavailable"
            } else if fallback_pending {
                "live_exit_gtc_pending_position_open"
            } else {
                "live_exit_gtc_failed_position_open"
            },
            context: Some(rejected_context),
        });
        return;
    }

    let exit_order = fallback_order_id
        .clone()
        .zip(fallback_order_status)
        .or_else(|| {
            last_response
                .as_ref()
                .map(|response| (response.order_id.clone(), response.status.to_string()))
        });
    let Some((exit_order_id, exit_order_status)) = exit_order else {
        health.trip(5);
        let _ = audit.emit(AuditEvent::ExecutionRejected {
            signal_ts_ms: signal.signal_ts_ms,
            side: signal.side.as_str(),
            reason: "live_exit_zero_fill_position_open",
            context: Some(exit_context),
        });
        return;
    };
    let exit_price = sale_proceeds / sold_shares;
    exit_context.filled_shares = Some(round_5(sold_shares));
    exit_context.exit_attempts = Some(attempts);
    if !exit_retry_attempts.is_empty() {
        exit_context.exit_retry_attempts = Some(exit_retry_attempts);
    }
    exit_context.fallback_order_id = fallback_order_id.clone();
    let closed = engine_state.close_live(entry_price, exit_price, sold_shares);
    info!(
        position_id,
        side = signal.side.as_str(),
        exit_price = round_5(exit_price),
        shares = closed.shares,
        net_pnl = closed.net_pnl,
        "Live exit closed"
    );
    let _ = audit.emit(AuditEvent::LiveExit {
        position_id,
        closed_at_ms: now_ms(),
        side: signal.side.as_str(),
        exit_reason: exit_decision.reason,
        held_ms: exit_decision.held_ms,
        exit_momentum_usd: exit_decision.exit_momentum_usd.map(round_5),
        token_id: token_id_text,
        order_id: exit_order_id,
        order_status: exit_order_status,
        entry_price: round_5(entry_price),
        exit_price: round_5(exit_price),
        shares: closed.shares,
        gross_pnl: closed.gross_pnl,
        entry_fee: closed.entry_fee,
        exit_fee: closed.exit_fee,
        net_pnl: closed.net_pnl,
        context: Some(exit_context),
    });
}

async fn buy_with_gtc_limit(
    live_executor: &LiveExecutor,
    token_id: U256,
    shares: u64,
    limit_price: f64,
) -> GtcEntryResult {
    let mut result = GtcEntryResult {
        filled_shares: 0.0,
        notional_usd: 0.0,
        order_id: None,
        order_status: None,
        error: None,
        pending: false,
        submit_latency_ms: None,
        build_latency_ms: None,
        sign_latency_ms: None,
        post_latency_ms: None,
        post_success: None,
        post_status: None,
        post_making_amount: None,
        post_taking_amount: None,
        poll_count: 0,
        poll_status: None,
        poll_size_matched: None,
        cancel_confirmed: None,
        final_status: None,
        final_size_matched: None,
    };
    let submit_started_ms = now_ms();
    let timed_response = match live_executor
        .place_gtc_buy(token_id, shares, limit_price)
        .await
    {
        Ok(timed_response) if timed_response.response.success => timed_response,
        Ok(timed_response) => {
            let response = timed_response.response;
            let status = response.status.to_string();
            result.submit_latency_ms = Some(now_ms().saturating_sub(submit_started_ms));
            result.build_latency_ms = Some(timed_response.build_latency_ms);
            result.sign_latency_ms = Some(timed_response.sign_latency_ms);
            result.post_latency_ms = Some(timed_response.post_latency_ms);
            result.order_id = Some(response.order_id.clone());
            result.order_status = Some(status.clone());
            result.post_success = Some(response.success);
            result.post_status = Some(status.clone());
            result.post_making_amount = decimal_to_f64(response.making_amount);
            result.post_taking_amount = decimal_to_f64(response.taking_amount);
            result.error = response.error_msg.or(Some(status));
            return result;
        }
        Err(error) => {
            result.submit_latency_ms = Some(now_ms().saturating_sub(submit_started_ms));
            result.error = Some(format!("{error:#}"));
            return result;
        }
    };
    let response = timed_response.response;
    result.submit_latency_ms = Some(now_ms().saturating_sub(submit_started_ms));
    result.build_latency_ms = Some(timed_response.build_latency_ms);
    result.sign_latency_ms = Some(timed_response.sign_latency_ms);
    result.post_latency_ms = Some(timed_response.post_latency_ms);

    let order_id = response.order_id.clone();
    result.order_id = Some(order_id.clone());
    result.order_status = Some(response.status.to_string());
    result.post_success = Some(response.success);
    result.post_status = Some(response.status.to_string());
    result.post_making_amount = decimal_to_f64(response.making_amount);
    result.post_taking_amount = decimal_to_f64(response.taking_amount);

    let target_shares = shares as f64;
    if let Some(filled) = buy_shares_filled(&response).filter(|filled| *filled > 0.0) {
        result.filled_shares = filled.min(target_shares);
        result.notional_usd = buy_notional_spent(&response)
            .filter(|notional| *notional > 0.0)
            .unwrap_or(result.filled_shares * limit_price);
        if result.filled_shares + 0.00001 >= target_shares {
            return result;
        }
    }
    let max_polls = if result.post_status.as_deref() == Some("LIVE")
        && result.post_making_amount.unwrap_or(0.0) <= 0.0
        && result.post_taking_amount.unwrap_or(0.0) <= 0.0
    {
        LIVE_ENTRY_GTC_FAST_POLLS
    } else {
        LIVE_ENTRY_GTC_POLLS
    };
    for _ in 0..max_polls {
        result.poll_count += 1;
        tokio::time::sleep(tokio::time::Duration::from_millis(LIVE_ENTRY_GTC_POLL_MS)).await;
        let status_started_ms = now_ms();
        let order = match live_executor.client.order(&order_id).await {
            Ok(order) => {
                info!(
                    op = "gtc_entry_order_status",
                    total_ms = status_started_ms.elapsed_to_now_ms(),
                    "Polymarket live HTTP timing"
                );
                order
            }
            Err(error) => {
                info!(
                    op = "gtc_entry_order_status",
                    total_ms = status_started_ms.elapsed_to_now_ms(),
                    error = true,
                    "Polymarket live HTTP timing"
                );
                result.error = Some(format!("GTC entry status failed: {error:#}"));
                result.pending = true;
                return result;
            }
        };
        let matched = decimal_to_f64(order.size_matched)
            .unwrap_or(0.0)
            .min(target_shares);
        result.poll_status = Some(order.status.to_string());
        result.poll_size_matched = Some(matched);
        if matched > result.filled_shares {
            result.notional_usd += (matched - result.filled_shares) * limit_price;
            result.filled_shares = matched;
        }
        result.order_status = Some(order.status.to_string());
        if result.filled_shares + 0.00001 >= target_shares {
            return result;
        }
    }

    let cancel_started_ms = now_ms();
    let cancellation = match live_executor.client.cancel_order(&order_id).await {
        Ok(cancellation) => {
            info!(
                op = "gtc_entry_cancel",
                total_ms = cancel_started_ms.elapsed_to_now_ms(),
                "Polymarket live HTTP timing"
            );
            cancellation
        }
        Err(error) => {
            info!(
                op = "gtc_entry_cancel",
                total_ms = cancel_started_ms.elapsed_to_now_ms(),
                error = true,
                "Polymarket live HTTP timing"
            );
            result.error = Some(format!("GTC entry cancel failed: {error:#}"));
            result.pending = true;
            return result;
        }
    };
    let cancellation_confirmed = cancellation
        .canceled
        .iter()
        .any(|canceled_id| canceled_id == &order_id);
    result.cancel_confirmed = Some(cancellation_confirmed);
    let final_status_started_ms = now_ms();
    let final_order = match live_executor.client.order(&order_id).await {
        Ok(order) => {
            info!(
                op = "gtc_entry_final_status",
                total_ms = final_status_started_ms.elapsed_to_now_ms(),
                "Polymarket live HTTP timing"
            );
            order
        }
        Err(error) => {
            info!(
                op = "gtc_entry_final_status",
                total_ms = final_status_started_ms.elapsed_to_now_ms(),
                error = true,
                "Polymarket live HTTP timing"
            );
            result.error = Some(format!(
                "GTC entry canceled order fill could not be verified: {error:#}"
            ));
            result.pending = !cancellation_confirmed;
            return result;
        }
    };
    let matched = decimal_to_f64(final_order.size_matched)
        .unwrap_or(0.0)
        .min(target_shares);
    result.final_status = Some(final_order.status.to_string());
    result.final_size_matched = Some(matched);
    if matched > result.filled_shares {
        result.notional_usd += (matched - result.filled_shares) * limit_price;
        result.filled_shares = matched;
    }
    result.order_status = Some(final_order.status.to_string());
    if !cancellation_confirmed && result.filled_shares + 0.00001 < target_shares {
        result.error = Some("GTC entry cancellation was not confirmed".to_owned());
        result.pending = true;
    }
    result
}

async fn sell_with_gtc_fallback(
    live_executor: &LiveExecutor,
    token_id: U256,
    shares: f64,
    side: OrderSide,
    book_state: &BookState,
) -> GtcExitResult {
    let target_shares = floor_to_2_decimals(shares);
    let mut result = GtcExitResult {
        sold_shares: 0.0,
        sale_proceeds: 0.0,
        order_id: None,
        order_status: None,
        error: None,
        pending: false,
    };

    for price_attempt in 0..=LIVE_EXIT_GTC_REPRICE_ATTEMPTS {
        let remaining_shares = floor_to_2_decimals(target_shares - result.sold_shares);
        if remaining_shares <= 0.0 {
            break;
        }
        let is_final_price = price_attempt == LIVE_EXIT_GTC_REPRICE_ATTEMPTS;
        let limit_price = if is_final_price {
            LIVE_EXIT_GTC_FLOOR_PRICE
        } else {
            outcome_book(book_state.load(), side)
                .bid
                .unwrap_or(LIVE_EXIT_GTC_FLOOR_PRICE)
                .clamp(LIVE_EXIT_GTC_FLOOR_PRICE, 0.99)
        };
        let response = match live_executor
            .place_gtc_sell(token_id, remaining_shares, limit_price)
            .await
        {
            Ok(response) if response.success => response,
            Ok(response) => {
                let status = response.status.to_string();
                result.error = response.error_msg.or(Some(status));
                return result;
            }
            Err(error) => {
                result.error = Some(format!("{error:#}"));
                return result;
            }
        };

        let order_id = response.order_id.clone();
        let mut observed_order_fill = sell_shares_filled(&response)
            .unwrap_or(0.0)
            .min(remaining_shares);
        result.sold_shares += observed_order_fill;
        result.sale_proceeds += observed_order_fill * sell_price(&response).unwrap_or(limit_price);
        result.order_id = Some(order_id.clone());
        result.order_status = Some(response.status.to_string());
        if result.sold_shares + 0.00001 >= target_shares {
            break;
        }

        let poll_attempts = if is_final_price {
            LIVE_EXIT_GTC_FINAL_POLLS
        } else {
            LIVE_EXIT_GTC_POLLS_PER_PRICE
        };
        for _ in 0..poll_attempts {
            tokio::time::sleep(tokio::time::Duration::from_millis(LIVE_EXIT_GTC_POLL_MS)).await;
            let status_started_ms = now_ms();
            let order = match live_executor.client.order(&order_id).await {
                Ok(order) => {
                    info!(
                        op = "gtc_exit_order_status",
                        total_ms = status_started_ms.elapsed_to_now_ms(),
                        "Polymarket live HTTP timing"
                    );
                    order
                }
                Err(error) => {
                    info!(
                        op = "gtc_exit_order_status",
                        total_ms = status_started_ms.elapsed_to_now_ms(),
                        error = true,
                        "Polymarket live HTTP timing"
                    );
                    result.error = Some(format!("GTC order status failed: {error:#}"));
                    result.pending = true;
                    return result;
                }
            };
            let matched = decimal_to_f64(order.size_matched)
                .unwrap_or(0.0)
                .min(remaining_shares);
            let new_fill = (matched - observed_order_fill).max(0.0);
            observed_order_fill = matched;
            result.sold_shares += new_fill;
            result.sale_proceeds += new_fill * limit_price;
            result.order_status = Some(order.status.to_string());
            if result.sold_shares + 0.00001 >= target_shares {
                return result;
            }
        }

        if is_final_price {
            result.pending = true;
            result.error = Some("GTC floor order is still open".to_owned());
            return result;
        }

        let cancel_started_ms = now_ms();
        let cancellation = match live_executor.client.cancel_order(&order_id).await {
            Ok(cancellation) => {
                info!(
                    op = "gtc_exit_cancel",
                    total_ms = cancel_started_ms.elapsed_to_now_ms(),
                    "Polymarket live HTTP timing"
                );
                cancellation
            }
            Err(error) => {
                info!(
                    op = "gtc_exit_cancel",
                    total_ms = cancel_started_ms.elapsed_to_now_ms(),
                    error = true,
                    "Polymarket live HTTP timing"
                );
                result.error = Some(format!("GTC cancel failed: {error:#}"));
                result.pending = true;
                return result;
            }
        };
        let cancellation_confirmed = cancellation
            .canceled
            .iter()
            .any(|canceled_id| canceled_id == &order_id);
        let final_status_started_ms = now_ms();
        let final_order = match live_executor.client.order(&order_id).await {
            Ok(order) => {
                info!(
                    op = "gtc_exit_final_status",
                    total_ms = final_status_started_ms.elapsed_to_now_ms(),
                    "Polymarket live HTTP timing"
                );
                order
            }
            Err(error) => {
                info!(
                    op = "gtc_exit_final_status",
                    total_ms = final_status_started_ms.elapsed_to_now_ms(),
                    error = true,
                    "Polymarket live HTTP timing"
                );
                result.error = Some(format!(
                    "GTC canceled order fill could not be verified: {error:#}"
                ));
                result.pending = !cancellation_confirmed;
                return result;
            }
        };
        let matched = decimal_to_f64(final_order.size_matched)
            .unwrap_or(0.0)
            .min(remaining_shares);
        let new_fill = (matched - observed_order_fill).max(0.0);
        result.sold_shares += new_fill;
        result.sale_proceeds += new_fill * limit_price;
        result.order_status = Some(final_order.status.to_string());
        if result.sold_shares + 0.00001 >= target_shares {
            return result;
        }
        if !cancellation_confirmed {
            result.error = Some("GTC cancellation was not confirmed".to_owned());
            result.pending = true;
            return result;
        }
    }

    result
}
