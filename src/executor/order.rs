use std::{env, str::FromStr, sync::Arc};

use alloy::signers::{
    local::{LocalSigner, PrivateKeySigner},
    Signer as _,
};
use anyhow::{Context, Result};
use polymarket_client_sdk_v2::{
    auth::{state::Authenticated, Credentials, Normal},
    clob::{
        types::{response::PostOrderResponse, Amount, OrderType, Side as ClobSide, SignatureType},
        Client as ClobClient, Config as ClobConfig,
    },
    types::{Address, Decimal, U256},
    POLYGON,
};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use crate::config::ExecutionMode;
use crate::emitter::audit::{AuditBookContext, AuditEmitter, AuditEvent};
use crate::engine::state::{round_5, EngineState};
use crate::health::HealthState;
use crate::market::{now_ms, ActiveMarket, MarketClock};
use crate::runtime_config::{StrategySnapshot, StrategyStore};
use crate::ws::polymarket::{BookSnapshot, BookState, OutcomeBook};

const DEFAULT_CLOB_API_URL: &str = "https://clob-v2.polymarket.com";

type AuthenticatedClobClient = ClobClient<Authenticated<Normal>>;

#[derive(Clone)]
pub struct LiveExecutor {
    client: AuthenticatedClobClient,
    signer: PrivateKeySigner,
}

impl LiveExecutor {
    pub async fn from_env() -> Result<Self> {
        let host = env_nonempty("POLYMARKET_CLOB_API_URL")
            .or_else(|| env_nonempty("CLOB_API_URL"))
            .unwrap_or_else(|| DEFAULT_CLOB_API_URL.to_owned());
        let private_key = env_nonempty("POLYMARKET_PRIVATE_KEY")
            .or_else(|| env_nonempty("POLYGO_PRIVATE_KEY"))
            .context("POLYMARKET_PRIVATE_KEY is required for live execution")?;
        let signer = LocalSigner::from_str(&private_key)
            .context("invalid POLYMARKET_PRIVATE_KEY")?
            .with_chain_id(Some(POLYGON));

        let mut builder = ClobClient::new(&host, ClobConfig::default())
            .context("failed to create Polymarket CLOB client")?
            .authentication_builder(&signer);

        let key = env_nonempty("POLYMARKET_API_KEY").or_else(|| env_nonempty("POLYGO_API_KEY"));
        let secret =
            env_nonempty("POLYMARKET_API_SECRET").or_else(|| env_nonempty("POLYGO_API_SECRET"));
        let passphrase = env_nonempty("POLYMARKET_API_PASSPHRASE")
            .or_else(|| env_nonempty("POLYGO_API_PASSPHRASE"));
        if let (Some(key), Some(secret), Some(passphrase)) = (key, secret, passphrase) {
            builder = builder.credentials(Credentials::new(
                key.parse().context("invalid POLYMARKET_API_KEY")?,
                secret,
                passphrase,
            ));
        }

        let funder = env_nonempty("POLYMARKET_FUNDER_ADDRESS")
            .or_else(|| env_nonempty("POLYMARKET_DEPOSIT_WALLET"))
            .or_else(|| env_nonempty("DEPOSIT_WALLET"));
        let signature_type = parse_signature_type(
            env_nonempty("POLYMARKET_SIGNATURE_TYPE").as_deref(),
            funder.is_some(),
        )?;
        if let Some(funder) = funder {
            builder = builder.funder(Address::from_str(&funder).context("invalid funder address")?);
        }
        builder = builder.signature_type(signature_type);

        let client = builder
            .authenticate()
            .await
            .context("Polymarket authentication failed")?;
        Ok(Self { client, signer })
    }

    async fn buy_shares(
        &self,
        token_id: U256,
        shares: u64,
        limit_price: f64,
    ) -> Result<PostOrderResponse> {
        self.client
            .market_order()
            .token_id(token_id)
            .side(ClobSide::Buy)
            .amount(Amount::shares(Decimal::from(shares))?)
            .price(decimal_from_f64(limit_price)?)
            .order_type(OrderType::FAK)
            .build_sign_and_post(&self.signer)
            .await
            .context("Polymarket buy order failed")
    }

    async fn sell_shares(&self, token_id: U256, shares: f64) -> Result<PostOrderResponse> {
        let sellable_shares = floor_to_2_decimals(shares);
        self.client
            .market_order()
            .token_id(token_id)
            .side(ClobSide::Sell)
            .amount(Amount::shares(decimal_from_f64(sellable_shares)?)?)
            .order_type(OrderType::FAK)
            .build_sign_and_post(&self.signer)
            .await
            .context("Polymarket sell order failed")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    BuyYes,
    BuyNo,
}

impl OrderSide {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BuyYes => "BUY_YES",
            Self::BuyNo => "BUY_NO",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderSignal {
    pub side: OrderSide,
    pub momentum_usd: f64,
    pub signal_ts_ms: u64,
    pub binance_price: f64,
    pub execute_at: tokio::time::Instant,
    pub config_generation: u64,
    pub hold_ms: u64,
}

impl OrderSignal {
    pub fn new(
        side: OrderSide,
        momentum_usd: f64,
        signal_ts_ms: u64,
        binance_price: f64,
        execute_at: tokio::time::Instant,
        config_generation: u64,
        hold_ms: u64,
    ) -> Self {
        Self {
            side,
            momentum_usd,
            signal_ts_ms,
            binance_price,
            execute_at,
            config_generation,
            hold_ms,
        }
    }
}

pub async fn run(
    mut order_rx: mpsc::Receiver<OrderSignal>,
    market_rx: watch::Receiver<Option<ActiveMarket>>,
    book_state: Arc<BookState>,
    engine_state: Arc<EngineState>,
    health: Arc<HealthState>,
    market_clock: Arc<MarketClock>,
    strategy_store: Arc<StrategyStore>,
    execution_mode: ExecutionMode,
    live_executor: Option<LiveExecutor>,
    audit: AuditEmitter,
) {
    info!(mode = execution_mode.as_str(), "Executor started");
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
        let task_state = Arc::clone(&engine_state);
        let task_health = Arc::clone(&health);
        let task_clock = Arc::clone(&market_clock);
        let task_store = Arc::clone(&strategy_store);
        let task_audit = audit.clone();
        let task_live_executor = live_executor.clone();
        tokio::spawn(async move {
            match execution_mode {
                ExecutionMode::DryRun => {
                    execute_dry_run(
                        signal,
                        task_book_state,
                        task_state,
                        task_health,
                        task_clock,
                        task_store,
                        task_audit,
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
                        task_state,
                        task_health,
                        task_clock,
                        task_store,
                        live_executor,
                        task_audit,
                    )
                    .await;
                }
            }
        });
    }
    warn!("Execution channel closed");
}

async fn execute_dry_run(
    signal: OrderSignal,
    book_state: Arc<BookState>,
    engine_state: Arc<EngineState>,
    health: Arc<HealthState>,
    market_clock: Arc<MarketClock>,
    strategy_store: Arc<StrategyStore>,
    audit: AuditEmitter,
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

    tokio::time::sleep(tokio::time::Duration::from_millis(signal.hold_ms)).await;
    let exit_at_ms = now_ms();
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
    let closed = engine_state.close(ask, exit_price, shares);
    let _ = audit.emit(AuditEvent::exit(position_id, signal.side.as_str(), closed));
}

async fn execute_live(
    signal: OrderSignal,
    market_rx: watch::Receiver<Option<ActiveMarket>>,
    book_state: Arc<BookState>,
    engine_state: Arc<EngineState>,
    health: Arc<HealthState>,
    market_clock: Arc<MarketClock>,
    strategy_store: Arc<StrategyStore>,
    live_executor: LiveExecutor,
    audit: AuditEmitter,
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
    let Some((ask, shares)) =
        revalidate_entry(book, executed_at_ms, &market_clock, &config, signal.side)
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
    let entry_context = audit_context(
        book,
        executed_at_ms,
        signal.side,
        Some(&active_market),
        Some(config.entry_slippage),
        Some(entry_limit_price),
        Some(shares),
    );
    let entry_response = match live_executor
        .buy_shares(token_id, shares, entry_limit_price)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            let error_chain = format!("{error:#}");
            warn!(%error_chain, side = signal.side.as_str(), shares, entry_limit_price, "Live entry failed");
            reject_with_context(
                signal,
                "live_entry_failed",
                &engine_state,
                &audit,
                Some(entry_context),
            );
            return;
        }
    };
    if !entry_response.success {
        warn!(
            status = %entry_response.status,
            error = ?entry_response.error_msg,
            side = signal.side.as_str(),
            shares,
            "Live entry rejected"
        );
        reject_with_context(
            signal,
            "live_entry_rejected",
            &engine_state,
            &audit,
            Some(entry_context),
        );
        return;
    }

    let filled_shares = buy_shares_filled(&entry_response).unwrap_or(0.0);
    if filled_shares <= 0.0 {
        warn!(
            side = signal.side.as_str(),
            shares, "Live entry produced zero fill"
        );
        reject_with_context(
            signal,
            "live_entry_zero_fill",
            &engine_state,
            &audit,
            Some(entry_context),
        );
        return;
    }

    let entry_price = buy_price(&entry_response).unwrap_or(ask);
    let position_id = engine_state.open_reserved();
    let _ = audit.emit(AuditEvent::LiveEntry {
        position_id,
        signal_ts_ms: signal.signal_ts_ms,
        executed_at_ms,
        latency_ms: executed_at_ms.saturating_sub(signal.signal_ts_ms),
        side: signal.side.as_str(),
        token_id: token_id_text.clone(),
        order_id: entry_response.order_id.clone(),
        order_status: entry_response.status.to_string(),
        price: round_5(entry_price),
        shares: round_5(filled_shares),
        notional_usd: round_5(entry_price * filled_shares),
        context: Some(entry_context),
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(signal.hold_ms)).await;
    let exit_at_ms = now_ms();
    let book = book_state.load();
    let exit_context = audit_context(
        book,
        exit_at_ms,
        signal.side,
        Some(&active_market),
        None,
        None,
        None,
    );
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

    let exit_response = match live_executor.sell_shares(token_id, sellable_shares).await {
        Ok(response) => response,
        Err(error) => {
            let error_chain = format!("{error:#}");
            warn!(%error_chain, side = signal.side.as_str(), shares = sellable_shares, filled_shares, "Live exit failed; position may remain open");
            health.trip(5);
            let _ = audit.emit(AuditEvent::ExecutionRejected {
                signal_ts_ms: signal.signal_ts_ms,
                side: signal.side.as_str(),
                reason: "live_exit_failed_position_open",
                context: Some(exit_context.clone()),
            });
            return;
        }
    };
    if !exit_response.success {
        warn!(
            status = %exit_response.status,
            error = ?exit_response.error_msg,
            side = signal.side.as_str(),
            shares = sellable_shares,
            filled_shares,
            "Live exit rejected; position may remain open"
        );
        health.trip(5);
        let _ = audit.emit(AuditEvent::ExecutionRejected {
            signal_ts_ms: signal.signal_ts_ms,
            side: signal.side.as_str(),
            reason: "live_exit_rejected_position_open",
            context: Some(exit_context.clone()),
        });
        return;
    }

    let sold_shares = sell_shares_filled(&exit_response).unwrap_or(0.0);
    if sold_shares <= 0.0 {
        health.trip(5);
        let _ = audit.emit(AuditEvent::ExecutionRejected {
            signal_ts_ms: signal.signal_ts_ms,
            side: signal.side.as_str(),
            reason: "live_exit_zero_fill_position_open",
            context: Some(exit_context.clone()),
        });
        return;
    }
    if sold_shares + 0.00001 < sellable_shares {
        health.trip(5);
        let _ = audit.emit(AuditEvent::ExecutionRejected {
            signal_ts_ms: signal.signal_ts_ms,
            side: signal.side.as_str(),
            reason: "live_exit_partial_fill_position_open",
            context: Some(exit_context.clone()),
        });
        return;
    }

    let exit_price = sell_price(&exit_response).unwrap_or(exit_bid);
    let closed = engine_state.close_live(entry_price, exit_price, sold_shares);
    let _ = audit.emit(AuditEvent::LiveExit {
        position_id,
        closed_at_ms: exit_at_ms,
        side: signal.side.as_str(),
        token_id: token_id_text,
        order_id: exit_response.order_id.clone(),
        order_status: exit_response.status.to_string(),
        entry_price: round_5(entry_price),
        exit_price: round_5(exit_price),
        shares: closed.shares,
        gross_pnl: closed.gross_pnl,
        entry_fee: closed.entry_fee,
        exit_fee: closed.exit_fee,
        net_pnl: closed.net_pnl,
    });
}

#[inline]
pub fn revalidate_entry(
    book: BookSnapshot,
    now_ms: u64,
    market_clock: &MarketClock,
    config: &StrategySnapshot,
    side: OrderSide,
) -> Option<(f64, u64)> {
    if !market_clock.progress_allowed(now_ms, config.min_progress, config.max_progress)
        || now_ms.saturating_sub(book.received_at_ms) > config.max_book_age_ms
    {
        return None;
    }
    let outcome = outcome_book(book, side);
    let (Some(bid), Some(ask)) = (outcome.bid, outcome.ask) else {
        return None;
    };
    if ask <= bid || ask - bid > config.max_spread {
        return None;
    }
    if ask < config.min_price || ask > config.max_price {
        return None;
    }
    let shares = order_size(ask, config.max_notional_usd, config.max_shares);
    if !expected_move_covers_fees(ask, shares, config.min_expected_price_move) {
        return None;
    }
    (shares > 0).then_some((ask, shares))
}

fn reject(
    signal: OrderSignal,
    reason: &'static str,
    engine_state: &EngineState,
    audit: &AuditEmitter,
) {
    engine_state.release_reservation();
    let _ = audit.emit(AuditEvent::ExecutionRejected {
        signal_ts_ms: signal.signal_ts_ms,
        side: signal.side.as_str(),
        reason,
        context: None,
    });
}

fn reject_with_context(
    signal: OrderSignal,
    reason: &'static str,
    engine_state: &EngineState,
    audit: &AuditEmitter,
    context: Option<AuditBookContext>,
) {
    engine_state.release_reservation();
    let _ = audit.emit(AuditEvent::ExecutionRejected {
        signal_ts_ms: signal.signal_ts_ms,
        side: signal.side.as_str(),
        reason,
        context,
    });
}

fn audit_context(
    book: BookSnapshot,
    now_ms: u64,
    side: OrderSide,
    market: Option<&ActiveMarket>,
    entry_slippage: Option<f64>,
    entry_limit_price: Option<f64>,
    intended_shares: Option<u64>,
) -> AuditBookContext {
    let selected = outcome_book(book, side);
    let token_id = market.map(|market| match side {
        OrderSide::BuyYes => market.yes_asset_id.clone(),
        OrderSide::BuyNo => market.no_asset_id.clone(),
    });
    let intended_notional_usd = intended_shares.map(|shares| {
        round_5(shares as f64 * entry_limit_price.or(selected.ask).unwrap_or_default())
    });

    AuditBookContext {
        market_slug: market.map(|market| market.slug.clone()),
        token_id,
        yes_bid: book.yes.bid.map(round_5),
        yes_ask: book.yes.ask.map(round_5),
        no_bid: book.no.bid.map(round_5),
        no_ask: book.no.ask.map(round_5),
        book_received_at_ms: book.received_at_ms,
        book_source_ts_ms: book.source_ts_ms,
        book_age_ms: now_ms.saturating_sub(book.received_at_ms),
        selected_bid: selected.bid.map(round_5),
        selected_ask: selected.ask.map(round_5),
        entry_slippage: entry_slippage.map(round_5),
        entry_limit_price: entry_limit_price.map(round_5),
        intended_shares,
        intended_notional_usd,
    }
}

fn outcome_book(snapshot: BookSnapshot, side: OrderSide) -> OutcomeBook {
    match side {
        OrderSide::BuyYes => snapshot.yes,
        OrderSide::BuyNo => snapshot.no,
    }
}

fn order_size(price: f64, max_notional_usd: f64, max_shares: u64) -> u64 {
    if !(0.0..=1.0).contains(&price) || price == 0.0 {
        return 0;
    }
    ((max_notional_usd / price).floor() as u64).min(max_shares)
}

fn expected_move_covers_fees(entry_price: f64, shares: u64, min_expected_price_move: f64) -> bool {
    if shares == 0 {
        return false;
    }
    if min_expected_price_move <= 0.0 {
        return true;
    }
    let exit_price = (entry_price + min_expected_price_move).min(1.0);
    let gross = min_expected_price_move * shares as f64;
    let fees = fee(entry_price, shares) + fee(exit_price, shares);
    gross > fees
}

fn fee(price: f64, shares: u64) -> f64 {
    shares as f64 * 0.07 * price * (1.0 - price)
}

fn buy_price(response: &PostOrderResponse) -> Option<f64> {
    price_ratio(response.making_amount, response.taking_amount)
}

fn sell_price(response: &PostOrderResponse) -> Option<f64> {
    price_ratio(response.taking_amount, response.making_amount)
}

fn buy_shares_filled(response: &PostOrderResponse) -> Option<f64> {
    decimal_to_f64(response.taking_amount)
}

fn sell_shares_filled(response: &PostOrderResponse) -> Option<f64> {
    decimal_to_f64(response.making_amount)
}

fn price_ratio(numerator: Decimal, denominator: Decimal) -> Option<f64> {
    let denominator = decimal_to_f64(denominator)?;
    if denominator <= 0.0 {
        return None;
    }
    Some(decimal_to_f64(numerator)? / denominator)
}

fn decimal_from_f64(value: f64) -> Result<Decimal> {
    Decimal::from_str(&round_5(value).to_string()).context("invalid decimal amount")
}

fn floor_to_2_decimals(value: f64) -> f64 {
    (value * 100.0).floor() / 100.0
}

fn decimal_to_f64(value: Decimal) -> Option<f64> {
    value.to_string().parse().ok()
}

fn env_nonempty(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.trim().is_empty())
}

fn parse_signature_type(value: Option<&str>, has_funder: bool) -> Result<SignatureType> {
    let value = value.unwrap_or(if has_funder { "3" } else { "0" });
    match value {
        "0" | "EOA" | "eoa" => Ok(SignatureType::Eoa),
        "1" | "PROXY" | "proxy" => Ok(SignatureType::Proxy),
        "2" | "GNOSIS_SAFE" | "gnosis_safe" => Ok(SignatureType::GnosisSafe),
        "3" | "POLY1271" | "poly1271" => Ok(SignatureType::Poly1271),
        _ => anyhow::bail!("invalid POLYMARKET_SIGNATURE_TYPE"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_with_notional_and_share_caps() {
        assert_eq!(order_size(0.50, 100.0, 500), 200);
        assert_eq!(order_size(0.10, 100.0, 500), 500);
        assert_eq!(order_size(0.0, 100.0, 500), 0);
    }
}
