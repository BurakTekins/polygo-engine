use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::info;

use crate::engine::state::EngineState;
use crate::engine::strategy::{MomentumStrategy, Strategy};
use crate::executor::order::{OrderSide, OrderSignal};
use crate::health::HealthState;
use crate::market::{now_ms, MarketClock};
use crate::runtime_config::StrategyStore;
use crate::ws::binance::PriceTick;
use crate::ws::polymarket::BookState;

pub async fn run(
    mut binance_rx: mpsc::Receiver<PriceTick>,
    book_state: Arc<BookState>,
    order_tx: mpsc::Sender<OrderSignal>,
    engine_state: Arc<EngineState>,
    health: Arc<HealthState>,
    market_clock: Arc<MarketClock>,
    strategy_store: Arc<StrategyStore>,
) -> Result<()> {
    let mut strategy = None;
    let mut strategy_generation = 0;
    let mut last_signal_ts_ms = 0;
    let mut last_diagnostic_log_ms = 0;
    info!("Decision engine started");

    while let Some(tick) = binance_rx.recv().await {
        let Some(config) = strategy_store.load() else {
            continue;
        };
        if config.generation != strategy_generation {
            strategy = Some(MomentumStrategy::with_params(
                config.momentum_window_ms,
                config.buy_yes_momentum_threshold_usd,
                config.buy_no_momentum_threshold_usd,
            ));
            strategy_generation = config.generation;
            last_signal_ts_ms = 0;
        }
        let received_at_ms = now_ms();
        if received_at_ms.saturating_sub(tick.received_at_ms) > config.max_book_age_ms {
            continue;
        }
        let candidate = strategy
            .as_mut()
            .and_then(|strategy| strategy.on_binance_tick(tick, received_at_ms));
        let Some(candidate) = candidate else {
            continue;
        };
        if candidate.signal_ts_ms.saturating_sub(last_signal_ts_ms) < config.hold_ms {
            if diagnostic_due(&mut last_diagnostic_log_ms, received_at_ms) {
                info!(
                    reason = "cooldown",
                    side = candidate.side.as_str(),
                    momentum_usd = candidate.momentum_usd,
                    "Momentum candidate rejected"
                );
            }
            continue;
        }
        if !health.is_running() {
            if diagnostic_due(&mut last_diagnostic_log_ms, received_at_ms) {
                info!(
                    reason = "engine_stopped",
                    side = candidate.side.as_str(),
                    momentum_usd = candidate.momentum_usd,
                    "Momentum candidate rejected"
                );
            }
            continue;
        }
        if !health.market_ready() {
            if diagnostic_due(&mut last_diagnostic_log_ms, received_at_ms) {
                info!(
                    reason = "market_not_ready",
                    side = candidate.side.as_str(),
                    momentum_usd = candidate.momentum_usd,
                    "Momentum candidate rejected"
                );
            }
            continue;
        }
        if !market_clock.progress_allowed(received_at_ms, config.min_progress, config.max_progress)
        {
            if diagnostic_due(&mut last_diagnostic_log_ms, received_at_ms) {
                info!(
                    reason = "market_progress",
                    side = candidate.side.as_str(),
                    momentum_usd = candidate.momentum_usd,
                    "Momentum candidate rejected"
                );
            }
            continue;
        }
        let book = book_state.load();
        let book_age_ms = received_at_ms.saturating_sub(book.received_at_ms);
        if book_age_ms > config.max_book_age_ms {
            if diagnostic_due(&mut last_diagnostic_log_ms, received_at_ms) {
                info!(
                    reason = "stale_book",
                    side = candidate.side.as_str(),
                    momentum_usd = candidate.momentum_usd,
                    book_age_ms,
                    "Momentum candidate rejected"
                );
            }
            continue;
        }
        let outcome = match candidate.side {
            OrderSide::BuyYes => book.yes,
            OrderSide::BuyNo => book.no,
        };
        let (Some(bid), Some(ask)) = (outcome.bid, outcome.ask) else {
            if diagnostic_due(&mut last_diagnostic_log_ms, received_at_ms) {
                info!(
                    reason = "missing_book",
                    side = candidate.side.as_str(),
                    momentum_usd = candidate.momentum_usd,
                    yes_bid = ?book.yes.bid,
                    yes_ask = ?book.yes.ask,
                    no_bid = ?book.no.bid,
                    no_ask = ?book.no.ask,
                    book_age_ms,
                    "Momentum candidate rejected"
                );
            }
            continue;
        };
        if ask <= bid {
            if diagnostic_due(&mut last_diagnostic_log_ms, received_at_ms) {
                info!(
                    reason = "crossed_book",
                    side = candidate.side.as_str(),
                    momentum_usd = candidate.momentum_usd,
                    bid,
                    ask,
                    "Momentum candidate rejected"
                );
            }
            continue;
        }
        let spread = ask - bid;
        if spread > config.max_spread + 0.00001 {
            if diagnostic_due(&mut last_diagnostic_log_ms, received_at_ms) {
                info!(
                    reason = "spread",
                    side = candidate.side.as_str(),
                    momentum_usd = candidate.momentum_usd,
                    bid,
                    ask,
                    spread,
                    max_spread = config.max_spread,
                    "Momentum candidate rejected"
                );
            }
            continue;
        }
        if ask < config.min_price || ask > config.max_price {
            if diagnostic_due(&mut last_diagnostic_log_ms, received_at_ms) {
                info!(
                    reason = "price_range",
                    side = candidate.side.as_str(),
                    momentum_usd = candidate.momentum_usd,
                    ask,
                    min_price = config.min_price,
                    max_price = config.max_price,
                    "Momentum candidate rejected"
                );
            }
            continue;
        }
        if engine_state.try_reserve().is_err() {
            if diagnostic_due(&mut last_diagnostic_log_ms, received_at_ms) {
                let active = engine_state.active_snapshot();
                info!(
                    reason = "position_reserved",
                    side = candidate.side.as_str(),
                    momentum_usd = candidate.momentum_usd,
                    active_opened = active.opened,
                    active_position_id = active.position_id,
                    active_age_ms = active.age_ms,
                    "Momentum candidate rejected"
                );
            }
            continue;
        }

        let signal = OrderSignal::new(
            candidate.side,
            candidate.momentum_usd,
            candidate.signal_ts_ms,
            candidate.binance_price,
            tokio::time::Instant::now()
                + tokio::time::Duration::from_millis(config.execution_latency_ms),
            config.generation,
            config.hold_ms,
        );
        if order_tx.try_send(signal).is_err() {
            engine_state.release_reservation();
            health.trip(3);
            anyhow::bail!("execution channel full or closed");
        }
        info!(
            side = candidate.side.as_str(),
            momentum_usd = candidate.momentum_usd,
            bid,
            ask,
            "Momentum candidate accepted"
        );
        last_signal_ts_ms = candidate.signal_ts_ms;
    }
    Ok(())
}

fn diagnostic_due(last_log_ms: &mut u64, now_ms: u64) -> bool {
    if now_ms.saturating_sub(*last_log_ms) < 1_000 {
        return false;
    }
    *last_log_ms = now_ms;
    true
}
