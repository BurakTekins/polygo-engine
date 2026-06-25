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
    let mut last_signal_side = None;
    info!("Decision engine started");

    while let Some(tick) = binance_rx.recv().await {
        let Some(config) = strategy_store.load() else {
            continue;
        };
        if config.generation != strategy_generation {
            strategy = Some(MomentumStrategy::with_params(
                config.momentum_window_ms,
                config.momentum_threshold_usd,
            ));
            strategy_generation = config.generation;
            last_signal_ts_ms = 0;
            last_signal_side = None;
        }
        let received_at_ms = now_ms();
        if received_at_ms.saturating_sub(tick.trade_time_ms) > config.max_book_age_ms {
            continue;
        }
        let candidate = strategy
            .as_mut()
            .and_then(|strategy| strategy.on_binance_tick(tick, received_at_ms));
        let Some(candidate) = candidate else {
            continue;
        };
        if last_signal_side == Some(candidate.side)
            && candidate.signal_ts_ms.saturating_sub(last_signal_ts_ms) < config.hold_ms
        {
            continue;
        }
        if !health.is_running()
            || !market_clock.progress_allowed(
                received_at_ms,
                config.min_progress,
                config.max_progress,
            )
        {
            continue;
        }
        let book = book_state.load();
        if received_at_ms.saturating_sub(book.received_at_ms) > config.max_book_age_ms {
            continue;
        }
        let outcome = match candidate.side {
            OrderSide::BuyYes => book.yes,
            OrderSide::BuyNo => book.no,
        };
        let (Some(bid), Some(ask)) = (outcome.bid, outcome.ask) else {
            continue;
        };
        if ask <= bid
            || ask < config.min_price
            || ask > config.max_price
            || engine_state.try_reserve().is_err()
        {
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
        last_signal_ts_ms = candidate.signal_ts_ms;
        last_signal_side = Some(candidate.side);
    }
    Ok(())
}
