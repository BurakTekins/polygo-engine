use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::emitter::audit::{AuditEmitter, AuditEvent};
use crate::engine::state::{round_5, EngineState};
use crate::health::HealthState;
use crate::market::{now_ms, MarketClock};
use crate::runtime_config::{StrategySnapshot, StrategyStore};
use crate::ws::polymarket::{BookSnapshot, BookState, OutcomeBook};

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
    book_state: Arc<BookState>,
    engine_state: Arc<EngineState>,
    health: Arc<HealthState>,
    market_clock: Arc<MarketClock>,
    strategy_store: Arc<StrategyStore>,
    audit: AuditEmitter,
) {
    info!("Dry-run executor started");
    while let Some(signal) = order_rx.recv().await {
        let _ = audit.emit(AuditEvent::signal(signal));
        let task_book_state = Arc::clone(&book_state);
        let task_state = Arc::clone(&engine_state);
        let task_health = Arc::clone(&health);
        let task_clock = Arc::clone(&market_clock);
        let task_store = Arc::clone(&strategy_store);
        let task_audit = audit.clone();
        tokio::spawn(async move {
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
        reject(signal, "entry_revalidation", &engine_state, &audit);
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
        });
        return;
    }
    let Some(exit_price) = outcome_book(book, signal.side).bid else {
        engine_state.release_reservation();
        let _ = audit.emit(AuditEvent::ExecutionRejected {
            signal_ts_ms: signal.signal_ts_ms,
            side: signal.side.as_str(),
            reason: "missing_exit_bid_position_released",
        });
        return;
    };
    let closed = engine_state.close(ask, exit_price, shares);
    let _ = audit.emit(AuditEvent::exit(position_id, signal.side.as_str(), closed));
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
    });
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
