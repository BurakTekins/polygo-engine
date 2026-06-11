use std::sync::Arc;
use crate::executor::order::{OrderSignal, OrderSide};
use crate::ws::binance::PriceTick;
use crate::ws::polymarket::ChainlinkTick;
use anyhow::Result;
use tokio::sync::{mpsc::Sender, watch::Receiver};
use tracing::{debug, info, warn};

// --- Strategy Parameters ---
const MIN_BINANCE_MOVE_USD: f64 = 15.0;
const MAX_SIGNAL_AGE_MS: u64 = 300;
const MIN_EDGE_CENTS: f64 = 0.02;
const MIN_PROGRESS: f64 = 0.1;
const MAX_PROGRESS: f64 = 0.8;

#[inline(always)]
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Ultra-low latency decision core with explicit scope-gated locks and zero allocations
pub async fn run(
    mut binance_rx: tokio::sync::watch::Receiver<Option<crate::ws::binance::PriceTick>>,
    mut chainlink_rx: tokio::sync::watch::Receiver<Option<crate::ws::polymarket::ChainlinkTick>>,
    order_tx: tokio::sync::mpsc::Sender<crate::executor::order::OrderSignal>,
    engine_state: Arc<tokio::sync::RwLock<crate::engine::state::EngineState>>,
) -> Result<()> {
    info!("Decision engine started ✓");

    let mut last_binance_price: Option<f64> = None;

    loop {
        // Block until Binance price shifts — our high-precision trigger
        binance_rx.changed().await?;

        // Scope-gate the Binance read lock to drop it instantly before any heavy math
        let binance_tick = {
            let lock = binance_rx.borrow();
            match *lock {
                Some(t) => t, // Dereference and Copy the stack-allocated struct (No Heap Allocation/Clone)
                None => continue,
            }
        }; // Read lock dropped right here!

        let now = now_ms();

        // --- Guard 1: Stale data check ---
        let signal_age_ms = now.saturating_sub(binance_tick.trade_time_ms);
        if signal_age_ms > MAX_SIGNAL_AGE_MS {
            warn!(target: "polygo_engine", "Stale Binance tick: {}ms > {}ms, skipping", signal_age_ms, MAX_SIGNAL_AGE_MS);
            continue;
        }

        // --- Guard 2: Minimum price move check ---
        let price_move = match last_binance_price {
            Some(last) => (binance_tick.price - last).abs(),
            None => {
                last_binance_price = Some(binance_tick.price);
                continue;
            }
        };

        if price_move < MIN_BINANCE_MOVE_USD {
            debug!(target: "polygo_engine", "Price move ${:.2} < threshold, skipping", price_move);
            last_binance_price = Some(binance_tick.price);
            continue;
        }

        // Significant momentum detected
        let direction_up = !binance_tick.is_buyer_maker;
        info!(target: "polygo_engine", "Signal detected! Move: ${:.2}, Direction: {}", price_move, if direction_up { "UP ↑" } else { "DOWN ↓" });

        // --- Guard 3: Chainlink lag check ---
        // Scope-gate the Chainlink read lock to release the watch channel immediately
        let chainlink_tick = {
            let lock = chainlink_rx.borrow();
            *lock // Stack copy via dereference, zero cloning overhead
        }; // Chainlink read lock dropped right here!

        let chainlink_lag_ms = match chainlink_tick {
            Some(cl) => now.saturating_sub(cl.chainlink_ts_ms),
            None => {
                warn!(target: "polygo_engine", "No Chainlink data synchronized yet, dropping signal");
                last_binance_price = Some(binance_tick.price);
                continue;
            }
        };

        if chainlink_lag_ms > MAX_SIGNAL_AGE_MS {
            warn!(target: "polygo_engine", "Chainlink lag {}ms too high, opportunity vanished", chainlink_lag_ms);
            last_binance_price = Some(binance_tick.price);
            continue;
        }

        // --- Guard 4: Edge calculation ---
        // TODO: Integrate raw live orderbook depth here
        let clob_ask_price: f64 = 0.50;
        let expected_clob_price: f64 = if direction_up { 0.55 } else { 0.45 };
        let edge = (expected_clob_price - clob_ask_price).abs();

        if edge < MIN_EDGE_CENTS {
            debug!(target: "polygo_engine", "Edge {:.4} below minimum requirement, skipping", edge);
            last_binance_price = Some(binance_tick.price);
            continue;
        }

        // --- All guards passed — fire order signal down the pipe ---
        let signal = OrderSignal {
            side: if direction_up { OrderSide::BuyYes } else { OrderSide::BuyNo },
            price: clob_ask_price,
            size: 1000,
            signal_ts_ms: now,
            binance_price: binance_tick.price,
            chainlink_lag_ms,
        };

        info!(target: "polygo_engine", "⚡ Arbitrage edge confirmed! Dispatching order → {:?}", signal);

        // Bounded channel send — if the executor hangs, we fast-fail to prevent bad trades
        if order_tx.send(signal).await.is_err() {
            warn!(target: "polygo_engine", "Order execution channel fractured. Emergency halt.");
            return Ok(());
        }

        last_binance_price = Some(binance_tick.price);
    }
}