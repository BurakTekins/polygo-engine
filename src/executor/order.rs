use std::sync::Arc;
use crate::ws::binance::PriceTick;
use crate::ws::polymarket::ChainlinkTick;
use anyhow::Result;
use tokio::sync::mpsc::Receiver;
use tracing::{error, info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    BuyYes,
    BuyNo,
}

impl OrderSide {
    /// Zero-copy mapping to Polymarket CLOB API string expectations
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BuyYes => "BUY_YES",
            Self::BuyNo => "BUY_NO",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OrderSignal {
    pub side: OrderSide,
    pub price: f64,
    pub size: u64,
    pub signal_ts_ms: u64,
    pub binance_price: f64,
    pub chainlink_lag_ms: u64
}

#[derive(Debug)]
pub struct ExecutionResult {
    pub signal: OrderSignal,
    pub success: bool,
    pub executed_at_ms: u64,
    pub latency_ms: u64,
    pub error: Option<String>,
}

#[inline(always)]
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Runs forever. Consumes signals sequentially to guarantee strict chronological order.
pub async fn run(
    mut order_rx: tokio::sync::mpsc::Receiver<crate::executor::order::OrderSignal>,
    engine_state: Arc<tokio::sync::RwLock<crate::engine::state::EngineState>>,
    audit_emitter: Arc<crate::emitter::audit::AuditEmitter>,
) {
    info!("Order executor operational with strict FIFO ordering ✓");

    // We process orders one-by-one. No tokio::spawn here to prevent race conditions (Task B overriding Task A)
    while let Some(signal) = order_rx.recv().await {
        let received_at = now_ms();
        let pipeline_latency = received_at.saturating_sub(signal.signal_ts_ms);

        info!(
            target: "polygo_engine",
            "Order intercept -> side: {:?}, price: {:.4}, size: {}, pipeline latency: {}ms",
            signal.side, signal.price, signal.size, pipeline_latency
        );

        // --- Execution latency guard ---
        // If the pipeline was clogged and this signal is stale, drop it before hitting the network
        if pipeline_latency > 150 {
            warn!(
                target: "polygo_engine",
                "Signal dropped! Pipeline latency ({}ms > 150ms) breached threshold.",
                pipeline_latency
            );
            continue;
        }

        // --- Strict Sequential Execution ---
        // The loop blocks here until the network request finishes, guaranteeing Order 1 hits the book before Order 2
        match execute_order_sequential(signal).await {
            Ok(result) => {
                info!(
                    target: "polygo_engine",
                    "✓ Order executed in sequence — Network Latency: {}ms, Status: {}",
                    result.latency_ms, result.success
                );
                // TODO: Forward result to audit emitter without blocking
            }
            Err(e) => {
                error!(target: "polygo_engine", "❌ Network rejection on sequential CLOB routing: {}", e);
            }
        }
    }

    warn!(target: "polygo_engine", "Upstream runtime channel dead. Initiating clean executor closure.");
}

/// Sends order to Polymarket CLOB API sequentially
async fn execute_order_sequential(signal: OrderSignal) -> Result<ExecutionResult> {
    let start = now_ms();

    info!(
        target: "polygo_engine",
        "[NETWORK OUTBOUND] Dispatching sequential packet for {:?} — {} contracts at limit price {:.4}",
        signal.side.as_str(), signal.size, signal.price
    );

    // Simulate cross-network infrastructure response latency (e.g., HTTP POST to CLOB)
    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;

    let end = now_ms();

    Ok(ExecutionResult {
        signal,
        success: true,
        executed_at_ms: end,
        latency_ms: end.saturating_sub(start),
        error: None,
    })
}