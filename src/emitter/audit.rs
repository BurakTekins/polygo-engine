use std::sync::Arc;

use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::{error, warn};

use crate::engine::state::ClosedPosition;
use crate::executor::order::OrderSignal;
use crate::health::HealthState;
use crate::market::now_ms;
use crate::runtime_config::StrategyStore;

#[derive(Debug, Clone, Serialize)]
pub struct AuditBookContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_slug: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_id: Option<String>,
    pub yes_bid: Option<f64>,
    pub yes_ask: Option<f64>,
    pub no_bid: Option<f64>,
    pub no_ask: Option<f64>,
    pub book_received_at_ms: u64,
    pub book_source_ts_ms: u64,
    pub book_age_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_bid: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_ask: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_slippage: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_limit_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intended_shares: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intended_notional_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available_shares: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_shares: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filled_shares: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_attempts: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback_order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_submit_latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_build_latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_sign_latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_post_latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtc_post_success: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtc_post_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtc_post_making_amount: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtc_post_taking_amount: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtc_poll_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtc_poll_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtc_poll_size_matched: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtc_cancel_confirmed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtc_final_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gtc_final_size_matched: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct AuditEnvelope {
    pub schema_version: u8,
    pub source: &'static str,
    pub session_id: String,
    pub execution_mode: &'static str,
    pub emitted_at_ms: u64,
    pub config_version: Option<String>,
    #[serde(flatten)]
    pub event: AuditEvent,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum AuditEvent {
    SignalAccepted {
        signal_ts_ms: u64,
        side: &'static str,
        momentum_usd: f64,
        binance_price: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        context: Option<AuditBookContext>,
    },
    DryRunEntry {
        position_id: u64,
        signal_ts_ms: u64,
        executed_at_ms: u64,
        latency_ms: u64,
        side: &'static str,
        price: f64,
        shares: u64,
        notional_usd: f64,
    },
    DryRunExit {
        position_id: u64,
        closed_at_ms: u64,
        side: &'static str,
        entry_price: f64,
        exit_price: f64,
        shares: u64,
        gross_pnl: f64,
        entry_fee: f64,
        exit_fee: f64,
        net_pnl: f64,
    },
    LiveEntry {
        position_id: u64,
        signal_ts_ms: u64,
        executed_at_ms: u64,
        latency_ms: u64,
        side: &'static str,
        token_id: String,
        order_id: String,
        order_status: String,
        price: f64,
        shares: f64,
        notional_usd: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        context: Option<AuditBookContext>,
    },
    LiveExit {
        position_id: u64,
        closed_at_ms: u64,
        side: &'static str,
        token_id: String,
        order_id: String,
        order_status: String,
        entry_price: f64,
        exit_price: f64,
        shares: f64,
        gross_pnl: f64,
        entry_fee: f64,
        exit_fee: f64,
        net_pnl: f64,
    },
    ExecutionRejected {
        signal_ts_ms: u64,
        side: &'static str,
        reason: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        context: Option<AuditBookContext>,
    },
}

impl AuditEvent {
    pub fn signal(signal: OrderSignal, context: Option<AuditBookContext>) -> Self {
        Self::SignalAccepted {
            signal_ts_ms: signal.signal_ts_ms,
            side: signal.side.as_str(),
            momentum_usd: signal.momentum_usd,
            binance_price: signal.binance_price,
            context,
        }
    }

    pub fn exit(position_id: u64, side: &'static str, closed: ClosedPosition) -> Self {
        Self::DryRunExit {
            position_id,
            closed_at_ms: now_ms(),
            side,
            entry_price: closed.entry_price,
            exit_price: closed.exit_price,
            shares: closed.shares,
            gross_pnl: closed.gross_pnl,
            entry_fee: closed.entry_fee,
            exit_fee: closed.exit_fee,
            net_pnl: closed.net_pnl,
        }
    }
}

#[derive(Clone)]
pub struct AuditEmitter {
    tx: mpsc::Sender<AuditEvent>,
    health: Arc<HealthState>,
}

impl AuditEmitter {
    pub fn new(
        endpoint: Option<String>,
        health: Arc<HealthState>,
        strategy_store: Arc<StrategyStore>,
        execution_mode: &'static str,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<AuditEvent>(1_024);
        let (local_tx, local_rx) = mpsc::channel::<Vec<u8>>(1_024);
        let (java_tx, java_rx) = mpsc::channel::<Vec<u8>>(1_024);
        tokio::spawn(local_worker(local_rx, Arc::clone(&health)));
        if let Some(endpoint) = endpoint {
            tokio::spawn(java_worker(java_rx, endpoint));
        } else {
            drop(java_rx);
        }
        let dispatcher_health = Arc::clone(&health);
        let session_id = engine_session_id();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let envelope = AuditEnvelope {
                    schema_version: 1,
                    source: "polygo-engine",
                    session_id: session_id.clone(),
                    execution_mode,
                    emitted_at_ms: now_ms(),
                    config_version: strategy_store.version().ok().flatten(),
                    event,
                };
                let payload = match serde_json::to_vec(&envelope) {
                    Ok(payload) => payload,
                    Err(error) => {
                        error!(%error, "Audit serialization failed");
                        dispatcher_health.trip(1);
                        continue;
                    }
                };
                if local_tx.try_send(payload.clone()).is_err() {
                    dispatcher_health.trip(1);
                }
                if !java_tx.is_closed() && java_tx.try_send(payload).is_err() {
                    warn!("Java audit queue full; remote delivery dropped");
                }
            }
        });
        Self { tx, health }
    }

    #[inline(always)]
    pub fn emit(&self, event: AuditEvent) -> bool {
        match self.tx.try_send(event) {
            Ok(()) => true,
            Err(_) => {
                self.health.trip(1);
                false
            }
        }
    }
}

fn engine_session_id() -> String {
    format!("engine-{}-{}", now_ms(), std::process::id())
}

async fn local_worker(mut rx: mpsc::Receiver<Vec<u8>>, health: Arc<HealthState>) {
    let mut file = match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("audit_log.jsonl")
        .await
    {
        Ok(file) => file,
        Err(error) => {
            error!(%error, "Local audit open failed");
            health.trip(1);
            return;
        }
    };
    while let Some(payload) = rx.recv().await {
        if file.write_all(&payload).await.is_err() || file.write_all(b"\n").await.is_err() {
            health.trip(1);
            return;
        }
    }
}

async fn java_worker(mut rx: mpsc::Receiver<Vec<u8>>, endpoint: String) {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(500))
        .build()
        .expect("HTTP client construction must succeed");
    while let Some(payload) = rx.recv().await {
        match client
            .post(&endpoint)
            .header("Content-Type", "application/json")
            .body(payload)
            .send()
            .await
        {
            Ok(response) if !response.status().is_success() => {
                warn!(status = %response.status(), "Java audit delivery returned non-success status");
            }
            Ok(_) => {}
            Err(error) => warn!(%error, "Java audit delivery failed"),
        }
    }
}
