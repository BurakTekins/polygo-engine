use std::sync::Arc;

use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::{error, warn};

use crate::engine::state::ClosedPosition;
use crate::executor::order::OrderSignal;
use crate::health::HealthState;
use crate::market::now_ms;

#[derive(Debug, Serialize)]
pub struct AuditEnvelope {
    pub schema_version: u8,
    pub source: &'static str,
    pub execution_mode: &'static str,
    pub emitted_at_ms: u64,
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
    },
}

impl AuditEvent {
    pub fn signal(signal: OrderSignal) -> Self {
        Self::SignalAccepted {
            signal_ts_ms: signal.signal_ts_ms,
            side: signal.side.as_str(),
            momentum_usd: signal.momentum_usd,
            binance_price: signal.binance_price,
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
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let envelope = AuditEnvelope {
                    schema_version: 1,
                    source: "polygo-engine",
                    execution_mode,
                    emitted_at_ms: now_ms(),
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
