use anyhow::Result;
use serde::Serialize;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tracing::{error, info, warn};

use crate::executor::order::OrderSignal;

#[derive(Debug, Serialize)]
#[serde(tag = "event_type")]
pub enum AuditEvent {
    SignalDetected {
        signal_ts_ms: u64,
        binance_price: f64,
        chainlink_lag_ms: u64,
        side: &'static str,
        price: f64,
        size: u64,
    },
    OrderExecuted {
        signal_ts_ms: u64,
        executed_at_ms: u64,
        latency_ms: u64,
        side: &'static str,
        price: f64,
        size: u64,
        success: bool,
        error: Option<String>,
    },
    PositionClosed {
        closed_at_ms: u64,
        entry_price: f64,
        exit_price: f64,
        size: u64,
        gross_pnl: f64,
        fee: f64,
        net_pnl: f64,
    },
    RiskLimitBreached {
        ts_ms: u64,
        reason: String,
        daily_loss_usdc: f64,
    },
    EngineError {
        ts_ms: u64,
        component: String,
        message: String,
    },
}

impl AuditEvent {
    pub fn from_signal(signal: &OrderSignal) -> Self {
        AuditEvent::SignalDetected {
            signal_ts_ms: signal.signal_ts_ms,
            binance_price: signal.binance_price,
            chainlink_lag_ms: signal.chainlink_lag_ms,
            side: signal.side.as_str(),
            price: signal.price,
            size: signal.size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuditEmitter {
    endpoint: Arc<str>,
    client: reqwest::Client,
    log_file_path: &'static str, // Path to local JSON lines repository
}

impl AuditEmitter {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint: Arc::from(endpoint.into_boxed_str()),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_millis(500))
                .build()
                .expect("Failed to build HTTP client"),
            log_file_path: "audit_log.json", // Yerel kayıt dosyamız kanka
        }
    }

    /// Fire-and-forget audit log — persists to local JSON and prepares for future Java HTTP routing
    pub fn emit(&self, event: AuditEvent) {
        let client = self.client.clone();
        let endpoint = Arc::clone(&self.endpoint);
        let file_path = self.log_file_path;

        tokio::spawn(async move {
            // 1. Serialize event to JSON string
            let payload = match serde_json::to_string(&event) {
                Ok(p) => p,
                Err(e) => {
                    error!(target: "polygo_engine", "Failed to serialize audit event: {}", e);
                    return;
                }
            };

            // 2. Local File Persistence (JSON Lines format for clean Java parsing)
            // Open file in Append mode, create if it doesn't exist
            match OpenOptions::new()
                .create(true)
                .append(true)
                .open(file_path)
                .await
            {
                Ok(mut file) => {
                    let mut line_data = payload.clone();
                    line_data.push('\n'); // Newline delimited
                    if let Err(e) = file.write_all(line_data.as_bytes()).await {
                        error!(target: "polygo_engine", "Failed to write audit to file: {}", e);
                    }
                }
                Err(e) => {
                    error!(target: "polygo_engine", "Failed to open local audit log file: {}", e);
                }
            }

            // 3. Forward to Java (Ignored if Java backend is down, allows standalone local testing)
            let _ = client
                .post(&*endpoint)
                .header("Content-Type", "application/json")
                .body(payload)
                .send()
                .await;
        });
    }
}