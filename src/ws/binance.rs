use std::sync::Arc;

use anyhow::Result;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

use crate::health::HealthState;
use crate::market::now_ms;

const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";

#[derive(Debug, Deserialize)]
struct BinanceTick {
    #[serde(rename = "T")]
    trade_time_ms: u64,
    #[serde(rename = "p")]
    price: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PriceTick {
    pub price: f64,
    pub trade_time_ms: u64,
    pub received_at_ms: u64,
}

impl PriceTick {
    pub fn new(price: f64, trade_time_ms: u64) -> Self {
        Self {
            price,
            trade_time_ms,
            received_at_ms: trade_time_ms,
        }
    }
}

#[inline]
pub fn parse_message(raw: &str) -> Result<PriceTick> {
    let tick: BinanceTick = serde_json::from_str(raw)?;
    Ok(PriceTick::new(tick.price.parse()?, tick.trade_time_ms))
}

pub async fn run(tx: Sender<PriceTick>, health: Arc<HealthState>) -> Result<()> {
    loop {
        info!("Connecting to Binance aggTrade stream");
        match connect_async(BINANCE_WS_URL).await {
            Ok((ws_stream, _)) => {
                info!("Binance aggTrade stream connected");
                let (_, mut read) = ws_stream.split();
                while let Some(message) = read.next().await {
                    match message {
                        Ok(message) if message.is_text() => {
                            let raw = message.into_text()?;
                            let mut tick = match parse_message(&raw) {
                                Ok(tick) => tick,
                                Err(error) => {
                                    warn!(%error, "Invalid Binance aggTrade payload");
                                    continue;
                                }
                            };
                            let received_at_ms = now_ms();
                            tick.received_at_ms = received_at_ms;
                            health.mark_binance(received_at_ms);
                            if tx.try_send(tick).is_err() {
                                health.trip(2);
                                anyhow::bail!(
                                    "Binance event channel full or closed; refusing lossy replay"
                                );
                            }
                        }
                        Ok(_) => {}
                        Err(error) => {
                            error!(%error, "Binance websocket error");
                            break;
                        }
                    }
                }
            }
            Err(error) => error!(%error, "Binance websocket connection failed"),
        }
        warn!("Binance websocket disconnected; reconnecting in 1s");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
