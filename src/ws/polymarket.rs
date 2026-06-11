use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::watch::Sender;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

const POLYMARKET_WS_URL: &str = "wss://ws-live-data.polymarket.com";

const SUBSCRIBE_MSG: &str = r#"{
    "action": "subscriptions",
    "subscriptions": [
        {
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": "{\"symbol\":\"btc/usd\"}"
        }
    ]
}"#;

/// Zero-copy deserialization target optimized for low-latency reference lifetimes
#[derive(Debug, Deserialize)]
pub struct PolymarketRaw<'a> {
    pub topic: Option<&'a str>, // Directly reference memory buffer inside the websocket frame
    pub payload: Option<ChainlinkPayload>,
}

#[derive(Debug, Deserialize)]
pub struct ChainlinkPayload {
    #[serde(rename = "timestamp")]
    pub timestamp: u64, // ms — when Chainlink wrote this price
    #[serde(rename = "value")]
    pub value: f64,     // BTC/USD price according to Chainlink
}

/// Parsed tick passed through watch channel to decision engine
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChainlinkTick {
    pub price: f64,
    pub chainlink_ts_ms: u64,
    pub local_ts_ms: u64,
}

#[inline(always)]
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Runs forever, reconnects on disconnect.
/// Forwards only Chainlink BTC/USD price updates.
pub async fn run(tx: Sender<Option<ChainlinkTick>>) -> Result<()> {
    loop {
        info!("Connecting to Polymarket RTDS...");

        match connect_async(POLYMARKET_WS_URL).await {
            Ok((ws_stream, _)) => {
                info!("Polymarket RTDS connected ✓");
                let (mut write, mut read) = ws_stream.split();

                // Send subscription message immediately after connect
                if let Err(e) = write
                    .send(Message::Text(SUBSCRIBE_MSG.to_string()))
                    .await
                {
                    error!("Failed to send subscribe message: {}", e);
                    continue; // Trigger reconnection loop instantly
                }
                info!("Polymarket RTDS subscribed to btc/usd chainlink ✓");

                let mut last_price = 0.0;

                while let Some(msg_result) = read.next().await {
                    match msg_result {
                        Ok(msg) => match msg {
                            Message::Text(text) => {
                                let local_ts = now_ms();

                                // 1. Handle Polymarket plain-text heartbeats/pings to stay alive
                                if text.contains("ping") || text.contains("PING") {
                                    debug!("Polymarket ping frame intercepted. Sending pong back...");
                                    let _ = write.send(Message::Text(r#"{"event":"pong"}"#.to_string())).await;
                                    continue;
                                }

                                // 2. Zero-copy deserialization using direct string slice references
                                if let Ok(raw_data) = serde_json::from_str::<PolymarketRaw<'_>>(&text) {
                                    if let Some(payload) = raw_data.payload {

                                        // Volatility gating — drop duplicate ticks at the networking gateway
                                        if payload.value != last_price {
                                            last_price = payload.value;

                                            let tick = ChainlinkTick {
                                                price: payload.value,
                                                chainlink_ts_ms: payload.timestamp,
                                                local_ts_ms: local_ts,
                                            };

                                            // Push immediately down to the decision channel
                                            let _ = tx.send(Some(tick));
                                        }
                                    }
                                }
                            }
                            Message::Ping(ping_bytes) => {
                                // Protocol-level WebSocket Ping frame handler
                                let _ = write.send(Message::Pong(ping_bytes)).await;
                            }
                            Message::Close(_) => {
                                warn!("Polymarket server closed connection stream gracefully.");
                                break; // Break loop to execute clean auto-reconnection
                            }
                            _ => {}
                        },
                        Err(e) => {
                            error!("Error reading frame from Polymarket WebSocket: {}", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to connect to Polymarket RTDS node: {}. Retrying in 1.5 seconds...", e);
                tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;
            }
        }
    }
}