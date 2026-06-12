use anyhow::Result;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::watch::Sender;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";

// Debug mode: forward every price change
const MIN_PRICE_MOVE: f64 = 0.0;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Deserialize)]
pub struct BinanceTick {
    #[serde(rename = "T")]
    pub trade_time: u64,

    #[serde(rename = "p")]
    pub price: String,

    #[serde(rename = "q")]
    pub quantity: String,

    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct PriceTick {
    pub price: f64,
    pub trade_time_ms: u64,
    pub local_ts_ms: u64,
    pub is_buyer_maker: bool,
}

pub async fn run(tx: Sender<Option<PriceTick>>) -> Result<()> {
    loop {
        info!("Connecting to Binance WS...");

        match connect_async(BINANCE_WS_URL).await {
            Ok((ws_stream, _)) => {
                info!("Binance WS connected ✓");

                let (_, mut read) = ws_stream.split();

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(msg) if msg.is_text() => {
                            let text = match msg.into_text() {
                                Ok(t) => t,
                                Err(e) => {
                                    warn!("Failed to decode text frame: {}", e);
                                    continue;
                                }
                            };
                            println!("RAW BINANCE: {}", text);

                            let tick: BinanceTick =
                                match serde_json::from_str(&text) {
                                    Ok(t) => t,
                                    Err(e) => {
                                        warn!("Failed to parse Binance tick: {}", e);
                                        continue;
                                    }
                                };

                            let price = match tick.price.parse::<f64>() {
                                Ok(p) => p,
                                Err(_) => {
                                    warn!("Invalid price: {}", tick.price);
                                    continue;
                                }
                            };

                            let should_forward = {
                                let last = tx.borrow();

                                match last.as_ref() {
                                    Some(last_tick) => {
                                        (price - last_tick.price).abs()
                                            >= MIN_PRICE_MOVE
                                    }
                                    None => true,
                                }
                            };

                            if !should_forward {
                                continue;
                            }

                            let parsed = PriceTick {
                                price,
                                trade_time_ms: tick.trade_time,
                                local_ts_ms: now_ms(),
                                is_buyer_maker: tick.is_buyer_maker,
                            };

                            if tx.send(Some(parsed)).is_err() {
                                error!("Decision engine receiver dropped");
                                return Ok(());
                            }
                        }

                        Ok(_) => {}

                        Err(e) => {
                            error!("Binance WS error: {}", e);
                            break;
                        }
                    }
                }
            }

            Err(e) => {
                error!("Binance connection failed: {}", e);
            }
        }

        warn!("Binance disconnected, reconnecting in 1 second...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}