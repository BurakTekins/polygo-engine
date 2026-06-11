use anyhow::Result;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::watch::Sender;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn, debug};

const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";

/// Minimum price move in USD to forward tick to decision engine
/// Filters out micro-fluctuations in high-frequency burst periods
const MIN_PRICE_MOVE: f64 = 10.0;

/// Raw message coming from Binance aggTrade stream
#[derive(Debug, Deserialize)]
pub struct BinanceTick {
    #[serde(rename = "T")]
    pub trade_time: u64,      // trade timestamp in ms (server-side)
    #[serde(rename = "p")]
    pub price: String,        // price as string, we parse to f64
    #[serde(rename = "q")]
    pub quantity: String,     // trade quantity
    #[serde(rename = "m")]
    pub is_buyer_maker: bool, // true = price going down, false = going up
}

/// Parsed tick passed through watch channel to decision engine
#[derive(Debug, Clone, Copy)]
pub struct PriceTick {
    pub price: f64,
    pub trade_time_ms: u64,
    pub is_buyer_maker: bool,
}

/// Runs forever, reconnects on disconnect.
/// Only forwards ticks where price moved more than MIN_PRICE_MOVE.
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
                            let text = msg.into_text()?;

                            match serde_json::from_str::<BinanceTick>(&text) {
                                Ok(tick) => {
                                    let new_price = match tick.price.parse::<f64>() {
                                        Ok(p) => p,
                                        Err(_) => {
                                            warn!("Failed to parse price: {}", tick.price);
                                            continue;
                                        }
                                    };

                                    // Threshold filter — skip insignificant moves
                                    let should_forward = {
                                        let last = tx.borrow();
                                        match last.as_ref() {
                                            Some(last_tick) => {
                                                (new_price - last_tick.price).abs() >= MIN_PRICE_MOVE
                                            }
                                            // No previous tick yet — always forward first tick
                                            None => true,
                                        }
                                    };

                                    if !should_forward {
                                        debug!("Tick filtered out — price move below threshold");
                                        continue;
                                    }

                                    let parsed = PriceTick {
                                        price: new_price,
                                        trade_time_ms: tick.trade_time,
                                        is_buyer_maker: tick.is_buyer_maker,
                                    };

                                    // watch::send replaces value — never blocks, never queues
                                    if tx.send(Some(parsed)).is_err() {
                                        error!("Decision engine watch receiver dropped, shutting down");
                                        return Ok(());
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to parse Binance tick: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Binance WS error: {}", e);
                            break; // reconnect
                        }
                        _ => {} // ping/pong/binary, ignore
                    }
                }
            }
            Err(e) => {
                error!("Binance WS connection failed: {}", e);
            }
        }

        warn!("Binance WS disconnected, reconnecting in 1s...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}