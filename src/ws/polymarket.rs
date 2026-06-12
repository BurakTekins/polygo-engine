use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::watch::Sender;
use tokio_tungstenite::{
    connect_async,
    tungstenite::client::IntoClientRequest,
    tungstenite::protocol::Message,
};
use tracing::{error, info};
use std::fs;

const POLYMARKET_WS_URL: &str =
    "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Debug, Clone, Copy)]
pub struct ChainlinkTick {
    pub price: f64,
    pub chainlink_ts_ms: u64,
    pub local_ts_ms: u64,
    pub best_bid: f64,
    pub best_ask: f64,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub async fn run(tx: Sender<Option<ChainlinkTick>>) -> Result<()> {
    let mut last_price: f64 = 0.0;

    loop {
        let config_str = fs::read_to_string("config.json").unwrap_or_default();
        let json: Value = serde_json::from_str(&config_str).unwrap_or_default();

        let asset_id = json["asset_id"]
            .as_str()
            .unwrap_or(
                "84504347392432335767997575050081291832854909541856155902350538179175425180376"
            );

        let sub_msg = serde_json::json!({
            "type": "market",
            "assets_ids": [asset_id]
        })
            .to_string();

        let mut request = POLYMARKET_WS_URL.into_client_request()?;
        request.headers_mut().insert(
            "Origin",
            "https://polymarket.com".parse().unwrap(),
        );

        info!("Connecting Polymarket WS... asset={}", asset_id);

        match connect_async(request).await {
            Ok((ws_stream, _)) => {
                info!("Polymarket connected ✓");

                let (mut write, mut read) = ws_stream.split();

                info!("Sending subscribe...");

                if let Err(e) = write.send(Message::Text(sub_msg.into())).await {
                    error!("Subscribe send failed: {}", e);
                    continue;
                }

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            println!("RAW POLY: {}", text);

                            if text.to_lowercase().contains("ping") {
                                let _ = write
                                    .send(Message::Text(
                                        "{\"type\":\"pong\"}".into(),
                                    ))
                                    .await;
                                continue;
                            }

                            let v: Value = match serde_json::from_str(&text) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };

                            if let Some(arr) = v.as_array() {
                                for item in arr {
                                    handle_tick(item, &mut last_price, &tx);
                                }
                                continue;
                            }

                            handle_tick(&v, &mut last_price, &tx);
                        }

                        Ok(Message::Ping(bytes)) => {
                            let _ = write.send(Message::Pong(bytes)).await;
                        }

                        Ok(Message::Close(_)) => {
                            error!("WS closed, reconnecting...");
                            break;
                        }

                        Err(e) => {
                            error!("WS error: {}", e);
                            break;
                        }

                        _ => {}
                    }
                }
            }

            Err(e) => {
                error!("Connection failed: {}, retrying...", e);
                tokio::time::sleep(
                    std::time::Duration::from_secs(2)
                )
                    .await;
            }
        }
    }
}

fn handle_tick(
    v: &Value,
    last_price: &mut f64,
    tx: &Sender<Option<ChainlinkTick>>,
) {
    let price = v
        .get("price")
        .and_then(|p| p.as_str())
        .and_then(|p| p.parse::<f64>().ok())
        .or_else(|| v.get("price").and_then(|p| p.as_f64()))
        .or_else(|| {
            v.get("bids")
                .and_then(|b| b.as_array())
                .and_then(|arr| arr.first())
                .and_then(|b| b.get("price"))
                .and_then(|p| p.as_str())
                .and_then(|p| p.parse::<f64>().ok())
        });

    let Some(price) = price else {
        return;
    };

    if (price - *last_price).abs() < f64::EPSILON {
        return;
    }

    let best_bid = v
        .get("best_bid")
        .and_then(|b| b.as_str())
        .and_then(|b| b.parse::<f64>().ok())
        .or_else(|| {
            v.get("bids")
                .and_then(|b| b.as_array())
                .and_then(|arr| arr.first())
                .and_then(|b| b.get("price"))
                .and_then(|p| p.as_str())
                .and_then(|p| p.parse::<f64>().ok())
        })
        .unwrap_or(price);

    let best_ask = v
        .get("best_ask")
        .and_then(|b| b.as_str())
        .and_then(|b| b.parse::<f64>().ok())
        .or_else(|| {
            v.get("asks")
                .and_then(|b| b.as_array())
                .and_then(|arr| arr.first())
                .and_then(|b| b.get("price"))
                .and_then(|p| p.as_str())
                .and_then(|p| p.parse::<f64>().ok())
        })
        .unwrap_or(price);

    *last_price = price;

    let ts = v
        .get("timestamp")
        .and_then(|t| t.as_u64())
        .unwrap_or_else(now_ms);

    let _ = tx.send(Some(ChainlinkTick {
        price,
        chainlink_ts_ms: ts,
        local_ts_ms: now_ms(),
        best_bid,
        best_ask,
    }));
}