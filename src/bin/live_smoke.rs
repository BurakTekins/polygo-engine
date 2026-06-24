use std::time::Instant;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use polygo_engine::config::EngineConfig;
use polygo_engine::market::{discover, now_ms};
use polygo_engine::ws::binance::parse_message;
use polygo_engine::ws::polymarket::{parse_and_apply, BookSnapshot};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, protocol::Message},
};

const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[tokio::main]
async fn main() -> Result<()> {
    let config = EngineConfig::load("config.json")?;
    let epoch_seconds = now_ms() / 1_000;
    let window_start =
        epoch_seconds / config.market.interval_seconds * config.market.interval_seconds;
    let slug = format!("{}-{window_start}", config.market.slug_prefix);
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()?;
    let market = discover(&client, &config.market, &slug, window_start).await?;
    let binance = tokio::spawn(binance_smoke());
    let polymarket = tokio::spawn(polymarket_smoke(market.yes_asset_id, market.no_asset_id));
    let (binance, polymarket) = tokio::try_join!(binance, polymarket)?;
    binance?;
    polymarket?;
    Ok(())
}

async fn binance_smoke() -> Result<()> {
    let (stream, _) = connect_async(BINANCE_WS_URL).await?;
    let (_, mut read) = stream.split();
    let mut cpu_ns = Vec::with_capacity(5_000);
    let mut network_age_ms = Vec::with_capacity(5_000);
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    while cpu_ns.len() < 5_000 && tokio::time::Instant::now() < deadline {
        let message = match tokio::time::timeout_at(deadline, read.next()).await {
            Ok(Some(Ok(message))) => message,
            Ok(Some(Err(error))) => return Err(error.into()),
            Ok(None) => anyhow::bail!("Binance stream ended"),
            Err(_) => break,
        };
        if !message.is_text() {
            continue;
        }
        let raw = message.into_text()?;
        let start = Instant::now();
        let tick = parse_message(&raw)?;
        cpu_ns.push(start.elapsed().as_nanos() as u64);
        network_age_ms.push(now_ms() as i64 - tick.trade_time_ms as i64);
    }
    print_stats("live_binance_parse_cpu", "ns", &mut cpu_ns);
    print_signed_stats("live_binance_network_age", "ms", &mut network_age_ms);
    Ok(())
}

async fn polymarket_smoke(yes_asset_id: String, no_asset_id: String) -> Result<()> {
    let mut request = POLYMARKET_WS_URL.into_client_request()?;
    request
        .headers_mut()
        .insert("Origin", "https://polymarket.com".parse()?);
    let (stream, _) = connect_async(request).await?;
    let (mut write, mut read) = stream.split();
    write
        .send(Message::Text(
            serde_json::json!({
                "type": "market",
                "assets_ids": [&yes_asset_id, &no_asset_id]
            })
            .to_string(),
        ))
        .await?;
    let mut cpu_ns = Vec::with_capacity(200);
    let mut network_age_ms = Vec::with_capacity(200);
    let mut snapshot = BookSnapshot::default();
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(15);
    let mut heartbeat = tokio::time::interval(tokio::time::Duration::from_secs(10));
    heartbeat.tick().await;
    while cpu_ns.len() < 200 && tokio::time::Instant::now() < deadline {
        tokio::select! {
            _ = heartbeat.tick() => write.send(Message::Text("PING".into())).await?,
            message = read.next() => {
                let message = message.context("Polymarket stream ended")??;
                if let Message::Text(raw) = message {
                    if raw == "PONG" {
                        continue;
                    }
                    let start = Instant::now();
                    let changed = parse_and_apply(
                        &raw,
                        &yes_asset_id,
                        &no_asset_id,
                        &mut snapshot,
                    )?;
                    let elapsed = start.elapsed().as_nanos() as u64;
                    if changed {
                        cpu_ns.push(elapsed);
                        if snapshot.source_ts_ms > 0 {
                            network_age_ms.push(now_ms() as i64 - snapshot.source_ts_ms as i64);
                        }
                    }
                }
            }
            _ = tokio::time::sleep_until(deadline) => break,
        }
    }
    print_stats("live_polymarket_parse_book_cpu", "ns", &mut cpu_ns);
    print_signed_stats("live_polymarket_network_age", "ms", &mut network_age_ms);
    Ok(())
}

fn print_stats(name: &str, unit: &str, values: &mut [u64]) {
    values.sort_unstable();
    if values.is_empty() {
        println!("{name} count=0");
        return;
    }
    println!(
        "{name} count={} p50={}{} p95={}{} p99={}{} max={}{}",
        values.len(),
        percentile(values, 50),
        unit,
        percentile(values, 95),
        unit,
        percentile(values, 99),
        unit,
        values[values.len() - 1],
        unit,
    );
}

fn print_signed_stats(name: &str, unit: &str, values: &mut [i64]) {
    values.sort_unstable();
    if values.is_empty() {
        println!("{name} count=0");
        return;
    }
    println!(
        "{name} count={} p50={}{} p95={}{} p99={}{} min={}{} max={}{}",
        values.len(),
        signed_percentile(values, 50),
        unit,
        signed_percentile(values, 95),
        unit,
        signed_percentile(values, 99),
        unit,
        values[0],
        unit,
        values[values.len() - 1],
        unit,
    );
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    values[(values.len() - 1) * percentile / 100]
}

fn signed_percentile(values: &[i64], percentile: usize) -> i64 {
    values[(values.len() - 1) * percentile / 100]
}
