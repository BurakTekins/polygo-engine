use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::watch;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, protocol::Message},
};
use tracing::{error, info, warn};

use crate::health::HealthState;
use crate::market::{now_ms, ActiveMarket};

const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct OutcomeBook {
    pub bid: Option<f64>,
    pub ask: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct BookSnapshot {
    pub yes: OutcomeBook,
    pub no: OutcomeBook,
    pub received_at_ms: u64,
    pub source_ts_ms: u64,
}

#[derive(Debug, Default)]
pub struct BookState {
    sequence: AtomicU64,
    yes_bid: AtomicU64,
    yes_ask: AtomicU64,
    no_bid: AtomicU64,
    no_ask: AtomicU64,
    received_at_ms: AtomicU64,
    source_ts_ms: AtomicU64,
}

impl BookState {
    #[inline(always)]
    pub fn store(&self, snapshot: BookSnapshot) {
        self.sequence.fetch_add(1, Ordering::AcqRel);
        self.yes_bid
            .store(option_to_bits(snapshot.yes.bid), Ordering::Relaxed);
        self.yes_ask
            .store(option_to_bits(snapshot.yes.ask), Ordering::Relaxed);
        self.no_bid
            .store(option_to_bits(snapshot.no.bid), Ordering::Relaxed);
        self.no_ask
            .store(option_to_bits(snapshot.no.ask), Ordering::Relaxed);
        self.received_at_ms
            .store(snapshot.received_at_ms, Ordering::Relaxed);
        self.source_ts_ms
            .store(snapshot.source_ts_ms, Ordering::Relaxed);
        self.sequence.fetch_add(1, Ordering::Release);
    }

    #[inline(always)]
    pub fn load(&self) -> BookSnapshot {
        loop {
            let before = self.sequence.load(Ordering::Acquire);
            if before & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }
            let snapshot = BookSnapshot {
                yes: OutcomeBook {
                    bid: bits_to_option(self.yes_bid.load(Ordering::Relaxed)),
                    ask: bits_to_option(self.yes_ask.load(Ordering::Relaxed)),
                },
                no: OutcomeBook {
                    bid: bits_to_option(self.no_bid.load(Ordering::Relaxed)),
                    ask: bits_to_option(self.no_ask.load(Ordering::Relaxed)),
                },
                received_at_ms: self.received_at_ms.load(Ordering::Relaxed),
                source_ts_ms: self.source_ts_ms.load(Ordering::Relaxed),
            };
            if before == self.sequence.load(Ordering::Acquire) {
                return snapshot;
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "event_type")]
enum MarketEvent<'a> {
    #[serde(rename = "book")]
    Book {
        #[serde(borrow)]
        asset_id: &'a str,
        #[serde(borrow)]
        bids: Vec<PriceLevel<'a>>,
        #[serde(borrow)]
        asks: Vec<PriceLevel<'a>>,
        #[serde(default, borrow)]
        timestamp: Option<WireU64<'a>>,
    },
    #[serde(rename = "price_change")]
    PriceChange {
        #[serde(borrow)]
        price_changes: Vec<PriceChange<'a>>,
        #[serde(default, borrow)]
        timestamp: Option<WireU64<'a>>,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct PriceLevel<'a> {
    #[serde(borrow)]
    price: &'a str,
}

#[derive(Debug, Deserialize)]
struct PriceChange<'a> {
    #[serde(borrow)]
    asset_id: &'a str,
    #[serde(default, borrow)]
    best_bid: Option<&'a str>,
    #[serde(default, borrow)]
    best_ask: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum WireU64<'a> {
    Number(u64),
    Text(#[serde(borrow)] &'a str),
}

impl WireU64<'_> {
    fn get(&self) -> Option<u64> {
        match self {
            Self::Number(value) => Some(*value),
            Self::Text(value) => value.parse().ok(),
        }
    }
}

pub async fn run(
    mut market_rx: watch::Receiver<Option<ActiveMarket>>,
    book_state: Arc<BookState>,
    health: Arc<HealthState>,
) -> Result<()> {
    loop {
        let market = loop {
            if let Some(market) = market_rx.borrow().clone() {
                break market;
            }
            market_rx.changed().await?;
        };
        let mut snapshot = BookSnapshot::default();
        let subscription = serde_json::json!({
            "type": "market",
            "assets_ids": [&market.yes_asset_id, &market.no_asset_id]
        })
        .to_string();
        let mut request = POLYMARKET_WS_URL.into_client_request()?;
        request
            .headers_mut()
            .insert("Origin", "https://polymarket.com".parse()?);
        info!(slug = %market.slug, "Connecting to Polymarket top-of-book stream");
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        write.send(Message::Text(subscription)).await?;
        let mut heartbeat = tokio::time::interval(tokio::time::Duration::from_secs(10));
        heartbeat.tick().await;

        loop {
            tokio::select! {
                changed = market_rx.changed() => {
                    changed?;
                    health.set_market_ready(false);
                    break;
                }
                _ = heartbeat.tick() => {
                    write.send(Message::Text("PING".into())).await?;
                }
                message = read.next() => {
                    let Some(message) = message else { break; };
                    match message {
                        Ok(Message::Text(text)) if text == "PONG" => {}
                        Ok(Message::Text(text)) => {
                            let received_at_ms = now_ms();
                            match parse_and_apply(
                                &text,
                                &market.yes_asset_id,
                                &market.no_asset_id,
                                &mut snapshot,
                            ) {
                                Ok(true) => {
                                    snapshot.received_at_ms = received_at_ms;
                                    book_state.store(snapshot);
                                    health.mark_book(received_at_ms);
                                    if snapshot.yes.bid.is_some()
                                        && snapshot.yes.ask.is_some()
                                        && snapshot.no.bid.is_some()
                                        && snapshot.no.ask.is_some()
                                    {
                                        health.set_market_ready(true);
                                    }
                                }
                                Ok(false) => {}
                                Err(error) => warn!(%error, "Invalid Polymarket payload"),
                            }
                        }
                        Ok(Message::Ping(payload)) => write.send(Message::Pong(payload)).await?,
                        Ok(Message::Close(_)) => break,
                        Ok(_) => {}
                        Err(error) => {
                            error!(%error, "Polymarket websocket error");
                            break;
                        }
                    }
                }
            }
        }
        health.set_market_ready(false);
        tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
    }
}

#[inline]
pub fn parse_and_apply(
    raw: &str,
    yes_asset_id: &str,
    no_asset_id: &str,
    snapshot: &mut BookSnapshot,
) -> Result<bool> {
    let mut changed = false;
    if raw.as_bytes().first() == Some(&b'[') {
        let events: Vec<MarketEvent<'_>> = serde_json::from_str(raw)?;
        for event in events {
            changed |= apply_event(event, yes_asset_id, no_asset_id, snapshot);
        }
    } else {
        let event: MarketEvent<'_> = serde_json::from_str(raw)?;
        changed = apply_event(event, yes_asset_id, no_asset_id, snapshot);
    }
    Ok(changed)
}

fn apply_event(
    event: MarketEvent<'_>,
    yes_asset_id: &str,
    no_asset_id: &str,
    snapshot: &mut BookSnapshot,
) -> bool {
    match event {
        MarketEvent::Book {
            asset_id,
            bids,
            asks,
            timestamp,
        } => {
            let Some(book) = select_book(asset_id, yes_asset_id, no_asset_id, snapshot) else {
                return false;
            };
            book.bid = best_price(&bids, f64::max);
            book.ask = best_price(&asks, f64::min);
            if let Some(timestamp) = timestamp.and_then(|value| value.get()) {
                snapshot.source_ts_ms = timestamp;
            }
            true
        }
        MarketEvent::PriceChange {
            price_changes,
            timestamp,
        } => {
            let mut changed = false;
            for change in price_changes {
                let Some(book) = select_book(change.asset_id, yes_asset_id, no_asset_id, snapshot)
                else {
                    continue;
                };
                if let Some(bid) = change.best_bid {
                    book.bid = parse_price(bid);
                }
                if let Some(ask) = change.best_ask {
                    book.ask = parse_price(ask);
                }
                changed = true;
            }
            if let Some(timestamp) = timestamp.and_then(|value| value.get()) {
                snapshot.source_ts_ms = timestamp;
            }
            changed
        }
        MarketEvent::Other => false,
    }
}

fn select_book<'a>(
    asset_id: &str,
    yes_asset_id: &str,
    no_asset_id: &str,
    snapshot: &'a mut BookSnapshot,
) -> Option<&'a mut OutcomeBook> {
    if asset_id == yes_asset_id {
        Some(&mut snapshot.yes)
    } else if asset_id == no_asset_id {
        Some(&mut snapshot.no)
    } else {
        None
    }
}

fn best_price(levels: &[PriceLevel<'_>], select: fn(f64, f64) -> f64) -> Option<f64> {
    levels
        .iter()
        .filter_map(|level| parse_price(level.price))
        .reduce(select)
}

fn parse_price(value: &str) -> Option<f64> {
    value
        .parse()
        .ok()
        .filter(|price| (0.0..=1.0).contains(price))
}

fn option_to_bits(value: Option<f64>) -> u64 {
    value.unwrap_or(f64::NAN).to_bits()
}

fn bits_to_option(value: u64) -> Option<f64> {
    let value = f64::from_bits(value);
    (!value.is_nan()).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_both_outcomes_by_asset_id() {
        let payload = r#"[{"event_type":"book","asset_id":"yes","bids":[{"price":"0.40"},{"price":"0.42"}],"asks":[{"price":"0.47"},{"price":"0.45"}],"timestamp":"1000"},{"event_type":"book","asset_id":"no","bids":[{"price":"0.53"}],"asks":[{"price":"0.57"}],"timestamp":"1001"}]"#;
        let mut snapshot = BookSnapshot::default();
        assert!(parse_and_apply(payload, "yes", "no", &mut snapshot).unwrap());
        assert_eq!(
            snapshot.yes,
            OutcomeBook {
                bid: Some(0.42),
                ask: Some(0.45)
            }
        );
        assert_eq!(
            snapshot.no,
            OutcomeBook {
                bid: Some(0.53),
                ask: Some(0.57)
            }
        );
        assert_eq!(snapshot.source_ts_ms, 1001);
    }

    #[test]
    fn atomic_snapshot_is_coherent() {
        let state = BookState::default();
        let snapshot = BookSnapshot {
            yes: OutcomeBook {
                bid: Some(0.4),
                ask: Some(0.5),
            },
            no: OutcomeBook {
                bid: Some(0.5),
                ask: Some(0.6),
            },
            received_at_ms: 10,
            source_ts_ms: 9,
        };
        state.store(snapshot);
        assert_eq!(state.load(), snapshot);
    }
}
