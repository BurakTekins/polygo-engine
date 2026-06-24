use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::config::MarketConfig;
use crate::health::HealthState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveMarket {
    pub slug: String,
    pub yes_asset_id: String,
    pub no_asset_id: String,
    pub open_ts_ms: u64,
    pub close_ts_ms: u64,
}

#[derive(Debug, Default)]
pub struct MarketClock {
    open_ts_ms: AtomicU64,
    close_ts_ms: AtomicU64,
    configured: AtomicBool,
}

impl MarketClock {
    pub fn update(&self, market: &ActiveMarket) {
        self.open_ts_ms.store(market.open_ts_ms, Ordering::Release);
        self.close_ts_ms
            .store(market.close_ts_ms, Ordering::Release);
        self.configured.store(true, Ordering::Release);
    }

    #[inline(always)]
    pub fn progress_allowed(&self, now_ms: u64, min_progress: f64, max_progress: f64) -> bool {
        if !self.configured.load(Ordering::Acquire) {
            return false;
        }
        let open = self.open_ts_ms.load(Ordering::Acquire);
        let close = self.close_ts_ms.load(Ordering::Acquire);
        if now_ms < open || now_ms > close || open >= close {
            return false;
        }
        let progress = (now_ms - open) as f64 / (close - open) as f64;
        progress >= min_progress && progress <= max_progress
    }
}

#[derive(Debug, Deserialize)]
struct GammaEvent {
    markets: Vec<GammaMarket>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    #[serde(deserialize_with = "json_string_array")]
    outcomes: Vec<String>,
    #[serde(deserialize_with = "json_string_array")]
    clob_token_ids: Vec<String>,
    active: bool,
    closed: bool,
    enable_order_book: bool,
}

pub async fn run(
    config: MarketConfig,
    market_tx: watch::Sender<Option<ActiveMarket>>,
    clock: Arc<MarketClock>,
    health: Arc<HealthState>,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()?;
    let mut current_slug = String::new();
    loop {
        let epoch_seconds = now_ms() / 1_000;
        let window_start = epoch_seconds / config.interval_seconds * config.interval_seconds;
        let slug = format!("{}-{window_start}", config.slug_prefix);
        if slug != current_slug {
            match discover(&client, &config, &slug, window_start).await {
                Ok(market) => {
                    health.set_market_ready(false);
                    clock.update(&market);
                    market_tx.send_replace(Some(market));
                    current_slug = slug;
                    info!(%current_slug, "Active market discovered by outcome label");
                }
                Err(error) => {
                    health.set_market_ready(false);
                    warn!(%error, %slug, "Market discovery failed");
                }
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(config.discovery_poll_ms)).await;
    }
}

pub async fn discover(
    client: &reqwest::Client,
    config: &MarketConfig,
    slug: &str,
    window_start_seconds: u64,
) -> Result<ActiveMarket> {
    let url = format!(
        "{}/events/slug/{slug}",
        config.gamma_base_url.trim_end_matches('/')
    );
    let event: GammaEvent = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("invalid Gamma event payload")?;
    let market = event
        .markets
        .into_iter()
        .find(|market| market.active && !market.closed && market.enable_order_book)
        .context("Gamma event has no active orderbook market")?;
    if market.outcomes.len() != market.clob_token_ids.len() {
        anyhow::bail!("Gamma outcomes/token IDs length mismatch");
    }
    let mut yes_asset_id = None;
    let mut no_asset_id = None;
    for (label, token_id) in market.outcomes.iter().zip(market.clob_token_ids) {
        if label.eq_ignore_ascii_case("up") || label.eq_ignore_ascii_case("yes") {
            yes_asset_id = Some(token_id);
        } else if label.eq_ignore_ascii_case("down") || label.eq_ignore_ascii_case("no") {
            no_asset_id = Some(token_id);
        }
    }
    let yes_asset_id = yes_asset_id.context("Gamma market has no UP/YES outcome")?;
    let no_asset_id = no_asset_id.context("Gamma market has no DOWN/NO outcome")?;
    Ok(ActiveMarket {
        slug: slug.to_owned(),
        yes_asset_id,
        no_asset_id,
        open_ts_ms: window_start_seconds * 1_000,
        close_ts_ms: (window_start_seconds + config.interval_seconds) * 1_000,
    })
}

fn json_string_array<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringArray {
        Json(String),
        Array(Vec<String>),
    }
    match StringArray::deserialize(deserializer)? {
        StringArray::Json(value) => serde_json::from_str(&value).map_err(serde::de::Error::custom),
        StringArray::Array(value) => Ok(value),
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_enforces_progress() {
        let clock = MarketClock::default();
        clock.update(&ActiveMarket {
            slug: "test".into(),
            yes_asset_id: "up".into(),
            no_asset_id: "down".into(),
            open_ts_ms: 1_000,
            close_ts_ms: 2_000,
        });
        assert!(!clock.progress_allowed(1_049, 0.05, 0.90));
        assert!(clock.progress_allowed(1_050, 0.05, 0.90));
        assert!(clock.progress_allowed(1_900, 0.05, 0.90));
        assert!(!clock.progress_allowed(1_901, 0.05, 0.90));
    }
}
