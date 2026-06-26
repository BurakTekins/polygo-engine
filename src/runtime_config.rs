use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::config::{RiskConfig, StrategyConfig};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JavaStrategyConfig {
    pub config_version: String,
    pub momentum_window_ms: u64,
    pub momentum_threshold_usd: f64,
    pub buy_yes_momentum_threshold_usd: f64,
    pub buy_no_momentum_threshold_usd: f64,
    pub execution_latency_ms: u64,
    pub hold_ms: u64,
    pub min_expected_price_move: f64,
    pub min_progress: f64,
    pub max_progress: f64,
    pub max_spread: f64,
    pub min_price: f64,
    pub max_price: f64,
    pub max_notional_usd: f64,
    pub max_shares: u64,
    pub up_outcome: String,
    pub down_outcome: String,
}

impl JavaStrategyConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.config_version.trim().is_empty() || self.config_version.len() > 128 {
            return Err("invalid_config_version");
        }
        if self.momentum_window_ms == 0
            || self.hold_ms == 0
            || !positive_finite(self.momentum_threshold_usd)
            || !positive_finite(self.buy_yes_momentum_threshold_usd)
            || !positive_finite(self.buy_no_momentum_threshold_usd)
            || !positive_finite(self.max_notional_usd)
            || self.max_shares == 0
        {
            return Err("invalid_strategy_limits");
        }
        if !self.min_expected_price_move.is_finite() || self.min_expected_price_move < 0.0 {
            return Err("invalid_min_expected_price_move");
        }
        if !self.min_progress.is_finite()
            || !self.max_progress.is_finite()
            || self.min_progress < 0.0
            || self.max_progress > 1.0
            || self.min_progress >= self.max_progress
        {
            return Err("invalid_progress_range");
        }
        if !self.max_spread.is_finite() || self.max_spread < 0.0 {
            return Err("invalid_max_spread");
        }
        if !self.min_price.is_finite()
            || !self.max_price.is_finite()
            || self.min_price < 0.0
            || self.max_price > 1.0
            || self.min_price >= self.max_price
        {
            return Err("invalid_price_range");
        }
        if self.up_outcome != "YES" || self.down_outcome != "NO" {
            return Err("invalid_outcome_mapping");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StrategySnapshot {
    pub generation: u64,
    pub momentum_window_ms: u64,
    pub momentum_threshold_usd: f64,
    pub buy_yes_momentum_threshold_usd: f64,
    pub buy_no_momentum_threshold_usd: f64,
    pub execution_latency_ms: u64,
    pub hold_ms: u64,
    pub min_expected_price_move: f64,
    pub min_progress: f64,
    pub max_progress: f64,
    pub max_spread: f64,
    pub min_price: f64,
    pub max_price: f64,
    pub max_notional_usd: f64,
    pub max_shares: u64,
    pub max_book_age_ms: u64,
}

#[derive(Debug)]
pub struct StrategyStore {
    sequence: AtomicU64,
    generation: AtomicU64,
    configured: AtomicBool,
    momentum_window_ms: AtomicU64,
    momentum_threshold_usd: AtomicU64,
    buy_yes_momentum_threshold_usd: AtomicU64,
    buy_no_momentum_threshold_usd: AtomicU64,
    execution_latency_ms: AtomicU64,
    hold_ms: AtomicU64,
    min_expected_price_move: AtomicU64,
    min_progress: AtomicU64,
    max_progress: AtomicU64,
    max_spread: AtomicU64,
    min_price: AtomicU64,
    max_price: AtomicU64,
    max_notional_usd: AtomicU64,
    max_shares: AtomicU64,
    max_book_age_ms: AtomicU64,
    config_version: RwLock<Option<String>>,
}

impl StrategyStore {
    pub fn new(defaults: &StrategyConfig, risk: &RiskConfig) -> Self {
        Self {
            sequence: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            configured: AtomicBool::new(false),
            momentum_window_ms: AtomicU64::new(defaults.momentum_window_ms),
            momentum_threshold_usd: AtomicU64::new(defaults.momentum_threshold_usd.to_bits()),
            buy_yes_momentum_threshold_usd: AtomicU64::new(
                defaults.buy_yes_momentum_threshold_usd.to_bits(),
            ),
            buy_no_momentum_threshold_usd: AtomicU64::new(
                defaults.buy_no_momentum_threshold_usd.to_bits(),
            ),
            execution_latency_ms: AtomicU64::new(defaults.execution_latency_ms),
            hold_ms: AtomicU64::new(defaults.hold_ms),
            min_expected_price_move: AtomicU64::new(defaults.min_expected_price_move.to_bits()),
            min_progress: AtomicU64::new(defaults.min_market_progress.to_bits()),
            max_progress: AtomicU64::new(defaults.max_market_progress.to_bits()),
            max_spread: AtomicU64::new(risk.max_spread.to_bits()),
            min_price: AtomicU64::new(0.05f64.to_bits()),
            max_price: AtomicU64::new(0.95f64.to_bits()),
            max_notional_usd: AtomicU64::new(risk.max_notional_usd.to_bits()),
            max_shares: AtomicU64::new(risk.max_shares),
            max_book_age_ms: AtomicU64::new(defaults.max_book_age_ms),
            config_version: RwLock::new(None),
        }
    }

    pub fn update(&self, config: &JavaStrategyConfig) -> Result<u64, &'static str> {
        config.validate()?;
        let mut version = self
            .config_version
            .write()
            .map_err(|_| "config_store_poisoned")?;
        self.sequence.fetch_add(1, Ordering::AcqRel);
        self.momentum_window_ms
            .store(config.momentum_window_ms, Ordering::Relaxed);
        self.momentum_threshold_usd
            .store(config.momentum_threshold_usd.to_bits(), Ordering::Relaxed);
        self.buy_yes_momentum_threshold_usd
            .store(config.buy_yes_momentum_threshold_usd.to_bits(), Ordering::Relaxed);
        self.buy_no_momentum_threshold_usd
            .store(config.buy_no_momentum_threshold_usd.to_bits(), Ordering::Relaxed);
        self.execution_latency_ms
            .store(config.execution_latency_ms, Ordering::Relaxed);
        self.hold_ms.store(config.hold_ms, Ordering::Relaxed);
        self.min_expected_price_move
            .store(config.min_expected_price_move.to_bits(), Ordering::Relaxed);
        self.min_progress
            .store(config.min_progress.to_bits(), Ordering::Relaxed);
        self.max_progress
            .store(config.max_progress.to_bits(), Ordering::Relaxed);
        self.max_spread
            .store(config.max_spread.to_bits(), Ordering::Relaxed);
        self.min_price
            .store(config.min_price.to_bits(), Ordering::Relaxed);
        self.max_price
            .store(config.max_price.to_bits(), Ordering::Relaxed);
        self.max_notional_usd
            .store(config.max_notional_usd.to_bits(), Ordering::Relaxed);
        self.max_shares.store(config.max_shares, Ordering::Relaxed);
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        *version = Some(config.config_version.clone());
        self.configured.store(true, Ordering::Release);
        self.sequence.fetch_add(1, Ordering::Release);
        Ok(generation)
    }

    #[inline(always)]
    pub fn load(&self) -> Option<StrategySnapshot> {
        if !self.configured.load(Ordering::Acquire) {
            return None;
        }
        loop {
            let before = self.sequence.load(Ordering::Acquire);
            if before & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }
            let snapshot = StrategySnapshot {
                generation: self.generation.load(Ordering::Relaxed),
                momentum_window_ms: self.momentum_window_ms.load(Ordering::Relaxed),
                momentum_threshold_usd: f64::from_bits(
                    self.momentum_threshold_usd.load(Ordering::Relaxed),
                ),
                buy_yes_momentum_threshold_usd: f64::from_bits(
                    self.buy_yes_momentum_threshold_usd.load(Ordering::Relaxed),
                ),
                buy_no_momentum_threshold_usd: f64::from_bits(
                    self.buy_no_momentum_threshold_usd.load(Ordering::Relaxed),
                ),
                execution_latency_ms: self.execution_latency_ms.load(Ordering::Relaxed),
                hold_ms: self.hold_ms.load(Ordering::Relaxed),
                min_expected_price_move: f64::from_bits(
                    self.min_expected_price_move.load(Ordering::Relaxed),
                ),
                min_progress: f64::from_bits(self.min_progress.load(Ordering::Relaxed)),
                max_progress: f64::from_bits(self.max_progress.load(Ordering::Relaxed)),
                max_spread: f64::from_bits(self.max_spread.load(Ordering::Relaxed)),
                min_price: f64::from_bits(self.min_price.load(Ordering::Relaxed)),
                max_price: f64::from_bits(self.max_price.load(Ordering::Relaxed)),
                max_notional_usd: f64::from_bits(self.max_notional_usd.load(Ordering::Relaxed)),
                max_shares: self.max_shares.load(Ordering::Relaxed),
                max_book_age_ms: self.max_book_age_ms.load(Ordering::Relaxed),
            };
            if before == self.sequence.load(Ordering::Acquire) {
                return Some(snapshot);
            }
        }
    }

    pub fn version(&self) -> Result<Option<String>, &'static str> {
        self.config_version
            .read()
            .map(|version| version.clone())
            .map_err(|_| "config_store_poisoned")
    }

    pub fn version_matches(&self, expected: &str) -> bool {
        self.config_version
            .read()
            .map(|version| version.as_deref() == Some(expected))
            .unwrap_or(false)
    }
}

fn positive_finite(value: f64) -> bool {
    value.is_finite() && value > 0.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_publishes_coherent_snapshot_and_version() {
        let defaults = StrategyConfig {
            momentum_window_ms: 100,
            momentum_threshold_usd: 8.0,
            buy_yes_momentum_threshold_usd: 12.0,
            buy_no_momentum_threshold_usd: 8.0,
            execution_latency_ms: 100,
            hold_ms: 5_000,
            min_expected_price_move: 0.05,
            min_market_progress: 0.05,
            max_market_progress: 0.9,
            max_book_age_ms: 300,
        };
        let risk = RiskConfig {
            max_spread: 0.02,
            max_notional_usd: 100.0,
            max_shares: 500,
            max_open_positions: 5,
            daily_loss_limit_usd: 500.0,
        };
        let store = StrategyStore::new(&defaults, &risk);
        assert!(store.load().is_none());
        let config = fixture();
        assert_eq!(store.update(&config), Ok(1));
        let snapshot = store.load().unwrap();
        assert_eq!(snapshot.momentum_window_ms, 100);
        assert_eq!(snapshot.max_shares, 500);
        assert!(store.version_matches("momentum-v1"));
    }

    fn fixture() -> JavaStrategyConfig {
        JavaStrategyConfig {
            config_version: "momentum-v1".into(),
            momentum_window_ms: 100,
            momentum_threshold_usd: 8.0,
            buy_yes_momentum_threshold_usd: 12.0,
            buy_no_momentum_threshold_usd: 8.0,
            execution_latency_ms: 100,
            hold_ms: 5_000,
            min_expected_price_move: 0.05,
            min_progress: 0.05,
            max_progress: 0.9,
            max_spread: 0.02,
            min_price: 0.05,
            max_price: 0.95,
            max_notional_usd: 100.0,
            max_shares: 500,
            up_outcome: "YES".into(),
            down_outcome: "NO".into(),
        }
    }
}
