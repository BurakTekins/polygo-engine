use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EngineConfig {
    pub execution_mode: ExecutionMode,
    pub market: MarketConfig,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    pub integration: IntegrationConfig,
    pub control: ControlConfig,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    DryRun,
    Live,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MarketConfig {
    pub gamma_base_url: String,
    pub slug_prefix: String,
    pub interval_seconds: u64,
    pub discovery_poll_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StrategyConfig {
    pub momentum_window_ms: u64,
    pub momentum_threshold_usd: f64,
    pub execution_latency_ms: u64,
    pub hold_ms: u64,
    pub min_market_progress: f64,
    pub max_market_progress: f64,
    pub max_book_age_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RiskConfig {
    pub max_spread: f64,
    pub max_notional_usd: f64,
    pub max_shares: u64,
    pub max_open_positions: usize,
    pub daily_loss_limit_usd: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IntegrationConfig {
    pub java_audit_endpoint: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ControlConfig {
    pub bind_addr: String,
}

impl EngineConfig {
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read engine config: {path}"))?;
        let config: Self =
            serde_json::from_str(&raw).with_context(|| format!("invalid engine config: {path}"))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.execution_mode == ExecutionMode::Live {
            bail!("live execution is disabled: no authenticated order adapter is installed");
        }
        if self.market.gamma_base_url.is_empty()
            || self.market.slug_prefix.is_empty()
            || self.market.interval_seconds == 0
            || self.market.discovery_poll_ms == 0
        {
            bail!("invalid market discovery config");
        }
        let strategy = &self.strategy;
        if strategy.momentum_window_ms == 0
            || strategy.momentum_threshold_usd <= 0.0
            || strategy.hold_ms == 0
            || !(0.0..1.0).contains(&strategy.min_market_progress)
            || !(0.0..=1.0).contains(&strategy.max_market_progress)
            || strategy.min_market_progress >= strategy.max_market_progress
        {
            bail!("invalid strategy limits");
        }
        let risk = &self.risk;
        if risk.max_spread <= 0.0
            || risk.max_notional_usd <= 0.0
            || risk.max_shares == 0
            || risk.max_open_positions == 0
            || risk.daily_loss_limit_usd <= 0.0
        {
            bail!("invalid risk limits");
        }
        if self.control.bind_addr.is_empty() {
            bail!("invalid control config");
        }
        Ok(())
    }
}
