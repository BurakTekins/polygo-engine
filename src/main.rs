use std::sync::Arc;

use polygo_engine::config::EngineConfig;
use polygo_engine::emitter::audit::AuditEmitter;
use polygo_engine::engine::state::EngineState;
use polygo_engine::executor::order::OrderSignal;
use polygo_engine::health::HealthState;
use polygo_engine::market::{ActiveMarket, MarketClock};
use polygo_engine::runtime_config::StrategyStore;
use polygo_engine::ws::binance::PriceTick;
use polygo_engine::ws::polymarket::BookState;
use tokio::sync::{mpsc, watch};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("polygo_engine=info".parse()?))
        .with_writer(non_blocking)
        .init();

    let config = Arc::new(EngineConfig::load("config.json")?);
    let health = Arc::new(HealthState::new());
    let market_clock = Arc::new(MarketClock::default());
    let book_state = Arc::new(BookState::default());
    let engine_state = Arc::new(EngineState::new(
        config.risk.max_open_positions,
        config.risk.daily_loss_limit_usd,
    ));
    let strategy_store = Arc::new(StrategyStore::new(&config.strategy, &config.risk));
    let audit = AuditEmitter::new(
        config.integration.java_audit_endpoint.clone(),
        Arc::clone(&health),
    );
    let (market_tx, market_rx) = watch::channel::<Option<ActiveMarket>>(None);
    let (binance_tx, binance_rx) = mpsc::channel::<PriceTick>(8_192);
    let (order_tx, order_rx) = mpsc::channel::<OrderSignal>(256);

    info!(mode = ?config.execution_mode, "Engine process started in stopped state");
    let market_handle = tokio::spawn(polygo_engine::market::run(
        config.market.clone(),
        market_tx,
        Arc::clone(&market_clock),
        Arc::clone(&health),
    ));
    let binance_handle = tokio::spawn(polygo_engine::ws::binance::run(
        binance_tx,
        Arc::clone(&health),
    ));
    let polymarket_handle = tokio::spawn(polygo_engine::ws::polymarket::run(
        market_rx,
        Arc::clone(&book_state),
        Arc::clone(&health),
    ));
    let decision_handle = tokio::spawn(polygo_engine::engine::decision::run(
        binance_rx,
        Arc::clone(&book_state),
        order_tx,
        Arc::clone(&engine_state),
        Arc::clone(&health),
        Arc::clone(&market_clock),
        Arc::clone(&strategy_store),
    ));
    let executor_handle = tokio::spawn(polygo_engine::executor::order::run(
        order_rx,
        Arc::clone(&book_state),
        Arc::clone(&engine_state),
        Arc::clone(&health),
        Arc::clone(&market_clock),
        Arc::clone(&strategy_store),
        audit,
    ));
    let control_handle = tokio::spawn(polygo_engine::control::run(
        Arc::clone(&config),
        Arc::clone(&health),
        Arc::clone(&strategy_store),
    ));
    let watchdog_handle = tokio::spawn(polygo_engine::control::watchdog(Arc::clone(&health)));

    tokio::select! {
        result = market_handle => error!(?result, "Market manager stopped"),
        result = binance_handle => error!(?result, "Binance task stopped"),
        result = polymarket_handle => error!(?result, "Polymarket task stopped"),
        result = decision_handle => error!(?result, "Decision task stopped"),
        result = executor_handle => error!(?result, "Executor task stopped"),
        result = control_handle => error!(?result, "Control task stopped"),
        result = watchdog_handle => error!(?result, "Watchdog task stopped"),
    }
    anyhow::bail!("critical engine task stopped")
}
