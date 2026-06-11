mod ws;
mod engine;
mod executor;
mod emitter;

use std::sync::Arc;
use tokio::sync::{mpsc, watch, RwLock};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use ws::binance::PriceTick;
use ws::polymarket::ChainlinkTick;
use executor::order::OrderSignal;
use engine::state::EngineState;
use emitter::audit::AuditEmitter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Non-blocking log ayarı
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive("polygo_engine=info".parse()?))
        .with_writer(non_blocking)
        .init();

    info!("Engine starting...");

    // 1. Paylaşılan state ve emitter
    let engine_state = Arc::new(RwLock::new(EngineState::new(5, 500.0)));
    let audit_emitter = Arc::new(AuditEmitter::new("http://localhost:8080/api/audit".to_string()));

    // 2. Kanallar
    let (binance_tx, binance_rx) = watch::channel::<Option<PriceTick>>(None);
    let (chainlink_tx, chainlink_rx) = watch::channel::<Option<ChainlinkTick>>(None);
    let (order_tx, order_rx) = mpsc::channel::<OrderSignal>(100);

    // 3. Task Başlatıcılar
    let binance_handle = tokio::spawn(ws::binance::run(binance_tx));
    let polymarket_handle = tokio::spawn(ws::polymarket::run(chainlink_tx));

    // Karar motoru: 4 parametre
    let decision_handle = tokio::spawn(engine::decision::run(
        binance_rx,
        chainlink_rx,
        order_tx,
        Arc::clone(&engine_state),
    ));

    // Executor: 3 parametre
    let executor_handle = tokio::spawn(executor::order::run(
        order_rx,
        Arc::clone(&engine_state),
        Arc::clone(&audit_emitter),
    ));

    // Fail-Fast izleme
    tokio::select! {
        res = binance_handle => error!("Binance WS task died: {:?}", res),
        res = polymarket_handle => error!("Polymarket RTDS task died: {:?}", res),
        res = decision_handle => error!("Decision engine task died: {:?}", res),
        res = executor_handle => error!("Order executor task failed: {:?}", res),
    }

    error!("Critical task failed — shutting down engine");
    Ok(())
}