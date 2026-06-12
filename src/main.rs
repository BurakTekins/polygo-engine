mod ws;
mod engine;
mod executor;
mod emitter;

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use tokio::sync::{mpsc, watch, RwLock};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use ws::binance::PriceTick;
use ws::polymarket::ChainlinkTick;
use executor::order::OrderSignal;
use engine::state::EngineState;
use emitter::audit::AuditEmitter;

fn init_arb_log() {
    let path = "arb_opportunities.csv";

    if !Path::new(path).exists() {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("Failed to create arb log");

        writeln!(
            file,
            "timestamp,binance_price,market_price,edge,direction,lag_ms"
        )
            .expect("Failed to write csv header");
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Non-blocking logs
    let (non_blocking, _guard) =
        tracing_appender::non_blocking(std::io::stdout());

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("polygo_engine=info".parse()?),
        )
        .with_writer(non_blocking)
        .init();

    info!("Engine starting...");

    // CSV init
    init_arb_log();

    // Shared state
    let engine_state =
        Arc::new(RwLock::new(EngineState::new(5, 500.0)));

    let audit_emitter = Arc::new(
        AuditEmitter::new(
            "http://localhost:8080/api/audit".to_string()
        )
    );

    // Channels
    let (binance_tx, binance_rx) =
        watch::channel::<Option<PriceTick>>(None);

    let (chainlink_tx, chainlink_rx) =
        watch::channel::<Option<ChainlinkTick>>(None);

    let (order_tx, order_rx) =
        mpsc::channel::<OrderSignal>(100);

    // Tasks
    let binance_handle =
        tokio::spawn(ws::binance::run(binance_tx));

    let polymarket_handle =
        tokio::spawn(ws::polymarket::run(chainlink_tx));

    let decision_handle = tokio::spawn(
        engine::decision::run(
            binance_rx,
            chainlink_rx,
            order_tx,
            Arc::clone(&engine_state),
        )
    );

    let executor_handle = tokio::spawn(
        executor::order::run(
            order_rx,
            Arc::clone(&engine_state),
            Arc::clone(&audit_emitter),
        )
    );

    // Fail-fast
    tokio::select! {
        res = binance_handle =>
            error!("Binance WS task died: {:?}", res),

        res = polymarket_handle =>
            error!("Polymarket WS task died: {:?}", res),

        res = decision_handle =>
            error!("Decision engine task died: {:?}", res),

        res = executor_handle =>
            error!("Order executor task died: {:?}", res),
    }

    error!("Critical task failed — shutting down engine");

    Ok(())
}