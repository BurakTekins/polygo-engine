use std::hint::black_box;
use std::time::{Duration, Instant};

use polygo_engine::config::EngineConfig;
use polygo_engine::emitter::audit::AuditEvent;
use polygo_engine::engine::state::EngineState;
use polygo_engine::engine::strategy::{MomentumStrategy, Strategy};
use polygo_engine::executor::order::{revalidate_entry, OrderSide, OrderSignal};
use polygo_engine::market::{ActiveMarket, MarketClock};
use polygo_engine::runtime_config::{JavaStrategyConfig, StrategyStore};
use polygo_engine::ws::binance::{parse_message, PriceTick};
use polygo_engine::ws::polymarket::{parse_and_apply, BookSnapshot, BookState, OutcomeBook};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc;

const BINANCE_MESSAGE: &str = r#"{"e":"aggTrade","E":1710000000123,"s":"BTCUSDT","a":123,"p":"68421.12000000","q":"0.00100000","f":1,"l":1,"T":1710000000120,"m":false}"#;
const POLYMARKET_MESSAGE: &str = r#"{"market":"0x1","price_changes":[{"asset_id":"yes-token","price":"0.51","size":"10","side":"BUY","hash":"0x2","best_bid":"0.50","best_ask":"0.52"},{"asset_id":"no-token","price":"0.49","size":"8","side":"SELL","hash":"0x3","best_bid":"0.48","best_ask":"0.50"}],"timestamp":"1710000000123","event_type":"price_change"}"#;

#[derive(Deserialize)]
struct OwnedBinanceTick {
    #[serde(rename = "T")]
    trade_time_ms: u64,
    #[serde(rename = "p")]
    price: String,
}

fn main() {
    let config = EngineConfig::load("config.json").unwrap();
    let strategy_config = config.strategy.clone();
    let risk_config = config.risk.clone();
    let strategy_store = StrategyStore::new(&strategy_config, &risk_config);
    strategy_store.update(&java_config()).unwrap();
    let runtime_config = strategy_store.load().unwrap();

    measure("binance_owned_string_parse_baseline", 20_000, 64, || {
        let tick: OwnedBinanceTick = serde_json::from_str(BINANCE_MESSAGE).unwrap();
        black_box((tick.trade_time_ms, tick.price.parse::<f64>().unwrap()));
    });
    measure("binance_production_parse", 20_000, 64, || {
        black_box(parse_message(BINANCE_MESSAGE).unwrap());
    });
    measure("polymarket_value_parse_update_baseline", 20_000, 64, || {
        let value: Value = serde_json::from_str(POLYMARKET_MESSAGE).unwrap();
        let changes = value["price_changes"].as_array().unwrap();
        for change in changes {
            black_box((
                change["asset_id"].as_str().unwrap(),
                change["best_bid"].as_str().unwrap().parse::<f64>().unwrap(),
                change["best_ask"].as_str().unwrap().parse::<f64>().unwrap(),
            ));
        }
    });
    let mut typed_snapshot = BookSnapshot::default();
    measure("polymarket_typed_borrowed_parse_update", 20_000, 64, || {
        black_box(
            parse_and_apply(
                POLYMARKET_MESSAGE,
                "yes-token",
                "no-token",
                &mut typed_snapshot,
            )
            .unwrap(),
        );
    });

    let mut strategy = MomentumStrategy::new(&strategy_config);
    let mut strategy_time = 1_000_u64;
    let mut strategy_up = false;
    measure("momentum_decision", 20_000, 64, || {
        strategy_time += 100;
        strategy_up = !strategy_up;
        let price = if strategy_up { 68_009.0 } else { 68_000.0 };
        black_box(strategy.on_binance_tick(PriceTick::new(price, strategy_time), strategy_time));
    });

    let (clock, book_state, now) = fixture();
    measure("polymarket_atomic_book_update", 20_000, 64, || {
        book_state.store(book_snapshot(now));
    });
    measure("execution_revalidation", 20_000, 64, || {
        black_box(revalidate_entry(
            book_state.load(),
            now,
            &clock,
            &runtime_config,
            OrderSide::BuyYes,
        ));
    });

    let (audit_tx, mut audit_rx) = mpsc::channel(1_024);
    let audit_event = AuditEvent::SignalAccepted {
        signal_ts_ms: now,
        side: "BUY_YES",
        momentum_usd: 9.0,
        binance_price: 68_009.0,
    };
    measure("audit_try_enqueue", 20_000, 64, || {
        audit_tx.try_send(audit_event).unwrap();
        black_box(audit_rx.try_recv().unwrap());
    });

    let engine_state = EngineState::new(5, 500.0);
    let (order_tx, mut order_rx) = mpsc::channel(256);
    let mut full_strategy = MomentumStrategy::new(&strategy_config);
    let mut full_time = now;
    let mut full_up = false;
    measure("synthetic_hot_path_total", 20_000, 64, || {
        let parsed = parse_message(BINANCE_MESSAGE).unwrap();
        full_time += 100;
        full_up = !full_up;
        let tick = PriceTick::new(
            if full_up {
                parsed.price + 9.0
            } else {
                parsed.price
            },
            full_time,
        );
        if let Some(candidate) = full_strategy.on_binance_tick(tick, full_time) {
            let book = book_state.load();
            if revalidate_entry(book, now, &clock, &runtime_config, candidate.side).is_some()
                && engine_state.try_reserve().is_ok()
            {
                order_tx
                    .try_send(OrderSignal::new(
                        candidate.side,
                        candidate.momentum_usd,
                        full_time,
                        tick.price,
                        tokio::time::Instant::now(),
                        runtime_config.generation,
                        runtime_config.hold_ms,
                    ))
                    .unwrap();
                black_box(order_rx.try_recv().unwrap());
                engine_state.release_reservation();
            }
        }
    });

    channel_load_test();
}

fn fixture() -> (MarketClock, BookState, u64) {
    let now = 1_500_u64;
    let clock = MarketClock::default();
    clock.update(&ActiveMarket {
        slug: "fixture".into(),
        yes_asset_id: "yes-token".into(),
        no_asset_id: "no-token".into(),
        open_ts_ms: 1_000,
        close_ts_ms: 2_000,
    });
    assert!(clock.progress_allowed(now, 0.05, 0.90));
    let state = BookState::default();
    state.store(book_snapshot(now));
    (clock, state, now)
}

fn java_config() -> JavaStrategyConfig {
    JavaStrategyConfig {
        config_version: "momentum-v1".into(),
        momentum_window_ms: 100,
        momentum_threshold_usd: 8.0,
        execution_latency_ms: 100,
        hold_ms: 5_000,
        min_progress: 0.05,
        max_progress: 0.90,
        max_spread: 0.02,
        max_notional_usd: 100.0,
        max_shares: 500,
        up_outcome: "YES".into(),
        down_outcome: "NO".into(),
    }
}

fn book_snapshot(now: u64) -> BookSnapshot {
    BookSnapshot {
        yes: OutcomeBook {
            bid: Some(0.50),
            ask: Some(0.52),
        },
        no: OutcomeBook {
            bid: Some(0.48),
            ask: Some(0.50),
        },
        received_at_ms: now,
        source_ts_ms: now,
    }
}

fn channel_load_test() {
    const CAPACITY: usize = 8_192;
    const BURST: usize = 4_096;
    const CYCLES: usize = 1_000;
    let (tx, mut rx) = mpsc::channel(CAPACITY);
    let tick = PriceTick::new(68_000.0, 1_000);
    let start = Instant::now();
    let mut sent = 0_usize;
    let mut received = 0_usize;
    for _ in 0..CYCLES {
        for _ in 0..BURST {
            tx.try_send(tick).unwrap();
            sent += 1;
        }
        for _ in 0..BURST {
            black_box(rx.try_recv().unwrap());
            received += 1;
        }
    }
    let overload_detected = (0..=CAPACITY).any(|_| tx.try_send(tick).is_err());
    println!(
        "channel_load capacity={CAPACITY} burst={BURST} sent={sent} received={received} lost={} throughput={:.0}/s overload_detected={overload_detected}",
        sent - received,
        sent as f64 / start.elapsed().as_secs_f64(),
    );
}

fn measure(name: &str, samples: usize, batch: usize, mut operation: impl FnMut()) {
    for _ in 0..100_000 {
        operation();
    }
    let mut values = Vec::with_capacity(samples);
    let total_start = Instant::now();
    for _ in 0..samples {
        let start = Instant::now();
        for _ in 0..batch {
            operation();
        }
        values.push(start.elapsed().as_nanos() as u64 / batch as u64);
    }
    let elapsed = total_start.elapsed();
    values.sort_unstable();
    println!(
        "{name} p50={}ns p95={}ns p99={}ns max={}ns throughput={:.0}/s",
        percentile(&values, 50),
        percentile(&values, 95),
        percentile(&values, 99),
        values[values.len() - 1],
        (samples * batch) as f64 / duration_secs(elapsed),
    );
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    values[(values.len() - 1) * percentile / 100]
}

fn duration_secs(duration: Duration) -> f64 {
    duration.as_secs_f64().max(f64::EPSILON)
}
