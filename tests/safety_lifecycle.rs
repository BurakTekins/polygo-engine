use std::sync::Arc;
use polygo_engine::engine::state::EngineState;
use polygo_engine::health::HealthState;
use polygo_engine::market::{ActiveMarket, MarketClock};

#[test]
fn start_requires_market_readiness_and_stop_is_idempotent() {
    let health = HealthState::new();
    assert_eq!(health.start(), Err("market_not_ready"));
    assert!(!health.is_running());
    health.set_market_ready(true);
    assert_eq!(health.start(), Ok(()));
    assert!(health.is_running());
    health.stop();
    health.stop();
    assert!(!health.is_running());
    assert_eq!(health.snapshot().state, "stopped");
}

#[test]
fn audit_backpressure_blocks_restart_even_after_stop() {
    let health = HealthState::new();
    health.set_market_ready(true);
    health.start().unwrap();
    health.trip(1);
    assert!(!health.is_running());
    assert!(!health.accepting_commands());
    assert_eq!(health.snapshot().fault, "audit_backpressure");
    assert_eq!(health.start(), Err("audit_unhealthy"));
    health.stop();
    assert_eq!(health.start(), Err("audit_unhealthy"));
    assert!(!health.accepting_commands());
}

#[test]
fn execution_fault_prevents_start_and_is_visible_in_snapshot() {
    let health = HealthState::new();
    health.set_market_ready(true);
    health.start().unwrap();
    health.mark_binance(123);
    health.mark_book(456);
    health.trip(3);
    assert_eq!(health.start(), Err("faulted_restart_process"));
    let snapshot = health.snapshot();
    assert_eq!(snapshot.state, "faulted");
    assert_eq!(snapshot.fault, "execution_backpressure");
    assert_eq!(snapshot.last_binance_ms, 123);
    assert_eq!(snapshot.last_book_ms, 456);
    assert!(!health.accepting_commands());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_signals_cannot_reserve_a_second_position() {
    let state = Arc::new(EngineState::new(5, 100.0));
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let state = Arc::clone(&state);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            state.try_reserve()
        }));
    }
    let mut successes = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(()) => successes += 1,
            Err(reason) => assert_eq!(reason, "position_active"),
        }
    }
    assert_eq!(successes, 1);
    assert!(state.active_snapshot().active);
    state.release_reservation();
    assert_eq!(state.try_reserve(), Ok(()));
}

#[test]
fn market_window_includes_boundaries_and_rejects_outside_progress() {
    let clock = MarketClock::default();
    assert!(!clock.progress_allowed(1500, 0.1, 0.9));
    clock.update(&ActiveMarket {
        slug: "synthetic".into(), yes_asset_id: "yes".into(), no_asset_id: "no".into(),
        open_ts_ms: 1000, close_ts_ms: 2000,
    });
    assert!(clock.progress_allowed(1100, 0.1, 0.9));
    assert!(clock.progress_allowed(1900, 0.1, 0.9));
    for timestamp in [999, 1099, 1901, 2001] {
        assert!(!clock.progress_allowed(timestamp, 0.1, 0.9));
    }
}

#[test]
fn invalid_market_interval_never_allows_trading() {
    let clock = MarketClock::default();
    for close_ts_ms in [999, 1000] {
        clock.update(&ActiveMarket {
            slug: "synthetic".into(), yes_asset_id: "yes".into(), no_asset_id: "no".into(),
            open_ts_ms: 1000, close_ts_ms,
        });
        assert!(!clock.progress_allowed(1000, 0.0, 1.0));
    }
}
