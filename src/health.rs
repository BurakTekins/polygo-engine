use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};

use serde::Serialize;

const STOPPED: u8 = 0;
const RUNNING: u8 = 1;
const FAULTED: u8 = 2;

#[derive(Debug)]
pub struct HealthState {
    state: AtomicU8,
    market_ready: AtomicBool,
    audit_healthy: AtomicBool,
    lease_active: AtomicBool,
    lease_timeout_ms: AtomicU64,
    last_heartbeat_ms: AtomicU64,
    last_binance_ms: AtomicU64,
    last_book_ms: AtomicU64,
    fault_code: AtomicU8,
}

#[derive(Debug, Serialize)]
pub struct HealthSnapshot {
    pub state: &'static str,
    pub market_ready: bool,
    pub audit_healthy: bool,
    pub last_heartbeat_ms: u64,
    pub last_binance_ms: u64,
    pub last_book_ms: u64,
    pub fault: &'static str,
    pub live_execution_enabled: bool,
}

impl HealthState {
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(STOPPED),
            market_ready: AtomicBool::new(false),
            audit_healthy: AtomicBool::new(true),
            lease_active: AtomicBool::new(false),
            lease_timeout_ms: AtomicU64::new(0),
            last_heartbeat_ms: AtomicU64::new(0),
            last_binance_ms: AtomicU64::new(0),
            last_book_ms: AtomicU64::new(0),
            fault_code: AtomicU8::new(0),
        }
    }

    #[inline(always)]
    pub fn is_running(&self) -> bool {
        self.state.load(Ordering::Acquire) == RUNNING
    }

    pub fn start(&self, now_ms: u64, lease_timeout_ms: u64) -> Result<(), &'static str> {
        if !self.market_ready.load(Ordering::Acquire) {
            return Err("market_not_ready");
        }
        if !self.audit_healthy.load(Ordering::Acquire) {
            return Err("audit_unhealthy");
        }
        if self.state.load(Ordering::Acquire) == FAULTED {
            return Err("faulted_restart_process");
        }
        if lease_timeout_ms == 0 {
            return Err("invalid_lease_timeout");
        }
        self.lease_timeout_ms
            .store(lease_timeout_ms, Ordering::Release);
        self.last_heartbeat_ms.store(now_ms, Ordering::Release);
        self.lease_active.store(true, Ordering::Release);
        self.state.store(RUNNING, Ordering::Release);
        Ok(())
    }

    pub fn stop(&self) {
        self.lease_active.store(false, Ordering::Release);
        self.state.store(STOPPED, Ordering::Release);
    }

    pub fn heartbeat(&self, now_ms: u64) -> Result<(), &'static str> {
        if !self.is_running() || !self.lease_active.load(Ordering::Acquire) {
            return Err("engine_not_running");
        }
        self.last_heartbeat_ms.store(now_ms, Ordering::Release);
        Ok(())
    }

    pub fn trip(&self, code: u8) {
        if code == 1 {
            self.audit_healthy.store(false, Ordering::Release);
        }
        self.lease_active.store(false, Ordering::Release);
        self.fault_code.store(code, Ordering::Release);
        self.state.store(FAULTED, Ordering::Release);
    }

    pub fn set_market_ready(&self, ready: bool) {
        self.market_ready.store(ready, Ordering::Release);
    }

    #[inline(always)]
    pub fn mark_binance(&self, now_ms: u64) {
        self.last_binance_ms.store(now_ms, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn mark_book(&self, now_ms: u64) {
        self.last_book_ms.store(now_ms, Ordering::Relaxed);
    }

    pub fn heartbeat_expired(&self, now_ms: u64) -> bool {
        self.is_running()
            && self.lease_active.load(Ordering::Acquire)
            && now_ms.saturating_sub(self.last_heartbeat_ms.load(Ordering::Acquire))
                > self.lease_timeout_ms.load(Ordering::Acquire)
    }

    pub fn is_faulted(&self) -> bool {
        self.state.load(Ordering::Acquire) == FAULTED
    }

    pub fn accepting_commands(&self) -> bool {
        !self.is_faulted() && self.audit_healthy.load(Ordering::Acquire)
    }

    pub fn lease_timeout_ms(&self) -> u64 {
        self.lease_timeout_ms.load(Ordering::Acquire)
    }

    pub fn snapshot(&self) -> HealthSnapshot {
        let state = self.state.load(Ordering::Acquire);
        HealthSnapshot {
            state: match state {
                RUNNING => "running",
                FAULTED => "faulted",
                _ => "stopped",
            },
            market_ready: self.market_ready.load(Ordering::Acquire),
            audit_healthy: self.audit_healthy.load(Ordering::Acquire),
            last_heartbeat_ms: self.last_heartbeat_ms.load(Ordering::Acquire),
            last_binance_ms: self.last_binance_ms.load(Ordering::Acquire),
            last_book_ms: self.last_book_ms.load(Ordering::Acquire),
            fault: match self.fault_code.load(Ordering::Acquire) {
                1 => "audit_backpressure",
                2 => "binance_backpressure",
                3 => "execution_backpressure",
                4 => "heartbeat_timeout",
                _ => "none",
            },
            live_execution_enabled: false,
        }
    }
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}
