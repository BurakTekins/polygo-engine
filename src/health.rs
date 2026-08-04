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
    last_binance_ms: AtomicU64,
    last_book_ms: AtomicU64,
    fault_code: AtomicU8,
}

#[derive(Debug, Serialize)]
pub struct HealthSnapshot {
    pub state: &'static str,
    pub market_ready: bool,
    pub audit_healthy: bool,
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
            last_binance_ms: AtomicU64::new(0),
            last_book_ms: AtomicU64::new(0),
            fault_code: AtomicU8::new(0),
        }
    }

    #[inline(always)]
    pub fn is_running(&self) -> bool {
        self.state.load(Ordering::Acquire) == RUNNING
    }

    pub fn start(&self) -> Result<(), &'static str> {
        if !self.market_ready.load(Ordering::Acquire) {
            return Err("market_not_ready");
        }
        if !self.audit_healthy.load(Ordering::Acquire) {
            return Err("audit_unhealthy");
        }
        if self.state.load(Ordering::Acquire) == FAULTED {
            return Err("faulted_restart_process");
        }
        self.state.store(RUNNING, Ordering::Release);
        Ok(())
    }

    pub fn stop(&self) {
        self.state.store(STOPPED, Ordering::Release);
    }

    pub fn trip(&self, code: u8) {
        if code == 1 {
            self.audit_healthy.store(false, Ordering::Release);
        }
        self.fault_code.store(code, Ordering::Release);
        self.state.store(FAULTED, Ordering::Release);
    }

    pub fn set_market_ready(&self, ready: bool) {
        self.market_ready.store(ready, Ordering::Release);
    }

    #[inline(always)]
    pub fn market_ready(&self) -> bool {
        self.market_ready.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn mark_binance(&self, now_ms: u64) {
        self.last_binance_ms.store(now_ms, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn mark_book(&self, now_ms: u64) {
        self.last_book_ms.store(now_ms, Ordering::Relaxed);
    }

    pub fn is_faulted(&self) -> bool {
        self.state.load(Ordering::Acquire) == FAULTED
    }

    pub fn accepting_commands(&self) -> bool {
        !self.is_faulted() && self.audit_healthy.load(Ordering::Acquire)
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
            last_binance_ms: self.last_binance_ms.load(Ordering::Acquire),
            last_book_ms: self.last_book_ms.load(Ordering::Acquire),
            fault: match self.fault_code.load(Ordering::Acquire) {
                1 => "audit_backpressure",
                2 => "binance_backpressure",
                3 => "execution_backpressure",
                5 => "position_open_manual_intervention",
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
