use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};

use crate::market::now_ms;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClosedPosition {
    pub entry_price: f64,
    pub exit_price: f64,
    pub shares: u64,
    pub gross_pnl: f64,
    pub entry_fee: f64,
    pub exit_fee: f64,
    pub net_pnl: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClosedLivePosition {
    pub entry_price: f64,
    pub exit_price: f64,
    pub shares: f64,
    pub gross_pnl: f64,
    pub entry_fee: f64,
    pub exit_fee: f64,
    pub net_pnl: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActivePositionSnapshot {
    pub active: bool,
    pub opened: bool,
    pub position_id: u64,
    pub age_ms: u64,
}

#[derive(Debug)]
pub struct EngineState {
    open_or_reserved: AtomicUsize,
    next_position_id: AtomicU64,
    max_open_positions: usize,
    reserved_at_ms: AtomicU64,
    opened_position_id: AtomicU64,
    daily_loss_limit_microusd: AtomicU64,
    daily_epoch_day: AtomicU64,
    daily_pnl_microusd: AtomicI64,
    total_pnl_microusd: AtomicI64,
    total_trades: AtomicU64,
}

impl EngineState {
    pub fn new(max_open_positions: usize, daily_loss_limit_usd: f64) -> Self {
        Self {
            open_or_reserved: AtomicUsize::new(0),
            next_position_id: AtomicU64::new(1),
            max_open_positions,
            reserved_at_ms: AtomicU64::new(0),
            opened_position_id: AtomicU64::new(0),
            daily_loss_limit_microusd: AtomicU64::new(to_microusd(daily_loss_limit_usd)),
            daily_epoch_day: AtomicU64::new(epoch_day(now_ms())),
            daily_pnl_microusd: AtomicI64::new(0),
            total_pnl_microusd: AtomicI64::new(0),
            total_trades: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn try_reserve(&self) -> Result<(), &'static str> {
        self.refresh_daily_pnl(now_ms());
        if self.daily_pnl_microusd.load(Ordering::Acquire)
            <= -(self.daily_loss_limit_microusd.load(Ordering::Acquire) as i64)
        {
            return Err("daily_loss_limit");
        }
        self.open_or_reserved
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current == 0 && self.max_open_positions > 0).then_some(current + 1)
            })
            .map(|_| {
                self.reserved_at_ms.store(now_ms(), Ordering::Release);
                self.opened_position_id.store(0, Ordering::Release);
            })
            .map_err(|_| "position_active")
    }

    #[inline(always)]
    pub fn release_reservation(&self) {
        self.opened_position_id.store(0, Ordering::Release);
        self.reserved_at_ms.store(0, Ordering::Release);
        self.open_or_reserved.fetch_sub(1, Ordering::AcqRel);
    }

    pub fn active_snapshot(&self) -> ActivePositionSnapshot {
        let active = self.open_or_reserved.load(Ordering::Acquire) > 0;
        let reserved_at_ms = self.reserved_at_ms.load(Ordering::Acquire);
        let position_id = self.opened_position_id.load(Ordering::Acquire);
        ActivePositionSnapshot {
            active,
            opened: position_id > 0,
            position_id,
            age_ms: if active && reserved_at_ms > 0 {
                now_ms().saturating_sub(reserved_at_ms)
            } else {
                0
            },
        }
    }

    pub fn set_daily_loss_limit_usd(&self, daily_loss_limit_usd: f64) {
        self.daily_loss_limit_microusd
            .store(to_microusd(daily_loss_limit_usd), Ordering::Release);
    }

    pub fn open_reserved(&self) -> u64 {
        self.total_trades.fetch_add(1, Ordering::Relaxed);
        let position_id = self.next_position_id.fetch_add(1, Ordering::Relaxed);
        self.opened_position_id
            .store(position_id, Ordering::Release);
        position_id
    }

    pub fn close(&self, entry_price: f64, exit_price: f64, shares: u64) -> ClosedPosition {
        let gross_pnl = round_5((exit_price - entry_price) * shares as f64);
        let entry_fee = taker_fee(shares, entry_price);
        let exit_fee = taker_fee(shares, exit_price);
        let net_pnl = round_5(gross_pnl - entry_fee - exit_fee);
        self.record_close(net_pnl);
        ClosedPosition {
            entry_price,
            exit_price,
            shares,
            gross_pnl,
            entry_fee,
            exit_fee,
            net_pnl,
        }
    }

    pub fn close_live(&self, entry_price: f64, exit_price: f64, shares: f64) -> ClosedLivePosition {
        let gross_pnl = round_5((exit_price - entry_price) * shares);
        let entry_fee = taker_fee_decimal(shares, entry_price);
        let exit_fee = taker_fee_decimal(shares, exit_price);
        let net_pnl = round_5(gross_pnl - entry_fee - exit_fee);
        self.record_close(net_pnl);
        ClosedLivePosition {
            entry_price,
            exit_price,
            shares: round_5(shares),
            gross_pnl,
            entry_fee,
            exit_fee,
            net_pnl,
        }
    }

    fn record_close(&self, net_pnl: f64) {
        self.refresh_daily_pnl(now_ms());
        add_signed_microusd(&self.daily_pnl_microusd, net_pnl);
        add_signed_microusd(&self.total_pnl_microusd, net_pnl);
        self.opened_position_id.store(0, Ordering::Release);
        self.reserved_at_ms.store(0, Ordering::Release);
        self.open_or_reserved.fetch_sub(1, Ordering::AcqRel);
    }

    fn refresh_daily_pnl(&self, now_ms: u64) {
        let today = epoch_day(now_ms);
        let current = self.daily_epoch_day.load(Ordering::Acquire);
        if current == today {
            return;
        }
        if self
            .daily_epoch_day
            .compare_exchange(current, today, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.daily_pnl_microusd.store(0, Ordering::Release);
        }
    }
}

pub fn taker_fee(shares: u64, price: f64) -> f64 {
    taker_fee_decimal(shares as f64, price)
}

pub fn taker_fee_decimal(shares: f64, price: f64) -> f64 {
    round_5(shares * 0.07 * price * (1.0 - price))
}

pub fn round_5(value: f64) -> f64 {
    (value * 100_000.0).round() / 100_000.0
}

fn to_microusd(value: f64) -> u64 {
    (value * 1_000_000.0).round() as u64
}

fn epoch_day(timestamp_ms: u64) -> u64 {
    timestamp_ms / 86_400_000
}

fn add_signed_microusd(target: &AtomicI64, value: f64) {
    let delta = (value * 1_000_000.0).round() as i64;
    target.fetch_add(delta, Ordering::AcqRel);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_and_close_use_five_decimal_rounding() {
        assert_eq!(taker_fee(100, 0.50), 1.75);
        let state = EngineState::new(1, 100.0);
        state.try_reserve().unwrap();
        state.open_reserved();
        let closed = state.close(0.50, 0.55, 100);
        assert_eq!(closed.entry_fee, 1.75);
        assert_eq!(closed.exit_fee, 1.7325);
        assert_eq!(closed.net_pnl, 1.5175);
    }

    #[test]
    fn atomic_reservations_enforce_single_active_position() {
        let state = EngineState::new(5, 100.0);
        assert!(state.try_reserve().is_ok());
        assert_eq!(state.try_reserve(), Err("position_active"));
    }

    #[test]
    fn updated_daily_loss_limit_applies_to_accumulated_loss() {
        let state = EngineState::new(1, 5.0);
        state.try_reserve().unwrap();
        state.open_reserved();
        state.close(0.50, 0.40, 100);
        assert_eq!(state.try_reserve(), Err("daily_loss_limit"));
        state.set_daily_loss_limit_usd(20.0);
        assert!(state.try_reserve().is_ok());
    }

    #[test]
    fn daily_limit_uses_net_realized_pnl() {
        let state = EngineState::new(1, 5.0);
        state.try_reserve().unwrap();
        state.open_reserved();
        state.close(0.30, 0.40, 100);

        state.try_reserve().unwrap();
        state.open_reserved();
        state.close(0.50, 0.45, 100);

        assert!(state.try_reserve().is_ok());
    }
}
