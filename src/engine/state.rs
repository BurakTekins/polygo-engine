use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};

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

#[derive(Debug)]
pub struct EngineState {
    open_or_reserved: AtomicUsize,
    next_position_id: AtomicU64,
    max_open_positions: usize,
    daily_loss_limit_microusd: u64,
    daily_loss_microusd: AtomicU64,
    total_pnl_microusd: AtomicI64,
    total_trades: AtomicU64,
}

impl EngineState {
    pub fn new(max_open_positions: usize, daily_loss_limit_usd: f64) -> Self {
        Self {
            open_or_reserved: AtomicUsize::new(0),
            next_position_id: AtomicU64::new(1),
            max_open_positions,
            daily_loss_limit_microusd: to_microusd(daily_loss_limit_usd),
            daily_loss_microusd: AtomicU64::new(0),
            total_pnl_microusd: AtomicI64::new(0),
            total_trades: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn try_reserve(&self) -> Result<(), &'static str> {
        if self.daily_loss_microusd.load(Ordering::Acquire) >= self.daily_loss_limit_microusd {
            return Err("daily_loss_limit");
        }
        self.open_or_reserved
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < self.max_open_positions).then_some(current + 1)
            })
            .map(|_| ())
            .map_err(|_| "max_open_positions")
    }

    #[inline(always)]
    pub fn release_reservation(&self) {
        self.open_or_reserved.fetch_sub(1, Ordering::AcqRel);
    }

    pub fn open_reserved(&self) -> u64 {
        self.total_trades.fetch_add(1, Ordering::Relaxed);
        self.next_position_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn close(&self, entry_price: f64, exit_price: f64, shares: u64) -> ClosedPosition {
        let gross_pnl = round_5((exit_price - entry_price) * shares as f64);
        let entry_fee = taker_fee(shares, entry_price);
        let exit_fee = taker_fee(shares, exit_price);
        let net_pnl = round_5(gross_pnl - entry_fee - exit_fee);
        if net_pnl < 0.0 {
            self.daily_loss_microusd
                .fetch_add(to_microusd(net_pnl.abs()), Ordering::AcqRel);
        }
        add_signed_microusd(&self.total_pnl_microusd, net_pnl);
        self.open_or_reserved.fetch_sub(1, Ordering::AcqRel);
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
}

pub fn taker_fee(shares: u64, price: f64) -> f64 {
    round_5(shares as f64 * 0.07 * price * (1.0 - price))
}

pub fn round_5(value: f64) -> f64 {
    (value * 100_000.0).round() / 100_000.0
}

fn to_microusd(value: f64) -> u64 {
    (value * 1_000_000.0).round() as u64
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
    fn atomic_reservations_enforce_position_limit() {
        let state = EngineState::new(1, 100.0);
        assert!(state.try_reserve().is_ok());
        assert_eq!(state.try_reserve(), Err("max_open_positions"));
    }
}
