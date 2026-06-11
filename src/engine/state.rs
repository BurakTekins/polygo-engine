use crate::executor::order::{OrderSignal, OrderSide};
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::info;

/// Represents a single open position on Polymarket CLOB
#[derive(Debug, Clone, Copy)]
pub struct OpenPosition {
    pub signal: OrderSignal,
    pub opened_at_ms: u64,
    pub entry_price: f64,
    pub size: u64,
    pub side: OrderSide,
}

/// Tracks engine state — open positions, P&L, risk limits
#[derive(Debug)]
pub struct EngineState {
    pub open_positions: Vec<OpenPosition>,
    pub total_pnl_usdc: f64,
    pub total_trades: u64,
    pub total_wins: u64,
    pub max_open_positions: usize,
    pub daily_loss_limit_usdc: f64,
    pub daily_loss_usdc: f64,

    /// Fast atomic gate to allow lock-free checking down the critical path
    pub trading_allowed: AtomicBool,
}

// Manual Default implementation to elegantly bypass AtomicBool derive limitations
impl Default for EngineState {
    fn default() -> Self {
        Self {
            open_positions: Vec::new(),
            total_pnl_usdc: 0.0,
            total_trades: 0,
            total_wins: 0,
            max_open_positions: 0,
            daily_loss_limit_usdc: 0.0,
            daily_loss_usdc: 0.0,
            trading_allowed: AtomicBool::new(true),
        }
    }
}

#[inline(always)]
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl EngineState {
    pub fn new(max_open_positions: usize, daily_loss_limit_usdc: f64) -> Self {
        Self {
            max_open_positions,
            daily_loss_limit_usdc,
            trading_allowed: AtomicBool::new(true),
            ..Default::default()
        }
    }

    /// High-frequency lock-free check. Decision engine can call this inline
    /// without awaiting or acquiring heavy OS mutex locks.
    #[inline(always)]
    pub fn can_trade_atomic(&self) -> bool {
        self.trading_allowed.load(Ordering::Relaxed)
    }

    /// Internal sync guard to re-evaluate structural risk boundaries
    fn update_trading_gate(&self) {
        let current_loss = self.daily_loss_usdc;
        let limit = self.daily_loss_limit_usdc;
        let open_count = self.open_positions.len();

        if current_loss >= limit || open_count >= self.max_open_positions {
            self.trading_allowed.store(false, Ordering::Release);
        } else {
            self.trading_allowed.store(true, Ordering::Release);
        }
    }

    /// Register a new open position
    pub fn open_position(&mut self, signal: OrderSignal) {
        let pos = OpenPosition {
            entry_price: signal.price,
            size: signal.size,
            side: signal.side,
            opened_at_ms: now_ms(),
            signal,
        };
        self.open_positions.push(pos);
        self.total_trades += 1;

        info!(
            target: "polygo_engine",
            "Position opened — side: {:?}, size: {}, entry: {:.4} | open positions: {}",
            signal.side, signal.size, signal.price, self.open_positions.len()
        );

        self.update_trading_gate();
    }

    /// Close position by index, calculate strict Polymarket fee matrix & net P&L
    pub fn close_position(&mut self, index: usize, exit_price: f64) {
        if index >= self.open_positions.len() {
            return;
        }

        let pos = self.open_positions.remove(index);

        // Polymarket dynamic CLOB fee calculation formula: 0.07 * p * (1 - p)
        let fee = (pos.size as f64) * 0.07 * pos.entry_price * (1.0 - pos.entry_price);
        let gross_pnl = (exit_price - pos.entry_price) * (pos.size as f64);
        let net_pnl = gross_pnl - fee;

        self.total_pnl_usdc += net_pnl;

        if net_pnl < 0.0 {
            self.daily_loss_usdc += net_pnl.abs();
        } else {
            self.total_wins += 1;
        }

        info!(
            target: "polygo_engine",
            "Position closed — gross: {:.2}, fee: {:.2}, net: {:.2} | total P&L: {:.2}",
            gross_pnl, fee, net_pnl, self.total_pnl_usdc
        );

        self.update_trading_gate();
    }

    pub fn win_rate(&self) -> f64 {
        if self.total_trades == 0 {
            return 0.0;
        }
        (self.total_wins as f64 / self.total_trades as f64) * 100.0
    }

    pub fn print_summary(&self) {
        info!(
            target: "polygo_engine",
            "=== Engine State Summary ===\n\
             Total trades : {}\n\
             Win rate     : {:.1}%\n\
             Total P&L    : {:.2} USDC\n\
             Daily loss   : {:.2} USDC\n\
             Open positions: {}",
            self.total_trades,
            self.win_rate(),
            self.total_pnl_usdc,
            self.daily_loss_usdc,
            self.open_positions.len()
        );
    }
}