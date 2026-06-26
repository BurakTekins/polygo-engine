use std::collections::VecDeque;

use crate::config::StrategyConfig;
use crate::executor::order::OrderSide;
use crate::ws::binance::PriceTick;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StrategySignal {
    pub side: OrderSide,
    pub momentum_usd: f64,
    pub binance_price: f64,
    pub signal_ts_ms: u64,
}

pub trait Strategy: Send {
    fn on_binance_tick(&mut self, tick: PriceTick, received_at_ms: u64) -> Option<StrategySignal>;
}

pub struct MomentumStrategy {
    window_ms: u64,
    buy_yes_threshold_usd: f64,
    buy_no_threshold_usd: f64,
    prices: VecDeque<PriceTick>,
}

impl MomentumStrategy {
    pub fn new(config: &StrategyConfig) -> Self {
        Self::with_params(
            config.momentum_window_ms,
            config.buy_yes_momentum_threshold_usd,
            config.buy_no_momentum_threshold_usd,
        )
    }

    pub fn with_params(
        window_ms: u64,
        buy_yes_threshold_usd: f64,
        buy_no_threshold_usd: f64,
    ) -> Self {
        Self {
            window_ms,
            buy_yes_threshold_usd,
            buy_no_threshold_usd,
            prices: VecDeque::with_capacity(512),
        }
    }
}

impl Strategy for MomentumStrategy {
    fn on_binance_tick(&mut self, tick: PriceTick, received_at_ms: u64) -> Option<StrategySignal> {
        self.prices.push_back(tick);
        let cutoff = tick.trade_time_ms.saturating_sub(self.window_ms);
        while self.prices.len() > 1 && self.prices[1].trade_time_ms <= cutoff {
            self.prices.pop_front();
        }

        let baseline = *self.prices.front()?;
        if baseline.trade_time_ms > cutoff {
            return None;
        }

        let momentum_usd = tick.price - baseline.price;
        let side = if momentum_usd > 0.0 {
            OrderSide::BuyYes
        } else {
            OrderSide::BuyNo
        };
        let threshold_usd = match side {
            OrderSide::BuyYes => self.buy_yes_threshold_usd,
            OrderSide::BuyNo => self.buy_no_threshold_usd,
        };
        if momentum_usd.abs() < threshold_usd {
            return None;
        }

        Some(StrategySignal {
            side,
            momentum_usd,
            binance_price: tick.price,
            signal_ts_ms: received_at_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> StrategyConfig {
        StrategyConfig {
            momentum_window_ms: 100,
            momentum_threshold_usd: 8.0,
            buy_yes_momentum_threshold_usd: 12.0,
            buy_no_momentum_threshold_usd: 8.0,
            execution_latency_ms: 100,
            hold_ms: 5_000,
            min_expected_price_move: 0.05,
            min_market_progress: 0.05,
            max_market_progress: 0.90,
            max_book_age_ms: 300,
        }
    }

    #[test]
    fn signals_from_signed_window_move() {
        let mut strategy = MomentumStrategy::new(&config());
        assert_eq!(
            strategy.on_binance_tick(PriceTick::new(100.0, 1_000), 1_001),
            None
        );
        assert_eq!(
            strategy
                .on_binance_tick(PriceTick::new(113.0, 1_100), 1_101)
                .unwrap()
                .side,
            OrderSide::BuyYes
        );

        let mut strategy = MomentumStrategy::new(&config());
        strategy.on_binance_tick(PriceTick::new(100.0, 1_000), 1_001);
        assert_eq!(
            strategy
                .on_binance_tick(PriceTick::new(91.0, 1_100), 1_101)
                .unwrap()
                .side,
            OrderSide::BuyNo
        );
    }

    #[test]
    fn does_not_compare_ticks_shorter_than_window() {
        let mut strategy = MomentumStrategy::new(&config());
        strategy.on_binance_tick(PriceTick::new(100.0, 1_000), 1_001);
        assert_eq!(
            strategy.on_binance_tick(PriceTick::new(120.0, 1_099), 1_100),
            None
        );
    }
}
