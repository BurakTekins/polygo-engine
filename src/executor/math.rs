use std::{str::FromStr, sync::OnceLock, time::Instant};

use anyhow::{Context, Result};
use polymarket_client_sdk_v2::{clob::types::response::PostOrderResponse, types::Decimal};

use crate::engine::state::round_5;
use crate::market::now_ms;

pub(super) trait ElapsedToNowMs {
    fn elapsed_to_now_ms(self) -> u64;
}

impl ElapsedToNowMs for u64 {
    fn elapsed_to_now_ms(self) -> u64 {
        now_ms().saturating_sub(self)
    }
}

pub(super) fn order_size(price: f64, max_notional_usd: f64, max_shares: u64) -> u64 {
    if !(0.0..=1.0).contains(&price) || price == 0.0 {
        return 0;
    }
    ((max_notional_usd / price).floor() as u64).min(max_shares)
}

pub(super) fn expected_move_covers_fees(
    entry_price: f64,
    shares: u64,
    min_expected_price_move: f64,
) -> bool {
    if shares == 0 {
        return false;
    }
    if min_expected_price_move <= 0.0 {
        return true;
    }
    let exit_price = (entry_price + min_expected_price_move).min(1.0);
    let gross = min_expected_price_move * shares as f64;
    let fees = fee(entry_price, shares) + fee(exit_price, shares);
    gross > fees
}

fn fee(price: f64, shares: u64) -> f64 {
    shares as f64 * 0.07 * price * (1.0 - price)
}

pub(super) fn sell_price(response: &PostOrderResponse) -> Option<f64> {
    price_ratio(response.taking_amount, response.making_amount)
}

pub(super) fn sell_shares_filled(response: &PostOrderResponse) -> Option<f64> {
    decimal_to_f64(response.making_amount)
}

pub(super) fn buy_shares_filled(response: &PostOrderResponse) -> Option<f64> {
    decimal_to_f64(response.taking_amount)
}

pub(super) fn buy_notional_spent(response: &PostOrderResponse) -> Option<f64> {
    decimal_to_f64(response.making_amount)
}

fn price_ratio(numerator: Decimal, denominator: Decimal) -> Option<f64> {
    let denominator = decimal_to_f64(denominator)?;
    if denominator <= 0.0 {
        return None;
    }
    Some(decimal_to_f64(numerator)? / denominator)
}

pub(super) fn decimal_from_f64(value: f64) -> Result<Decimal> {
    Decimal::from_str(&round_5(value).to_string()).context("invalid decimal amount")
}

pub(super) fn floor_to_2_decimals(value: f64) -> f64 {
    (value * 100.0).floor() / 100.0
}

pub(super) fn decimal_to_f64(value: Decimal) -> Option<f64> {
    value.to_string().parse().ok()
}

pub(super) fn monotonic_ns() -> u128 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_with_notional_and_share_caps() {
        assert_eq!(order_size(0.50, 100.0, 500), 200);
        assert_eq!(order_size(0.10, 100.0, 500), 500);
        assert_eq!(order_size(0.0, 100.0, 500), 0);
    }
}
