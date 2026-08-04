use super::math::{expected_move_covers_fees, order_size};
use super::types::OrderSide;
use crate::market::MarketClock;
use crate::runtime_config::StrategySnapshot;
use crate::ws::polymarket::{BookSnapshot, DepthSnapshot, OutcomeBook};

#[inline]
pub fn revalidate_entry(
    book: BookSnapshot,
    now_ms: u64,
    market_clock: &MarketClock,
    config: &StrategySnapshot,
    side: OrderSide,
) -> Option<(f64, u64)> {
    if !market_clock.progress_allowed(now_ms, config.min_progress, config.max_progress)
        || now_ms.saturating_sub(book.received_at_ms) > config.max_book_age_ms
    {
        return None;
    }
    let outcome = outcome_book(book, side);
    let (Some(bid), Some(ask)) = (outcome.bid, outcome.ask) else {
        return None;
    };
    if ask <= bid || ask - bid > config.max_spread + 0.00001 {
        return None;
    }
    if ask < config.min_price || ask > config.max_price {
        return None;
    }
    let shares = order_size(ask, config.max_notional_usd, config.max_shares);
    if !expected_move_covers_fees(ask, shares, config.min_expected_price_move) {
        return None;
    }
    (shares > 0).then_some((ask, shares))
}

pub(super) fn executable_ask_shares(
    depth: &DepthSnapshot,
    side: OrderSide,
    limit_price: f64,
) -> f64 {
    let asks = match side {
        OrderSide::BuyYes => &depth.yes.asks,
        OrderSide::BuyNo => &depth.no.asks,
    };
    asks.iter()
        .filter(|level| level.price <= limit_price)
        .map(|level| level.size)
        .sum()
}

pub(super) fn outcome_book(snapshot: BookSnapshot, side: OrderSide) -> OutcomeBook {
    match side {
        OrderSide::BuyYes => snapshot.yes,
        OrderSide::BuyNo => snapshot.no,
    }
}
