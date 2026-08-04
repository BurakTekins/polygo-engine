use super::revalidation::outcome_book;
use super::types::OrderSide;
use crate::emitter::audit::AuditBookContext;
use crate::engine::state::round_5;
use crate::market::ActiveMarket;
use crate::ws::polymarket::BookSnapshot;

pub(super) fn audit_context(
    book: BookSnapshot,
    now_ms: u64,
    side: OrderSide,
    market: Option<&ActiveMarket>,
    entry_slippage: Option<f64>,
    entry_limit_price: Option<f64>,
    intended_shares: Option<u64>,
) -> AuditBookContext {
    let selected = outcome_book(book, side);
    let token_id = market.map(|market| match side {
        OrderSide::BuyYes => market.yes_asset_id.clone(),
        OrderSide::BuyNo => market.no_asset_id.clone(),
    });
    let intended_notional_usd = intended_shares.map(|shares| {
        round_5(shares as f64 * entry_limit_price.or(selected.ask).unwrap_or_default())
    });

    AuditBookContext {
        market_slug: market.map(|market| market.slug.clone()),
        token_id,
        yes_bid: book.yes.bid.map(round_5),
        yes_ask: book.yes.ask.map(round_5),
        no_bid: book.no.bid.map(round_5),
        no_ask: book.no.ask.map(round_5),
        book_received_at_ms: book.received_at_ms,
        book_source_ts_ms: book.source_ts_ms,
        book_age_ms: now_ms.saturating_sub(book.received_at_ms),
        selected_bid: selected.bid.map(round_5),
        selected_ask: selected.ask.map(round_5),
        entry_slippage: entry_slippage.map(round_5),
        entry_limit_price: entry_limit_price.map(round_5),
        intended_shares,
        intended_notional_usd,
        available_shares: None,
        required_shares: None,
        exchange_error: None,
        filled_shares: None,
        exit_attempts: None,
        exit_retry_attempts: None,
        exit_attempt_funder_address: None,
        entry_matched_funder_address: None,
        signature_type: None,
        fallback_order_id: None,
        entry_submit_latency_ms: None,
        entry_build_latency_ms: None,
        entry_sign_latency_ms: None,
        entry_post_latency_ms: None,
        gtc_post_success: None,
        gtc_post_status: None,
        gtc_post_making_amount: None,
        gtc_post_taking_amount: None,
        gtc_poll_count: None,
        gtc_poll_status: None,
        gtc_poll_size_matched: None,
        gtc_cancel_confirmed: None,
        gtc_final_status: None,
        gtc_final_size_matched: None,
    }
}
