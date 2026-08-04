use super::revalidation::outcome_book;
use super::types::{OrderSide, OrderSignal};
use crate::engine::state::{round_5, taker_fee_decimal};
use crate::engine::strategy::BtcPriceHistory;
use crate::market::now_ms;
use crate::runtime_config::StrategySnapshot;
use crate::ws::polymarket::{BookSnapshot, BookState};

#[derive(Debug, Clone, Copy)]
pub(super) struct ExitDecision {
    pub(super) reason: &'static str,
    pub(super) decided_at_ms: u64,
    pub(super) held_ms: u64,
    pub(super) exit_momentum_usd: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
struct ExitMark {
    net_pnl: f64,
    net_pnl_pct: f64,
}

pub(super) async fn wait_for_exit_decision(
    signal: &OrderSignal,
    entry_at_ms: u64,
    entry_price: f64,
    shares: f64,
    config: &StrategySnapshot,
    book_state: &BookState,
    btc_price_history: &BtcPriceHistory,
) -> ExitDecision {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(
            config.exit_check_interval_ms,
        ))
        .await;
        let decided_at_ms = now_ms();
        let held_ms = decided_at_ms.saturating_sub(entry_at_ms);
        let book = book_state.load();
        let mark = exit_mark(
            book,
            decided_at_ms,
            signal.side,
            entry_price,
            shares,
            config,
        );
        let exit_momentum_usd = btc_price_history.recent_momentum(config.exit_reversal_window_ms);
        let is_adverse = adverse_reversal(
            signal.side,
            exit_momentum_usd,
            config.exit_reversal_threshold_usd,
        );

        if mark
            .map(|mark| mark.net_pnl_pct <= config.exit_stop_loss_pct)
            .unwrap_or(false)
        {
            return ExitDecision {
                reason: "stop_loss",
                decided_at_ms,
                held_ms,
                exit_momentum_usd,
            };
        }
        if let Some(mark) = mark.filter(|_| held_ms >= config.exit_min_hold_ms && is_adverse) {
            return ExitDecision {
                reason: if mark.net_pnl >= config.exit_take_profit_net_usd {
                    "profit_reversal"
                } else {
                    "momentum_reversal"
                },
                decided_at_ms,
                held_ms,
                exit_momentum_usd,
            };
        }
        if held_ms >= config.exit_max_hold_ms {
            return ExitDecision {
                reason: "max_hold",
                decided_at_ms,
                held_ms,
                exit_momentum_usd,
            };
        }
    }
}

fn exit_mark(
    book: BookSnapshot,
    now_ms: u64,
    side: OrderSide,
    entry_price: f64,
    shares: f64,
    config: &StrategySnapshot,
) -> Option<ExitMark> {
    if now_ms.saturating_sub(book.received_at_ms) > config.max_book_age_ms {
        return None;
    }
    let exit_price = outcome_book(book, side).bid?;
    let gross_pnl = (exit_price - entry_price) * shares;
    let entry_fee = taker_fee_decimal(shares, entry_price);
    let exit_fee = taker_fee_decimal(shares, exit_price);
    let entry_notional = entry_price * shares;
    if entry_notional <= 0.0 {
        return None;
    }
    let net_pnl = round_5(gross_pnl - entry_fee - exit_fee);
    Some(ExitMark {
        net_pnl,
        net_pnl_pct: round_5((net_pnl / entry_notional) * 100.0),
    })
}

fn adverse_reversal(side: OrderSide, momentum_usd: Option<f64>, threshold_usd: f64) -> bool {
    let Some(momentum_usd) = momentum_usd else {
        return false;
    };
    match side {
        OrderSide::BuyYes => momentum_usd <= -threshold_usd,
        OrderSide::BuyNo => momentum_usd >= threshold_usd,
    }
}
