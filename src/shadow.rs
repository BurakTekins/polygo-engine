use std::sync::Arc;

use serde::Serialize;
use tokio::sync::broadcast;
use tokio::time::{sleep_until, Duration, Instant};

use crate::config::ShadowConfig;
use crate::emitter::audit::{AuditEmitter, AuditEvent};
use crate::engine::state::{round_5, taker_fee_decimal};
use crate::executor::order::OrderSide;
use crate::market::now_ms;
use crate::ws::polymarket::{
    BookSnapshot, BookState, DepthSnapshot, OutcomeDepth, TradePrint, TradeSide,
};

const PRICE_TICK: f64 = 0.01;

#[derive(Clone)]
pub struct ShadowExitEngine {
    config: Arc<ShadowConfig>,
    book_state: Arc<BookState>,
    audit: AuditEmitter,
}

impl ShadowExitEngine {
    pub fn new(config: ShadowConfig, book_state: Arc<BookState>, audit: AuditEmitter) -> Self {
        Self {
            config: Arc::new(config),
            book_state,
            audit,
        }
    }

    #[inline(always)]
    pub fn enabled(&self) -> bool {
        self.config.shadow_exit_enabled
    }

    pub fn observe_exit(&self, request: ShadowExitRequest) {
        if !self.enabled() {
            return;
        }
        let engine = self.clone();
        let policy_e_trades = self.book_state.subscribe_trades();
        let policy_d_trades = self.book_state.subscribe_trades();
        tokio::spawn(async move {
            let record = engine
                .simulate(request, policy_e_trades, policy_d_trades)
                .await;
            let _ = engine.audit.emit(AuditEvent::ShadowExitResult {
                record: Box::new(record),
            });
        });
    }

    async fn simulate(
        &self,
        request: ShadowExitRequest,
        policy_e_trades: broadcast::Receiver<TradePrint>,
        policy_d_trades: broadcast::Receiver<TradePrint>,
    ) -> ShadowExitRecord {
        let baseline = self.simulate_taker(&request, "same_latency_taker");
        let policy_e = self.simulate_policy(&request, ShadowPolicy::PolicyE, policy_e_trades);
        let policy_d = self.simulate_policy(&request, ShadowPolicy::PolicyD, policy_d_trades);
        let (mut baseline, mut policy_e, mut policy_d) = tokio::join!(baseline, policy_e, policy_d);
        baseline.policy_positive = baseline.pnl.net_pnl > 0.0;
        baseline.baseline_positive = baseline.policy_positive;
        let policy_e_diff_vs_baseline = diff(&policy_e.pnl, &baseline.pnl);
        let policy_d_diff_vs_baseline = diff(&policy_d.pnl, &baseline.pnl);
        annotate_policy(
            &mut policy_e,
            baseline.pnl.net_pnl,
            policy_e_diff_vs_baseline.net_pnl_diff,
        );
        annotate_policy(
            &mut policy_d,
            baseline.pnl.net_pnl,
            policy_d_diff_vs_baseline.net_pnl_diff,
        );
        ShadowExitRecord {
            position_id: request.position_id,
            side: request.side.as_str(),
            entry_ms: request.entry_ms,
            decision_ms: request.decision_ms,
            exit_reason: request.exit_reason,
            entry_price: round_5(request.entry_price),
            original_shares: round_5(request.shares),
            entry_notional: round_5(request.entry_price * request.shares),
            entry_fee: taker_fee_decimal(request.shares, request.entry_price),
            baseline_taker_active_ms: request
                .decision_ms
                .saturating_add(self.config.shadow_execution_latency_ms),
            policy_e_maker_active_ms: policy_e.timeline.maker_active_ms,
            policy_e_maker_timeout_ms: policy_e.timeline.maker_timeout_ms,
            policy_e_cancel_effective_ms: policy_e.timeline.cancel_effective_ms,
            policy_e_fallback_taker_active_ms: policy_e.timeline.fallback_taker_active_ms,
            policy_d_maker_active_ms: policy_d.timeline.maker_active_ms,
            policy_d_maker_timeout_ms: policy_d.timeline.maker_timeout_ms,
            policy_d_cancel_effective_ms: policy_d.timeline.cancel_effective_ms,
            policy_d_fallback_taker_active_ms: policy_d.timeline.fallback_taker_active_ms,
            decision_processing_started_ns: request.decision_processing_started_ns,
            decision_processing_finished_ns: request.decision_processing_finished_ns,
            event_receive_ns: request.event_receive_ns,
            event_exchange_timestamp_ms: request.event_exchange_timestamp_ms,
            book_age_ms: request.book_age_ms,
            baseline,
            policy_e,
            policy_d,
            policy_e_diff_vs_baseline,
            policy_d_diff_vs_baseline,
        }
    }

    async fn simulate_taker(
        &self,
        request: &ShadowExitRequest,
        policy: &'static str,
    ) -> ShadowPolicyResult {
        let active_ms = request
            .decision_ms
            .saturating_add(self.config.shadow_execution_latency_ms);
        sleep_until(deadline_from_now(
            request.decision_ms,
            self.config.shadow_execution_latency_ms,
        ))
        .await;
        let mut result = ShadowPolicyResult::new(policy);
        result.state = ShadowState::TakerPending;
        match executable_bid_vwap(
            self.book_state.load_depth(),
            self.book_state.load(),
            request.side,
            request.shares,
            active_ms,
        ) {
            Ok(fill) => {
                result.fallback_taker_vwap = Some(round_5(fill.vwap));
                result.fallback_taker_shares = round_5(request.shares);
                result.outcome = ShadowOutcome::TakerBypass;
                result.state = ShadowState::Completed;
                result.pnl = pnl(
                    request.entry_price,
                    request.shares,
                    0.0,
                    0.0,
                    request.shares,
                    fill.vwap,
                );
                result.decision_to_completion_ms =
                    Some(active_ms.saturating_sub(request.decision_ms));
            }
            Err(_) => {
                result.outcome = ShadowOutcome::InsufficientFutureData;
                result.selection_reason = "missing_taker_active_book";
                result.state = ShadowState::InsufficientData;
            }
        }
        result
    }

    async fn simulate_policy(
        &self,
        request: &ShadowExitRequest,
        policy: ShadowPolicy,
        mut trades: broadcast::Receiver<TradePrint>,
    ) -> ShadowPolicyResult {
        if !policy_uses_maker(policy, request.exit_reason) {
            return self.simulate_taker(request, policy.as_str()).await;
        }

        let maker_active_ms = request
            .decision_ms
            .saturating_add(self.config.shadow_execution_latency_ms);
        let maker_timeout_ms = maker_active_ms.saturating_add(self.config.shadow_maker_wait_ms);
        let cancel_effective_ms =
            maker_timeout_ms.saturating_add(self.config.shadow_cancel_latency_ms);
        let fallback_taker_active_ms =
            cancel_effective_ms.saturating_add(self.config.shadow_fallback_taker_latency_ms);

        let mut result = ShadowPolicyResult::new(policy.as_str());
        result.timeline = ShadowPolicyTimeline {
            maker_active_ms: Some(maker_active_ms),
            maker_timeout_ms: Some(maker_timeout_ms),
            cancel_effective_ms: Some(cancel_effective_ms),
            fallback_taker_active_ms: Some(fallback_taker_active_ms),
        };
        result.state = ShadowState::PendingActivation;

        sleep_until(deadline_from_now(
            request.decision_ms,
            self.config.shadow_execution_latency_ms,
        ))
        .await;
        let book = self.book_state.load();
        let depth = self.book_state.load_depth();
        if stale_at(book.received_at_ms, maker_active_ms) {
            result.outcome = ShadowOutcome::InsufficientFutureData;
            result.selection_reason = "missing_taker_active_book";
            result.state = ShadowState::InsufficientData;
            return result;
        }
        let Some(best_bid) = selected_book(book, request.side).bid else {
            result.outcome = ShadowOutcome::InsufficientFutureData;
            result.selection_reason = "missing_taker_active_book";
            result.state = ShadowState::InsufficientData;
            return result;
        };
        let limit_price = round_5((best_bid + PRICE_TICK).min(0.99));
        result.limit_price = Some(limit_price);
        let selected_depth = selected_depth(&depth, request.side);
        let best_ask = selected_book(book, request.side).ask;
        if best_ask.is_some_and(|ask| limit_price + 0.00001 >= ask) {
            result.outcome = ShadowOutcome::PostOnlyRejected;
            result.selection_reason = "post_only_rejected";
            return self
                .complete_fallback_taker(request, result, fallback_taker_active_ms)
                .await;
        }
        let queue_ahead = ask_queue_ahead(selected_depth, limit_price);
        let queue_ratio = if request.shares > 0.0 {
            queue_ahead / request.shares
        } else {
            f64::INFINITY
        };
        result.queue_snapshot_ms = Some(depth.received_at_ms);
        result.queue_ahead = Some(round_5(queue_ahead));
        result.queue_ratio = Some(round_5(queue_ratio));
        if queue_ratio > self.config.shadow_queue_ratio_threshold {
            result.selection_reason = "queue_ratio_above_threshold";
            result.outcome = ShadowOutcome::TakerBypass;
            return self
                .complete_fallback_taker(request, result, maker_active_ms)
                .await;
        }

        result.selected_for_maker = true;
        result.selection_reason = "queue_ratio_within_threshold";
        result.state = ShadowState::MakerResting;
        let maker_deadline = deadline_from_now(
            request.decision_ms,
            self.config.shadow_execution_latency_ms + self.config.shadow_maker_wait_ms,
        );
        let mut traded_at_or_above = 0.0;
        loop {
            tokio::select! {
                _ = sleep_until(maker_deadline) => break,
                trade = trades.recv() => {
                    match trade {
                        Ok(trade) if trade_supports_sell_fill(trade, request.side, limit_price, maker_active_ms, maker_timeout_ms) => {
                            traded_at_or_above += trade.size;
                            let filled = (traded_at_or_above - queue_ahead)
                                .max(0.0)
                                .min(request.shares);
                            result.maker_filled_shares = round_5(filled);
                            if filled > 0.0 {
                                result.maker_fill_vwap = Some(limit_price);
                            }
                            if filled + 0.00001 >= request.shares {
                                result.outcome = ShadowOutcome::FullFillTradeSupported;
                                result.state = ShadowState::Completed;
                                result.pnl = pnl(
                                    request.entry_price,
                                    request.shares,
                                    request.shares,
                                    limit_price,
                                    0.0,
                                    0.0,
                                );
                                result.decision_to_completion_ms = Some(
                                    trade.received_at_ms.saturating_sub(request.decision_ms),
                                );
                                result.maker_active_to_completion_ms = Some(
                                    trade.received_at_ms.saturating_sub(maker_active_ms),
                                );
                                return result;
                            }
                        }
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            result.outcome = ShadowOutcome::InsufficientFutureData;
                            result.selection_reason = "trade_stream_lagged";
                            result.state = ShadowState::InsufficientData;
                            return result;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            result.outcome = ShadowOutcome::InsufficientFutureData;
                            result.selection_reason = "trade_stream_closed";
                            result.state = ShadowState::InsufficientData;
                            return result;
                        }
                    }
                }
            }
        }
        result.state = ShadowState::CancelPending;
        sleep_until(deadline_from_now(
            request.decision_ms,
            self.config.shadow_execution_latency_ms
                + self.config.shadow_maker_wait_ms
                + self.config.shadow_cancel_latency_ms,
        ))
        .await;
        self.complete_fallback_taker(request, result, fallback_taker_active_ms)
            .await
    }

    async fn complete_fallback_taker(
        &self,
        request: &ShadowExitRequest,
        mut result: ShadowPolicyResult,
        active_ms: u64,
    ) -> ShadowPolicyResult {
        let delay_ms = active_ms.saturating_sub(now_ms());
        if delay_ms > 0 {
            sleep_until(Instant::now() + Duration::from_millis(delay_ms)).await;
        }
        let remaining = (request.shares - result.maker_filled_shares).max(0.0);
        result.state = ShadowState::FallbackTakerPending;
        match executable_bid_vwap(
            self.book_state.load_depth(),
            self.book_state.load(),
            request.side,
            remaining,
            active_ms,
        ) {
            Ok(fill) => {
                result.fallback_taker_vwap = Some(round_5(fill.vwap));
                result.fallback_taker_shares = round_5(remaining);
                result.outcome = if result.maker_filled_shares > 0.0 {
                    ShadowOutcome::PartialFillTradeSupported
                } else if result.selected_for_maker {
                    ShadowOutcome::ZeroFillTimeoutFallback
                } else if result.outcome == ShadowOutcome::PostOnlyRejected {
                    ShadowOutcome::PostOnlyRejected
                } else {
                    ShadowOutcome::TakerBypass
                };
                result.state = ShadowState::Completed;
                result.pnl = pnl(
                    request.entry_price,
                    request.shares,
                    result.maker_filled_shares,
                    result.maker_fill_vwap.unwrap_or(0.0),
                    remaining,
                    fill.vwap,
                );
                result.decision_to_completion_ms =
                    Some(active_ms.saturating_sub(request.decision_ms));
                result.maker_active_to_completion_ms = result
                    .timeline
                    .maker_active_ms
                    .map(|maker_active_ms| active_ms.saturating_sub(maker_active_ms));
            }
            Err(reason) => {
                result.outcome = ShadowOutcome::InsufficientFutureData;
                result.selection_reason = reason;
                result.state = ShadowState::InsufficientData;
            }
        }
        result
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ShadowExitRequest {
    pub position_id: u64,
    pub side: OrderSide,
    pub entry_ms: u64,
    pub decision_ms: u64,
    pub exit_reason: &'static str,
    pub entry_price: f64,
    pub shares: f64,
    pub decision_processing_started_ns: u128,
    pub decision_processing_finished_ns: u128,
    pub event_receive_ns: u128,
    pub event_exchange_timestamp_ms: u64,
    pub book_age_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShadowExitRecord {
    pub position_id: u64,
    pub side: &'static str,
    pub entry_ms: u64,
    pub decision_ms: u64,
    pub exit_reason: &'static str,
    pub entry_price: f64,
    pub original_shares: f64,
    pub entry_notional: f64,
    pub entry_fee: f64,
    pub baseline_taker_active_ms: u64,
    pub policy_e_maker_active_ms: Option<u64>,
    pub policy_e_maker_timeout_ms: Option<u64>,
    pub policy_e_cancel_effective_ms: Option<u64>,
    pub policy_e_fallback_taker_active_ms: Option<u64>,
    pub policy_d_maker_active_ms: Option<u64>,
    pub policy_d_maker_timeout_ms: Option<u64>,
    pub policy_d_cancel_effective_ms: Option<u64>,
    pub policy_d_fallback_taker_active_ms: Option<u64>,
    pub decision_processing_started_ns: u128,
    pub decision_processing_finished_ns: u128,
    pub event_receive_ns: u128,
    pub event_exchange_timestamp_ms: u64,
    pub book_age_ms: u64,
    pub baseline: ShadowPolicyResult,
    pub policy_e: ShadowPolicyResult,
    pub policy_d: ShadowPolicyResult,
    pub policy_e_diff_vs_baseline: ShadowDiff,
    pub policy_d_diff_vs_baseline: ShadowDiff,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShadowPolicyResult {
    pub policy: &'static str,
    pub state: ShadowState,
    pub selected_for_maker: bool,
    pub selection_reason: &'static str,
    pub queue_snapshot_ms: Option<u64>,
    pub queue_ahead: Option<f64>,
    pub queue_ratio: Option<f64>,
    pub limit_price: Option<f64>,
    pub maker_filled_shares: f64,
    pub fallback_taker_shares: f64,
    pub maker_fill_vwap: Option<f64>,
    pub fallback_taker_vwap: Option<f64>,
    pub outcome: ShadowOutcome,
    pub pnl: ShadowPnl,
    pub baseline_positive: bool,
    pub policy_positive: bool,
    pub sign_flip: bool,
    pub incremental_diff: f64,
    pub worst_adverse_price_move: Option<f64>,
    pub decision_to_completion_ms: Option<u64>,
    pub maker_active_to_completion_ms: Option<u64>,
    #[serde(skip_serializing)]
    timeline: ShadowPolicyTimeline,
}

impl ShadowPolicyResult {
    fn new(policy: &'static str) -> Self {
        Self {
            policy,
            state: ShadowState::PendingActivation,
            selected_for_maker: false,
            selection_reason: "taker_baseline",
            queue_snapshot_ms: None,
            queue_ahead: None,
            queue_ratio: None,
            limit_price: None,
            maker_filled_shares: 0.0,
            fallback_taker_shares: 0.0,
            maker_fill_vwap: None,
            fallback_taker_vwap: None,
            outcome: ShadowOutcome::InsufficientFutureData,
            pnl: ShadowPnl::default(),
            baseline_positive: false,
            policy_positive: false,
            sign_flip: false,
            incremental_diff: 0.0,
            worst_adverse_price_move: None,
            decision_to_completion_ms: None,
            maker_active_to_completion_ms: None,
            timeline: ShadowPolicyTimeline::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ShadowPolicyTimeline {
    maker_active_ms: Option<u64>,
    maker_timeout_ms: Option<u64>,
    cancel_effective_ms: Option<u64>,
    fallback_taker_active_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShadowState {
    PendingActivation,
    TakerPending,
    MakerResting,
    CancelPending,
    FallbackTakerPending,
    Completed,
    InsufficientData,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShadowOutcome {
    TakerBypass,
    FullFillTradeSupported,
    PartialFillTradeSupported,
    ZeroFillTimeoutFallback,
    PostOnlyRejected,
    InsufficientFutureData,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct ShadowPnl {
    pub entry_notional: f64,
    pub entry_fee: f64,
    pub maker_exit_notional: f64,
    pub maker_exit_fee: f64,
    pub fallback_exit_notional: f64,
    pub fallback_exit_fee: f64,
    pub total_exit_notional: f64,
    pub total_exit_fee: f64,
    pub net_pnl: f64,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct ShadowDiff {
    pub net_pnl_diff: f64,
    pub price_component: f64,
    pub fee_component: f64,
}

#[derive(Debug, Clone, Copy)]
enum ShadowPolicy {
    PolicyE,
    PolicyD,
}

impl ShadowPolicy {
    fn as_str(self) -> &'static str {
        match self {
            Self::PolicyE => "policy_e",
            Self::PolicyD => "policy_d",
        }
    }
}

struct VwapFill {
    vwap: f64,
}

fn policy_uses_maker(policy: ShadowPolicy, exit_reason: &str) -> bool {
    match policy {
        ShadowPolicy::PolicyE => exit_reason == "profit_reversal",
        ShadowPolicy::PolicyD => {
            exit_reason == "momentum_reversal" || exit_reason == "profit_reversal"
        }
    }
}

fn trade_supports_sell_fill(
    trade: TradePrint,
    side: OrderSide,
    limit_price: f64,
    maker_active_ms: u64,
    maker_timeout_ms: u64,
) -> bool {
    let selected_is_yes = side == OrderSide::BuyYes;
    trade.is_yes == selected_is_yes
        && trade.side == TradeSide::Buy
        && trade.price + 0.00001 >= limit_price
        && trade.received_at_ms >= maker_active_ms
        && trade.received_at_ms <= maker_timeout_ms
}

fn deadline_from_now(decision_ms: u64, delay_ms: u64) -> Instant {
    let target_ms = decision_ms.saturating_add(delay_ms);
    let remaining_ms = target_ms.saturating_sub(now_ms());
    Instant::now() + Duration::from_millis(remaining_ms)
}

fn selected_book(snapshot: BookSnapshot, side: OrderSide) -> crate::ws::polymarket::OutcomeBook {
    match side {
        OrderSide::BuyYes => snapshot.yes,
        OrderSide::BuyNo => snapshot.no,
    }
}

fn selected_depth(snapshot: &DepthSnapshot, side: OrderSide) -> &OutcomeDepth {
    match side {
        OrderSide::BuyYes => &snapshot.yes,
        OrderSide::BuyNo => &snapshot.no,
    }
}

fn stale_at(received_at_ms: u64, target_ms: u64) -> bool {
    received_at_ms == 0 || target_ms.saturating_sub(received_at_ms) > 1_000
}

fn ask_queue_ahead(depth: &OutcomeDepth, limit_price: f64) -> f64 {
    depth
        .asks
        .iter()
        .filter(|level| level.price <= limit_price + 0.00001)
        .map(|level| level.size)
        .sum()
}

fn executable_bid_vwap(
    depth: DepthSnapshot,
    book: BookSnapshot,
    side: OrderSide,
    shares: f64,
    active_ms: u64,
) -> Result<VwapFill, &'static str> {
    if stale_at(book.received_at_ms, active_ms) || stale_at(depth.received_at_ms, active_ms) {
        return Err("missing_fallback_book");
    }
    if shares <= 0.0 {
        return Ok(VwapFill { vwap: 0.0 });
    }
    let levels = &selected_depth(&depth, side).bids;
    let mut remaining = shares;
    let mut notional = 0.0;
    let mut sorted = levels.iter().collect::<Vec<_>>();
    sorted.sort_by(|left, right| right.price.total_cmp(&left.price));
    for level in sorted {
        if remaining <= 0.0 {
            break;
        }
        let filled = remaining.min(level.size);
        remaining -= filled;
        notional += filled * level.price;
    }
    if remaining > 0.00000001 {
        return Err("missing_fallback_book");
    }
    Ok(VwapFill {
        vwap: notional / shares,
    })
}

fn pnl(
    entry_price: f64,
    shares: f64,
    maker_shares: f64,
    maker_price: f64,
    fallback_shares: f64,
    fallback_price: f64,
) -> ShadowPnl {
    let entry_notional = round_5(entry_price * shares);
    let entry_fee = taker_fee_decimal(shares, entry_price);
    let maker_exit_notional = round_5(maker_shares * maker_price);
    let maker_exit_fee = taker_fee_decimal(maker_shares, maker_price);
    let fallback_exit_notional = round_5(fallback_shares * fallback_price);
    let fallback_exit_fee = taker_fee_decimal(fallback_shares, fallback_price);
    let total_exit_notional = round_5(maker_exit_notional + fallback_exit_notional);
    let total_exit_fee = round_5(maker_exit_fee + fallback_exit_fee);
    ShadowPnl {
        entry_notional,
        entry_fee,
        maker_exit_notional,
        maker_exit_fee,
        fallback_exit_notional,
        fallback_exit_fee,
        total_exit_notional,
        total_exit_fee,
        net_pnl: round_5(total_exit_notional - entry_notional - entry_fee - total_exit_fee),
    }
}

fn annotate_policy(result: &mut ShadowPolicyResult, baseline_net_pnl: f64, net_pnl_diff: f64) {
    result.baseline_positive = baseline_net_pnl > 0.0;
    result.policy_positive = result.pnl.net_pnl > 0.0;
    result.sign_flip = result.baseline_positive && result.pnl.net_pnl < 0.0;
    result.incremental_diff = net_pnl_diff;
}

fn diff(policy: &ShadowPnl, baseline: &ShadowPnl) -> ShadowDiff {
    let price_component = round_5(
        (policy.total_exit_notional - policy.entry_notional)
            - (baseline.total_exit_notional - baseline.entry_notional),
    );
    let fee_component = round_5(
        (baseline.entry_fee + baseline.total_exit_fee) - (policy.entry_fee + policy.total_exit_fee),
    );
    ShadowDiff {
        net_pnl_diff: round_5(policy.net_pnl - baseline.net_pnl),
        price_component,
        fee_component,
    }
}
