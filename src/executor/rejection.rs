use tracing::info;

use super::types::OrderSignal;
use crate::emitter::audit::{AuditBookContext, AuditEmitter, AuditEvent};
use crate::engine::state::EngineState;

pub(super) fn reject(
    signal: OrderSignal,
    reason: &'static str,
    engine_state: &EngineState,
    audit: &AuditEmitter,
) {
    reject_with_context(signal, reason, engine_state, audit, None);
}

pub(super) fn reject_with_context(
    signal: OrderSignal,
    reason: &'static str,
    engine_state: &EngineState,
    audit: &AuditEmitter,
    context: Option<AuditBookContext>,
) {
    engine_state.release_reservation();
    info!(
        signal_ts_ms = signal.signal_ts_ms,
        side = signal.side.as_str(),
        reason,
        "Execution rejected"
    );
    let _ = audit.emit(AuditEvent::ExecutionRejected {
        signal_ts_ms: signal.signal_ts_ms,
        side: signal.side.as_str(),
        reason,
        context,
    });
}
