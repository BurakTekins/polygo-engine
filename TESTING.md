# Automated testing

## Audit (2026-10-31)

Audited base commit: `2ff346113c5ce0cf2d09940461ed8c4c0def2564`.

Rust 2021/Tokio application with WebSocket market ingestion, momentum strategy,
atomic risk/position state, control-plane HTTP, execution adapters, and audit emission.
Native tests were already embedded in control, strategy, market, state, model parsing,
math, and error modules. Baseline: 17 native tests passed with all targets/features.
No checked-in CI workflow existed. Cargo.lock was already tracked.

## Added or repaired tests

- Control module: oversized declared body rejection, truncated-body EOF handling, case-insensitive Content-Length parsing over real loopback TCP (3 async unit cases). A bounded timeout prevents a broken parser from hanging the test harness; this is not production read-timeout coverage.
- Runtime configuration: zero notional, inverted progress/hold windows, and NaN momentum rejection (1 unit case).
- `tests/safety_lifecycle.rs`: market-readiness gate, idempotent stop, audit-fault restart denial, execution fault reporting, 16 competing async reservations yield one winner, inclusive market-window boundaries, invalid intervals (6 integration cases).
- Reused existing fixtures; added no Rust dependencies and changed no production statements.

## Running the suite

Final local Rust version: 1.99.1, installed into temporary directories because no
Rust toolchain was initially available on PATH. Existing crates remain locked.
Run `cargo test --locked --all-targets --all-features` and
`cargo test --locked --doc --all-features` from the repository.
The first command passed 21 unit plus 6 integration cases; documentation tests ran
successfully with 0 cases. Cargo also executed the binary test targets with 0 cases.
Tests use synthetic values and loopback sockets; no trading credentials or live
exchange operations are required.

## Verified result (2026-10-31)

27 tests executed, 27 passed, 0 failures, 0 errors, 0 skipped.
10 cases were added in this change. Results come from actual local execution,
not an inferred GitHub Actions result.

## CI

`.github/workflows/tests.yml` runs on push, pull request, and manual dispatch.
Actions are pinned to verified commit SHAs, permissions are read-only, overlapping
runs are cancelled, and jobs have a 20-minute timeout. Java/frontend workflows
upload test reports even on failure. All four workflows passed actionlint 1.7.7.
GitHub-hosted execution has not been verified. Local test execution was completed
on 2026-10-31; commit metadata dates do not represent additional test runs.

## Remaining gaps and boundaries

- WebSocket reconnection/backoff, audit queue exhaustion, exchange request timeouts/retries, execution revalidation, exit decisions, and SDK error contracts need additional coverage.
- Tests cover defined fault gates, not all possible fault/stop/start sequences.
- Rust 1.99.1 emits a deprecation warning for the existing fetch_update call and a future-compatibility notice for proc-macro-error2. No live trading, market-data smoke run, or deployment was performed.
