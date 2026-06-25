use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::config::EngineConfig;
use crate::health::HealthState;
use crate::market::now_ms;
use crate::runtime_config::{JavaStrategyConfig, StrategyStore};

const MAX_REQUEST_BYTES: usize = 16_384;

struct ControlContext {
    health: Arc<HealthState>,
    strategy_store: Arc<StrategyStore>,
    mutation: Mutex<()>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum TradingMode {
    DryRun,
    Live,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StartRequest {
    mode: TradingMode,
    config_version: String,
    lease_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HeartbeatRequest {
    config_version: String,
    sent_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StopRequest {
    reason: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    status: &'static str,
    accepting_commands: bool,
    config_version: Option<String>,
}

struct HttpRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

pub async fn run(
    config: Arc<EngineConfig>,
    health: Arc<HealthState>,
    strategy_store: Arc<StrategyStore>,
) -> Result<()> {
    let listener = TcpListener::bind(&config.control.bind_addr).await?;
    info!(bind = %config.control.bind_addr, "Control plane listening");
    serve(listener, health, strategy_store).await
}

async fn serve(
    listener: TcpListener,
    health: Arc<HealthState>,
    strategy_store: Arc<StrategyStore>,
) -> Result<()> {
    let context = Arc::new(ControlContext {
        health,
        strategy_store,
        mutation: Mutex::new(()),
    });
    loop {
        let (stream, _) = listener.accept().await?;
        let connection_context = Arc::clone(&context);
        tokio::spawn(async move {
            if let Err(error) = handle(stream, connection_context).await {
                warn!(%error, "Control request failed");
            }
        });
    }
}

pub async fn watchdog(health: Arc<HealthState>) {
    loop {
        let lease_timeout_ms = health.lease_timeout_ms();
        let poll_ms = (lease_timeout_ms / 2).clamp(1, 1_000);
        tokio::time::sleep(tokio::time::Duration::from_millis(poll_ms)).await;
        if health.heartbeat_expired(now_ms()) {
            health.trip(4);
        }
    }
}

async fn handle(mut stream: TcpStream, context: Arc<ControlContext>) -> Result<()> {
    let request = match read_request(&mut stream).await {
        Ok(request) => request,
        Err(error) => {
            write_json_error(&mut stream, "400 Bad Request", &error.to_string()).await?;
            return Ok(());
        }
    };
    let response = route(&request, &context).await;
    write_response(&mut stream, response).await
}

async fn route(request: &HttpRequest, context: &ControlContext) -> HttpResponse {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/v1/health") => health_response(context),
        ("PUT", "/v1/config") => update_config(request, context).await,
        ("POST", "/v1/control/start") => start(request, context).await,
        ("POST", "/v1/control/heartbeat") => heartbeat(request, context).await,
        ("POST", "/v1/control/stop") => stop(request, context).await,
        _ => HttpResponse::json("404 Not Found", &serde_json::json!({"error":"not_found"})),
    }
}

fn health_response(context: &ControlContext) -> HttpResponse {
    let config_version = context.strategy_store.version().unwrap_or(None);
    HttpResponse::json(
        "200 OK",
        &HealthResponse {
            status: if context.health.is_faulted() {
                "DOWN"
            } else {
                "UP"
            },
            accepting_commands: context.health.accepting_commands(),
            config_version,
        },
    )
}

async fn update_config(request: &HttpRequest, context: &ControlContext) -> HttpResponse {
    let config: JavaStrategyConfig = match parse_body(request) {
        Ok(config) => config,
        Err(error) => {
            context.health.stop();
            return HttpResponse::error("400 Bad Request", error);
        }
    };
    if let Err(error) = config.validate() {
        context.health.stop();
        return HttpResponse::error("422 Unprocessable Entity", error);
    }
    let _guard = context.mutation.lock().await;
    context.health.stop();
    match context.strategy_store.update(&config) {
        Ok(_) => HttpResponse::empty("204 No Content"),
        Err(error) => HttpResponse::error("500 Internal Server Error", error),
    }
}

async fn start(request: &HttpRequest, context: &ControlContext) -> HttpResponse {
    let request: StartRequest = match parse_body(request) {
        Ok(request) => request,
        Err(error) => {
            context.health.stop();
            return HttpResponse::error("400 Bad Request", error);
        }
    };
    let _requested_mode = request.mode;
    if !valid_config_version(&request.config_version)
        || request.lease_timeout_ms == 0
        || request.lease_timeout_ms > 300_000
    {
        context.health.stop();
        return HttpResponse::error("422 Unprocessable Entity", "invalid_start_request");
    }
    let _guard = context.mutation.lock().await;
    if !context
        .strategy_store
        .version_matches(&request.config_version)
    {
        context.health.stop();
        return HttpResponse::error("409 Conflict", "config_version_mismatch");
    }
    if context.health.is_running() && context.health.lease_timeout_ms() != request.lease_timeout_ms
    {
        context.health.stop();
        return HttpResponse::error("409 Conflict", "start_parameters_conflict");
    }
    match context.health.start(now_ms(), request.lease_timeout_ms) {
        Ok(()) => HttpResponse::empty("204 No Content"),
        Err(error) => HttpResponse::error("409 Conflict", error),
    }
}

async fn heartbeat(request: &HttpRequest, context: &ControlContext) -> HttpResponse {
    let request: HeartbeatRequest = match parse_body(request) {
        Ok(request) => request,
        Err(error) => {
            context.health.stop();
            return HttpResponse::error("400 Bad Request", error);
        }
    };
    if !valid_config_version(&request.config_version) || !valid_instant(&request.sent_at) {
        context.health.stop();
        return HttpResponse::error("422 Unprocessable Entity", "invalid_heartbeat");
    }
    let _guard = context.mutation.lock().await;
    if !context
        .strategy_store
        .version_matches(&request.config_version)
    {
        context.health.stop();
        return HttpResponse::error("409 Conflict", "config_version_mismatch");
    }
    match context.health.heartbeat(now_ms()) {
        Ok(()) => HttpResponse::empty("204 No Content"),
        Err(error) => HttpResponse::error("409 Conflict", error),
    }
}

async fn stop(request: &HttpRequest, context: &ControlContext) -> HttpResponse {
    context.health.stop();
    let request: StopRequest = match parse_body(request) {
        Ok(request) => request,
        Err(error) => return HttpResponse::error("400 Bad Request", error),
    };
    if request.reason.trim().is_empty() || request.reason.len() > 512 {
        return HttpResponse::error("422 Unprocessable Entity", "invalid_stop_reason");
    }
    let _guard = context.mutation.lock().await;
    HttpResponse::empty("204 No Content")
}

fn parse_body<T: DeserializeOwned>(request: &HttpRequest) -> Result<T, &'static str> {
    if request.body.is_empty() {
        return Err("missing_request_body");
    }
    serde_json::from_slice(&request.body).map_err(|_| "invalid_json_body")
}

fn valid_instant(value: &str) -> bool {
    value.len() >= 20
        && value.as_bytes().get(4) == Some(&b'-')
        && value.as_bytes().get(7) == Some(&b'-')
        && value.contains('T')
        && (value.ends_with('Z')
            || value
                .get(value.len().saturating_sub(6)..)
                .is_some_and(|offset| offset.starts_with('+') || offset.starts_with('-')))
}

fn valid_config_version(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 128
}

async fn read_request(stream: &mut TcpStream) -> Result<HttpRequest> {
    let mut bytes = Vec::with_capacity(1_024);
    let mut chunk = [0_u8; 1_024];
    let header_end = loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            anyhow::bail!("connection closed before request completed");
        }
        bytes.extend_from_slice(&chunk[..read]);
        if bytes.len() > MAX_REQUEST_BYTES {
            anyhow::bail!("request too large");
        }
        if let Some(position) = find_header_end(&bytes) {
            break position;
        }
    };
    let headers = std::str::from_utf8(&bytes[..header_end])?;
    let mut lines = headers.lines();
    let mut request_line = lines
        .next()
        .context("missing request line")?
        .split_whitespace();
    let method = request_line.next().context("missing method")?.to_owned();
    let path = request_line.next().context("missing path")?.to_owned();
    let content_length = lines
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    if header_end + 4 + content_length > MAX_REQUEST_BYTES {
        anyhow::bail!("request body too large");
    }
    let required = header_end + 4 + content_length;
    while bytes.len() < required {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            anyhow::bail!("connection closed before body completed");
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    Ok(HttpRequest {
        method,
        path,
        body: bytes[header_end + 4..required].to_vec(),
    })
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

struct HttpResponse {
    status: &'static str,
    body: Vec<u8>,
}

impl HttpResponse {
    fn empty(status: &'static str) -> Self {
        Self {
            status,
            body: Vec::new(),
        }
    }

    fn error(status: &'static str, error: &str) -> Self {
        Self::json(status, &serde_json::json!({"error":error}))
    }

    fn json(status: &'static str, body: &impl Serialize) -> Self {
        Self {
            status,
            body: serde_json::to_vec(body)
                .unwrap_or_else(|_| br#"{"error":"serialization"}"#.to_vec()),
        }
    }
}

async fn write_response(stream: &mut TcpStream, response: HttpResponse) -> Result<()> {
    let headers = format!(
        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        response.status,
        response.body.len()
    );
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(&response.body).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn write_json_error(stream: &mut TcpStream, status: &'static str, error: &str) -> Result<()> {
    write_response(stream, HttpResponse::error(status, error)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        ControlConfig, ExecutionMode, IntegrationConfig, MarketConfig, RiskConfig, StrategyConfig,
    };

    #[tokio::test]
    async fn java_v1_contract_and_lease_are_fail_closed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let config = Arc::new(test_config(address.to_string()));
        let health = Arc::new(HealthState::new());
        let strategy_store = Arc::new(StrategyStore::new(&config.strategy, &config.risk));
        health.set_market_ready(true);
        let server = tokio::spawn(serve(
            listener,
            Arc::clone(&health),
            Arc::clone(&strategy_store),
        ));

        let initial = request(address, "GET", "/v1/health", None).await;
        assert_eq!(initial.status, 200);
        assert_eq!(initial.json["status"], "UP");
        assert_eq!(initial.json["acceptingCommands"], true);
        assert!(initial.json["configVersion"].is_null());
        assert_eq!(request(address, "GET", "/health", None).await.status, 404);

        let mut invalid_config = strategy_json("invalid");
        invalid_config["upOutcome"] = serde_json::Value::String("UP".into());
        assert_eq!(
            request(address, "PUT", "/v1/config", Some(invalid_config))
                .await
                .status,
            422
        );

        assert_eq!(
            request(
                address,
                "PUT",
                "/v1/config",
                Some(strategy_json("momentum-v1"))
            )
            .await
            .status,
            204
        );
        let configured = request(address, "GET", "/v1/health", None).await;
        assert_eq!(configured.json["configVersion"], "momentum-v1");

        let start_body = serde_json::json!({
            "mode":"DRY_RUN",
            "configVersion":"momentum-v1",
            "leaseTimeoutMs":20
        });
        assert_eq!(
            request(
                address,
                "POST",
                "/v1/control/start",
                Some(start_body.clone())
            )
            .await
            .status,
            204
        );
        assert_eq!(
            request(address, "POST", "/v1/control/start", Some(start_body))
                .await
                .status,
            204
        );
        assert_eq!(
            request(
                address,
                "POST",
                "/v1/control/heartbeat",
                Some(serde_json::json!({
                    "configVersion":"wrong",
                    "sentAt":"2026-06-23T12:00:00Z"
                })),
            )
            .await
            .status,
            204
        );
        assert!(!health.is_running());
        assert_eq!(
            request(
                address,
                "POST",
                "/v1/control/start",
                Some(serde_json::json!({
                    "mode":"DRY_RUN",
                    "configVersion":"momentum-v1",
                    "leaseTimeoutMs":20
                }))
            )
            .await
            .status,
            204
        );
        assert_eq!(
            request(
                address,
                "POST",
                "/v1/control/heartbeat",
                Some(serde_json::json!({
                    "configVersion":"momentum-v1",
                    "sentAt":"2026-06-23T12:00:00Z"
                })),
            )
            .await
            .status,
            204
        );
        assert_eq!(
            request(
                address,
                "POST",
                "/v1/control/stop",
                Some(serde_json::json!({"reason":"operator request"})),
            )
            .await
            .status,
            204
        );
        assert_eq!(
            request(
                address,
                "POST",
                "/v1/control/stop",
                Some(serde_json::json!({"reason":"operator request"})),
            )
            .await
            .status,
            204
        );

        assert_eq!(
            request(
                address,
                "POST",
                "/v1/control/start",
                Some(serde_json::json!({
                    "mode":"LIVE",
                    "configVersion":"momentum-v1",
                    "leaseTimeoutMs":20
                })),
            )
            .await
            .status,
            409
        );
        assert_eq!(
            request(
                address,
                "POST",
                "/v1/control/start",
                Some(serde_json::json!({
                    "mode":"DRY_RUN",
                    "configVersion":"wrong",
                    "leaseTimeoutMs":20
                }))
            )
            .await
            .status,
            409
        );

        assert_eq!(
            request(
                address,
                "POST",
                "/v1/control/start",
                Some(serde_json::json!({
                    "mode":"DRY_RUN",
                    "configVersion":"momentum-v1",
                    "leaseTimeoutMs":20
                }))
            )
            .await
            .status,
            204
        );
        let lease = tokio::spawn(watchdog(Arc::clone(&health)));
        tokio::time::sleep(tokio::time::Duration::from_millis(60)).await;
        assert!(health.is_faulted());
        assert_eq!(health.snapshot().fault, "heartbeat_timeout");

        lease.abort();
        server.abort();
    }

    #[tokio::test]
    async fn config_update_stops_running_engine_and_swaps_snapshot() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let config = Arc::new(test_config(address.to_string()));
        let health = Arc::new(HealthState::new());
        let strategy_store = Arc::new(StrategyStore::new(&config.strategy, &config.risk));
        health.set_market_ready(true);
        let server = tokio::spawn(serve(
            listener,
            Arc::clone(&health),
            Arc::clone(&strategy_store),
        ));
        request(address, "PUT", "/v1/config", Some(strategy_json("v1"))).await;
        request(
            address,
            "POST",
            "/v1/control/start",
            Some(serde_json::json!({
                "mode":"DRY_RUN","configVersion":"v1","leaseTimeoutMs":2000
            })),
        )
        .await;
        assert!(health.is_running());
        assert_eq!(
            request(address, "PUT", "/v1/config", Some(strategy_json("v2")))
                .await
                .status,
            204
        );
        assert!(!health.is_running());
        assert!(strategy_store.version_matches("v2"));
        assert_eq!(strategy_store.load().unwrap().momentum_threshold_usd, 8.0);
        server.abort();
    }

    struct TestResponse {
        status: u16,
        json: serde_json::Value,
    }

    async fn request(
        address: std::net::SocketAddr,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> TestResponse {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let body = body.map(|body| body.to_string()).unwrap_or_default();
        let raw = format!(
            "{method} {path} HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(raw.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        let status = headers.split_whitespace().nth(1).unwrap().parse().unwrap();
        TestResponse {
            status,
            json: if body.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::from_str(body).unwrap()
            },
        }
    }

    fn strategy_json(version: &str) -> serde_json::Value {
        serde_json::json!({
            "configVersion":version,
            "momentumWindowMs":100,
            "momentumThresholdUsd":8,
            "executionLatencyMs":100,
            "holdMs":5000,
            "minProgress":0.05,
            "maxProgress":0.90,
            "maxSpread":0.02,
            "maxNotionalUsd":100,
            "maxShares":500,
            "upOutcome":"YES",
            "downOutcome":"NO"
        })
    }

    fn test_config(bind_addr: String) -> EngineConfig {
        EngineConfig {
            execution_mode: ExecutionMode::DryRun,
            market: MarketConfig {
                gamma_base_url: "https://gamma-api.polymarket.com".into(),
                slug_prefix: "btc-updown-5m".into(),
                interval_seconds: 300,
                discovery_poll_ms: 1_000,
            },
            strategy: StrategyConfig {
                momentum_window_ms: 100,
                momentum_threshold_usd: 8.0,
                execution_latency_ms: 100,
                hold_ms: 5_000,
                min_market_progress: 0.05,
                max_market_progress: 0.90,
                max_book_age_ms: 300,
            },
            risk: RiskConfig {
                max_spread: 0.02,
                max_notional_usd: 100.0,
                max_shares: 500,
                max_open_positions: 5,
                daily_loss_limit_usd: 500.0,
            },
            integration: IntegrationConfig {
                java_audit_endpoint: None,
            },
            control: ControlConfig { bind_addr },
        }
    }
}
