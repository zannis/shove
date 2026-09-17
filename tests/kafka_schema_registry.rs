#![cfg(feature = "kafka-schema-registry")]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::response::{IntoResponse as _, Response};
use axum::{
    Json, Router, extract::Path, extract::State, http::HeaderMap, http::StatusCode, routing::get,
};
use shove::schema_registry::{SchemaId, SchemaRegistry, SchemaRegistryAuth, SchemaRegistryError};

#[derive(Clone)]
struct MockState {
    calls: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct StatusMockState {
    calls: Arc<AtomicUsize>,
    status: u16,
}

async fn status_versions(State(s): State<StatusMockState>, Path(_id): Path<u32>) -> StatusCode {
    s.calls.fetch_add(1, Ordering::SeqCst);
    StatusCode::from_u16(s.status).unwrap()
}

async fn status_schema(State(s): State<StatusMockState>, Path(_id): Path<u32>) -> StatusCode {
    s.calls.fetch_add(1, Ordering::SeqCst);
    StatusCode::from_u16(s.status).unwrap()
}

/// Answers `status` for the first `failures` requests, then normally.
#[derive(Clone)]
struct RecoveringMockState {
    calls: Arc<AtomicUsize>,
    failures_left: Arc<AtomicUsize>,
    status: u16,
}

impl RecoveringMockState {
    fn answer(&self, body: serde_json::Value) -> Response {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let failing = self
            .failures_left
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        if failing {
            StatusCode::from_u16(self.status).unwrap().into_response()
        } else {
            Json(body).into_response()
        }
    }
}

async fn recovering_versions(
    State(s): State<RecoveringMockState>,
    Path(_id): Path<u32>,
) -> Response {
    s.answer(serde_json::json!([{ "subject": "orders-value", "version": 3 }]))
}

async fn recovering_schema(State(s): State<RecoveringMockState>, Path(_id): Path<u32>) -> Response {
    s.answer(serde_json::json!({ "schema": "{}", "schemaType": "JSON" }))
}

/// Spawn a mock registry that answers `status` to the first `failures`
/// requests and normally afterwards, returning (base_url, calls-counter).
async fn spawn_recovering_mock(status: u16, failures: usize) -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let state = RecoveringMockState {
        calls: calls.clone(),
        failures_left: Arc::new(AtomicUsize::new(failures)),
        status,
    };
    let app = Router::new()
        .route("/schemas/ids/{id}/versions", get(recovering_versions))
        .route("/schemas/ids/{id}", get(recovering_schema))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), calls)
}

/// Spawn a mock registry that always returns the given HTTP status, returning (base_url, calls-counter).
async fn spawn_status_mock(status: u16) -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let state = StatusMockState {
        calls: calls.clone(),
        status,
    };
    let app = Router::new()
        .route("/schemas/ids/{id}/versions", get(status_versions))
        .route("/schemas/ids/{id}", get(status_schema))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), calls)
}

async fn versions(State(s): State<MockState>, Path(_id): Path<u32>) -> Json<serde_json::Value> {
    s.calls.fetch_add(1, Ordering::SeqCst);
    Json(serde_json::json!([{ "subject": "orders-value", "version": 3 }]))
}

async fn schema(State(s): State<MockState>, Path(_id): Path<u32>) -> Json<serde_json::Value> {
    s.calls.fetch_add(1, Ordering::SeqCst);
    Json(serde_json::json!({ "schema": "{}", "schemaType": "JSON" }))
}

/// Spawn the mock registry, return (base_url, calls-counter).
async fn spawn_mock() -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let state = MockState {
        calls: calls.clone(),
    };
    let app = Router::new()
        .route("/schemas/ids/{id}/versions", get(versions))
        .route("/schemas/ids/{id}", get(schema))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), calls)
}

/// Captures the inbound headers seen by each registry endpoint.
#[derive(Clone)]
struct HeaderMockState {
    versions_headers: Arc<Mutex<Option<HeaderMap>>>,
    schema_headers: Arc<Mutex<Option<HeaderMap>>>,
}

async fn header_versions(
    State(s): State<HeaderMockState>,
    headers: HeaderMap,
    Path(_id): Path<u32>,
) -> Json<serde_json::Value> {
    *s.versions_headers.lock().unwrap() = Some(headers);
    Json(serde_json::json!([{ "subject": "orders-value", "version": 3 }]))
}

async fn header_schema(
    State(s): State<HeaderMockState>,
    headers: HeaderMap,
    Path(_id): Path<u32>,
) -> Json<serde_json::Value> {
    *s.schema_headers.lock().unwrap() = Some(headers);
    Json(serde_json::json!({ "schema": "{}", "schemaType": "JSON" }))
}

/// Spawn a mock registry that records the headers each endpoint received.
async fn spawn_header_mock() -> (String, HeaderMockState) {
    let state = HeaderMockState {
        versions_headers: Arc::new(Mutex::new(None)),
        schema_headers: Arc::new(Mutex::new(None)),
    };
    let app = Router::new()
        .route("/schemas/ids/{id}/versions", get(header_versions))
        .route("/schemas/ids/{id}", get(header_schema))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), state)
}

/// Redirects every request to the same path on `target`, counting the hops.
#[derive(Clone)]
struct RedirectMockState {
    target: String,
    hops: Arc<AtomicUsize>,
}

async fn redirect_versions(
    State(s): State<RedirectMockState>,
    Path(id): Path<u32>,
) -> (StatusCode, [(&'static str, String); 1]) {
    s.hops.fetch_add(1, Ordering::SeqCst);
    (
        StatusCode::FOUND,
        [(
            "location",
            format!("{}/schemas/ids/{id}/versions", s.target),
        )],
    )
}

async fn redirect_schema(
    State(s): State<RedirectMockState>,
    Path(id): Path<u32>,
) -> (StatusCode, [(&'static str, String); 1]) {
    s.hops.fetch_add(1, Ordering::SeqCst);
    (
        StatusCode::FOUND,
        [("location", format!("{}/schemas/ids/{id}", s.target))],
    )
}

/// Spawn a mock that 302s every registry endpoint to `target`, returning
/// (base_url, hop-counter).
async fn spawn_redirecting_mock(target: String) -> (String, Arc<AtomicUsize>) {
    let hops = Arc::new(AtomicUsize::new(0));
    let state = RedirectMockState {
        target,
        hops: hops.clone(),
    };
    let app = Router::new()
        .route("/schemas/ids/{id}/versions", get(redirect_versions))
        .route("/schemas/ids/{id}", get(redirect_schema))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), hops)
}

/// Base URL userinfo is a credential channel: `reqwest` strips `user:pass@`
/// from the URL and replays it as an `Authorization: Basic` header, so a
/// `http://user:pass@registry` base URL puts a reusable secret on a plaintext
/// wire without any [`SchemaRegistryAuth`] ever being configured.
#[tokio::test]
async fn url_userinfo_reaches_the_registry_as_basic_auth() {
    let (url, state) = spawn_header_mock().await;
    let with_userinfo = url.replace("http://", "http://sr-user:sr-pass@");
    // The opt-in is required precisely because this is a credential. That it
    // still reaches the registry is the point: the guard classifies userinfo
    // correctly rather than merely rejecting a URL shape.
    let registry = SchemaRegistry::builder(with_userinfo)
        .allow_plaintext_credentials()
        .build();
    registry.resolve(SchemaId(1)).await.unwrap();

    let headers = state
        .versions_headers
        .lock()
        .unwrap()
        .clone()
        .expect("endpoint should have been called");
    assert_eq!(
        headers.get("authorization").unwrap(),
        // base64("sr-user:sr-pass")
        "Basic c3ItdXNlcjpzci1wYXNz",
        "URL userinfo must be recognised as a credential: it is on the wire"
    );
}

/// `url` treats a missing or repeated `//` after a special scheme as a
/// recoverable syntax violation, so these spellings reach the same origin as
/// `http://…` and put the same credentials on the same plaintext wire. Each was
/// confirmed against a live mock registry before this guard existed: all three
/// resolved successfully and sent `Authorization: Basic c3ItdXNlcjpzci1wYXNz`.
#[test]
#[should_panic(expected = "schema registry credentials require TLS")]
fn single_slash_url_with_userinfo_is_refused() {
    let _ = SchemaRegistry::builder("http:sr-user:sr-pass@sr:8081").build();
}

#[test]
#[should_panic(expected = "schema registry credentials require TLS")]
fn triple_slash_url_with_userinfo_is_refused() {
    let _ = SchemaRegistry::builder("http:///sr-user:sr-pass@sr:8081").build();
}

/// The same normalisation defeated the `SchemaRegistryAuth` check as shipped:
/// `"http:host".starts_with("http://")` is false, so a bearer token against
/// `http:registry:8081` built with no opt-in and went out in cleartext.
#[test]
#[should_panic(expected = "schema registry credentials require TLS")]
fn single_slash_url_does_not_bypass_the_auth_guard() {
    let _ = SchemaRegistry::builder("http:sr:8081")
        .auth(SchemaRegistryAuth::Bearer("token-abc".into()))
        .build();
}

/// A base URL that does not parse cannot be shown to be encrypted, so
/// credentials against it are refused rather than assumed safe.
#[test]
#[should_panic(expected = "schema registry credentials require TLS")]
fn unparseable_base_url_with_credentials_is_refused() {
    let _ = SchemaRegistry::builder("sr:8081")
        .auth(SchemaRegistryAuth::Bearer("token-abc".into()))
        .build();
}

#[tokio::test]
async fn configured_headers_are_sent_on_registry_requests() {
    let (url, state) = spawn_header_mock().await;
    // `header()` treats its value as a secret, so a plaintext mock needs the
    // explicit opt-in. Before that was true this test asserted a Cloudflare
    // Access client secret travelling over plaintext with nothing objecting.
    let registry = SchemaRegistry::builder(url)
        .header("CF-Access-Client-Id", "client-id-123")
        .header("CF-Access-Client-Secret", "client-secret-456")
        .allow_plaintext_credentials()
        .build();
    registry.resolve(SchemaId(1)).await.unwrap();

    for captured in [&state.versions_headers, &state.schema_headers] {
        let headers = captured
            .lock()
            .unwrap()
            .clone()
            .expect("endpoint should have been called");
        assert_eq!(
            headers.get("cf-access-client-id").unwrap(),
            "client-id-123",
            "configured client-id header must reach the registry"
        );
        assert_eq!(
            headers.get("cf-access-client-secret").unwrap(),
            "client-secret-456",
            "configured client-secret header must reach the registry"
        );
    }
}

#[tokio::test]
async fn custom_headers_coexist_with_bearer_auth() {
    let (url, state) = spawn_header_mock().await;
    // The mock is plaintext loopback, so the credential guard in `build()`
    // applies and has to be opted out of explicitly. This doubles as the
    // end-to-end proof that the opt-in yields a working client, not merely one
    // that survives `build()`.
    let registry = SchemaRegistry::builder(url)
        .auth(SchemaRegistryAuth::Bearer("token-abc".into()))
        .allow_plaintext_credentials()
        .header("CF-Access-Client-Id", "client-id-123")
        .build();
    registry.resolve(SchemaId(1)).await.unwrap();

    let headers = state
        .versions_headers
        .lock()
        .unwrap()
        .clone()
        .expect("endpoint should have been called");
    assert_eq!(
        headers.get("authorization").unwrap(),
        "Bearer token-abc",
        "bearer auth must still be applied alongside custom headers"
    );
    assert_eq!(
        headers.get("cf-access-client-id").unwrap(),
        "client-id-123",
        "custom header must coexist with auth"
    );
}

#[test]
#[should_panic(expected = "schema registry credentials require TLS")]
fn url_userinfo_over_plaintext_is_refused_without_opt_in() {
    let _ = SchemaRegistry::builder("http://sr-user:sr-pass@sr:8081").build();
}

#[test]
#[should_panic(expected = "schema registry credentials require TLS")]
fn secret_header_over_plaintext_is_refused_without_opt_in() {
    let _ = SchemaRegistry::builder("http://sr:8081")
        .header("CF-Access-Client-Secret", "client-secret-456")
        .build();
}

/// A header the caller states carries no secret needs no opt-in, and still
/// reaches the registry. Without this escape the only way to send an `Accept`
/// header to a development registry would be the credential opt-in — which is
/// not scoped to one header and would also permit a real bearer token.
#[tokio::test]
async fn non_secret_header_needs_no_opt_in_and_is_still_sent() {
    let (url, state) = spawn_header_mock().await;
    let registry = SchemaRegistry::builder(url)
        .non_secret_header("X-Client-Build", "2026.08.30")
        .build();
    registry.resolve(SchemaId(1)).await.unwrap();

    let headers = state
        .versions_headers
        .lock()
        .unwrap()
        .clone()
        .expect("endpoint should have been called");
    assert_eq!(
        headers.get("x-client-build").unwrap(),
        "2026.08.30",
        "a non-secret header must still reach the registry"
    );
}

/// `reqwest` follows redirects by default and, across an origin change, strips
/// only `Authorization`, `Cookie`, `Proxy-Authorization` and
/// `WWW-Authenticate`. A custom secret header is not on that list, so a
/// redirect would replay it to whatever host — and whatever scheme — the
/// `Location` names. A credential-bearing client therefore declines to follow.
#[tokio::test]
async fn a_credential_bearing_client_does_not_follow_redirects() {
    let (leak_url, leak_state) = spawn_header_mock().await;
    let (registry_url, _redirects) = spawn_redirecting_mock(leak_url).await;

    let registry = SchemaRegistry::builder(registry_url)
        .header("CF-Access-Client-Secret", "client-secret-456")
        .allow_plaintext_credentials()
        .build();

    let error = registry
        .resolve(SchemaId(1))
        .await
        .expect_err("the redirect must surface as an error, not be followed");
    assert!(
        leak_state.versions_headers.lock().unwrap().is_none(),
        "the redirect target must never see the secret header"
    );
    // The failure has to name the redirect and the base URL, or an operator
    // debugs the registry instead of their configuration.
    let message = error.to_string();
    assert!(
        message.contains("redirect") && message.contains("base URL"),
        "the error must explain the redirect and what to change: {message}"
    );
    // The converse of `an_unfollowable_redirect_does_not_blame_absent_credentials`:
    // here credentials really are why the hop was refused, so the message must
    // say so.
    assert!(
        message.contains("credential"),
        "a refusal caused by credentials must name them: {message}"
    );
}

/// Without credentials there is nothing to disclose, so redirect handling is
/// left alone — the guard must not silently change behaviour for the common
/// unauthenticated registry.
#[tokio::test]
async fn an_unauthenticated_client_still_follows_redirects() {
    let (target_url, target_state) = spawn_header_mock().await;
    let (registry_url, _redirects) = spawn_redirecting_mock(target_url).await;

    let registry = SchemaRegistry::builder(registry_url).build();
    registry
        .resolve(SchemaId(1))
        .await
        .expect("an unauthenticated client follows the redirect as before");

    assert!(
        target_state.versions_headers.lock().unwrap().is_some(),
        "the redirect target should have been reached"
    );
}

#[test]
#[should_panic(expected = "valid HTTP header name")]
fn invalid_header_name_is_rejected() {
    let _ = SchemaRegistry::builder("http://localhost:8081").header("Bad Header Name", "value");
}

#[test]
#[should_panic(expected = "valid HTTP header value")]
fn invalid_header_value_is_rejected() {
    let _ = SchemaRegistry::builder("http://localhost:8081").header("X-Custom", "bad\nvalue");
}

#[tokio::test]
async fn resolve_returns_subject_and_type() {
    let (url, _calls) = spawn_mock().await;
    let registry = SchemaRegistry::builder(url).build();
    let resolved = registry.resolve(SchemaId(1)).await.unwrap();
    assert_eq!(resolved.primary_subject(), Some("orders-value"));
}

#[tokio::test]
async fn second_resolve_is_served_from_cache() {
    let (url, calls) = spawn_mock().await;
    let registry = SchemaRegistry::builder(url).build();
    registry.resolve(SchemaId(1)).await.unwrap();
    let after_first = calls.load(Ordering::SeqCst);
    registry.resolve(SchemaId(1)).await.unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        after_first,
        "cache hit must not call registry"
    );
}

#[tokio::test]
async fn concurrent_cold_misses_single_flight_to_one_fetch() {
    let (url, calls) = spawn_mock().await;
    let registry = SchemaRegistry::builder(url).build();
    let mut handles = Vec::new();
    for _ in 0..32 {
        let r = registry.clone();
        handles.push(tokio::spawn(async move {
            r.resolve(SchemaId(9)).await.unwrap()
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    // /versions + /schema = 2 HTTP calls for exactly one fetch.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "single-flight must collapse to one fetch"
    );
}

#[tokio::test]
async fn not_found_is_negative_cached() {
    let (url, calls) = spawn_status_mock(404).await;
    let registry = SchemaRegistry::builder(url).build();
    // First resolve hits the registry; /versions returns 404 -> NotFound immediately,
    // without calling /schema. Exactly 1 HTTP call.
    assert!(registry.resolve(SchemaId(1)).await.is_err());
    let after_first = calls.load(Ordering::SeqCst);
    assert_eq!(
        after_first, 1,
        "first resolve makes exactly one HTTP call (versions 404)"
    );
    // Second resolve within TTL is served from the negative cache: no new HTTP.
    assert!(registry.resolve(SchemaId(1)).await.is_err());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        after_first,
        "NotFound must be negative-cached"
    );
}

/// Not every 3xx that reaches the redirect arm was refused by the no-redirect
/// policy. `reqwest` cannot follow a 3xx that carries no `Location`, and never
/// follows a 300 or a 304 at all, so an unauthenticated client — which builds
/// with the *default* policy — lands in the same arm. Its diagnosis must
/// therefore describe what actually happened rather than blame credentials the
/// caller never configured, which would send an operator auditing an empty
/// configuration.
#[tokio::test]
async fn an_unfollowable_redirect_does_not_blame_absent_credentials() {
    for status in [300, 302, 304] {
        let (url, _calls) = spawn_status_mock(status).await;
        let registry = SchemaRegistry::builder(url).build();

        let error = registry
            .resolve(SchemaId(1))
            .await
            .expect_err("an unfollowable redirect must surface as an error");
        let message = error.to_string();

        assert!(
            message.contains("redirect"),
            "the error must still name the redirect ({status}): {message}"
        );
        assert!(
            !message.contains("credential"),
            "no credentials are configured, so the message must not blame them \
             ({status}): {message}"
        );
    }
}

#[tokio::test]
async fn retriable_error_is_not_negative_cached() {
    let (url, calls) = spawn_status_mock(503).await;
    // max_retries(0) so a 503 returns immediately as Transport{retriable:true} with no backoff sleep.
    let registry = SchemaRegistry::builder(url).max_retries(0).build();
    assert!(registry.resolve(SchemaId(1)).await.is_err());
    let after_first = calls.load(Ordering::SeqCst);
    // Second resolve must RETRY (not be suppressed by the negative cache) -> more HTTP calls.
    assert!(registry.resolve(SchemaId(1)).await.is_err());
    assert!(
        calls.load(Ordering::SeqCst) > after_first,
        "retriable errors must not be negative-cached"
    );
}

/// A registry that answers `status` once and then normally is resolved on
/// the client's own retry: the status is an answer about now, not about the
/// deployment, so it must neither end the consumer nor be negative-cached.
async fn recovers_after_one(status: u16) {
    let (url, calls) = spawn_recovering_mock(status, 1).await;
    let registry = SchemaRegistry::builder(url).max_retries(1).build();
    let schema = registry
        .resolve(SchemaId(1))
        .await
        .unwrap_or_else(|e| panic!("a {status} followed by a normal answer must resolve: {e}"));
    assert_eq!(schema.primary_subject(), Some("orders-value"));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "one {status}, its retry, then the schema fetch"
    );
}

/// With no retries left the same status surfaces as a retriable transport
/// error, which the consume loops wait out instead of dead-lettering.
async fn is_retriable_without_retries(status: u16) {
    let (url, _) = spawn_status_mock(status).await;
    let registry = SchemaRegistry::builder(url).max_retries(0).build();
    let err = registry
        .resolve(SchemaId(1))
        .await
        .expect_err("the status is an error until the registry answers");
    assert!(
        matches!(
            err,
            SchemaRegistryError::Transport {
                retriable: true,
                ..
            }
        ),
        "{status} must be retriable, got {err:?}"
    );
}

/// A registry whose connection drops before its declared body length has
/// arrived: every request gets a `200` with `Content-Length: 200`, a few
/// body bytes, and then the socket is closed.
async fn spawn_truncating_mock() -> (String, Arc<AtomicUsize>) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let calls = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served = calls.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            served.fetch_add(1, Ordering::SeqCst);
            let mut request = [0u8; 4096];
            let _ = socket.read(&mut request).await;
            let _ = socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                      Content-Length: 200\r\nConnection: close\r\n\r\n\
                      [{\"subject\": \"orders-value\"",
                )
                .await;
            let _ = socket.shutdown().await;
        }
    });
    (format!("http://{addr}"), calls)
}

/// A registry that answers `200` with a complete body that is not JSON.
async fn spawn_malformed_mock() -> String {
    let app = Router::new()
        .route("/schemas/ids/{id}/versions", get(|| async { "not json" }))
        .route("/schemas/ids/{id}", get(|| async { "not json" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

/// A `200` whose body is cut short is an outage, not the registry's verdict:
/// the client retries the read like a failed send, and with no retries left
/// it surfaces as a retriable transport error, which the consume loops wait
/// out instead of ending the consumer on it.
#[tokio::test]
async fn an_interrupted_success_body_is_a_retriable_transport_error() {
    let (url, calls) = spawn_truncating_mock().await;
    let registry = SchemaRegistry::builder(url).max_retries(1).build();
    let err = registry
        .resolve(SchemaId(1))
        .await
        .expect_err("a body cut short cannot resolve");
    assert!(
        matches!(
            err,
            SchemaRegistryError::Transport {
                retriable: true,
                ..
            }
        ),
        "an interrupted body is an outage, not a malformed answer: {err:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the read failure is retried once before it surfaces"
    );
}

/// A complete `200` body that does not parse is the registry's fault and
/// stays `Decode`, which the consume loops treat as a deployment fault.
#[tokio::test]
async fn a_malformed_success_body_is_a_decode_error() {
    let url = spawn_malformed_mock().await;
    let registry = SchemaRegistry::builder(url).max_retries(0).build();
    let err = registry
        .resolve(SchemaId(1))
        .await
        .expect_err("a body that is not JSON cannot resolve");
    assert!(
        matches!(err, SchemaRegistryError::Decode(_)),
        "a complete body that does not parse is a decode error: {err:?}"
    );
}

#[tokio::test]
async fn a_rate_limited_registry_is_retried_and_recovers() {
    recovers_after_one(429).await;
    is_retriable_without_retries(429).await;
}

#[tokio::test]
async fn a_request_timeout_from_the_registry_is_retried_and_recovers() {
    recovers_after_one(408).await;
    is_retriable_without_retries(408).await;
}

#[tokio::test]
async fn a_server_error_from_the_registry_is_retried_and_recovers() {
    recovers_after_one(503).await;
}

// ---------------------------------------------------------------------------
// latest_id (producer-side subject -> id lookup)
// ---------------------------------------------------------------------------

async fn subject_latest(
    State(s): State<MockState>,
    Path(subject): Path<String>,
) -> Json<serde_json::Value> {
    s.calls.fetch_add(1, Ordering::SeqCst);
    Json(serde_json::json!({
        "subject": subject,
        "version": 1,
        "id": 42,
        "schema": "{}",
    }))
}

/// Spawn a mock serving `GET /subjects/{subject}/versions/latest`, returning
/// (base_url, calls-counter).
async fn spawn_subject_latest_mock() -> (String, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let state = MockState {
        calls: calls.clone(),
    };
    let app = Router::new()
        .route("/subjects/{subject}/versions/latest", get(subject_latest))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), calls)
}

#[tokio::test]
async fn latest_id_returns_the_registered_id() {
    let (url, _calls) = spawn_subject_latest_mock().await;
    let registry = SchemaRegistry::builder(url).build();
    let id = registry
        .latest_id("orders-value")
        .await
        .expect("latest_id resolves the registered subject");
    assert_eq!(id, SchemaId(42));
}

#[tokio::test]
async fn second_latest_id_is_served_from_cache() {
    let (url, calls) = spawn_subject_latest_mock().await;
    let registry = SchemaRegistry::builder(url).build();

    assert_eq!(
        registry.latest_id("orders-value").await.unwrap(),
        SchemaId(42)
    );
    let after_first = calls.load(Ordering::SeqCst);
    assert_eq!(
        after_first, 1,
        "first latest_id makes exactly one HTTP call"
    );

    assert_eq!(
        registry.latest_id("orders-value").await.unwrap(),
        SchemaId(42)
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        after_first,
        "subject -> id must be cached: no second HTTP call"
    );
}
