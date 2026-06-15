#![cfg(feature = "kafka-schema-registry")]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::{
    Json, Router, extract::Path, extract::State, http::HeaderMap, http::StatusCode, routing::get,
};
use shove::schema_registry::{SchemaId, SchemaRegistry, SchemaRegistryAuth};

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

#[tokio::test]
async fn configured_headers_are_sent_on_registry_requests() {
    let (url, state) = spawn_header_mock().await;
    let registry = SchemaRegistry::builder(url)
        .header("CF-Access-Client-Id", "client-id-123")
        .header("CF-Access-Client-Secret", "client-secret-456")
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
    let registry = SchemaRegistry::builder(url)
        .auth(SchemaRegistryAuth::Bearer("token-abc".into()))
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
