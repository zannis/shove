#![cfg(feature = "kafka-schema-registry")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::{Json, Router, extract::Path, extract::State, routing::get};
use shove::schema_registry::{SchemaId, SchemaRegistry};

#[derive(Clone)]
struct MockState {
    calls: Arc<AtomicUsize>,
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

#[tokio::test]
async fn resolve_returns_subject_and_type() {
    let (url, _calls) = spawn_mock().await;
    let registry = SchemaRegistry::builder(url).build();
    let resolved = registry.resolve(SchemaId(1)).await.unwrap();
    assert_eq!(resolved.primary_subject(), Some("orders-value"));
}
