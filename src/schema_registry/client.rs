//! Async, cached Confluent Schema Registry client.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures_util::FutureExt as _;
use futures_util::future::{BoxFuture, Shared};
use reqwest::header::HeaderMap;

use crate::retry::Backoff;

use super::error::SchemaRegistryError;
use super::schema::{CachedSchema, SchemaType};
use super::wire::SchemaId;

/// Authentication for registry HTTP calls.
#[derive(Clone)]
pub enum SchemaRegistryAuth {
    None,
    Bearer(String),
    Basic { user: String, pass: String },
}

// Hand-written so the bearer token / basic-auth password never print. Do NOT
// switch to `#[derive(Debug)]` — derive would leak the secrets.
impl fmt::Debug for SchemaRegistryAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::Bearer(_) => f.debug_tuple("Bearer").field(&"<redacted>").finish(),
            Self::Basic { user, .. } => f
                .debug_struct("Basic")
                .field("user", user)
                .field("pass", &"<redacted>")
                .finish(),
        }
    }
}

type Result<T> = std::result::Result<T, SchemaRegistryError>;
type SharedResolve = Shared<BoxFuture<'static, Result<Arc<CachedSchema>>>>;

/// Cached registry client. `Arc`-shared; clone is a refcount bump. `'static`.
pub struct SchemaRegistry {
    base_url: Arc<str>,
    http: reqwest::Client,
    auth: SchemaRegistryAuth,
    headers: HeaderMap,
    max_retries: u32,
    negative_cache_ttl: Duration,
    // Tier 1: resolved schemas, immutable by id.
    resolved: DashMap<SchemaId, Arc<CachedSchema>>,
    // Tier 2: in-flight single-flight futures.
    inflight: DashMap<SchemaId, SharedResolve>,
    // Negative cache: id -> (instant inserted, error that caused the miss).
    negative: DashMap<SchemaId, (std::time::Instant, SchemaRegistryError)>,
    // Producer-side subject -> latest id cache (TopicNameStrategy lookups).
    subject_ids: DashMap<Arc<str>, SchemaId>,
}

impl SchemaRegistry {
    /// Start building a registry client for `base_url` (e.g. `http://sr:8081`).
    pub fn builder(base_url: impl Into<String>) -> SchemaRegistryBuilder {
        SchemaRegistryBuilder {
            base_url: base_url.into(),
            auth: SchemaRegistryAuth::None,
            headers: HeaderMap::new(),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            negative_cache_ttl: Duration::from_secs(30),
        }
    }

    /// Resolve a schema by id. Serves from cache on second call; collapses
    /// concurrent cold misses into a single HTTP fetch via single-flight.
    pub async fn resolve(&self, id: SchemaId) -> Result<Arc<CachedSchema>> {
        // Tier 1: resolved cache (lock-free read, no await).
        if let Some(hit) = self.resolved.get(&id) {
            return Ok(hit.clone());
        }
        // Negative cache: suppress hammering on a known-bad id within the TTL.
        if let Some(entry) = self.negative.get(&id) {
            let (at, err) = entry.value();
            if at.elapsed() < self.negative_cache_ttl {
                return Err(err.clone());
            }
        }
        // Tier 2: single-flight — collapse concurrent misses into one fetch.
        let shared = self.shared_fetch(id);
        let result = shared.await;
        match &result {
            Ok(schema) => {
                self.resolved.insert(id, schema.clone());
                self.inflight.remove(&id);
                self.negative.remove(&id);
            }
            Err(e) => {
                if !e.is_retriable() {
                    self.negative
                        .insert(id, (std::time::Instant::now(), e.clone()));
                }
                self.inflight.remove(&id);
            }
        }
        result
    }

    /// Resolve a subject to its latest schema id via
    /// `GET /subjects/{subject}/versions/latest`. Cached subject→id so the
    /// publisher hot path is a single lock-free map read after the first
    /// lookup. Producer-side counterpart to [`resolve`](Self::resolve).
    pub async fn latest_id(&self, subject: &str) -> Result<SchemaId> {
        if let Some(hit) = self.subject_ids.get(subject) {
            return Ok(*hit);
        }
        let ctx = FetchCtx {
            base_url: self.base_url.clone(),
            http: self.http.clone(),
            auth: self.auth.clone(),
            headers: self.headers.clone(),
            max_retries: self.max_retries,
        };
        let url = format!("{}/subjects/{}/versions/latest", self.base_url, subject);
        let not_found = SchemaRegistryError::Transport {
            retriable: false,
            message: format!("subject `{subject}` has no registered version (404)"),
        };
        let body = ctx.get_json(&url, not_found).await?;
        let id = body.get("id").and_then(|v| v.as_u64()).ok_or_else(|| {
            SchemaRegistryError::Decode("missing id in latest-version response".into())
        })?;
        let id = SchemaId(id as u32);
        self.subject_ids.insert(Arc::from(subject), id);
        Ok(id)
    }

    fn shared_fetch(&self, id: SchemaId) -> SharedResolve {
        use dashmap::mapref::entry::Entry;
        match self.inflight.entry(id) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let ctx = FetchCtx {
                    base_url: self.base_url.clone(),
                    http: self.http.clone(),
                    auth: self.auth.clone(),
                    headers: self.headers.clone(),
                    max_retries: self.max_retries,
                };
                let fut = async move { ctx.fetch(id).await };
                let shared: SharedResolve = fut.boxed().shared();
                e.insert(shared.clone());
                shared
            }
        }
    }
}

/// Owned context for a single boxed fetch future, ensuring `Send + 'static`.
struct FetchCtx {
    base_url: Arc<str>,
    http: reqwest::Client,
    auth: SchemaRegistryAuth,
    headers: HeaderMap,
    max_retries: u32,
}

impl FetchCtx {
    async fn fetch(self, id: SchemaId) -> Result<Arc<CachedSchema>> {
        let versions = self.get_versions(id).await?;
        let (raw, schema_type) = self.get_schema(id).await?;
        Ok(Arc::new(CachedSchema {
            id: id.0,
            schema_type,
            raw: Arc::from(raw.as_str()),
            subjects: versions.into(),
        }))
    }

    async fn get_versions(&self, id: SchemaId) -> Result<Vec<(Arc<str>, i32)>> {
        let url = format!("{}/schemas/ids/{}/versions", self.base_url, id.0);
        let body: serde_json::Value = self
            .get_json(&url, SchemaRegistryError::NotFound(id.0))
            .await?;
        let arr = body
            .as_array()
            .ok_or_else(|| SchemaRegistryError::Decode("expected array from /versions".into()))?;
        let mut out = Vec::with_capacity(arr.len());
        for v in arr {
            let subject = v
                .get("subject")
                .and_then(|s| s.as_str())
                .ok_or_else(|| SchemaRegistryError::Decode("missing subject".into()))?;
            let version = v.get("version").and_then(|n| n.as_i64()).unwrap_or(0) as i32;
            out.push((Arc::from(subject), version));
        }
        Ok(out)
    }

    async fn get_schema(&self, id: SchemaId) -> Result<(String, SchemaType)> {
        let url = format!("{}/schemas/ids/{}", self.base_url, id.0);
        let body: serde_json::Value = self
            .get_json(&url, SchemaRegistryError::NotFound(id.0))
            .await?;
        let raw = body
            .get("schema")
            .and_then(|s| s.as_str())
            .ok_or_else(|| SchemaRegistryError::Decode("missing schema".into()))?
            .to_string();
        let schema_type =
            SchemaType::from_registry(body.get("schemaType").and_then(|s| s.as_str()));
        Ok((raw, schema_type))
    }

    /// GET `url` as JSON with retry/auth/headers. `not_found` is returned on a
    /// 404 so callers can attribute it to the right resource (a schema id or a
    /// subject).
    async fn get_json(
        &self,
        url: &str,
        not_found: SchemaRegistryError,
    ) -> Result<serde_json::Value> {
        let mut attempt = 0;
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(5));
        loop {
            let mut req = self.http.get(url);
            if !self.headers.is_empty() {
                req = req.headers(self.headers.clone());
            }
            req = match &self.auth {
                SchemaRegistryAuth::None => req,
                SchemaRegistryAuth::Bearer(t) => req.bearer_auth(t),
                SchemaRegistryAuth::Basic { user, pass } => req.basic_auth(user, Some(pass)),
            };
            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    return resp
                        .json()
                        .await
                        .map_err(|e| SchemaRegistryError::Decode(e.to_string()));
                }
                Ok(resp) if resp.status().as_u16() == 404 => {
                    return Err(not_found);
                }
                Ok(resp) if resp.status().is_server_error() => {
                    if attempt >= self.max_retries {
                        return Err(SchemaRegistryError::Transport {
                            retriable: true,
                            message: format!("server error {}", resp.status()),
                        });
                    }
                }
                Ok(resp) => {
                    return Err(SchemaRegistryError::Transport {
                        retriable: false,
                        message: format!("unexpected status {}", resp.status()),
                    });
                }
                Err(e) => {
                    if attempt >= self.max_retries {
                        return Err(SchemaRegistryError::Transport {
                            retriable: true,
                            message: e.to_string(),
                        });
                    }
                }
            }
            let delay = backoff
                .next()
                .expect("backoff iterator is infinite; this is a bug");
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }
}

/// Builder for [`SchemaRegistry`].
pub struct SchemaRegistryBuilder {
    base_url: String,
    auth: SchemaRegistryAuth,
    headers: HeaderMap,
    timeout: Duration,
    max_retries: u32,
    negative_cache_ttl: Duration,
}

impl SchemaRegistryBuilder {
    pub fn auth(mut self, auth: SchemaRegistryAuth) -> Self {
        self.auth = auth;
        self
    }

    /// Attach a static header sent on every registry request, layered with any
    /// configured [`SchemaRegistryAuth`]. Call repeatedly to set multiple
    /// headers (e.g. Cloudflare Access `CF-Access-Client-Id` and
    /// `CF-Access-Client-Secret`). Panics if `name` or `value` is not a valid
    /// HTTP header.
    pub fn header(mut self, name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        use reqwest::header::{HeaderName, HeaderValue};
        let name = HeaderName::from_bytes(name.as_ref().as_bytes())
            .expect("schema registry header name is a valid HTTP header name");
        let value = HeaderValue::from_str(value.as_ref())
            .expect("schema registry header value is a valid HTTP header value");
        self.headers.insert(name, value);
        self
    }
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }
    pub fn negative_cache_ttl(mut self, ttl: Duration) -> Self {
        self.negative_cache_ttl = ttl;
        self
    }
    pub fn build(self) -> Arc<SchemaRegistry> {
        let http = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()
            .expect("reqwest client builds with default TLS");
        Arc::new(SchemaRegistry {
            base_url: Arc::from(self.base_url.trim_end_matches('/')),
            http,
            auth: self.auth,
            headers: self.headers,
            max_retries: self.max_retries,
            negative_cache_ttl: self.negative_cache_ttl,
            resolved: DashMap::new(),
            inflight: DashMap::new(),
            negative: DashMap::new(),
            subject_ids: DashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SchemaRegistryAuth;

    #[test]
    fn bearer_token_is_redacted_in_debug() {
        let auth = SchemaRegistryAuth::Bearer("super-secret-token".to_string());
        let dbg = format!("{auth:?}");
        assert!(!dbg.contains("super-secret-token"), "token leaked: {dbg}");
        assert!(
            dbg.contains("<redacted>"),
            "missing redaction marker: {dbg}"
        );
    }

    #[test]
    fn basic_password_is_redacted_but_user_shown() {
        let auth = SchemaRegistryAuth::Basic {
            user: "alice".to_string(),
            pass: "super-secret-token".to_string(),
        };
        let dbg = format!("{auth:?}");
        assert!(
            !dbg.contains("super-secret-token"),
            "password leaked: {dbg}"
        );
        assert!(
            dbg.contains("<redacted>"),
            "missing redaction marker: {dbg}"
        );
        assert!(dbg.contains("alice"), "user should be visible: {dbg}");
    }
}
