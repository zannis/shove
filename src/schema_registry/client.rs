//! Async, cached Confluent Schema Registry client.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures_util::FutureExt as _;
use futures_util::future::{BoxFuture, Shared};
use reqwest::header::HeaderMap;
use reqwest::{Url, redirect::Policy};

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

/// Cap attacker-controlled misses so arbitrary Confluent schema IDs cannot
/// grow the process for its entire lifetime.
const MAX_NEGATIVE_CACHE_ENTRIES: usize = 4096;

/// Headers whose whole purpose is to carry a credential. Declaring one of these
/// non-secret is never a true statement about a configuration, so
/// [`SchemaRegistryBuilder::non_secret_header`] refuses them rather than let a
/// caller disable the plaintext refusal and the redirect guard by accident.
///
/// Deliberately short. This is not an attempt to recognise secrets by name —
/// that is what the fail-closed default on
/// [`SchemaRegistryBuilder::header`] is for — only to reject an assertion that
/// cannot hold.
const ALWAYS_SECRET_HEADERS: [&str; 3] = ["authorization", "proxy-authorization", "cookie"];

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
    // Serialises bounded eviction + insertion so concurrent unique misses
    // cannot all observe spare capacity and overfill the cache.
    negative_insert_lock: tokio::sync::Mutex<()>,
    // Producer-side subject -> latest id cache (TopicNameStrategy lookups).
    subject_ids: DashMap<Arc<str>, SchemaId>,
}

impl SchemaRegistry {
    /// Start building a registry client for `base_url` (e.g. `https://sr:8081`).
    ///
    /// A plaintext `http://` URL is fine on its own. Combining it with a
    /// credential is not — and three things count as one: any
    /// [`SchemaRegistryAuth`] other than `None`, `user:pass@` userinfo in this
    /// URL, and any header set with [`SchemaRegistryBuilder::header`]. See
    /// [`SchemaRegistryBuilder::allow_plaintext_credentials`].
    pub fn builder(base_url: impl Into<String>) -> SchemaRegistryBuilder {
        SchemaRegistryBuilder {
            base_url: base_url.into(),
            auth: SchemaRegistryAuth::None,
            headers: HeaderMap::new(),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            negative_cache_ttl: Duration::from_secs(30),
            allow_plaintext_credentials: false,
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
            let fresh = at.elapsed() < self.negative_cache_ttl;
            let err = err.clone();
            drop(entry);
            if fresh {
                return Err(err);
            }
            self.negative.remove(&id);
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
                    self.cache_negative(id, e.clone()).await;
                }
                self.inflight.remove(&id);
            }
        }
        result
    }

    /// True when `id` holds a negative entry that has not yet expired.
    fn negative_is_fresh(&self, id: SchemaId) -> bool {
        match self.negative.get(&id) {
            Some(entry) => entry.value().0.elapsed() < self.negative_cache_ttl,
            None => false,
        }
    }

    async fn cache_negative(&self, id: SchemaId, error: SchemaRegistryError) {
        // Every waiter of one shared fetch runs this, so a burst on a single
        // bad id arrives here N times for the same key. Once a sibling waiter
        // has cached the failure there is nothing left to insert, so the rest
        // of the burst neither queues on the lock nor touches the map.
        //
        // Safe to skip: reaching this point means `resolve` got past its
        // freshness check and started a fetch, so any entry visible now was
        // inserted by a concurrent waiter on that same failed fetch.
        if self.negative_is_fresh(id) {
            return;
        }
        let _guard = self.negative_insert_lock.lock().await;
        // Load-bearing, not defensive padding: the whole burst can clear the
        // check above before any one of them inserts, so this is the
        // authoritative test.
        if self.negative_is_fresh(id) {
            return;
        }
        // Replacing an entry does not grow the map, so it must not evict.
        // Paying for a replacement with an eviction is what let a duplicate
        // burst drop unrelated negatives — each one costing a fresh registry
        // round trip — and leave the cache permanently one below its cap.
        let replacing = self.negative.contains_key(&id);
        if !replacing && self.negative.len() >= MAX_NEGATIVE_CACHE_ENTRIES {
            // Copy the key and drop the iterator guard before removing: a
            // DashMap removal while holding a shard read guard can deadlock.
            let victim = { self.negative.iter().next().map(|entry| *entry.key()) };
            if let Some(victim) = victim {
                self.negative.remove(&victim);
            }
        }
        self.negative.insert(id, (std::time::Instant::now(), error));
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
                // Only reachable on a credential-bearing client: that is the
                // configuration `build()` gives a no-redirect policy, so an
                // unauthenticated client follows the hop and never lands here.
                // Say what happened and what to change — a bare
                // "unexpected status 301" would send the reader hunting the
                // registry rather than the base URL.
                Ok(resp) if resp.status().is_redirection() => {
                    return Err(SchemaRegistryError::Transport {
                        retriable: false,
                        message: format!(
                            "registry responded with a redirect ({}), which is not \
                             followed because credentials are configured: the hop \
                             would replay them to the host named in Location. Point \
                             the base URL at the registry's final address.",
                            resp.status()
                        ),
                    });
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

/// Parse the base URL the way the HTTP client will.
///
/// A security decision must never be made by scanning the configuration
/// string. `Url::parse` normalises spellings a prefix test does not recognise:
/// for a special scheme it skips any number of leading slashes and treats the
/// missing `//` as a non-fatal syntax violation, so `http:sr:8081`,
/// `http:/sr:8081` and `http:///sr:8081` all parse to the same origin as
/// `http://sr:8081` and all reach the network. A `starts_with("http://")` test
/// calls those three encrypted.
fn parse_base_url(base_url: &str) -> Option<Url> {
    Url::parse(base_url).ok()
}

/// True when the base URL is carried by TLS.
///
/// Fail closed on both axes: a URL that does not parse cannot be shown to be
/// encrypted, and `reqwest` speaks only `http` and `https`, so anything that is
/// not `https` is plaintext.
fn transport_is_encrypted(base_url: Option<&Url>) -> bool {
    base_url.is_some_and(|url| url.scheme() == "https")
}

/// Names every channel through which this configuration would put a reusable
/// secret on the wire. Empty means there is nothing to disclose.
///
/// Deliberately independent of the transport: the same answer decides whether
/// [`SchemaRegistryBuilder::build`] refuses a plaintext base URL *and* whether
/// the client is allowed to follow redirects.
///
/// Only channel *names* are produced. A header name is safe to put in a panic
/// message or a log line; its value is exactly what must never appear there.
fn credential_channels(
    base_url: Option<&Url>,
    auth: &SchemaRegistryAuth,
    headers: &HeaderMap,
) -> Vec<String> {
    let mut channels = Vec::new();
    match auth {
        SchemaRegistryAuth::None => {}
        SchemaRegistryAuth::Bearer(_) => channels.push("SchemaRegistryAuth::Bearer".to_owned()),
        SchemaRegistryAuth::Basic { .. } => channels.push("SchemaRegistryAuth::Basic".to_owned()),
    }
    // `reqwest` lifts `user:pass@` out of the URL and replays it as an
    // `Authorization: Basic` header, so URL userinfo is a credential even when
    // no `SchemaRegistryAuth` is configured. This is the exact condition
    // reqwest's own `extract_authority` uses, so the two cannot disagree.
    if base_url.is_some_and(|url| !url.username().is_empty() || url.password().is_some()) {
        channels.push("base URL userinfo".to_owned());
    }
    for (name, value) in headers {
        if value.is_sensitive() {
            channels.push(format!("header `{name}`"));
        }
    }
    channels
}

/// Builder for [`SchemaRegistry`].
pub struct SchemaRegistryBuilder {
    base_url: String,
    auth: SchemaRegistryAuth,
    headers: HeaderMap,
    timeout: Duration,
    max_retries: u32,
    negative_cache_ttl: Duration,
    allow_plaintext_credentials: bool,
}

impl SchemaRegistryBuilder {
    pub fn auth(mut self, auth: SchemaRegistryAuth) -> Self {
        self.auth = auth;
        self
    }

    /// Permit credentials against a base URL that is not `https://`, which
    /// [`SchemaRegistryBuilder::build`] otherwise refuses.
    ///
    /// **Unscoped.** This opts in every credential channel at once —
    /// [`SchemaRegistryAuth`], `user:pass@` userinfo in the base URL, and every
    /// header set with [`header`](Self::header) — not just the one that
    /// tripped the refusal. All of them are then sent in cleartext on every
    /// schema fetch, readable by anything on the path. Call this only for a
    /// registry that is genuinely unreachable from an untrusted network — a
    /// local development stack, or a test against a loopback mock.
    ///
    /// If a header that carries no secret is the only thing being refused,
    /// move it to [`non_secret_header`](Self::non_secret_header) rather than
    /// reaching for this: that keeps the refusal working for everything else.
    pub fn allow_plaintext_credentials(mut self) -> Self {
        self.allow_plaintext_credentials = true;
        self
    }

    /// Attach a static header sent on every registry request, layered with any
    /// configured [`SchemaRegistryAuth`]. Call repeatedly to set multiple
    /// headers (e.g. Cloudflare Access `CF-Access-Client-Id` and
    /// `CF-Access-Client-Secret`). Panics if `name` or `value` is not a valid
    /// HTTP header.
    ///
    /// **The value is treated as a secret.** This method is documented for, and
    /// overwhelmingly used to carry, credentials, and the builder cannot tell
    /// which vendor-specific header name happens to hold one — so it assumes
    /// every value here does. Consequences: the header counts as a credential
    /// for the plaintext check in [`build`](Self::build), the client will not
    /// follow redirects, and the value is marked sensitive so it prints as
    /// `Sensitive` rather than in clear.
    ///
    /// For a header that genuinely carries no secret, use
    /// [`non_secret_header`](Self::non_secret_header) instead of reaching for
    /// [`allow_plaintext_credentials`](Self::allow_plaintext_credentials) —
    /// that opt-in is not scoped to one header, so using it to quiet an
    /// `Accept` header would also permit a real bearer token in cleartext.
    pub fn header(self, name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.insert_header(name, value, true)
    }

    /// Attach a static header whose value is **not** a secret — an `Accept`, a
    /// build identifier, a request tag.
    ///
    /// Identical on the wire to [`header`](Self::header); the difference is the
    /// assertion the caller makes by choosing it. A non-secret header does not
    /// require TLS and does not restrict redirects. Panics if `name` or `value`
    /// is not a valid HTTP header.
    ///
    /// Getting this wrong is a silent credential disclosure: passing a token
    /// here disables both the plaintext refusal and the redaction that
    /// [`header`](Self::header) would have applied. When in doubt, use
    /// [`header`](Self::header).
    ///
    /// # Panics
    ///
    /// Panics for a header that carries a credential by definition —
    /// `Authorization`, `Proxy-Authorization`, `Cookie` — because there is no
    /// configuration in which that assertion is true. This cannot catch a
    /// vendor-specific name such as `CF-Access-Client-Secret`; naming the
    /// header here is the caller's assertion that it holds no secret, and only
    /// the caller knows.
    ///
    /// Also panics if `name` or `value` is not a valid HTTP header.
    pub fn non_secret_header(self, name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        let name = name.as_ref();
        assert!(
            !ALWAYS_SECRET_HEADERS
                .iter()
                .any(|known| known.eq_ignore_ascii_case(name)),
            "`{name}` carries a credential by definition and cannot be declared \
             non-secret. Use SchemaRegistryBuilder::header() — or, for bearer and \
             basic auth, SchemaRegistryBuilder::auth()."
        );
        self.insert_header(name, value, false)
    }

    fn insert_header(
        mut self,
        name: impl AsRef<str>,
        value: impl AsRef<str>,
        secret: bool,
    ) -> Self {
        use reqwest::header::{HeaderName, HeaderValue};
        let name = HeaderName::from_bytes(name.as_ref().as_bytes())
            .expect("schema registry header name is a valid HTTP header name");
        let mut value = HeaderValue::from_str(value.as_ref())
            .expect("schema registry header value is a valid HTTP header value");
        value.set_sensitive(secret);
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
    /// # Panics
    ///
    /// Panics when the configuration would put a reusable secret on a plaintext
    /// transport and
    /// [`SchemaRegistryBuilder::allow_plaintext_credentials`] was not called.
    /// Three channels count as a secret: [`SchemaRegistryAuth`], `user:pass@`
    /// userinfo in the base URL (which `reqwest` replays as an
    /// `Authorization: Basic` header), and any header set via
    /// [`header`](Self::header). A misconfiguration that puts a reusable secret
    /// on the wire is caught at startup rather than on the first schema fetch;
    /// this builder cannot return an error because it yields the client
    /// directly.
    ///
    /// Also panics if the underlying `reqwest` client cannot be constructed.
    pub fn build(self) -> Arc<SchemaRegistry> {
        let parsed = parse_base_url(&self.base_url);
        let channels = credential_channels(parsed.as_ref(), &self.auth, &self.headers);

        // sec-SR-1: refuse reusable credentials over an unencrypted transport.
        // A warning is too easy to miss — the token is already on the wire by
        // the time anyone reads the log. The base URL is deliberately kept out
        // of the message: it may carry `user:pass@` userinfo, and a panic
        // message reaches logs and crash reports. Channel names are safe;
        // channel values are the thing being protected.
        if !channels.is_empty() && !transport_is_encrypted(parsed.as_ref()) {
            assert!(
                self.allow_plaintext_credentials,
                "schema registry credentials require TLS: use an https:// base URL. \
                 These configured credentials would be sent in cleartext, readable by \
                 any network observer: {}. For a registry on a trusted network, call \
                 SchemaRegistryBuilder::allow_plaintext_credentials() to opt in. A \
                 header that carries no secret belongs in \
                 SchemaRegistryBuilder::non_secret_header() instead.",
                channels.join(", ")
            );
            tracing::warn!(
                credential_channels = %channels.join(", "),
                "schema registry credentials are configured against a plaintext base URL \
                 with allow_plaintext_credentials(); they are sent in cleartext on every \
                 request. Use https:// for any registry reachable outside a trusted \
                 network."
            );
        }

        let mut http = reqwest::Client::builder().timeout(self.timeout);
        if !channels.is_empty() {
            // A credential-bearing client must not follow redirects. `reqwest`
            // follows up to 10 by default and, across an origin change, strips
            // only `Authorization`, `Cookie`, `Proxy-Authorization` and
            // `WWW-Authenticate` — a custom secret header such as
            // `CF-Access-Client-Secret` is replayed to whatever host a 302
            // names, over whatever scheme it names. No build-time check can see
            // that hop, and a schema registry has no legitimate reason to
            // redirect, so the credential-bearing case declines to follow one.
            // The 3xx then surfaces as an `unexpected status` transport error
            // rather than as a silent disclosure.
            http = http.redirect(Policy::none());
        }
        let http = http
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
            negative_insert_lock: tokio::sync::Mutex::new(()),
            subject_ids: DashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// `credential_channels` + `transport_is_encrypted` in the combination
    /// `build()` uses them.
    fn refuses(base_url: &str, auth: &SchemaRegistryAuth, headers: &HeaderMap) -> bool {
        let parsed = parse_base_url(base_url);
        !credential_channels(parsed.as_ref(), auth, headers).is_empty()
            && !transport_is_encrypted(parsed.as_ref())
    }

    fn secret_header(name: &str, value: &str) -> HeaderMap {
        SchemaRegistry::builder("https://sr:8081")
            .header(name, value)
            .headers
    }

    #[test]
    fn cleartext_credential_guard_tracks_scheme_and_auth() {
        let none = HeaderMap::new();
        let bearer = SchemaRegistryAuth::Bearer("t".into());
        let basic = SchemaRegistryAuth::Basic {
            user: "u".into(),
            pass: "p".into(),
        };

        // Credentials + plaintext transport is the case `build()` refuses.
        assert!(refuses("http://sr:8081", &bearer, &none));
        assert!(refuses("HTTP://sr:8081", &basic, &none));

        // TLS carries them safely, in either casing.
        assert!(!refuses("https://sr:8081", &bearer, &none));
        assert!(!refuses("HTTPS://sr:8081", &basic, &none));

        // No credentials means nothing to disclose.
        assert!(!refuses("http://sr:8081", &SchemaRegistryAuth::None, &none));
    }

    /// `Url::parse` normalises spellings a string scan does not recognise. Each
    /// of these reaches the same origin as `http://sr:8081` and sends the
    /// bearer token in cleartext, so each must be refused. A
    /// `starts_with("http://")` test calls all three encrypted.
    #[test]
    fn alternate_plaintext_spellings_are_refused() {
        let none = HeaderMap::new();
        let bearer = SchemaRegistryAuth::Bearer("t".into());

        for spelling in ["http:sr:8081", "http:/sr:8081", "http:///sr:8081"] {
            assert!(
                refuses(spelling, &bearer, &none),
                "{spelling} reaches the network in cleartext and must be refused"
            );
        }

        // `sr:8081` does parse — scheme `sr`, path `8081` — it is simply not
        // https, which is the fail-closed half of the same rule.
        assert!(parse_base_url("sr:8081").is_some());
        assert!(refuses("sr:8081", &bearer, &none));

        // This one genuinely does not parse, so nothing about its transport can
        // be established and it is treated as plaintext.
        assert!(parse_base_url("not a url").is_none());
        assert!(refuses("not a url", &bearer, &none));
    }

    /// `reqwest` lifts URL userinfo into an `Authorization: Basic` header, so
    /// it is a credential even with `SchemaRegistryAuth::None`.
    #[test]
    fn url_userinfo_counts_as_a_credential() {
        let none = HeaderMap::new();
        let no_auth = SchemaRegistryAuth::None;

        assert!(refuses("http://u:p@sr:8081", &no_auth, &none));
        assert!(refuses("http://u@sr:8081", &no_auth, &none));
        assert!(refuses("http://:p@sr:8081", &no_auth, &none));
        // The authority-bounded spellings a naive `@` scan misses.
        assert!(refuses("http:u:p@sr:8081", &no_auth, &none));
        assert!(refuses("http:///u:p@sr:8081", &no_auth, &none));

        // TLS carries userinfo exactly as it carries `SchemaRegistryAuth`.
        assert!(!refuses("https://u:p@sr:8081", &no_auth, &none));

        // An `@` in the path is not userinfo, and empty userinfo is not a
        // credential — this matches reqwest's own `extract_authority`, which
        // sets no header in either case.
        assert!(!refuses("http://sr:8081/a@b", &no_auth, &none));
        assert!(!refuses("http://@sr:8081", &no_auth, &none));
    }

    #[test]
    fn header_secrecy_decides_whether_the_guard_fires() {
        let no_auth = SchemaRegistryAuth::None;

        // `header()` is fail-closed: its value is assumed to be a secret.
        assert!(refuses(
            "http://sr:8081",
            &no_auth,
            &secret_header("CF-Access-Client-Secret", "s")
        ));
        // Even a name that sounds harmless — the builder cannot know.
        assert!(refuses(
            "http://sr:8081",
            &no_auth,
            &secret_header("X-Tag", "s")
        ));
        // TLS carries it.
        assert!(!refuses(
            "https://sr:8081",
            &no_auth,
            &secret_header("CF-Access-Client-Secret", "s")
        ));

        // `non_secret_header()` asserts there is nothing to disclose.
        let public = SchemaRegistry::builder("http://sr:8081")
            .non_secret_header("Accept", "application/json")
            .headers;
        assert!(!refuses("http://sr:8081", &no_auth, &public));
    }

    /// The escape hatch must not be usable to switch the guard off for a header
    /// that is a credential by definition — that would make the whole contract
    /// opt-out with one call.
    #[test]
    #[should_panic(expected = "carries a credential by definition")]
    fn non_secret_header_refuses_authorization() {
        let _ = SchemaRegistry::builder("http://sr:8081")
            .non_secret_header("Authorization", "Bearer token-abc");
    }

    #[test]
    #[should_panic(expected = "carries a credential by definition")]
    fn non_secret_header_refuses_cookie_case_insensitively() {
        let _ = SchemaRegistry::builder("http://sr:8081").non_secret_header("CoOkIe", "sid=abc");
    }

    #[test]
    #[should_panic(expected = "carries a credential by definition")]
    fn non_secret_header_refuses_proxy_authorization() {
        let _ = SchemaRegistry::builder("http://sr:8081")
            .non_secret_header("proxy-authorization", "Basic abc");
    }

    #[test]
    fn the_channel_list_names_every_configured_credential() {
        let parsed = parse_base_url("http://sr-user:url-secret@sr:8081");
        let channels = credential_channels(
            parsed.as_ref(),
            &SchemaRegistryAuth::Bearer("bearer-secret".into()),
            &secret_header("CF-Access-Client-Secret", "header-secret"),
        );

        assert_eq!(
            channels,
            vec![
                "SchemaRegistryAuth::Bearer".to_owned(),
                "base URL userinfo".to_owned(),
                "header `cf-access-client-secret`".to_owned(),
            ]
        );
        // The panic message is built from this list, so it must never carry a
        // secret value or the base URL.
        let rendered = channels.join(", ");
        for secret in ["bearer-secret", "url-secret", "header-secret", "sr-user"] {
            assert!(
                !rendered.contains(secret),
                "{secret} leaked into {rendered}"
            );
        }
        assert!(!rendered.contains("sr:8081"), "no base URL: {rendered}");
    }

    // The matrix above pins which configurations the predicate selects. These
    // pin what `build()` does with that verdict, on both sides of the opt-in.

    #[test]
    #[should_panic(expected = "schema registry credentials require TLS")]
    fn plaintext_credentials_are_refused_at_build() {
        let _ = SchemaRegistry::builder("http://sr:8081")
            .auth(SchemaRegistryAuth::Bearer("t".into()))
            .build();
    }

    #[test]
    fn plaintext_credentials_build_once_opted_in() {
        let registry = SchemaRegistry::builder("http://sr:8081")
            .auth(SchemaRegistryAuth::Bearer("t".into()))
            .allow_plaintext_credentials()
            .build();

        assert!(matches!(registry.auth, SchemaRegistryAuth::Bearer(_)));
    }

    #[test]
    fn the_guard_leaves_safe_configurations_alone() {
        // TLS carries the credentials, so no opt-in is needed.
        let _ = SchemaRegistry::builder("https://sr:8081")
            .auth(SchemaRegistryAuth::Basic {
                user: "u".into(),
                pass: "p".into(),
            })
            .build();

        // Plaintext with nothing to disclose stays allowed: this is the common
        // unauthenticated development registry, and the guard must not break it.
        let _ = SchemaRegistry::builder("http://sr:8081").build();
    }

    #[tokio::test]
    async fn negative_cache_evicts_at_its_capacity() {
        let registry = SchemaRegistry::builder("http://registry.invalid").build();

        for raw_id in 0..(MAX_NEGATIVE_CACHE_ENTRIES + 32) {
            let id = SchemaId(raw_id as u32);
            registry
                .cache_negative(id, SchemaRegistryError::NotFound(id.0))
                .await;
        }

        assert_eq!(registry.negative.len(), MAX_NEGATIVE_CACHE_ENTRIES);
    }

    /// Every waiter of one shared single-flight future runs `resolve`'s error
    /// arm, so a burst on a single bad id calls `cache_negative` N times for
    /// the same key. At capacity those duplicates must not evict: they replace
    /// an entry that is already there, so they cost no space, and paying for
    /// them in evictions flushes unrelated negatives back into HTTP fetches.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn duplicate_waiters_do_not_flush_unrelated_negatives() {
        let registry = SchemaRegistry::builder("http://registry.invalid").build();

        for raw_id in 0..MAX_NEGATIVE_CACHE_ENTRIES {
            let id = SchemaId(raw_id as u32);
            registry
                .cache_negative(id, SchemaRegistryError::NotFound(id.0))
                .await;
        }
        assert_eq!(registry.negative.len(), MAX_NEGATIVE_CACHE_ENTRIES);

        // One more id fails, with a burst of waiters all reporting it.
        let duplicate = SchemaId(MAX_NEGATIVE_CACHE_ENTRIES as u32);
        let mut waiters = Vec::with_capacity(64);
        for _ in 0..64 {
            let registry = registry.clone();
            waiters.push(tokio::spawn(async move {
                registry
                    .cache_negative(duplicate, SchemaRegistryError::NotFound(duplicate.0))
                    .await;
            }));
        }
        for waiter in waiters {
            waiter.await.expect("waiter task must not panic");
        }

        assert_eq!(
            registry.negative.len(),
            MAX_NEGATIVE_CACHE_ENTRIES,
            "the cap must still be full: duplicates replace, they do not shrink the cache"
        );
        assert!(
            registry.negative.contains_key(&duplicate),
            "the failing id must be negative-cached"
        );
        let survivors = (0..MAX_NEGATIVE_CACHE_ENTRIES)
            .filter(|raw_id| registry.negative.contains_key(&SchemaId(*raw_id as u32)))
            .count();
        assert_eq!(
            survivors,
            MAX_NEGATIVE_CACHE_ENTRIES - 1,
            "a burst on one id may cost exactly the single eviction that made room for it"
        );
    }

    /// The freshness fast path above short-circuits the burst before it can
    /// reach the replacement branch, so on its own it proves nothing about
    /// replacement. A zero TTL disables that fast path — no entry is ever
    /// fresh — which leaves "do not evict when replacing" as the only thing
    /// holding the cap. Delete that check and this test goes red while the
    /// burst test above stays green.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn replacing_an_entry_at_capacity_does_not_evict() {
        let registry = SchemaRegistry::builder("http://registry.invalid")
            .negative_cache_ttl(Duration::ZERO)
            .build();

        for raw_id in 0..MAX_NEGATIVE_CACHE_ENTRIES {
            let id = SchemaId(raw_id as u32);
            registry
                .cache_negative(id, SchemaRegistryError::NotFound(id.0))
                .await;
        }
        assert_eq!(registry.negative.len(), MAX_NEGATIVE_CACHE_ENTRIES);

        // Eviction takes whatever `iter().next()` yields, so the id being
        // replaced must not be that entry — otherwise an unguarded eviction
        // would remove exactly the id about to be re-inserted and cancel
        // itself out, leaving the test unable to observe the defect.
        let would_be_evicted = registry
            .negative
            .iter()
            .next()
            .map(|entry| *entry.key())
            .expect("the cache was just filled");
        let present = (0..MAX_NEGATIVE_CACHE_ENTRIES)
            .map(|raw_id| SchemaId(raw_id as u32))
            .find(|id| *id != would_be_evicted)
            .expect("the cache holds more than one entry");

        // Re-report a failure for an id the cache already holds. Nothing needs
        // to be made room for, so nothing may be evicted.
        for _ in 0..64 {
            registry
                .cache_negative(present, SchemaRegistryError::NotFound(present.0))
                .await;
        }

        assert_eq!(
            registry.negative.len(),
            MAX_NEGATIVE_CACHE_ENTRIES,
            "replacing an entry must not evict a different one"
        );
        assert!(
            registry.negative.contains_key(&present),
            "the replaced id must still be cached"
        );
        assert!(
            registry.negative.contains_key(&would_be_evicted),
            "an unrelated entry was evicted to pay for a replacement"
        );
    }
}
