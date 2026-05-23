use std::sync::Arc;

use aws_config::BehaviorVersion;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_msk_iam_sasl_signer::generate_auth_token_from_credentials_provider;
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::consumer::ConsumerContext;

use crate::ShoveError;
use crate::error::Result;

/// Signs short-lived presigned URLs against `kafka-cluster:Connect` using
/// the AWS credential chain. Held inside `MskIamContext` and called from
/// librdkafka's OAUTHBEARER refresh thread.
#[allow(dead_code)]
pub(super) struct MskIamTokenProvider {
    region: String,
    credentials: SharedCredentialsProvider,
    /// Tokio runtime handle captured at construction time (inside an async
    /// context). Used by `generate_oauth_token` to bridge from librdkafka's
    /// non-Tokio poll thread to the async signer.
    handle: tokio::runtime::Handle,
}

#[allow(dead_code)]
impl MskIamTokenProvider {
    pub(super) async fn new(region: String, profile: Option<String>) -> Result<Self> {
        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(region.clone()));
        if let Some(p) = profile {
            loader = loader.profile_name(p);
        }
        let cfg = loader.load().await;
        let credentials = cfg.credentials_provider().ok_or_else(|| {
            ShoveError::Connection("no AWS credentials provider available for MSK IAM auth".into())
        })?;
        // Capture the handle here, while we are inside an async context.
        // `generate_oauth_token` is called from librdkafka's internal C
        // thread, which has no Tokio runtime; using this pre-captured handle
        // avoids the `Handle::current()` panic on that thread.
        let handle = tokio::runtime::Handle::current();
        Ok(Self {
            region,
            credentials,
            handle,
        })
    }
}

/// `ClientContext` impl that bridges librdkafka's synchronous OAUTHBEARER
/// callback to the async signer. Cloneable so producer, admin client, and
/// stream consumer can each hold their own handle to the same provider.
#[allow(dead_code)]
#[derive(Clone)]
pub(super) struct MskIamContext {
    provider: Arc<MskIamTokenProvider>,
}

#[allow(dead_code)]
impl MskIamContext {
    pub(super) fn new(provider: Arc<MskIamTokenProvider>) -> Self {
        Self { provider }
    }
}

impl ConsumerContext for MskIamContext {}

impl ClientContext for MskIamContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _principal_name: Option<&str>,
    ) -> std::result::Result<OAuthToken, Box<dyn std::error::Error>> {
        // librdkafka invokes this synchronously on its own poll thread, which
        // is a plain C thread with no Tokio runtime. We use the handle captured
        // during `MskIamTokenProvider::new` (constructed inside an async context)
        // rather than `Handle::current()`, which would panic here.
        let provider = self.provider.clone();
        let handle = provider.handle.clone();
        let (token, expiry_ms) = handle.block_on(async move {
            generate_auth_token_from_credentials_provider(
                aws_config::Region::new(provider.region.clone()),
                provider.credentials.clone(),
            )
            .await
        })?;
        Ok(OAuthToken {
            token,
            principal_name: String::new(),
            lifetime_ms: expiry_ms,
        })
    }
}
