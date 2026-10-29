use std::sync::{Arc, mpsc};
use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_msk_iam_sasl_signer::generate_auth_token_from_credentials_provider;
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::consumer::ConsumerContext;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};

use crate::ShoveError;
use crate::error::Result;

/// How long the token callback waits for the signer before it fails the
/// refresh; librdkafka keeps any token it still holds and retries ten
/// seconds later. The AWS SDK gives loading an identity the same budget.
const SIGN_TIMEOUT: Duration = Duration::from_secs(5);

/// A presigned token and its expiry in epoch milliseconds.
type Signed = Result<(String, i64)>;

/// Signs short-lived presigned URLs against `kafka-cluster:Connect` using
/// the AWS credential chain.
///
/// librdkafka calls the token callback synchronously on whichever thread
/// polls the client: the producer's polling thread, a consumer's `recv()`
/// on a Tokio worker, or a producer flush inside `async fn shutdown`. The
/// signer is async, so it runs on a dedicated thread that drives its own
/// current-thread runtime and signs each request on a task of its own. The
/// callback hands that thread a request and blocks until the reply arrives
/// or [`SIGN_TIMEOUT`] passes. Nothing it waits on needs the caller's
/// runtime, so it works on a multi-thread worker, on the only thread of a
/// `current_thread` runtime, and on a thread with no runtime at all.
///
/// The thread exits once the last handle to the provider is dropped.
pub(super) struct MskIamTokenProvider {
    requests: tokio_mpsc::UnboundedSender<mpsc::Sender<Signed>>,
}

impl MskIamTokenProvider {
    pub(super) async fn new(region: String, profile: Option<String>) -> Result<Self> {
        let region = aws_config::Region::new(region);
        let mut loader = aws_config::defaults(BehaviorVersion::latest()).region(region.clone());
        if let Some(p) = profile {
            loader = loader.profile_name(p);
        }
        let cfg = loader.load().await;
        let credentials = cfg.credentials_provider().ok_or_else(|| {
            ShoveError::Connection("no AWS credentials provider available for MSK IAM auth".into())
        })?;
        let (requests, pending) = tokio_mpsc::unbounded_channel();
        let (started_tx, started) = oneshot::channel();
        std::thread::Builder::new()
            .name("shove-msk-iam-signer".into())
            .spawn(move || run_signer(region, credentials, pending, started_tx))
            .map_err(|e| {
                ShoveError::Connection(format!("failed to spawn the MSK IAM signer thread: {e}"))
            })?;
        started
            .await
            .map_err(|_| {
                ShoveError::Connection("the MSK IAM signer thread exited before it started".into())
            })?
            .map_err(|e| {
                ShoveError::Connection(format!("failed to build the MSK IAM signer runtime: {e}"))
            })?;
        Ok(Self { requests })
    }

    /// Mint one token on the signer thread, blocking the calling thread until
    /// the signer answers or [`SIGN_TIMEOUT`] passes.
    fn sign(&self) -> Signed {
        let (reply, answer) = mpsc::channel();
        self.requests
            .send(reply)
            .map_err(|_| ShoveError::Connection("the MSK IAM signer thread has stopped".into()))?;
        match answer.recv_timeout(SIGN_TIMEOUT) {
            Ok(signed) => signed,
            Err(mpsc::RecvTimeoutError::Timeout) => Err(ShoveError::Connection(format!(
                "MSK IAM token signing did not finish within {SIGN_TIMEOUT:?}"
            ))),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(ShoveError::Connection(
                "the MSK IAM signer dropped the request".into(),
            )),
        }
    }
}

/// The signer thread: build its runtime, report whether that worked, then
/// sign every request on its own task until the provider is dropped. The
/// runtime is built here, because dropping one inside an async context
/// panics and a failed spawn drops its closure on the caller's thread.
fn run_signer(
    region: aws_config::Region,
    credentials: SharedCredentialsProvider,
    mut pending: tokio_mpsc::UnboundedReceiver<mpsc::Sender<Signed>>,
    started: oneshot::Sender<std::io::Result<()>>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            let _ = started.send(Err(e));
            return;
        }
    };
    let _ = started.send(Ok(()));
    runtime.block_on(async move {
        while let Some(reply) = pending.recv().await {
            let region = region.clone();
            let credentials = credentials.clone();
            tokio::spawn(async move {
                let signed = generate_auth_token_from_credentials_provider(region, credentials)
                    .await
                    .map_err(|e| {
                        ShoveError::Connection(format!("failed to sign an MSK IAM token: {e}"))
                    });
                // The callback may have stopped waiting.
                let _ = reply.send(signed);
            });
        }
    });
}

/// `ClientContext` impl whose synchronous OAUTHBEARER callback returns a
/// token minted on the provider's signer thread. Cloneable so producer, admin
/// client, and stream consumer can each hold their own handle to the same
/// provider.
#[derive(Clone)]
pub(super) struct MskIamContext {
    provider: Arc<MskIamTokenProvider>,
}

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
        let (token, expiry_ms) = self.provider.sign()?;
        Ok(OAuthToken {
            token,
            principal_name: String::new(),
            lifetime_ms: expiry_ms,
        })
    }
}

/// Test-only seam (see the `test-support` feature): the OAUTHBEARER context
/// `connect` installs on the librdkafka clients of an MSK IAM `KafkaClient`,
/// or `None` for a `KafkaClient` without MSK IAM, so a test can call its
/// token callback from each kind of thread librdkafka calls it on.
#[cfg(feature = "test-support")]
pub fn msk_iam_context_for_test(
    client: &super::client::KafkaClient,
) -> Option<impl ClientContext + use<>> {
    client.msk_context()
}
