use std::collections::HashMap;
use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use axum::body::{Body, Bytes};
use axum::extract::{Query, Request, State};
use axum::http::StatusCode;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::get};
use axum_server::tls_rustls::RustlsConfig;
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream, WatchStream};
use tokio_stream::{Stream, StreamExt};

use crate::{DEFAULT_ADDR, RegistryLabel, Value};

/// The authoritative side of a synced variable, owned by the server that produced it.
pub struct SyncedVarSource<T> {
    tx: watch::Sender<T>,
}

impl<T: Value> SyncedVarSource<T> {
    fn new(val: T) -> Self {
        let (tx, _) = watch::channel(val);
        SyncedVarSource { tx }
    }

    pub fn get(&self) -> T {
        self.tx.borrow().clone()
    }

    pub fn set(&self, val: T) {
        self.tx.send_replace(val);
    }
}

/// The authoritative side of a synced *stream*, owned by the server that produced it.
///
/// Unlike [`SyncedVarSource`] there is no current value: each [`set`](Self::set) emits one
/// event to every currently-connected subscriber. Events sent while nobody is subscribed are
/// dropped, and a slow subscriber that falls more than `cap` events behind skips the gap.
pub struct SyncedBroadcastSource<T> {
    tx: broadcast::Sender<T>,
}

impl<T: Value> SyncedBroadcastSource<T> {
    fn new(cap: usize) -> Self {
        let (tx, _) = broadcast::channel(cap);
        SyncedBroadcastSource { tx }
    }

    /// Emits `val` to every currently-connected subscriber.
    pub fn set(&self, val: T) {
        let _ = self.tx.send(val);
    }

    /// Number of clients currently connected and receiving this stream's events.
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

/// The authoritative side of a synced *stream* fed by a caller-owned [`mpsc::Receiver`].
///
/// Single-consumer: the first client to connect takes the receiver and drains it; later
/// connections (and any reconnect) receive nothing, since an mpsc receiver cannot be
/// re-subscribed. Events sent before that first client connects are buffered by the channel, not
/// dropped. Use [`SyncedBroadcastSource`] instead to fan out to multiple clients.
pub struct SyncedReceiverSource<T> {
    rx: Mutex<Option<mpsc::Receiver<T>>>,
}

impl<T: Value> SyncedReceiverSource<T> {
    fn new(rx: mpsc::Receiver<T>) -> Self {
        SyncedReceiverSource {
            rx: Mutex::new(Some(rx)),
        }
    }
}

/// Type-erased view of a source so a single `Registry` can hold sources of differing types.
trait ErasedSource: Send + Sync {
    /// Encoded payloads, one per frame: a var replays its current value then each change; a
    /// broadcast source yields each event sent while subscribed.
    fn encoded_stream(&self) -> Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>;
}

impl<T: Value> ErasedSource for SyncedVarSource<T> {
    fn encoded_stream(&self) -> Pin<Box<dyn Stream<Item = Vec<u8>> + Send>> {
        Box::pin(
            WatchStream::new(self.tx.subscribe()).filter_map(|v| match v.encode() {
                Ok(bytes) => Some(bytes),
                Err(e) => {
                    log::warn!("failed to encode synced value: {e}");
                    None
                }
            }),
        )
    }
}

impl<T: Value> ErasedSource for SyncedBroadcastSource<T> {
    fn encoded_stream(&self) -> Pin<Box<dyn Stream<Item = Vec<u8>> + Send>> {
        Box::pin(BroadcastStream::new(self.tx.subscribe()).filter_map(|v| {
            let val = match v {
                Ok(val) => val,
                Err(e) => {
                    log::warn!("broadcast subscriber lagged: {e}");
                    return None;
                }
            };
            match val.encode() {
                Ok(bytes) => Some(bytes),
                Err(e) => {
                    log::warn!("failed to encode synced value: {e}");
                    None
                }
            }
        }))
    }
}

impl<T: Value> ErasedSource for SyncedReceiverSource<T> {
    fn encoded_stream(&self) -> Pin<Box<dyn Stream<Item = Vec<u8>> + Send>> {
        // The receiver is single-consumer: hand it to the first caller, leave later callers empty.
        match self.rx.lock().take() {
            Some(rx) => Box::pin(ReceiverStream::new(rx).filter_map(|v| match v.encode() {
                Ok(bytes) => Some(bytes),
                Err(e) => {
                    log::warn!("failed to encode synced value: {e}");
                    None
                }
            })),
            None => Box::pin(tokio_stream::iter(std::iter::empty::<Vec<u8>>())),
        }
    }
}

type Vars = Arc<RwLock<HashMap<RegistryLabel, Weak<dyn ErasedSource>>>>;
type Streams = Arc<RwLock<HashMap<RegistryLabel, Weak<dyn ErasedSource>>>>;

/// Combined router state: variables and streams live in separate id namespaces.
#[derive(Clone)]
struct AppState {
    vars: Vars,
    streams: Streams,
}

#[derive(Debug, PartialEq)]
pub enum SetError {
    IdTaken,
}

impl std::fmt::Display for SetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetError::IdTaken => write!(f, "id already taken"),
        }
    }
}

impl std::error::Error for SetError {}

pub struct Registry {
    vars: Vars,
    streams: Streams,
    server: JoinHandle<()>,
    addr: SocketAddr,
}

/// TLS options for a [`Registry`].
pub enum Tls {
    /// Generate an in-memory self-signed certificate valid for the given DNS names / IP
    /// addresses. Clients must trust it out-of-band or set
    /// [`ClientConfig::danger_accept_invalid_certs`](crate::client::ClientConfig::danger_accept_invalid_certs).
    SelfSigned { subject_alt_names: Vec<String> },
    /// Serve with a caller-provided PEM certificate chain and private key.
    Pem { cert: Vec<u8>, key: Vec<u8> },
}

impl Tls {
    /// Self-signed certificate covering `localhost` and `127.0.0.1`.
    pub fn self_signed_localhost() -> Self {
        Tls::SelfSigned {
            subject_alt_names: vec!["localhost".to_string(), "127.0.0.1".to_string()],
        }
    }

    /// Resolves to a PEM certificate chain and private key, generating one if needed.
    fn into_pem(self) -> Result<(Vec<u8>, Vec<u8>), rcgen::Error> {
        match self {
            Tls::SelfSigned { subject_alt_names } => {
                let generated = rcgen::generate_simple_self_signed(subject_alt_names)?;
                Ok((
                    generated.cert.pem().into_bytes(),
                    generated.signing_key.serialize_pem().into_bytes(),
                ))
            }
            Tls::Pem { cert, key } => Ok((cert, key)),
        }
    }
}

/// Server-side options for a [`Registry`].
pub struct ServerConfig {
    /// Require `Authorization: Bearer <token>` on every request. `None` disables auth.
    pub auth_token: Option<String>,
    /// Serve over HTTPS with the given TLS configuration. `None` serves plain HTTP.
    pub tls: Option<Tls>,
}

impl Default for ServerConfig {
    /// TLS on with a self-signed `localhost` certificate and no auth token.
    fn default() -> Self {
        ServerConfig {
            auth_token: None,
            tls: Some(Tls::self_signed_localhost()),
        }
    }
}

impl Registry {
    pub async fn new(addr: SocketAddr) -> io::Result<Self> {
        Registry::with_config(addr, ServerConfig::default()).await
    }

    /// Like [`new`](Self::new) but requires callers to present `Authorization: Bearer <token>`.
    pub async fn with_auth(addr: SocketAddr, token: impl Into<String>) -> io::Result<Self> {
        let config = ServerConfig {
            auth_token: Some(token.into()),
            ..ServerConfig::default()
        };
        Registry::with_config(addr, config).await
    }

    /// Builds a registry with explicit auth and TLS options.
    pub async fn with_config(addr: SocketAddr, config: ServerConfig) -> io::Result<Self> {
        let ServerConfig { auth_token, tls } = config;
        let vars: Vars = Arc::new(RwLock::new(HashMap::new()));
        let streams: Streams = Arc::new(RwLock::new(HashMap::new()));
        let listener = std::net::TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;

        let state = AppState {
            vars: vars.clone(),
            streams: streams.clone(),
        };
        let mut app = Router::new().route("/data", get(data)).with_state(state);
        if let Some(token) = auth_token {
            let token: Arc<str> = Arc::from(token);
            app = app.layer(middleware::from_fn_with_state(token, require_auth));
        }

        let server = match tls {
            Some(tls) => {
                let (cert, key) = tls.into_pem().map_err(io::Error::other)?;
                crate::install_crypto_provider();
                let tls_config = RustlsConfig::from_pem(cert, key).await?;
                let server = axum_server::from_tcp_rustls(listener, tls_config)?;
                let make = app.into_make_service();
                tokio::spawn(async move {
                    if let Err(e) = server.serve(make).await {
                        log::error!("registry server stopped: {e}");
                    }
                })
            }
            None => {
                let listener = TcpListener::from_std(listener)?;
                tokio::spawn(async move {
                    if let Err(e) = axum::serve(listener, app).await {
                        log::error!("registry server stopped: {e}");
                    }
                })
            }
        };
        Ok(Registry {
            vars,
            server,
            addr,
            streams,
        })
    }

    pub async fn default() -> io::Result<Self> {
        Registry::new(DEFAULT_ADDR).await
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Registers a new variable and returns the owning handle. The registry keeps only a
    /// `Weak` reference, so the returned handle's lifetime governs the variable.
    #[must_use = "dropping the returned handle immediately removes the variable"]
    pub async fn set<T: Value>(
        &self,
        val: T,
        id: impl Into<RegistryLabel>,
    ) -> Result<Arc<SyncedVarSource<T>>, SetError> {
        let id = id.into();
        let mut vars = self.vars.write().await;
        vars.retain(|_, weak| weak.strong_count() > 0);
        if vars.contains_key(&id) {
            return Err(SetError::IdTaken);
        }
        let var = Arc::new(SyncedVarSource::new(val));
        let erased: Arc<dyn ErasedSource> = var.clone();
        vars.insert(id, Arc::downgrade(&erased));
        Ok(var)
    }

    /// Registers a new broadcast stream and returns the owning handle. `cap` bounds the
    /// per-subscriber buffer: a subscriber lagging more than `cap` events behind skips the gap.
    /// The registry keeps only a `Weak` reference, so the returned handle's lifetime governs
    /// the stream. Streams and variables occupy separate id namespaces.
    #[must_use = "dropping the returned handle immediately removes the stream"]
    pub async fn stream<T: Value>(
        &self,
        id: impl Into<RegistryLabel>,
        cap: usize,
    ) -> Result<Arc<SyncedBroadcastSource<T>>, SetError> {
        let id = id.into();
        let mut streams = self.streams.write().await;
        streams.retain(|_, weak| weak.strong_count() > 0);
        if streams.contains_key(&id) {
            return Err(SetError::IdTaken);
        }
        let src = Arc::new(SyncedBroadcastSource::new(cap));
        let erased: Arc<dyn ErasedSource> = src.clone();
        streams.insert(id, Arc::downgrade(&erased));
        Ok(src)
    }

    /// Registers a stream fed by a caller-owned [`mpsc::Receiver`] and returns the owning handle.
    /// The caller keeps the paired [`mpsc::Sender`] to emit events. Single-consumer: the first
    /// client to connect drains the receiver (see [`SyncedReceiverSource`]). The registry keeps
    /// only a `Weak` reference, so the returned handle's lifetime governs the stream. Streams and
    /// variables occupy separate id namespaces.
    #[must_use = "dropping the returned handle immediately removes the stream"]
    pub async fn stream_from<T: Value>(
        &self,
        rx: mpsc::Receiver<T>,
        id: impl Into<RegistryLabel>,
    ) -> Result<Arc<SyncedReceiverSource<T>>, SetError> {
        let id = id.into();
        let mut streams = self.streams.write().await;
        streams.retain(|_, weak| weak.strong_count() > 0);
        if streams.contains_key(&id) {
            return Err(SetError::IdTaken);
        }
        let src = Arc::new(SyncedReceiverSource::new(rx));
        let erased: Arc<dyn ErasedSource> = src.clone();
        streams.insert(id, Arc::downgrade(&erased));
        Ok(src)
    }
}

impl Drop for Registry {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[derive(serde::Deserialize)]
struct DataParams {
    id: Option<usize>,
    id_str: Option<String>,
}

impl DataParams {
    fn label(self) -> Option<RegistryLabel> {
        if let Some(s) = self.id_str {
            Some(RegistryLabel::Text(s))
        } else {
            self.id.map(RegistryLabel::Number)
        }
    }
}

async fn data(
    State(state): State<AppState>,
    Query(params): Query<DataParams>,
) -> impl IntoResponse {
    let source = match params.label() {
        Some(label) => {
            let var = state.vars.read().await.get(&label).and_then(Weak::upgrade);
            match var {
                Some(var) => Some(var),
                None => state
                    .streams
                    .read()
                    .await
                    .get(&label)
                    .and_then(Weak::upgrade),
            }
        }
        None => None,
    };
    let stream: Pin<Box<dyn Stream<Item = Result<Bytes, Infallible>> + Send>> = match source {
        Some(src) => Box::pin(src.encoded_stream().map(|payload| Ok(frame(payload)))),
        None => Box::pin(tokio_stream::iter(std::iter::empty::<
            Result<Bytes, Infallible>,
        >())),
    };
    (
        [(CONTENT_TYPE, "application/octet-stream")],
        Body::from_stream(stream),
    )
}

/// Rejects requests lacking a matching `Authorization: Bearer <token>` header.
async fn require_auth(
    State(token): State<Arc<str>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let authorized = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|t| t == &*token);
    if authorized {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// Wraps a payload as a length-prefixed frame (`u32` LE length + payload).
fn frame(payload: Vec<u8>) -> Bytes {
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    framed.extend_from_slice(&payload);
    Bytes::from(framed)
}
