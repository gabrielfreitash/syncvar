use std::collections::HashMap;
use std::convert::Infallible;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Query, State};
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::{Router, routing::get};
use rkyv::api::high::{HighDeserializer, HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::{Stream, StreamExt};

pub const DEFAULT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6613);

const MIN_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// A value that can be synced: shareable across tasks and rkyv round-trippable over the wire.
///
/// Blanket-implemented for any `Clone + Send + Sync + 'static` type deriving rkyv's
/// `Archive`/`Serialize`/`Deserialize`.
pub trait Value: Clone + Send + Sync + 'static {
    fn encode(&self) -> Result<Vec<u8>, rancor::Error>;
    fn decode(bytes: &[u8]) -> Option<Self>
    where
        Self: Sized;
}

impl<T> Value for T
where
    T: Clone
        + Send
        + Sync
        + 'static
        + Archive
        + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    T::Archived: for<'a> CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, HighDeserializer<rancor::Error>>,
{
    fn encode(&self) -> Result<Vec<u8>, rancor::Error> {
        Ok(rkyv::to_bytes::<rancor::Error>(self)?.to_vec())
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        // base64/network buffers are unaligned; rkyv's checked reader needs alignment.
        let mut aligned = AlignedVec::<16>::new();
        aligned.extend_from_slice(bytes);
        rkyv::from_bytes::<T, rancor::Error>(&aligned).ok()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum RegistryLabel {
    Text(String),
    Number(usize),
}

impl RegistryLabel {
    fn query(&self) -> (&'static str, String) {
        match self {
            RegistryLabel::Number(n) => ("id", n.to_string()),
            RegistryLabel::Text(s) => ("id_str", s.clone()),
        }
    }
}

impl From<usize> for RegistryLabel {
    fn from(n: usize) -> Self {
        RegistryLabel::Number(n)
    }
}

impl From<&str> for RegistryLabel {
    fn from(s: &str) -> Self {
        RegistryLabel::Text(s.to_owned())
    }
}

impl From<String> for RegistryLabel {
    fn from(s: String) -> Self {
        RegistryLabel::Text(s)
    }
}

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

/// Type-erased view of a source so a single `Registry` can hold vars of differing types.
trait ErasedSource: Send + Sync {
    /// Encoded payloads: the current value first, then one per change.
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

/// The receiving side of a synced variable, living on a client that mirrors a remote source.
///
/// `get` returns `None` until the first value arrives (and while a nonexistent id is requested).
/// The background connection reconnects with backoff, so a value survives server restarts.
pub struct SyncedVar<T> {
    value: watch::Receiver<Option<T>>,
    task: JoinHandle<()>,
}

impl<T: Value> SyncedVar<T> {
    pub fn new(source: SocketAddr, id: impl Into<RegistryLabel>) -> Self {
        let (tx, value) = watch::channel(None);
        let task = tokio::spawn(receive_loop::<T>(source, id.into(), tx));
        SyncedVar { value, task }
    }

    pub fn default(id: impl Into<RegistryLabel>) -> Self {
        SyncedVar::new(DEFAULT_ADDR, id)
    }

    pub fn get(&self) -> Option<T> {
        self.value.borrow().clone()
    }

    /// Await the next update and return the new value (`None` if the connection task ended).
    pub async fn changed(&mut self) -> Option<T> {
        self.value.changed().await.ok()?;
        self.value.borrow().clone()
    }
}

impl<T> Drop for SyncedVar<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn receive_loop<T: Value>(
    source: SocketAddr,
    id: RegistryLabel,
    tx: watch::Sender<Option<T>>,
) {
    let client = reqwest::Client::new();
    let mut backoff = MIN_BACKOFF;
    loop {
        match stream_once::<T>(&client, source, &id, &tx).await {
            Ok(true) => backoff = MIN_BACKOFF,
            Ok(false) => {}
            Err(e) => log::warn!("sync stream to {source} disconnected: {e}"),
        }
        if tx.is_closed() {
            return;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Streams one connection's worth of frames. Returns whether any value was received.
async fn stream_once<T: Value>(
    client: &reqwest::Client,
    source: SocketAddr,
    id: &RegistryLabel,
    tx: &watch::Sender<Option<T>>,
) -> reqwest::Result<bool> {
    let resp = client
        .get(format!("http://{source}/data"))
        .query(&[id.query()])
        .send()
        .await?
        .error_for_status()?;
    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    let mut received = false;
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk?);
        while let Some(frame) = take_frame(&mut buf) {
            match T::decode(&frame) {
                Some(val) => {
                    received = true;
                    if tx.send(Some(val)).is_err() {
                        return Ok(received);
                    }
                }
                None => log::warn!("failed to decode synced value"),
            }
        }
    }
    Ok(received)
}

/// Pops one length-prefixed frame (`u32` LE length + payload) from the front of `buf`.
fn take_frame(buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    if buf.len() < 4 {
        return None;
    }
    let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
    if buf.len() < 4 + len {
        return None;
    }
    let frame = buf[4..4 + len].to_vec();
    buf.drain(..4 + len);
    Some(frame)
}

fn frame(payload: Vec<u8>) -> Bytes {
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    framed.extend_from_slice(&payload);
    Bytes::from(framed)
}

type Vars = Arc<RwLock<HashMap<RegistryLabel, Weak<dyn ErasedSource>>>>;

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
    server: JoinHandle<()>,
    addr: SocketAddr,
}

impl Registry {
    pub async fn new(addr: SocketAddr) -> io::Result<Self> {
        let vars: Vars = Arc::new(RwLock::new(HashMap::new()));
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let app = Router::new()
            .route("/data", get(data))
            .with_state(vars.clone());
        let server = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                log::error!("registry server stopped: {e}");
            }
        });
        Ok(Registry { vars, server, addr })
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

async fn data(State(vars): State<Vars>, Query(params): Query<DataParams>) -> impl IntoResponse {
    let source = match params.label() {
        Some(label) => vars.read().await.get(&label).and_then(Weak::upgrade),
        None => None,
    };
    let stream: Pin<Box<dyn Stream<Item = Result<Bytes, Infallible>> + Send>> = match source {
        Some(src) => Box::pin(src.encoded_stream().map(|payload| Ok(frame(payload)))),
        None => Box::pin(tokio_stream::iter(std::iter::empty::<Result<Bytes, Infallible>>())),
    };
    (
        [(CONTENT_TYPE, "application/octet-stream")],
        Body::from_stream(stream),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loopback() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 0))
    }

    async fn eventually<T>(var: &SyncedVar<T>, want: &Option<T>)
    where
        T: Value + PartialEq + std::fmt::Debug,
    {
        for _ in 0..100 {
            if &var.get() == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(&var.get(), want);
    }

    #[tokio::test]
    async fn source_get_set() {
        let source = SyncedVarSource::new("ola".to_string());
        assert_eq!(source.get(), "ola");
        source.set("oi".to_string());
        assert_eq!(source.get(), "oi");
    }

    #[tokio::test]
    async fn syncs_current_value_on_connect() {
        let registry = Registry::new(loopback()).await.unwrap();
        let _source = registry.set("init".to_string(), 1usize).await.unwrap();

        let var = SyncedVar::<String>::new(registry.addr(), 1usize);
        eventually(&var, &Some("init".to_string())).await;
    }

    #[tokio::test]
    async fn mirrors_updates() {
        let registry = Registry::new(loopback()).await.unwrap();
        let source = registry.set("init".to_string(), 1usize).await.unwrap();

        let var = SyncedVar::<String>::new(registry.addr(), 1usize);
        eventually(&var, &Some("init".to_string())).await;

        source.set("updated".to_string());
        eventually(&var, &Some("updated".to_string())).await;
    }

    #[tokio::test]
    async fn routes_by_id() {
        let registry = Registry::new(loopback()).await.unwrap();
        let one = registry.set("a".to_string(), 1usize).await.unwrap();
        let _two = registry.set("b".to_string(), 2usize).await.unwrap();

        let var1 = SyncedVar::<String>::new(registry.addr(), 1usize);
        let var2 = SyncedVar::<String>::new(registry.addr(), 2usize);
        eventually(&var1, &Some("a".to_string())).await;
        eventually(&var2, &Some("b".to_string())).await;

        one.set("a2".to_string());
        eventually(&var1, &Some("a2".to_string())).await;
        assert_eq!(var2.get(), Some("b".to_string()));
    }

    #[tokio::test]
    async fn missing_id_stays_none() {
        let registry = Registry::new(loopback()).await.unwrap();
        let var = SyncedVar::<String>::new(registry.addr(), 99usize);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(var.get(), None);
    }

    #[tokio::test]
    async fn set_rejects_taken_id() {
        let registry = Registry::new(loopback()).await.unwrap();
        let _first = registry.set("first".to_string(), "a").await.unwrap();
        let err = registry.set("second".to_string(), "a").await;
        assert_eq!(err.err(), Some(SetError::IdTaken));
    }

    #[tokio::test]
    async fn set_reuses_dropped_id() {
        let registry = Registry::new(loopback()).await.unwrap();
        drop(registry.set("first".to_string(), "a").await.unwrap());
        let reused = registry.set("second".to_string(), "a").await.unwrap();
        assert_eq!(reused.get(), "second");
    }

    #[tokio::test]
    async fn syncs_non_string_value() {
        #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
        struct Point {
            x: i32,
            y: i32,
        }

        let registry = Registry::new(loopback()).await.unwrap();
        let source = registry.set(Point { x: 0, y: 0 }, 1usize).await.unwrap();

        let var = SyncedVar::<Point>::new(registry.addr(), 1usize);
        eventually(&var, &Some(Point { x: 0, y: 0 })).await;

        source.set(Point { x: 3, y: 7 });
        eventually(&var, &Some(Point { x: 3, y: 7 })).await;
    }

    #[tokio::test]
    async fn reconnects_after_server_restart() {
        let registry = Registry::new(loopback()).await.unwrap();
        let addr = registry.addr();
        let source = registry.set("first".to_string(), 1usize).await.unwrap();

        let var = SyncedVar::<String>::new(addr, 1usize);
        eventually(&var, &Some("first".to_string())).await;

        drop(source);
        drop(registry);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let registry = Registry::new(addr).await.unwrap();
        let _source = registry.set("second".to_string(), 1usize).await.unwrap();
        eventually(&var, &Some("second".to_string())).await;
    }
}
