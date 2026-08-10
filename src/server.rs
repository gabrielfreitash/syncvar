use std::collections::HashMap;
use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use axum::body::{Body, Bytes};
use axum::extract::{Query, State};
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::{Router, routing::get};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::WatchStream;
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

/// Wraps a payload as a length-prefixed frame (`u32` LE length + payload).
fn frame(payload: Vec<u8>) -> Bytes {
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    framed.extend_from_slice(&payload);
    Bytes::from(framed)
}
