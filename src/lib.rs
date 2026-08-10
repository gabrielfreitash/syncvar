use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Weak};

use axum::extract::{Query, State};
use axum::response::sse::{Event, Sse};
use axum::{Router, routing::get};
use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use eventsource_stream::Eventsource;
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

/// A synced value: shareable across tasks and rkyv round-trippable over the wire.
pub trait Value: Default + Clone + Send + Sync + 'static {
    fn encode(&self) -> String;
    fn decode(data: &str) -> Option<Self>
    where
        Self: Sized;
}

impl<T> Value for T
where
    T: Default
        + Clone
        + Send
        + Sync
        + 'static
        + Archive
        + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    T::Archived: for<'a> CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, HighDeserializer<rancor::Error>>,
{
    fn encode(&self) -> String {
        BASE64_STANDARD.encode(rkyv::to_bytes::<rancor::Error>(self).unwrap())
    }

    fn decode(data: &str) -> Option<Self> {
        let bytes = BASE64_STANDARD.decode(data).ok()?;
        let mut aligned = AlignedVec::<16>::new();
        aligned.extend_from_slice(&bytes);
        rkyv::from_bytes::<T, rancor::Error>(&aligned).ok()
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub enum RegistryLabel {
    TEXT(String),
    NUMBER(usize),
}

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

    fn subscribe(&self) -> watch::Receiver<T> {
        self.tx.subscribe()
    }
}

pub struct SyncedVar<T> {
    value: watch::Receiver<T>,
    task: JoinHandle<()>,
}

impl<T: Value> SyncedVar<T> {
    pub async fn new(source: SocketAddr, id: RegistryLabel) -> reqwest::Result<Self> {
        let query = match id {
            RegistryLabel::NUMBER(n) => ("id", n.to_string()),
            RegistryLabel::TEXT(s) => ("id_str", s),
        };
        let resp = reqwest::Client::new()
            .get(format!("http://{source}/data"))
            .query(&[query])
            .send()
            .await?
            .error_for_status()?;
        let (tx, value) = watch::channel(T::default());
        let task = tokio::spawn(async move {
            let mut events = Box::pin(resp.bytes_stream().eventsource());
            while let Some(Ok(event)) = events.next().await {
                if let Some(val) = T::decode(&event.data) {
                    tx.send_replace(val);
                }
            }
        });
        Ok(SyncedVar { value, task })
    }

    pub async fn default(id: RegistryLabel) -> reqwest::Result<Self> {
        SyncedVar::new(DEFAULT_ADDR, id).await
    }

    pub fn get(&self) -> T {
        self.value.borrow().clone()
    }
}

impl<T> Drop for SyncedVar<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

type Vars<T> = Arc<RwLock<HashMap<RegistryLabel, Weak<SyncedVarSource<T>>>>>;

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

pub struct Registry<T> {
    vars: Vars<T>,
    server: JoinHandle<()>,
    addr: SocketAddr,
}

impl<T: Value> Registry<T> {
    pub async fn new(source: SocketAddr) -> Self {
        let vars: Vars<T> = Arc::new(RwLock::new(HashMap::new()));
        let listener = TcpListener::bind(source).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/data", get(data::<T>))
            .with_state(vars.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Registry { vars, server, addr }
    }

    pub async fn default() -> Self {
        Registry::new(DEFAULT_ADDR).await
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn set(
        &self,
        val: T,
        id: RegistryLabel,
    ) -> Result<Arc<SyncedVarSource<T>>, SetError> {
        let mut vars = self.vars.write().await;
        if vars.get(&id).and_then(Weak::upgrade).is_some() {
            return Err(SetError::IdTaken);
        }
        let var = Arc::new(SyncedVarSource::new(val));
        vars.insert(id, Arc::downgrade(&var));
        Ok(var)
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
            Some(RegistryLabel::TEXT(s))
        } else {
            self.id.map(RegistryLabel::NUMBER)
        }
    }
}

async fn data<T: Value>(
    State(vars): State<Vars<T>>,
    Query(params): Query<DataParams>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let rx = match params.label() {
        Some(label) => vars
            .read()
            .await
            .get(&label)
            .and_then(Weak::upgrade)
            .map(|v| v.subscribe()),
        None => None,
    };
    let rx = rx.unwrap_or_else(|| watch::channel(T::default()).1);
    let stream = WatchStream::from_changes(rx).map(|val| Ok(Event::default().data(val.encode())));
    Sse::new(stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loopback() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 0))
    }

    #[tokio::test]
    async fn it_works() {
        let syncvar = SyncedVarSource::new("ola".to_string());
        assert_eq!(syncvar.get(), "ola");
        syncvar.set("oi".to_string());
        assert_eq!(syncvar.get(), "oi");
    }

    #[tokio::test]
    async fn synced_var_mirrors_source_over_sse() {
        use std::time::Duration;

        let registry = Registry::new(loopback()).await;
        let source = registry
            .set("init".to_string(), RegistryLabel::NUMBER(1))
            .await
            .unwrap();

        let var = SyncedVar::new(registry.addr, RegistryLabel::NUMBER(1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        source.set("updated".to_string());

        let mut got = String::new();
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            got = var.get();
            if got == "updated" {
                break;
            }
        }
        registry.server.abort();

        assert_eq!(got, "updated");
    }

    #[tokio::test]
    async fn data_endpoint_streams_var_changes() {
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let registry = Registry::new(loopback()).await;
        let v1 = registry
            .set("hello".to_string(), RegistryLabel::NUMBER(1))
            .await
            .unwrap();
        let v2 = registry
            .set("other".to_string(), RegistryLabel::NUMBER(2))
            .await
            .unwrap();

        let mut stream = TcpStream::connect(registry.addr).await.unwrap();
        stream
            .write_all(b"GET /data?id=1 HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        v2.set("other2".to_string());
        v1.set("world".to_string());
        let world_enc = "world".to_string().encode();
        let other_enc = "other2".to_string().encode();

        let mut resp = String::new();
        loop {
            let mut buf = [0u8; 1024];
            let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
                .await
                .unwrap()
                .unwrap();
            assert!(n > 0, "connection closed before event: {resp}");
            resp.push_str(&String::from_utf8_lossy(&buf[..n]));
            if resp.contains(&world_enc) {
                break;
            }
        }
        registry.server.abort();

        assert!(resp.contains("content-type: text/event-stream"));
        assert!(!resp.contains(&other_enc));
    }

    #[tokio::test]
    async fn set_rejects_taken_id() {
        let registry = Registry::new(loopback()).await;
        let _first = registry
            .set("first".to_string(), RegistryLabel::TEXT("a".to_string()))
            .await
            .unwrap();
        let err = registry
            .set("second".to_string(), RegistryLabel::TEXT("a".to_string()))
            .await;
        registry.server.abort();

        assert_eq!(err.err(), Some(SetError::IdTaken));
    }

    #[tokio::test]
    async fn set_reuses_dropped_id() {
        let registry = Registry::new(loopback()).await;
        drop(
            registry
                .set("first".to_string(), RegistryLabel::TEXT("a".to_string()))
                .await
                .unwrap(),
        );
        let reused = registry
            .set("second".to_string(), RegistryLabel::TEXT("a".to_string()))
            .await
            .unwrap();
        registry.server.abort();

        assert_eq!(reused.get(), "second");
    }

    #[tokio::test]
    async fn syncs_non_string_value() {
        use std::time::Duration;

        #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Default, Debug, PartialEq)]
        struct Point {
            x: i32,
            y: i32,
        }

        let registry = Registry::<Point>::new(loopback()).await;
        let source = registry
            .set(Point::default(), RegistryLabel::NUMBER(1))
            .await
            .unwrap();

        let var = SyncedVar::<Point>::new(registry.addr, RegistryLabel::NUMBER(1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        source.set(Point { x: 3, y: 7 });

        let mut got = Point::default();
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            got = var.get();
            if got == (Point { x: 3, y: 7 }) {
                break;
            }
        }
        registry.server.abort();

        assert_eq!(got, Point { x: 3, y: 7 });
    }
}
