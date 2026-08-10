use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Weak};

use axum::extract::{Query, State};
use axum::response::sse::{Event, Sse};
use axum::{Router, routing::get};
use eventsource_stream::Eventsource;
use tokio::net::TcpListener;
use tokio::sync::{RwLock, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::{Stream, StreamExt};

pub const DEFAULT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6613);

#[derive(Clone, Eq, Hash, PartialEq)]
pub enum RegistryLabel {
    TEXT(String),
    NUMBER(usize),
}

pub type Val = String;
pub struct SyncedVarSource {
    tx: watch::Sender<Val>,
}

impl SyncedVarSource {
    fn new(val: Val) -> Self {
        let (tx, _) = watch::channel(val);
        SyncedVarSource { tx }
    }

    pub fn get(&self) -> Val {
        self.tx.borrow().clone()
    }

    pub fn set(&self, val: Val) {
        self.tx.send_replace(val);
    }

    fn subscribe(&self) -> watch::Receiver<Val> {
        self.tx.subscribe()
    }
}

pub struct SyncedVar {
    value: watch::Receiver<Val>,
    task: JoinHandle<()>,
}

impl SyncedVar {
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
        let (tx, value) = watch::channel(Val::new());
        let task = tokio::spawn(async move {
            let mut events = Box::pin(resp.bytes_stream().eventsource());
            while let Some(Ok(event)) = events.next().await {
                tx.send_replace(event.data);
            }
        });
        Ok(SyncedVar { value, task })
    }

    pub async fn default(id: RegistryLabel) -> reqwest::Result<Self> {
        SyncedVar::new(DEFAULT_ADDR, id).await
    }

    pub fn get(&self) -> Val {
        self.value.borrow().clone()
    }
}

impl Drop for SyncedVar {
    fn drop(&mut self) {
        self.task.abort();
    }
}

type Vars = Arc<RwLock<HashMap<RegistryLabel, Weak<SyncedVarSource>>>>;

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
    pub async fn new(source: SocketAddr) -> Self {
        let vars: Vars = Arc::new(RwLock::new(HashMap::new()));
        let listener = TcpListener::bind(source).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/data", get(data))
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

    pub async fn set(&self, val: Val, id: RegistryLabel) -> Result<Arc<SyncedVarSource>, SetError> {
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

async fn data(
    State(vars): State<Vars>,
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
    let rx = rx.unwrap_or_else(|| watch::channel(Val::new()).1);
    let stream = WatchStream::from_changes(rx).map(|val| Ok(Event::default().data(val)));
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

        let mut resp = String::new();
        loop {
            let mut buf = [0u8; 1024];
            let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
                .await
                .unwrap()
                .unwrap();
            assert!(n > 0, "connection closed before event: {resp}");
            resp.push_str(&String::from_utf8_lossy(&buf[..n]));
            if resp.contains("data: world") {
                break;
            }
        }
        registry.server.abort();

        assert!(resp.contains("content-type: text/event-stream"));
        assert!(!resp.contains("data: other2"));
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
}
