use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use rkyv::api::high::{HighDeserializer, HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

pub mod client;
pub mod server;

pub const DEFAULT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6613);

/// Installs the ring TLS crypto provider as the process default, once per process.
///
/// Both the server (serving over rustls) and the client (reqwest's rustls backend) rely on a
/// process-default [`CryptoProvider`](rustls::crypto::CryptoProvider); this sets it. A no-op if
/// one is already installed.
pub(crate) fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        if rustls::crypto::ring::default_provider()
            .install_default()
            .is_err()
        {
            log::debug!("rustls crypto provider already installed");
        }
    });
}

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
        // network buffers are unaligned; rkyv's checked reader needs alignment.
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
    /// The query parameter (`id`/`id_str`) identifying this label on the wire.
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

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use super::Value;
    use crate::client::{ClientConfig, SyncedStream, SyncedVar};
    use crate::server::{Registry, ServerConfig, SetError, SyncedBroadcastSource, Tls};

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
        let registry = Registry::new(loopback()).await.unwrap();
        let source = registry.set("ola".to_string(), 1usize).await.unwrap();
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

    #[tokio::test]
    async fn auth_allows_matching_token() {
        let registry = Registry::with_auth(loopback(), "secret").await.unwrap();
        let _source = registry.set("guarded".to_string(), 1usize).await.unwrap();

        let config = ClientConfig {
            auth_token: Some("secret".to_string()),
            ..Default::default()
        };
        let var = SyncedVar::<String>::with_config(registry.addr(), 1usize, config);
        eventually(&var, &Some("guarded".to_string())).await;
    }

    #[tokio::test]
    async fn auth_rejects_missing_token() {
        let registry = Registry::with_auth(loopback(), "secret").await.unwrap();
        let _source = registry.set("guarded".to_string(), 1usize).await.unwrap();

        let var = SyncedVar::<String>::new(registry.addr(), 1usize);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(var.get(), None);
    }

    #[tokio::test]
    async fn auth_rejects_wrong_token() {
        let registry = Registry::with_auth(loopback(), "secret").await.unwrap();
        let _source = registry.set("guarded".to_string(), 1usize).await.unwrap();

        let config = ClientConfig {
            auth_token: Some("nope".to_string()),
            ..Default::default()
        };
        let var = SyncedVar::<String>::with_config(registry.addr(), 1usize, config);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(var.get(), None);
    }

    #[tokio::test]
    async fn self_signed_tls_syncs() {
        let config = ServerConfig {
            tls: Some(Tls::self_signed_localhost()),
            ..Default::default()
        };
        let registry = Registry::with_config(loopback(), config).await.unwrap();
        let _source = registry.set("secure".to_string(), 1usize).await.unwrap();

        let client = ClientConfig {
            tls: true,
            danger_accept_invalid_certs: true,
            ..Default::default()
        };
        let var = SyncedVar::<String>::with_config(registry.addr(), 1usize, client);
        eventually(&var, &Some("secure".to_string())).await;
    }

    #[tokio::test]
    async fn tls_rejects_untrusted_cert_when_verifying() {
        let config = ServerConfig {
            tls: Some(Tls::self_signed_localhost()),
            ..Default::default()
        };
        let registry = Registry::with_config(loopback(), config).await.unwrap();
        let _source = registry.set("secure".to_string(), 1usize).await.unwrap();

        let client = ClientConfig {
            tls: true,
            danger_accept_invalid_certs: false,
            ..Default::default()
        };
        let var = SyncedVar::<String>::with_config(registry.addr(), 1usize, client);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(var.get(), None);
    }

    #[tokio::test]
    async fn tls_with_auth_syncs() {
        let config = ServerConfig {
            auth_token: Some("secret".to_string()),
            tls: Some(Tls::self_signed_localhost()),
        };
        let registry = Registry::with_config(loopback(), config).await.unwrap();
        let _source = registry.set("both".to_string(), 1usize).await.unwrap();

        let client = ClientConfig {
            auth_token: Some("secret".to_string()),
            tls: true,
            danger_accept_invalid_certs: true,
        };
        let var = SyncedVar::<String>::with_config(registry.addr(), 1usize, client);
        eventually(&var, &Some("both".to_string())).await;
    }

    #[tokio::test]
    async fn default_config_uses_self_signed_tls() {
        assert!(ServerConfig::default().tls.is_some());
        assert!(ServerConfig::default().auth_token.is_none());
        assert!(ClientConfig::default().tls);
        assert!(ClientConfig::default().danger_accept_invalid_certs);

        let registry = Registry::with_config(loopback(), ServerConfig::default())
            .await
            .unwrap();
        let _source = registry.set("default".to_string(), 1usize).await.unwrap();

        let var = SyncedVar::<String>::with_config(registry.addr(), 1usize, ClientConfig::default());
        eventually(&var, &Some("default".to_string())).await;
    }

    /// Awaits one stream event, failing the test if none arrives promptly.
    async fn recv_soon<T: Value>(stream: &mut SyncedStream<T>) -> Option<T> {
        tokio::time::timeout(Duration::from_secs(2), stream.recv())
            .await
            .expect("timed out waiting for stream event")
    }

    /// Waits until at least `n` clients are connected to a broadcast source. A broadcast source
    /// has no value to replay, so events emitted before a subscriber connects are lost; tests
    /// must confirm the subscriber before emitting.
    async fn wait_subscribers<T: Value>(source: &SyncedBroadcastSource<T>, n: usize) {
        for _ in 0..200 {
            if source.subscriber_count() >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("expected {n} subscribers, saw {}", source.subscriber_count());
    }

    #[tokio::test]
    async fn stream_delivers_events_in_order() {
        let registry = Registry::new(loopback()).await.unwrap();
        let source = registry.stream::<i32>(1usize, 16).await.unwrap();

        let mut stream = SyncedStream::<i32>::new(registry.addr(), 1usize);
        wait_subscribers(&source, 1).await;

        source.set(1);
        source.set(2);
        source.set(3);

        assert_eq!(recv_soon(&mut stream).await, Some(1));
        assert_eq!(recv_soon(&mut stream).await, Some(2));
        assert_eq!(recv_soon(&mut stream).await, Some(3));
    }

    #[tokio::test]
    async fn stream_reaches_multiple_subscribers() {
        let registry = Registry::new(loopback()).await.unwrap();
        let source = registry.stream::<String>(1usize, 16).await.unwrap();

        let mut a = SyncedStream::<String>::new(registry.addr(), 1usize);
        let mut b = SyncedStream::<String>::new(registry.addr(), 1usize);
        wait_subscribers(&source, 2).await;

        source.set("hi".to_string());
        assert_eq!(recv_soon(&mut a).await, Some("hi".to_string()));
        assert_eq!(recv_soon(&mut b).await, Some("hi".to_string()));
    }

    #[tokio::test]
    async fn stream_rejects_taken_id() {
        let registry = Registry::new(loopback()).await.unwrap();
        let _first = registry.stream::<i32>(1usize, 8).await.unwrap();
        let err = registry.stream::<i32>(1usize, 8).await;
        assert_eq!(err.err(), Some(SetError::IdTaken));
    }

    #[tokio::test]
    async fn stream_reuses_dropped_id() {
        let registry = Registry::new(loopback()).await.unwrap();
        drop(registry.stream::<i32>(1usize, 8).await.unwrap());
        let reused = registry.stream::<i32>(1usize, 8).await;
        assert!(reused.is_ok());
    }

    #[tokio::test]
    async fn var_and_stream_coexist() {
        let registry = Registry::new(loopback()).await.unwrap();
        let var_src = registry.set("v".to_string(), 1usize).await.unwrap();
        let stream_src = registry.stream::<i32>(2usize, 16).await.unwrap();

        let var = SyncedVar::<String>::new(registry.addr(), 1usize);
        eventually(&var, &Some("v".to_string())).await;

        let mut stream = SyncedStream::<i32>::new(registry.addr(), 2usize);
        wait_subscribers(&stream_src, 1).await;
        stream_src.set(42);
        assert_eq!(recv_soon(&mut stream).await, Some(42));

        assert_eq!(var_src.get(), "v");
    }

    #[tokio::test]
    async fn receiver_stream_delivers_buffered_events() {
        let registry = Registry::new(loopback()).await.unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel::<i32>(16);
        // A receiver source buffers events until its consumer connects, so emit before connecting.
        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();
        tx.send(3).await.unwrap();
        let _handle = registry.stream_from(rx, 1usize).await.unwrap();

        let mut stream = SyncedStream::<i32>::new(registry.addr(), 1usize);
        assert_eq!(recv_soon(&mut stream).await, Some(1));
        assert_eq!(recv_soon(&mut stream).await, Some(2));
        assert_eq!(recv_soon(&mut stream).await, Some(3));
    }

    #[tokio::test]
    async fn receiver_stream_is_single_consumer() {
        let registry = Registry::new(loopback()).await.unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel::<i32>(16);
        let _handle = registry.stream_from(rx, 1usize).await.unwrap();

        tx.send(1).await.unwrap();
        let mut first = SyncedStream::<i32>::new(registry.addr(), 1usize);
        assert_eq!(recv_soon(&mut first).await, Some(1));

        // The receiver was taken by the first connection; a second client gets nothing.
        let mut second = SyncedStream::<i32>::new(registry.addr(), 1usize);
        let got = tokio::time::timeout(Duration::from_millis(300), second.recv()).await;
        assert!(got.is_err(), "second consumer should receive no events");
    }

    #[tokio::test]
    async fn stream_from_rejects_taken_id() {
        let registry = Registry::new(loopback()).await.unwrap();
        let (_tx, rx) = tokio::sync::mpsc::channel::<i32>(4);
        let _first = registry.stream_from(rx, 1usize).await.unwrap();
        let (_tx2, rx2) = tokio::sync::mpsc::channel::<i32>(4);
        let err = registry.stream_from(rx2, 1usize).await;
        assert_eq!(err.err(), Some(SetError::IdTaken));
    }

    #[tokio::test]
    async fn plain_http_syncs_without_tls() {
        let config = ServerConfig {
            tls: None,
            ..Default::default()
        };
        let registry = Registry::with_config(loopback(), config).await.unwrap();
        let _source = registry.set("cleartext".to_string(), 1usize).await.unwrap();

        let client = ClientConfig {
            tls: false,
            ..Default::default()
        };
        let var = SyncedVar::<String>::with_config(registry.addr(), 1usize, client);
        eventually(&var, &Some("cleartext".to_string())).await;
    }
}
