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
    use crate::client::SyncedVar;
    use crate::server::{Registry, SetError};

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
}
