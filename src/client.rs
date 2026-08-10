use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use crate::{DEFAULT_ADDR, RegistryLabel, Value};

const MIN_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

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
