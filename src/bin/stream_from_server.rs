use std::time::Duration;

use syncvar::DEFAULT_ADDR;
use syncvar::server::{Registry, ServerConfig};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // Default config: self-signed TLS on localhost, no auth. Set SYNCVAR_TOKEN to require a token.
    let config = ServerConfig {
        auth_token: std::env::var("SYNCVAR_TOKEN").ok(),
        ..Default::default()
    };
    let registry = Registry::with_config(DEFAULT_ADDR, config).await?;

    // A receiver-backed stream: we feed a caller-owned mpsc channel and serve its receiver.
    // Single-consumer — the first client to connect drains it, and events emitted before that
    // client connects are buffered (up to the channel capacity) rather than dropped.
    let (tx, rx) = mpsc::channel::<String>(64);
    let _handle = registry.stream_from(rx, 1usize).await?;

    println!(
        "streaming a generated feed id=1 at https://{}/data?id=1",
        registry.addr()
    );

    // Producer: emit a tick every second. `send` blocks once the 64-slot buffer fills (no client
    // yet) and errors once the single consumer disconnects, which ends the feed.
    let mut tick: u64 = 0;
    loop {
        tick += 1;
        if tx.send(format!("tick {tick}")).await.is_err() {
            println!("consumer gone; stopping feed");
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}
