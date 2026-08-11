use std::io::Write;

use syncvar::DEFAULT_ADDR;
use syncvar::server::{Registry, ServerConfig};
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // Default config: self-signed TLS on localhost, no auth. Set SYNCVAR_TOKEN to require a token.
    let config = ServerConfig::default();
    let registry = Registry::with_config(DEFAULT_ADDR, config).await?;
    // A stream, not a var: each line typed is broadcast as one event to every connected client.
    // The 64-event buffer lets a briefly-slow subscriber catch up before it starts skipping.
    let stream = registry.stream::<String>(1usize, 64).await?;

    println!(
        "streaming events id=1 at https://{}/data?id=1",
        registry.addr()
    );

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        print!("event> ");
        std::io::stdout().flush()?;
        match lines.next_line().await? {
            Some(line) => stream.set(line),
            None => break,
        }
    }
    Ok(())
}
