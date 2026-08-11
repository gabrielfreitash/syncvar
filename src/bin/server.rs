use std::io::Write;

use syncvar::DEFAULT_ADDR;
use syncvar::server::{Registry, ServerConfig};
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // Default config: self-signed TLS on localhost, no auth. Set SYNCVAR_TOKEN to require a token.
    let config = ServerConfig {
        auth_token: std::env::var("SYNCVAR_TOKEN").ok(),
        ..Default::default()
    };
    let registry = Registry::with_config(DEFAULT_ADDR, config).await?;
    let var = registry.set(String::new(), 1usize).await?;

    println!(
        "serving variable id=1 at https://{}/data?id=1",
        registry.addr()
    );

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        print!("value> ");
        std::io::stdout().flush()?;
        match lines.next_line().await? {
            Some(line) => var.set(line),
            None => break,
        }
    }
    Ok(())
}
