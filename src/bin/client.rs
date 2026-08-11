use std::time::Duration;

use syncvar::DEFAULT_ADDR;
use syncvar::client::{ClientConfig, SyncedVar};

#[tokio::main]
async fn main() {
    env_logger::init();

    // Default config: TLS on, trusting the server's self-signed cert. Set SYNCVAR_TOKEN if required.
    let config = ClientConfig {
        auth_token: std::env::var("SYNCVAR_TOKEN").ok(),
        ..Default::default()
    };
    let var = SyncedVar::<String>::with_config(DEFAULT_ADDR, 1usize, config);

    loop {
        println!("{}", var.get().unwrap_or_default());
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
