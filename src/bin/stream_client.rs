use syncvar::DEFAULT_ADDR;
use syncvar::client::{ClientConfig, SyncedStream};

#[tokio::main]
async fn main() {
    env_logger::init();

    // Default config: TLS on, trusting the server's self-signed cert. Set SYNCVAR_TOKEN if required.
    let config = ClientConfig {
        auth_token: std::env::var("SYNCVAR_TOKEN").ok(),
        ..Default::default()
    };
    let mut stream = SyncedStream::<String>::with_config(DEFAULT_ADDR, 1usize, config);

    // Unlike a SyncedVar, which mirrors only the latest value, a stream yields every event in
    // order as it arrives. recv returns None only when the background connection task ends.
    println!("waiting for events (id=1)...");
    while let Some(event) = stream.recv().await {
        println!("{event}");
    }
}
