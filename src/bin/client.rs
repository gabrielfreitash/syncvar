use std::time::Duration;

use syncvar::{RegistryLabel, SyncedVar};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let var = SyncedVar::default(RegistryLabel::NUMBER(1)).await?;

    loop {
        println!("{}", var.get());
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
