use std::time::Duration;

use syncvar::SyncedVar;

#[tokio::main]
async fn main() {
    env_logger::init();

    let var = SyncedVar::<String>::default(1usize);

    loop {
        println!("{}", var.get().unwrap_or_default());
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
