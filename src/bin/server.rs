use std::io::Write;

use syncvar::{Registry, RegistryLabel};
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let registry = Registry::default().await;
    let var = registry.set(String::new(), RegistryLabel::NUMBER(1)).await?;

    println!("serving variable id=1 at http://{}/data?id=1", registry.addr());

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
