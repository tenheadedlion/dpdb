use super::config;
use crate::db;
use crate::net::receiver::Receiver;
use crate::Error;
use std::net::SocketAddr;

#[tokio::main]
pub async fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
    config::init(matches);
    db::init().await?;
    start().await?;
    Ok(())
}

async fn start() -> Result<(), Error> {
    let addr = "127.0.0.1:5860".to_string().parse::<SocketAddr>()?;
    let receiver = Receiver::new(addr).await?;
    let db = db::DB.get().unwrap();
    loop {
        let mut rpcend = receiver.new_conn().await?;
        dbg!(&rpcend);
        tokio::spawn(async move {
            while let Ok(Some(line)) = rpcend.receive().await {
                let response = db.lock().await.execute(&line);
                let response = response.serialize().unwrap_or_else(|_| "".to_string());
                // what else do you want in a loop?
                let _ = rpcend.send(response.as_str()).await;
            }
        });
    }
}
