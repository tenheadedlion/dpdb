use log::info;

use super::config;
use crate::db;
use crate::net::receiver::Receiver;
use crate::Error;
use std::net::SocketAddr;

#[tokio::main]
pub async fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
    config::init(matches);
    db::init().await?;
    start("127.0.0.1:5860").await?;
    Ok(())
}

async fn start(addr: &str) -> Result<(), Error> {
    let addr = addr.to_string().parse::<SocketAddr>()?;
    let receiver = Receiver::new(addr).await?;
    let db = db::DB.get().unwrap();
    loop {
        let mut rpcend = receiver.new_conn().await?;
        tokio::spawn(async move {
            while let Ok(Some(line)) = rpcend.receive().await {
                info!("sql: {}", &line);
                let response = db.lock().await.execute(&line);
                let response = response.serialize().unwrap_or_else(|_| "".to_string());
                // what else do you want in a loop?
                let _ = rpcend.send(response.as_str()).await;
            }
        });
    }
}

// todo: think of a better way to run the test
#[cfg(test)]
mod test {
    use super::*;
    use crate::{cli::config::Config, net::RpcEnd};
    use config::CF;
    use tokio::net::TcpStream;
    async fn set_value() -> Result<(), Error> {
        let addr = "127.0.0.1:5861".to_string().parse::<SocketAddr>()?;
        let socket = TcpStream::connect(addr).await?;
        let mut rpcend = RpcEnd::new(socket);
        rpcend.send("set a 2").await?;
        Ok(())
    }
    async fn run_client() -> Result<(), Error> {
        let addr = "127.0.0.1:5861".to_string().parse::<SocketAddr>()?;
        let socket = TcpStream::connect(addr).await?;
        let mut rpcend = RpcEnd::new(socket);
        loop {
            rpcend.send("get a").await?;
        }
    }
    #[tokio::test]
    async fn test() -> Result<(), Error> {
        let _ = CF.set(Config {
            path: String::from("/media/root_/SLC16/test"),
        });
        db::init().await?;
        tokio::spawn(async move {
            let _ = start("127.0.0.1:5861").await;
        });
        let _ = tokio::join!(set_value());
        let mut joins = vec![];
        for _ in 1..20 {
            joins.push(tokio::spawn(run_client()));
        }
        for j in joins {
            let _ = j.await;
        }

        Ok(())
    }
}
