use crate::Result;
use std::env;
use std::net::SocketAddr;
use tokio::net::TcpListener;

use crate::dpdb_core::db;

use futures::SinkExt;

use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use super::config;

#[tokio::main]
pub async fn init(matches: &clap::ArgMatches) -> Result<()> {
    config::init(matches);
    db::init().await?;
    start().await?;
    Ok(())
}

async fn start() -> Result<()> {
    let addr = "127.0.0.1:5860".to_string().parse::<SocketAddr>()?;
    dbg!(&addr);
    let listener = TcpListener::bind(&addr).await?;
    let db = db::DB.get().unwrap();
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                tokio::spawn(async move {
                    let mut lines = Framed::new(socket, LinesCodec::new());
                    while let Some(result) = lines.next().await {
                        match result {
                            Ok(line) => {
                                let response = db.lock().await.execute(&line);
                                let response =
                                    response.serialize().unwrap_or_else(|_| "".to_string());
                                if let Err(e) = lines.send(response.as_str()).await {
                                    println!("error on sending response; error = {:?}", e);
                                }
                            }
                            Err(e) => {
                                println!("error on decoding from socket; error = {:?}", e);
                            }
                        }
                    }
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}
