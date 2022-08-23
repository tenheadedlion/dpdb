use dpdb::Result;
use std::env;
use std::net::SocketAddr;
use tokio::net::TcpListener;

use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use futures::SinkExt;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<()> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:5860".to_string())
        .parse::<SocketAddr>()?;
    let listener = TcpListener::bind(&addr).await?;
    let executor = dpdb::Executor::new().expect("this should not fail");
    let db = Arc::new(Mutex::new(executor));

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let db = db.clone();
                tokio::spawn(async move {
                    let mut lines = Framed::new(socket, LinesCodec::new());
                    while let Some(result) = lines.next().await {
                        match result {
                            Ok(line) => {
                                let response = db.lock().unwrap().execute(&line);
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
