use crate::Error;
use std::net::SocketAddr;
use tokio::net::{TcpListener};

use super::RpcEnd;

pub struct Receiver {
    listener: TcpListener,
}

impl Receiver {
    pub async fn new(addr: SocketAddr) -> Result<Self, Error> {
        let listener = TcpListener::bind(&addr).await?;
        Ok(Receiver { listener })
    }

    pub async fn new_conn(&self) -> Result<RpcEnd, Error> {
        match self.listener.accept().await {
            Ok((socket, _)) => Ok(RpcEnd::new(socket)),
            Err(_) => Err(Error::Network),
        }
    }
}
