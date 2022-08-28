pub mod receiver;
use futures::SinkExt;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed as TokioFramed, LinesCodec};

use tokio::net::TcpStream;
pub type Framed = TokioFramed<TcpStream, LinesCodec>;
use crate::Error;

#[derive(Debug)]
pub struct RpcEnd {
    framed: Framed,
}

impl RpcEnd {
    pub fn new(socket: TcpStream) -> Self {
        let framed = Framed::new(socket, LinesCodec::new());
        RpcEnd { framed }
    }
    pub async fn receive(&mut self) -> Result<Option<String>, Error> {
        match self.framed.next().await {
            Some(line) => Ok(Some(line?)),
            None => Ok(None),
        }
    }
    pub async fn send(&mut self, line: &str) -> Result<(), Error> {
        self.framed.send(line).await?;
        Ok(())
    }
}
