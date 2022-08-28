pub(crate) mod receiver;
pub(crate) mod sender;
use crate::Result;
use futures::SinkExt;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed as TokioFramed, LinesCodec};

use tokio::net::TcpStream;
type Framed = TokioFramed<TcpStream, LinesCodec>;

#[derive(Debug)]
pub struct RpcEnd {
    framed: Framed,
}

impl RpcEnd {
    pub fn new(socket: TcpStream) -> Self {
        let framed = Framed::new(socket, LinesCodec::new());
        RpcEnd { framed }
    }
    pub async fn receive(&mut self) -> Result<Option<String>> {
        match self.framed.next().await {
            Some(line) => Ok(Some(line?)),
            None => Ok(None),
        }
    }
    pub async fn send(&mut self, line: &str) -> Result<()> {
        self.framed.send(line).await?;
        Ok(())
    }
}
