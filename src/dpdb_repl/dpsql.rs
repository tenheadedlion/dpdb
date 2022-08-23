use futures::SinkExt;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
//, Result as RustyResult};
use std::env;
use std::net::SocketAddr;
use tokio_util::codec::{Framed, LinesCodec};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // `()` can be used when no completer is required
    let mut rl = Editor::<()>::new()?;
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:5860".to_string())
        .parse::<SocketAddr>()?;
    let socket = TcpStream::connect(addr).await?;
    let mut lines = Framed::new(socket, LinesCodec::new());
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                if line.trim().is_empty() {
                    continue;
                }
                rl.add_history_entry(line.as_str());
                if let Err(e) = lines.send(line.as_str()).await {
                    println!("error on sending response; error = {:?}", e);
                }
                while let Some(result) = lines.next().await {
                    let result = result?;
                    if result.eq("<BEGIN>") {
                        continue;
                    }
                    if result.eq("<END>") {
                        break;
                    }
                    println!("{}", result);
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history("history.txt")?;
    Ok(())
}
