use crate::net::RpcEnd;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use crate::Error;

#[tokio::main]
pub async fn init() -> Result<(), Error> {
    // `()` can be used when no completer is required
    let mut rl = Editor::<()>::new()?;
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    let addr = "127.0.0.1:5860".to_string().parse::<SocketAddr>()?;
    let socket = TcpStream::connect(addr).await?;
    let mut rpcend = RpcEnd::new(socket);
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                if line.trim().is_empty() {
                    continue;
                }
                rl.add_history_entry(line.as_str());
                if let Err(e) = rpcend.send(line.as_str()).await {
                    println!("error on sending response; error = {:?}", e);
                }
                // further work is needed to hide the protocol detail
                while let Ok(Some(result)) = rpcend.receive().await {
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
