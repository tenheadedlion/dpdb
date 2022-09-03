use std::{
    fmt::format,
    fs::File,
    io::{BufRead, BufReader},
    net::SocketAddr,
    path::Path,
    time::Instant,
};

use crate::{net::RpcEnd, Error};
use futures::executor::block_on;
use log::info;
use rand::seq::SliceRandom;
use rand::thread_rng;
use tokio::task::{spawn_blocking, JoinHandle};
use tokio::{
    fs::OpenOptions,
    io::{AsyncBufReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};
use uuid::Uuid;

#[tokio::main]
pub async fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
    let r#type = matches.value_of("type").unwrap().to_owned();
    let pairs = load("test/data").await?;

    match r#type.as_str() {
        "write" => write(pairs).await?,
        "generate" => generate("test/data").await?,
        _ => {}
    }
    Ok(())
}

// todo: during this test, when the number of queries reaches a certain threshold, 
//  the server will go through a freezing phase, figure out why.
async fn write(data: Vec<(String, String)>) -> Result<(), Error> {
    let mut tasks: Vec<JoinHandle<()>> = Vec::new();
    for _ in 1..2 {
        let mut rng = thread_rng();
        let mut data = data.clone();
        data.shuffle(&mut rng);
        let quests_len = data.len() as u128;
        let task = tokio::spawn(async move {
            let now = Instant::now();
            let addr = "127.0.0.1:5860".to_string().parse::<SocketAddr>().unwrap();
            let socket = TcpStream::connect(addr).await.unwrap();
            let mut rpcend = RpcEnd::new(socket);
            for (i, d) in data.into_iter().enumerate() {
                if i > 100_000 {
                    break;
                }
                let sql = format!("set {} {}", d.0, d.1);
                _ = rpcend.send(&sql).await;
            }
            let time_elapsed = now.elapsed();
            info!(
                "test write, time_elapsed: {:?}, qps: {}",
                time_elapsed,
                quests_len / time_elapsed.as_micros() * 1000
            );
        });
        tasks.push(task);
    }

    for t in tasks {
        _ = t.await;
    }

    Ok(())
}

async fn load(file: &str) -> Result<Vec<(String, String)>, Error> {
    let file = File::open(file)?;
    let reader = BufReader::new(file);
    let lines = reader.lines();
    let mut res: Vec<(String, String)> = Vec::new();
    // let's put them in memory
    for line in lines.flatten() {
        let mut pair = line.split(' ');
        let k = pair.next().ok_or(Error::Fs)?;
        let v = pair.next().ok_or(Error::Fs)?;
        res.push((k.to_owned(), v.to_owned()));
    }
    Ok(res)
}

async fn generate(file: &str) -> Result<(), Error> {
    info!("generating {}", file);
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(file)
        .await?;
    let mut writer = BufWriter::new(file);
    for _ in 1..100_000 {
        let key = Uuid::new_v4();
        let value = Uuid::new_v4();
        let line = format!("{} {}\n", key, value);
        writer.write_all(line.as_bytes()).await?;
    }
    Ok(())
}
