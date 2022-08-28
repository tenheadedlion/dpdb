mod config;
mod dpsql;
mod server;
use crate::Error;
use clap::{Arg, Command};
pub use config::CF;
use std::path::Path;

fn path_valid(v: &str) -> Result<(), Error> {
    let path = Path::new(v);
    if !path.is_dir() {
        return Err(Error::Fs);
    }
    Ok(())
}

pub fn init() {
    let setup = Command::new("Dpdb says hello");
    let setup = setup.subcommand(
        Command::new("start").arg(
            Arg::new("path")
                .index(1)
                .required(true)
                .validator(path_valid)
                .help("Database path used for storing data"),
        ),
    );
    let setup = setup.subcommand(Command::new("connect"));

    let matches = setup.get_matches();
    let output = match matches.subcommand() {
        Some(("start", m)) => server::init(m),
        Some(("connect", _m)) => dpsql::init(),
        _ => Ok(()),
    };
    if let Err(e) = output {
        println!("{}", e);
    }
}
