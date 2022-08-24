mod config;
mod server;
use std::path::Path;

use crate::dpdb_core::{Error, ErrorKind, Result};
use clap::{Arg, Command};
pub use config::CF;

fn path_valid(v: &str) -> Result<()> {
    let path = Path::new(v);
    if !path.is_dir() {
        return Err(Error {
            kind: ErrorKind::File,
        });
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

    let matches = setup.get_matches();
    let output = match matches.subcommand() {
        Some(("start", m)) => server::init(m),
        _ => Ok(()),
    };
    if let Err(e) = output {
        println!("{}", e);
    }
}
