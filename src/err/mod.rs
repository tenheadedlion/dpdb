use std::net::AddrParseError;

use dpdb_core::Error as DbError;
use rustyline::error::ReadlineError;
use thiserror::Error;
use tokio_util::codec::LinesCodecError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Database error: {0}")]
    Db(#[from] DbError),
    #[error("Repl error")]
    Repl,
    #[error("Network error")]
    Network,
    // todo: should move this to dpdb_core
    #[error("File system error")]
    Fs,
}

impl From<ReadlineError> for Error {
    fn from(_: ReadlineError) -> Self {
        Error::Repl
    }
}

impl From<LinesCodecError> for Error {
    fn from(_: LinesCodecError) -> Self {
        Error::Network
    }
}

impl From<AddrParseError> for Error {
    fn from(_: AddrParseError) -> Self {
        Error::Network
    }
}

impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        Error::Network
    }
}
