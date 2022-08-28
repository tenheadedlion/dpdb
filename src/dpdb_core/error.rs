use std::{array::TryFromSliceError, net::AddrParseError, result::Result as StdResult};

use rustyline::error::ReadlineError;
use tokio_util::codec::LinesCodecError;

pub type Result<T, E = Error> = StdResult<T, E>;

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
}

#[derive(Debug)]
pub enum ErrorKind {
    Parser,
    IO,
    Display,
    Key,
    File,
    Unknown,
    Socket,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            ErrorKind::Parser => {
                write!(f, "invalid sql")
            }
            ErrorKind::IO => {
                write!(f, "filesystem failure")
            }
            ErrorKind::Display => {
                write!(f, "display error")
            }
            ErrorKind::Key => {
                write!(f, "no value for the key")
            }
            ErrorKind::File => {
                write!(f, "wrong file or directory")
            }
            ErrorKind::Socket => {
                write!(f, "network problem")
            }
            ErrorKind::Unknown => {
                write!(f, "unknown error")
            }
        }
    }
}

impl From<nom::Err<nom::error::Error<&str>>> for Error {
    fn from(_: nom::Err<nom::error::Error<&str>>) -> Self {
        Error {
            kind: ErrorKind::Parser,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        Error {
            kind: ErrorKind::IO,
        }
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(_: std::str::Utf8Error) -> Self {
        Error {
            kind: ErrorKind::Display,
        }
    }
}

impl From<TryFromSliceError> for Error {
    fn from(_: TryFromSliceError) -> Self {
        Error {
            kind: ErrorKind::Parser,
        }
    }
}

impl From<AddrParseError> for Error {
    fn from(_: AddrParseError) -> Self {
        Error {
            kind: ErrorKind::Unknown,
        }
    }
}

impl From<LinesCodecError> for Error {
    fn from(_: LinesCodecError) -> Self {
        Error {
            kind: ErrorKind::Socket,
        }
    }
}

impl From<ReadlineError> for Error {
    fn from(_: ReadlineError) -> Self {
        Error {
            kind: ErrorKind::Display,
        }
    }
}
