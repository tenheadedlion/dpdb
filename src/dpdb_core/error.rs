use std::result::Result as StdResult;

pub type Result<T, E = Error> = StdResult<T, E>;

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
}

#[derive(Debug)]
pub enum ErrorKind {
    Parser,
    IO,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid operation")
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
