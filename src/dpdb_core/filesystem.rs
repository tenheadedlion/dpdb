use crate::dpdb_core::{Error, ErrorKind, Result};

use std::fs::{remove_file, File, OpenOptions};
use std::io::prelude::*;
use std::io::BufReader;
/// remove the database file
pub fn reset() -> std::io::Result<Option<String>> {
    remove_file("foo.db")?;
    Ok(None)
}

pub fn set(key: &str, value: &str) -> std::io::Result<Option<String>> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("foo.db")
        .unwrap();
    file.write_all(key.as_bytes())?;
    file.write_all(" ".as_bytes())?;
    file.write_all(value.as_bytes())?;
    file.write_all("\n".as_bytes())?;
    file.sync_all()?;
    Ok(None)
}

pub fn get(key: &str) -> Result<Option<String>> {
    let db = File::open("foo.db")?;
    let reader = BufReader::new(db);
    let lines = reader.lines();
    for line in lines.flatten() {
        let mut pair = line.split(' ');
        let k = pair.next().ok_or(Error {
            kind: ErrorKind::IO,
        })?;
        if k.eq(key) {
            let v = pair.next().ok_or(Error {
                kind: ErrorKind::IO,
            })?;
            return Ok(Some(v.to_string()));
        }
    }
    Ok(None)
}
