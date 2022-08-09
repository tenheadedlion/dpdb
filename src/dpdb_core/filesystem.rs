use std::io::SeekFrom;

use crate::dpdb_core::Result;
use std::fs::{remove_file, File, OpenOptions};
use std::io::prelude::*;
/// remove the database file
pub fn reset() -> std::io::Result<Option<Vec<u8>>> {
    remove_file("foo.db")?;
    Ok(None)
}

/// the layout of a pair is: len(key)|len(value)|key|value
pub fn set(key: &[u8], value: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("foo.db")
        .unwrap();
    let key_meta = key.len().to_be_bytes();
    let value_meta = value.len().to_be_bytes();
    file.write_all(&key_meta)?;
    file.write_all(&value_meta)?;
    file.write_all(key)?;
    file.write_all(value)?;
    file.sync_all()?;
    Ok(None)
}
fn eq_u8(lhs: &[u8], rhs: &[u8]) -> bool {
    if lhs.len() != rhs.len() {
        return false;
    }
    for (u1, u2) in lhs.iter().zip(rhs.iter()) {
        if u1 != u2 {
            return false;
        }
    }
    true
}

pub fn get(key: &[u8]) -> Result<Option<Vec<u8>>> {
    let mut db = File::open("foo.db")?;
    loop {
        let mut key_meta = [0u8; 8];
        let mut value_meta = [0u8; 8];
        if db.read_exact(&mut key_meta).is_err() {}
        db.read_exact(&mut value_meta)?;
        let key_len = usize::from_be_bytes(key_meta);
        let value_len = usize::from_be_bytes(value_meta);
        let mut key_loaded: Vec<u8> = vec![0u8; key_len];
        db.read_exact(&mut key_loaded)?;
        if eq_u8(key, &key_loaded) {
            let mut value_loaded: Vec<u8> = vec![0u8; value_len];
            db.read_exact(&mut value_loaded)?;
            return Ok(Some(value_loaded));
        } else {
            db.seek(SeekFrom::Current(
                (value_len) as i64,
            ))?;
        }
    }
}
