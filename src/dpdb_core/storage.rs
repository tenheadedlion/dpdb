use crate::dpdb_core::{utils::eq_u8, Result};
use std::fs::{remove_file, File, OpenOptions};
use std::io::prelude::*;

use super::{Error, ErrorKind};

pub struct Storage {
    file: String,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            file: "foo.db".to_string(),
        }
    }

    /// remove the database file
    pub fn clear(&self) -> std::io::Result<Option<Vec<u8>>> {
        remove_file(&self.file)?;
        Ok(None)
    }

    pub fn reset(self, file: &str) -> Result<Self> {
        if std::fs::metadata(file).is_ok() {
            return Err(Error {
                kind: ErrorKind::IO,
            });
        }
        std::fs::copy(&self.file, file)?;
        remove_file(&self.file)?;
        Ok(Storage {
            file: file.to_string(),
        })
    }

    /// the layout of a pair is: len(key)|len(value)|key|value
    pub fn set(&self, key: &[u8], value: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&self.file)
            .unwrap();
        let key_meta = key.len().to_be_bytes();
        let value_meta = value.len().to_be_bytes();
        let buf = [&key_meta, &value_meta, key, value].concat();
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(None)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut db = File::open(&self.file)?;
        loop {
            let mut pair_meta = [0u8; 16];
            if db.read_exact(&mut pair_meta).is_err() {
                return Ok(None);
            }
            let key_len = usize::from_be_bytes(pair_meta[..8].try_into()?);
            let value_len = usize::from_be_bytes(pair_meta[8..16].try_into()?);
            let mut pair_loaded: Vec<u8> = vec![0u8; key_len + value_len];
            db.read_exact(&mut pair_loaded)?;
            let key_loaded = &pair_loaded[..key_len];
            let value_loaded = &pair_loaded[key_len..(key_len + value_len)];
            if eq_u8(key, key_loaded) {
                return Ok(Some(value_loaded.to_vec()));
            }
        }
    }
}
