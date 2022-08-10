use crate::dpdb_core::Result;
use std::collections::HashMap;
use std::fs::{remove_file, File, OpenOptions};
use std::io::{prelude::*, SeekFrom};

use super::utils::eq_u8;
use super::{Error, ErrorKind};

pub struct Storage {
    file: String,
    index: HashMap<Vec<u8>, u64>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            file: "foo.db".to_string(),
            index: HashMap::new(),
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
            index: self.index,
        })
    }

    /// the layout of a pair is: len(key)|len(value)|key|value
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let mut db = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&self.file)
            .unwrap();
        let key_meta = key.len().to_be_bytes();
        let value_meta = value.len().to_be_bytes();
        let buf = [&key_meta, &value_meta, key, value].concat();
        db.write_all(&buf)?;
        self.index.insert(
            key.to_vec(),
            db.stream_position()?
                - ((std::mem::size_of::<usize>() * 2 + key.len() + value.len()) as u64),
        );
        db.sync_all()?;
        Ok(None)
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut db = File::open(&self.file)?;
        let offset = self.index.get(key);
        match offset {
            Some(offset) => db.seek(SeekFrom::Start(*offset))?,
            None => return self.scan_for_key(key),
        };

        let mut pair_meta = [0u8; std::mem::size_of::<usize>() * 2];
        if db.read_exact(&mut pair_meta).is_err() {
            return Ok(None);
        }
        let key_len = usize::from_be_bytes(pair_meta[..8].try_into()?);
        let value_len = usize::from_be_bytes(pair_meta[8..16].try_into()?);
        let mut pair_loaded: Vec<u8> = vec![0u8; key_len + value_len];
        db.read_exact(&mut pair_loaded)?;
        //let key_loaded = &pair_loaded[..key_len];
        let value_loaded = &pair_loaded[key_len..(key_len + value_len)];
        Ok(Some(value_loaded.to_vec()))
    }

    pub fn scan_for_key(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut db = File::open(&self.file)?;
        loop {
            let mut pair_meta = [0u8; 16];
            if db.read_exact(&mut pair_meta).is_err() {
                return Err(Error {
                    kind: ErrorKind::Key,
                });
            }
            let key_len = usize::from_be_bytes(pair_meta[..8].try_into()?);
            let value_len = usize::from_be_bytes(pair_meta[8..16].try_into()?);
            let mut pair_loaded: Vec<u8> = vec![0u8; key_len + value_len];
            db.read_exact(&mut pair_loaded)?;
            let key_loaded = &pair_loaded[..key_len];
            let value_loaded = &pair_loaded[key_len..(key_len + value_len)];
            let offset = db.stream_position()?
                    - ((std::mem::size_of::<usize>() * 2 + key_len + value_len) as u64);
                self.index.insert(key.to_vec(), offset);
            if eq_u8(key, key_loaded) {
                return Ok(Some(value_loaded.to_vec()));
            }
        }
    }
}
