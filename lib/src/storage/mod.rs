mod fs;
mod index;
use self::fs::FileSystem;
use crate::error::Result;
use crate::response::Response;
use crate::storage::index::Index;
use crate::utils::eq_u8;
use crate::{Error, ErrorKind};
use log::info;
use std::io::{prelude::*, SeekFrom};

pub struct Storage {
    index: Index,
    // the database files are immutable and append-only
    // so we just keep a record of the scanning progress
    index_seek: u64,
    fs: FileSystem,
}

impl Storage {
    pub fn new(dir: &str, file: &str) -> Result<Storage> {
        Ok(Storage {
            index: Index::new(),
            index_seek: 16,
            fs: FileSystem::new(dir, file)?,
        })
    }

    pub fn clear(&mut self) -> Result<Response> {
        self.fs.clear()?;
        self.index.clear();
        Ok(Response::Ok)
    }

    /// the layout of a pair is: len(key)|len(value)|key|value
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<Response> {
        let key_meta = key.len().to_be_bytes();
        let value_meta = value.len().to_be_bytes();
        let buf = [&key_meta, &value_meta, key, value].concat();
        self.fs.write_handle.write_all(&buf)?;
        self.index.insert(
            key,
            self.fs.write_handle.stream_position()?
                - ((std::mem::size_of::<usize>() * 2 + key.len() + value.len()) as u64),
        );
        self.fs.write_handle.sync_all()?;
        Ok(Response::Record {
            key: key.to_owned(),
            value: value.to_owned(),
        })
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Response> {
        let offset = self.index.get(key);
        match offset {
            Some(offset) => self.fs.read_handle.seek(SeekFrom::Start(*offset))?,
            None => return self.scan_for_key(key),
        };

        let mut pair_meta = [0u8; std::mem::size_of::<usize>() * 2];
        if self.fs.write_handle.read_exact(&mut pair_meta).is_err() {
            return Ok(Response::Record {
                key: key.to_owned(),
                value: Vec::new(),
            });
        }
        let key_len = usize::from_be_bytes(pair_meta[..8].try_into()?);
        let value_len = usize::from_be_bytes(pair_meta[8..16].try_into()?);
        let mut pair_loaded: Vec<u8> = vec![0u8; key_len + value_len];
        self.fs.read_handle.read_exact(&mut pair_loaded)?;
        //let key_loaded = &pair_loaded[..key_len];
        let value_loaded = &pair_loaded[key_len..(key_len + value_len)];
        Ok(Response::Record {
            key: key.to_vec(),
            value: value_loaded.to_vec(),
        })
    }

    fn scan_for_key(&mut self, key: &[u8]) -> Result<Response> {
        info!("key: {:?}", key);
        self.fs.read_handle.seek(SeekFrom::Start(self.index_seek))?;
        loop {
            let mut pair_meta = [0u8; 16];
            if self.fs.read_handle.read_exact(&mut pair_meta).is_err() {
                return Err(Error {
                    kind: ErrorKind::Key,
                });
            }
            let key_len = usize::from_be_bytes(pair_meta[..8].try_into()?);
            let value_len = usize::from_be_bytes(pair_meta[8..16].try_into()?);
            let mut pair_loaded: Vec<u8> = vec![0u8; key_len + value_len];
            self.fs.read_handle.read_exact(&mut pair_loaded)?;
            let key_loaded = &pair_loaded[..key_len];
            let value_loaded = &pair_loaded[key_len..(key_len + value_len)];
            let offset = self.fs.read_handle.stream_position()?
                - ((std::mem::size_of::<usize>() * 2 + key_len + value_len) as u64);
            self.index.insert(key, offset);
            if eq_u8(key, key_loaded) {
                return Ok(Response::Record {
                    key: key.to_vec(),
                    value: value_loaded.to_vec(),
                });
            }
        }
    }
}
