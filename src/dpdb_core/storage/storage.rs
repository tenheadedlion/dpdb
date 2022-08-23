use crate::dpdb_core::{Response, Result};
use std::collections::HashMap;
use std::fs::{remove_file, File, OpenOptions};
use std::io::{prelude::*, SeekFrom};

use crate::utils::eq_u8;
use crate::{Error, ErrorKind};

static MAGIC: &[u8] = "dpdb-feff-1234-1".as_bytes();
pub struct Storage {
    file: String,
    index: HashMap<Vec<u8>, u64>,
    // the database files are immutable and append-only
    // so we just keep a record of the last time of index scanning
    index_seek: u64,
    file_handle: File,
}

impl Storage {
    pub fn default() -> Result<Self> {
        let file = "foo.db";
        Storage::new(file)
    }

    fn open_file_safely(file: &str) -> Result<File> {
        // Open it anyway, if it is empty, and write magic number into it
        // else read 16 bytes from it, if that fails, abort,
        // else check if the 16 bytes match the magic number, if so, return the file handle
        // else, it's not safe to touch the file, abort
        let mut db = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .append(true)
            .open(file)?;
        let file_metadata = db.metadata()?;
        if file_metadata.len() == 0 {
            db.write_all(MAGIC)?;
            return Ok(db);
        }
        // dpdb-feff-1234-1
        let mut db_meta = [0u8; 16];
        // may fail to read 16 bytes
        db.read_exact(&mut db_meta)?;
        if eq_u8(MAGIC, &db_meta) {
            return Ok(db);
        }
        Err(Error {
            kind: ErrorKind::IO,
        })
    }

    pub fn new(file: &str) -> Result<Self> {
        Ok(Storage {
            file: file.to_string(),
            file_handle: Storage::open_file_safely(file)?,
            index: HashMap::new(),
            index_seek: 16,
        })
    }

    /// remove the database file
    pub fn clear(&self) -> std::io::Result<Response> {
        remove_file(&self.file)?;
        Ok(Response::Ok)
    }

    pub fn move_file(self, file: &str) -> Result<Self> {
        if std::fs::metadata(file).is_ok() {
            return Err(Error {
                kind: ErrorKind::IO,
            });
        }
        std::fs::copy(&self.file, file)?;
        remove_file(&self.file)?;
        drop(self.file_handle);
        Ok(Storage {
            file_handle: Storage::open_file_safely(file)?,
            file: file.to_string(),
            index: self.index,
            index_seek: self.index_seek,
        })
    }
    /// attach to another database file
    /// first ensure that file can be opened
    pub fn attach_file(self, file: &str) -> Result<Self> {
        Storage::new(file)
    }

    /// the layout of a pair is: len(key)|len(value)|key|value
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<Response> {
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
        Ok(Response::Record {
            key: key.to_owned(),
            value: value.to_owned(),
        })
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Response> {
        let mut db = File::open(&self.file)?;
        let offset = self.index.get(key);
        match offset {
            Some(offset) => db.seek(SeekFrom::Start(*offset))?,
            None => return self.scan_for_key(key),
        };

        let mut pair_meta = [0u8; std::mem::size_of::<usize>() * 2];
        if db.read_exact(&mut pair_meta).is_err() {
            return Ok(Response::Record {
                key: key.to_owned(),
                value: Vec::new(),
            });
        }
        let key_len = usize::from_be_bytes(pair_meta[..8].try_into()?);
        let value_len = usize::from_be_bytes(pair_meta[8..16].try_into()?);
        let mut pair_loaded: Vec<u8> = vec![0u8; key_len + value_len];
        db.read_exact(&mut pair_loaded)?;
        //let key_loaded = &pair_loaded[..key_len];
        let value_loaded = &pair_loaded[key_len..(key_len + value_len)];
        Ok(Response::Record {
            key: key.to_vec(),
            value: value_loaded.to_vec(),
        })
    }

    pub fn scan_for_key(&mut self, key: &[u8]) -> Result<Response> {
        let mut db = File::open(&self.file)?;
        db.seek(SeekFrom::Start(self.index_seek))?;
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
                return Ok(Response::Record {
                    key: key.to_vec(),
                    value: value_loaded.to_vec(),
                });
            }
        }
    }
}
