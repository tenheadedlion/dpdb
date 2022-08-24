use crate::dpdb_core::{Response, Result};
use std::collections::HashMap;
use std::fs::{self, remove_file, File, OpenOptions};
use std::io::{prelude::*, SeekFrom};
use std::path::Path;

use crate::utils::eq_u8;
use crate::{Error, ErrorKind};

static MAGIC: &[u8] = "dpdb-feff-1234-1".as_bytes();
pub struct Storage {
    // we won't expose the file to users
    // the directory represents the table
    dir: String,
    // by default the name is `data`
    // the more recent segment file has the smaller number suffix
    // for example, data.1 is younger than data.2
    file: String,
    index: HashMap<Vec<u8>, u64>,
    // the database files are immutable and append-only
    // so we just keep a record of the scanning progress
    index_seek: u64,
    file_handle: File,
}

impl Storage {
    fn open_file_safely(file: &Path) -> Result<File> {
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
        // magic number: dpdb-feff-1234-1
        // is it really necessary?
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

    pub fn new(dir: &str, file: &str) -> Result<Self> {
        Ok(Storage {
            dir: dir.to_string(),
            file: file.to_string(),
            file_handle: Storage::open_file_safely(&Path::new(dir).join(file))?,
            index: HashMap::new(),
            index_seek: 16,
        })
    }

    /// remove the database file
    pub fn clear(&self) -> std::io::Result<Response> {
        remove_file(&self.file)?;
        Ok(Response::Ok)
    }

    pub fn move_dir(&mut self, dir: &str) -> Result<()> {
        let new_dir = Path::new(dir);
        // make sure it's directory
        if !new_dir.is_dir() {
            return Err(Error {
                kind: ErrorKind::File,
            });
        }
        // the dir should be empty
        let entries = new_dir.read_dir()?;
        if entries.count() > 0 {
            return Err(Error {
                kind: ErrorKind::File,
            });
        }
        // copy all files to the new dir
        let curr_dir = Path::new(&self.dir);
        for entry in curr_dir.read_dir()? {
            let entry = entry?;
            let curr_file = curr_dir.join(entry.path());
            fs::copy(&curr_file, new_dir.join(entry.path()))?;
            remove_file(curr_file)?;
        }
        // clean up and open new file
        // drop(self.file_handle.try_clone());
        *self = Storage {
            dir: dir.to_string(),
            file_handle: Storage::open_file_safely(&new_dir.join(&self.file))?,
            file: self.file.clone(),
            index: self.index.clone(),
            index_seek: self.index_seek,
        };
        Ok(())
    }

    pub fn attach_dir(&mut self, dir: &str) -> Result<()> {
        *self = Storage::new(dir, "data")?;
        Ok(())
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
