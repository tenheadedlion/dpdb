mod fs;
mod index;
use self::fs::FileSystem;
use crate::error::Result;
use crate::response::Response;
use crate::storage::index::Index;
use crate::utils::eq_u8;
use crate::Error;
use log::info;
use std::io::{prelude::*, SeekFrom};
use std::path::Path;
mod data_format;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub struct Storage {
    index: Index,
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,
    // when the size of memtable reaches a certain threshold,
    // (in this case, 1 Byte, for education and test purpose)
    // (or, todo: a configurable value)
    // the memtable will be migrated to the disk
    threshold: usize,
    // but do we really want to be that accurate? counting bytes?
    // I prefer counting entries, but what to count by is trivial.
    memtable_size: usize,
    // the database files are immutable and append-only
    // so we just keep a record of the scanning progress
    index_seek: u64,
    pub fs: FileSystem,
}

impl Storage {
    pub fn new(dir: &str, file: &str) -> Result<Storage> {
        Ok(Storage {
            index: Index::new(),
            memtable: BTreeMap::new(),
            index_seek: 16,
            fs: FileSystem::new(dir, file)?,
            threshold: 1,
            memtable_size: 0,
        })
    }

    pub fn clear(&mut self) -> Result<Response> {
        self.fs.clear()?;
        self.index.clear();
        Ok(Response::Ok)
    }

    /// the layout of a pair is: len(key)|len(value)|key|value
    pub fn migrate_memtable(&mut self, file: &Path) -> Result<()> {
        info!("migrating memtable to disk: {}", file.display());
        let mut file = FileSystem::open_file_safely(file)?;
        for (key, value) in &self.memtable {
            let buf = data_format::encode(key, value);
            file.write_all(&buf)?;
        }
        self.memtable.clear();
        self.memtable_size = 0;
        file.sync_all()?;
        Ok(())
    }

    // first insert the new value to the tree
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<Response> {
        let prev = self.memtable.insert(key.to_vec(), value.to_vec());
        if prev.is_none() {
            self.memtable_size += key.len() + value.len();
        }
        if self.memtable_size > self.threshold {
            // first, pick a name
            // the work is delegated to fs who knows what files are in the data directory,
            // underneath, the fs will do a heavy load of file operations(mainly file renaming)
            let file_name = self.fs.allocate_data_file()?;
            self.migrate_memtable(&file_name)?;
        }
        Ok(Response::Record {
            key: key.to_owned(),
            value: value.to_owned(),
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Response> {
        match self.memtable.get(key) {
            Some(value) => Ok(Response::Record {
                key: key.to_vec(),
                value: value.clone(),
            }),
            None => {
                // todo: go search the file in the disk
                Err(Error {
                    kind: crate::ErrorKind::Key,
                })
            }
        }
    }
    /*
        pub fn get_from_disk(&mut self, key: &[u8]) -> Result<Response> {
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
            let record = self.fs.read_record()?;
            Ok(Response::Record {
                key: record.key,
                value: record.value,
            })
        }
    fn scan_for_key(&mut self, key: &[u8]) -> Result<Response> {
        info!("key: {:?}", key);
        self.fs.read_handle.seek(SeekFrom::Start(self.index_seek))?;
        loop {
            let record = self.fs.read_record()?;
            let offset = self.fs.read_handle.stream_position()?
                - ((std::mem::size_of::<usize>() * 2 + record.klen + record.vlen) as u64);
            self.index.insert(key, offset);
            if eq_u8(key, &record.key) {
                return Ok(Response::Record {
                    key: key.to_vec(),
                    value: record.value,
                });
            }
        }
    }
    */
    // 1. collect the entries into a sorted map
    // 2. write to a new segment file and refresh the index
    // the thing is, when to compact?
    //  when the index reaches certain threshold
    //      it will be dump to a file and compacted
    fn compact(dir: &str, file: &str) -> Result<PathBuf> {
        let file = Path::new(file);
        let mut handle = FileSystem::open_file_safely(file)?;
        let mut records = BTreeMap::new();
        while let Ok((h, r)) = FileSystem::read_record_with(handle) {
            handle = h;
            records.insert(r.key, r.value);
        }
        // write to new file
        let new_file = file.with_extension("new");
        let mut storage = Storage::new(dir, new_file.to_str().unwrap())?;
        for (k, v) in records {
            storage.set(&k, &v)?;
        }
        Ok(new_file)
    }
}
