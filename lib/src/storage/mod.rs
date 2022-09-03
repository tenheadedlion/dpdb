mod fs;
mod index;
use self::fs::{DBFile, FileSystem};
use crate::error::Result;
use crate::response::Response;
use crate::storage::index::Index;
use crate::utils::eq_u8;
use crate::{Error, ErrorKind};
use log::info;
use std::fs::File;
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
    pub fs: FileSystem,
}

impl Storage {
    pub fn new(dir: &str, file: &str) -> Result<Storage> {
        Ok(Storage {
            index: Index::new(),
            memtable: BTreeMap::new(),
            fs: FileSystem::new(dir, file)?,
            threshold: 4096 * 1024,
            memtable_size: 0,
        })
    }

    pub fn clear(&mut self) -> Result<Response> {
        self.fs.clear()?;
        self.index.clear();
        Ok(Response::Ok)
    }

    /// the layout of a pair is: len(key)|len(value)|key|value
    pub fn migrate_memtable(&mut self, path: &Path) -> Result<()> {
        info!("migrating memtable to disk: {}", path.display());
        let mut file = FileSystem::open_file_safely(path)?;
        let mut offset: u64 = fs::META_SIZE;
        for (key, value) in &self.memtable {
            let buf = data_format::encode(key, value);
            file.write_all(&buf)?;
            offset += buf.len() as u64;
            // todo: when the runtime crashes, find a way to restore the index
            self.index
                .insert(key, path.to_str().unwrap(), offset)
                .unwrap();
        }
        // this is the uglyness of OOP
        self.memtable.clear();
        self.memtable_size = 0;
        file.sync_all()?;
        Ok(())
    }

    // first insert the new value to the tree
    // consider this scenario: the user reads a key immediately after inserting it to the db,
    //  the user should get the key from the memtable, rather than segment files.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<Response> {
        let prev = self.memtable.get(key);
        if prev.is_none() {
            // the new key triggers the flushing of memtable, but the key itself stays in memtable
            self.memtable_size += key.len() + value.len();
            if self.memtable_size > self.threshold {
                // first, pick a name
                // the work is delegated to fs who knows what files are in the data directory,
                // underneath, the fs will do a heavy(is it?) load of file operations(mainly file renaming)
                let file_name = self.fs.allocate_data_file()?;
                self.migrate_memtable(&file_name)?;
            }
        }
        _ = self.memtable.insert(key.to_vec(), value.to_vec());
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
            None => self.get_from_segments(key),
        }
    }

    pub fn get_from_segments(&self, key: &[u8]) -> Result<Response> {
        let node = self.index.get(key).map(Ok).unwrap_or(Err(Error {
            kind: ErrorKind::Key,
        }))?;
        let mut seg = File::open(&node.segment)?;
        _ = seg.seek(SeekFrom::Start(node.offset));
        let rec = FileSystem::read_record_with(&mut seg)?;
        Ok(Response::Record {
            key: rec.key,
            value: rec.value,
        })
    }

    pub fn get_from_file(file: &Path, key: &[u8]) -> Result<Response> {
        let dbfile = DBFile::new(file)?;
        for record in dbfile {
            if eq_u8(key, &record.key) {
                return Ok(Response::Record {
                    key: key.to_vec(),
                    value: record.value,
                });
            }
        }
        Err(Error {
            kind: ErrorKind::Key,
        })
    }

    pub fn get_from_files(dir: &Path, key: &[u8]) -> Result<Response> {
        let files = FileSystem::scan_data_files(dir)?;
        for file in files {
            if let Ok(r) = Storage::get_from_file(&file, key) {
                return Ok(r);
            }
        }

        Err(Error {
            kind: ErrorKind::Key,
        })
    }
    /*
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
    /*
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
    */
}
