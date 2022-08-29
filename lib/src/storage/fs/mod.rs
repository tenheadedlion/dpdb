use crate::{response::Response, utils::eq_u8, Error, ErrorKind, Result};
use log::info;
use std::{
    fs::{self, remove_file, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use super::data_format::Record;

static MAGIC: &[u8] = "dpdb-feff-1234-1".as_bytes();
pub struct FileSystem {
    // we won't expose the file to users
    // the directory represents the table
    pub dir: String,
    // by default the name is `data`
    // the more recent segment file has the smaller number suffix
    // for example, data.1 is younger than data.2
    pub file: String,
    pub wal_handle: File,
}

impl FileSystem {
    pub fn new(dir: &str, file: &str) -> Result<Self> {
        let dbf = Path::new(dir).join(file);
        let wal = Path::new(dir).join("wal");
        info!("Open database file: {:?}", &dbf.to_str());
        Ok(FileSystem {
            dir: dir.to_owned(),
            file: file.to_owned(),
            // write append log
            wal_handle: OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&wal)?,
        })
    }

    pub fn wal(&mut self, line: &str) -> Result<()> {
        self.wal_handle.write_fmt(format_args!("{}\n", line))?;
        self.wal_handle.sync_all()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn open_file_safely(file: &Path) -> Result<File> {
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

    /// remove the database file
    pub fn clear(&self) -> std::io::Result<Response> {
        remove_file(&self.file)?;
        Ok(Response::Ok)
    }

    #[allow(dead_code)]
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
        Ok(())
    }

    #[allow(dead_code)]
    pub fn attach_dir(&mut self, dir: &str) -> Result<()> {
        *self = FileSystem::new(dir, "data")?;
        Ok(())
    }

    //pub fn read_record(&mut self) -> Result<Record> {
    //    let h = self.read_handle.try_clone()?;
    //    let (h, r) = FileSystem::read_record_with(h)?;
    //    self.read_handle = h;
    //    Ok(r)
    //}
    pub fn read_record_with(read_handle: &mut File) -> Result<Record> {
        let mut pair_meta = [0u8; 16];
        if read_handle.read_exact(&mut pair_meta).is_err() {
            return Err(Error {
                // corrupted file, or more likely reaching the end of database file
                // but still can't find the ky
                kind: ErrorKind::Key,
            });
        }
        let key_len = usize::from_be_bytes(pair_meta[..8].try_into()?);
        let value_len = usize::from_be_bytes(pair_meta[8..16].try_into()?);
        let mut pair_loaded: Vec<u8> = vec![0u8; key_len + value_len];
        read_handle.read_exact(&mut pair_loaded)?;
        let key_loaded = &pair_loaded[..key_len];
        let value_loaded = &pair_loaded[key_len..(key_len + value_len)];
        Ok(Record {
            klen: key_len,
            vlen: value_len,
            key: key_loaded.to_vec(),
            value: value_loaded.to_vec(),
        })
    }

    pub fn scan_data_files(dir: &Path) -> Result<Vec<PathBuf>> {
        let mut files = fs::read_dir(dir)?
            .map(|res| res.map(|e| e.path()))
            .filter(|e| match e {
                Ok(path) => {
                    // absolute path
                    let path = path.file_name().unwrap().to_str().unwrap();
                    if path.starts_with("data") {
                        return true;
                    }
                    false
                }
                Err(_) => false,
            })
            .collect::<std::result::Result<Vec<_>, std::io::Error>>()?;
        files.sort();
        Ok(files)
    }

    // the fs will keep a record of files on the disk
    // but no need -- the files won't go away
    pub fn allocate_data_file(&self) -> Result<PathBuf> {
        let files = FileSystem::scan_data_files(Path::new(&self.dir))?;
        // the first one must be `data` which is later renamed to `data.1`
        let len = files.len();
        for (i, f) in files.iter().rev().enumerate() {
            info!("{}", i);
            let new = f.with_extension((len - i).to_string());
            info!("rename {} to {}", f.display(), new.display());
            std::fs::rename(f, new)?;
        }
        Ok(Path::new(&self.dir).join("data"))
    }
}

pub struct DBFile {
    file: File,
}

impl DBFile {
    pub fn new(file: &Path) -> Result<Self> {
        let file = FileSystem::open_file_safely(file)?;
        Ok(DBFile { file })
    }
}

impl Iterator for DBFile {
    type Item = Record;
    fn next(&mut self) -> Option<Self::Item> {
        if let Ok(record) = FileSystem::read_record_with(&mut self.file) {
            return Some(record);
        }
        None
    }
}
