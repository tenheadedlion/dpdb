use crate::{response::Response, storage::index::Index, utils::eq_u8, Error, ErrorKind, Result};
use log::info;
use std::{
    ffi::OsStr,
    fs::{self, remove_file, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub static MAGIC: &[u8] = "dpdb-feff-1234-1".as_bytes();
pub static META_SIZE: u64 = 16;

use super::{super::data_format::Record, FS};

pub struct FileSystem {
    // we won't expose the file to users
    // the directory represents the table
    pub dir: Box<PathBuf>,
    // by default the name is `data`
    // the more recent segment file has the smaller number suffix
    // for example, data.1 is younger than data.2
    pub file: String,
    pub wal_handle: File,
}

impl FS for FileSystem {
    fn new(dir: &str) -> Result<Self> {
        FileSystem::new(dir, "data")
    }

    fn dir(&self) -> &Path {
        &self.dir
    }

    fn allocate_data_file(&self) -> Result<PathBuf> {
        let files = FileSystem::scan_data_files(&self.dir)?;
        // the first one must be `data` which is later renamed to `data.1`
        let len = files.len();
        let mut suffix = 0;
        if len != 0 {
            suffix = len;
        }
        Ok(self.dir.join("data").with_extension(suffix.to_string()))
    }

    fn meta_size() -> u64 {
        META_SIZE
    }

    fn clear(&self) -> Result<()> {
        Ok(())
    }
}

impl FileSystem {
    pub fn new(dir: &str, file: &str) -> Result<Self> {
        let dir = Path::new(dir);
        let dbf = dir.join(file);
        let wal = dir.join("wal");
        info!("Open database file: {:?}", &dbf.to_str());
        Ok(FileSystem {
            dir: Box::new(dir.to_path_buf()),
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

    #[allow(dead_code)]
    pub fn move_dir(&mut self, dir: &str) -> Result<()> {
        let new_dir = Path::new(dir);
        // make sure it's a directory
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
        let curr_dir = &self.dir;
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
        let get_ext_as_num = |path: &PathBuf| {
            path.extension()
                .unwrap_or_else(|| OsStr::new("0"))
                .to_str()
                .unwrap()
                .parse::<i32>()
                .unwrap()
        };
        files.sort_by(|a, b| {
            let a = get_ext_as_num(a);
            let b = get_ext_as_num(b);
            // a is smaller than b
            // ascending order
            a.cmp(&b)
        });
        Ok(files)
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
