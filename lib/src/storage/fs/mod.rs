use log::info;

use crate::{response::Response, utils::eq_u8, Error, ErrorKind, Result};
use std::{
    fs::{self, remove_file, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

static MAGIC: &[u8] = "dpdb-feff-1234-1".as_bytes();
pub struct FileSystem {
    // we won't expose the file to users
    // the directory represents the table
    pub dir: String,
    // by default the name is `data`
    // the more recent segment file has the smaller number suffix
    // for example, data.1 is younger than data.2
    pub file: String,
    pub write_handle: File,
    pub read_handle: File,
}

impl FileSystem {
    pub fn new(dir: &str, file: &str) -> Result<Self> {
        let dbf = Path::new(dir).join(file);
        info!("Open database file: {:?}", &dbf.to_str());
        Ok(FileSystem {
            dir: dir.to_owned(),
            file: file.to_owned(),
            write_handle: OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&dbf)?,
            read_handle: File::open(&dbf)?,
        })
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
}
