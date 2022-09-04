mod fs;
mod index;
pub(crate) use self::storage::Storage;
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
mod storage;
use fs::FS;
pub use fs::FileSystem;

pub struct Builder<T: FS> {
    storage: Storage<T>,
}

//impl Builder {
//    pub fn new() -> Self {
//        Self {
//            storage: Storage::new("/tmp", "data").unwrap(),
//        }
//    }
//
//    pub fn fs<T: FS>(fs: T) -> &mut Self {
//
//    }
//}
