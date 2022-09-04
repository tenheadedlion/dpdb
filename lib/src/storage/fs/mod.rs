mod fs;
mod test;


use std::path::Path;
use std::path::PathBuf;
use crate::Result;
pub use fs::DBFile;
pub use fs::FileSystem;

pub trait FS: Sized {
    fn new(dir: &str) -> Result<Self>;
    fn dir(&self) -> &Path;
    fn clear(&self) -> Result<()>;
    fn allocate_data_file(&self) -> Result<PathBuf>;
    fn meta_size() -> u64;
}
