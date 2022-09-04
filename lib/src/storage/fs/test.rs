use crate::executor::Executor;
use crate::storage::Storage;
use crate::Result;
use std::{fs::File, path::{PathBuf, Path}};
use tempfile::{tempdir, tempfile, NamedTempFile, TempDir};

struct MockFileSystem {
    dir: TempDir,
}

impl MockFileSystem {
    fn new() -> Result<Self> {
        let dir = tempdir()?;
        let mut file1 = NamedTempFile::new()?;
        Ok(MockFileSystem { dir })
    }

    fn dir(&self) -> &Path {
        self.dir.path()
    }

    fn clear() -> Result<()> {
        Ok(())
    }

    pub fn allocate_data_file(&self) -> Result<PathBuf> {
        Ok(self.dir.path().join("data"))
    }
}

