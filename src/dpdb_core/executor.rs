use crate::dpdb_core::Result;

use std::time::{Duration, Instant};

use super::{parser, statement::Keyword, storage::Storage};

pub struct Report {
    pub time_elapsed: Duration,
    pub msg: Option<String>,
}

pub struct Executor {
    storage: Storage,
}

impl Executor {
    pub fn new() -> Result<Self> {
        Ok(Executor {
            storage: Storage::default()?,
        })
    }
}

impl Executor {
    pub fn execute(&mut self, line: &str) -> Result<Report> {
        let now = Instant::now();
        let (_, statement) = parser::parse_sql(line)?;
        let msg = match statement.verb {
            Keyword::Clear => self.storage.clear()?,
            Keyword::Set => self
                .storage
                .set(statement.key.as_bytes(), statement.value.as_bytes())?,
            Keyword::Get => self.storage.get(statement.key.as_bytes())?,
            Keyword::MoveFile => {
                self.storage = Storage::default()?.move_file(statement.key.as_str())?;
                None
            }
            Keyword::AttachFile => {
                self.storage = Storage::default()?.attach_file(statement.key.as_str())?;
                None
            }
        };
        Ok(Report {
            time_elapsed: now.elapsed(),
            msg: match msg {
                Some(msg) => Some(std::str::from_utf8(&msg)?.to_string()),
                None => None,
            },
        })
    }
    pub fn merge(&mut self) {}
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test() {
        {
            let mut executor = Executor::new().unwrap();
            let _ = executor.execute("attach bench.db");
            let _ = executor.execute("set sdafasdf sdfasdfasdfsadf");
            let _ = executor.execute("set sdafasdf sdfasdfasdfsadf");
            let _ = executor.execute("set sdafasdf sdfasdfasdfsadf");
            let _ = executor.execute("set needle hay");
        }
        {
            let mut executor = Executor::new().unwrap();
            let _ = executor.execute("attach bench.db");
            let val = executor.execute("get needle").unwrap();
            assert_eq!(val.msg.unwrap(), "hay");
        }
    }
}
