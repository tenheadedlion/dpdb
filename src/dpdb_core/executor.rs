use crate::dpdb_core::Result;

use std::{
    time::{Duration, Instant},
};

use super::{
    parser,
    statement::{Keyword},
    storage::Storage,
};

pub struct Report {
    pub time_elapsed: Duration,
    pub msg: Option<String>,
}

pub struct Executor {
    storage: Storage,
}

impl Default for Executor {
    fn default() -> Self {
        Executor {
            storage: Storage::new(),
        }
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
            Keyword::Reset => {
                self.storage = Storage::new().reset(statement.key.as_str())?;
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
}
