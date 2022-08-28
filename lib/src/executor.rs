use crate::Result;

use std::time::{Duration, Instant};

use super::{parser, statement::Keyword, storage::Storage};

pub enum Response {
    Record { key: Vec<u8>, value: Vec<u8> },
    Ok,
    Error { msg: String },
}

pub struct Report {
    pub time_elapsed: Duration,
    pub response: Response,
}

impl Report {
    pub fn serialize(&self) -> Result<String> {
        Ok(format!(
            // OK, we need a protocol here
            // Or a frame
            "<BEGIN>\r\n{}\r\n{:?}\r\n<END>",
            match self.response {
                Response::Record { ref key, ref value } => format!(
                    "{}: {}",
                    std::str::from_utf8(key)?,
                    std::str::from_utf8(value)?
                ),
                Response::Error { ref msg } => format!("error: {}", msg),
                Response::Ok => "Ok".to_string(),
            },
            self.time_elapsed
        ))
    }
}

pub struct Executor {
    storage: Storage,
}

impl Executor {
    pub async fn new(path: &str) -> Result<Self> {
        Ok(Executor {
            storage: Storage::new(path, "data")?,
        })
    }
}

impl Executor {
    pub fn execute(&mut self, line: &str) -> Report {
        let now = Instant::now();
        let res = self.execute_internal(line);
        let time_elapsed = now.elapsed();
        Report {
            time_elapsed,
            response: match res {
                Ok(response) => response,
                Err(msg) => Response::Error {
                    msg: msg.to_string(),
                },
            },
        }
    }

    pub fn execute_internal(&mut self, line: &str) -> Result<Response> {
        let (_, statement) = parser::parse_sql(line)?;
        let response = match statement.verb {
            Keyword::Clear => self.storage.clear()?,
            Keyword::Set => self
                .storage
                .set(statement.key.as_bytes(), statement.value.as_bytes())?,
            Keyword::Get => self.storage.get(statement.key.as_bytes())?,
            Keyword::MoveFile => {
                self.storage.move_dir(statement.key.as_str())?;
                Response::Ok
            }
            Keyword::AttachFile => {
                self.storage.attach_dir(statement.key.as_str())?;
                Response::Ok
            }
        };
        Ok(response)
    }
    pub fn merge(&mut self) {}
}

#[cfg(test)]
mod test {
    use super::*;
    #[tokio::test]
    async fn test() {
        let dir = "/tmp/dpdb-test-storage";
        std::fs::create_dir(dir).unwrap();
        {
            let mut executor = Executor::new(dir).await.unwrap();
            let _ = executor.execute("attach bench.db");
            let _ = executor.execute("set sdafasdf sdfasdfasdfsadf");
            let _ = executor.execute("set sdafasdf sdfasdfasdfsadf");
            let _ = executor.execute("set sdafasdf sdfasdfasdfsadf");
            let _ = executor.execute("set needle hay");
        }
        {
            let mut executor = Executor::new(dir).await.unwrap();
            let _ = executor.execute("attach bench.db");
            let report = executor.execute("get needle");
            assert!(matches!(report.response, Response::Record { .. }));
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
