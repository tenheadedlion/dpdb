use crate::dpdb_core::{filesystem, Result};

use std::time::{Duration, Instant};

use super::statement::{Keyword, Statement};

pub struct Report {
    pub time_elapsed: Duration,
    pub msg: Option<String>,
}

pub(crate) fn execute(statement: Statement) -> Result<Report> {
    let now = Instant::now();
    let msg = match statement.verb {
        Keyword::Reset => filesystem::reset()?,
        Keyword::Set => filesystem::set(&statement.key, &statement.value)?,
        Keyword::Get => filesystem::get(&statement.key)?,
    };
    Ok(Report {
        time_elapsed: now.elapsed(),
        msg,
    })
}
