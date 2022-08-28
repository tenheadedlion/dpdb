use crate::error::Result;
use crate::response::Response;
use std::time::Duration;

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
