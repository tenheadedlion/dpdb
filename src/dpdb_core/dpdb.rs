use crate::dpdb_core::*;

pub fn handle_statement(line: &str) -> Result<Report> {
    let (_, statement) = parser::parse_sql(line)?;
    executor::execute(statement)
}
