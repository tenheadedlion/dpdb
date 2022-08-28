extern crate nom;
use nom::{
    branch::alt,
    bytes::complete::{is_not, tag},
    character::complete::multispace0,
    combinator::eof,
    error::ParseError,
    sequence::delimited,
    IResult,
};

fn ws<'a, F: 'a, O, E: ParseError<&'a str>>(
    inner: F,
) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
where
    F: Fn(&'a str) -> IResult<&'a str, O, E>,
{
    delimited(multispace0, inner, multispace0)
}

use crate::statement::{Keyword, Statement};

pub(crate) fn parse_sql(input: &str) -> IResult<&str, Statement> {
    alt((
        parse_clear,
        parse_set,
        parse_get,
        parse_move_file,
        parse_attach_file,
    ))(input)
}

fn parse_clear(input: &str) -> IResult<&str, Statement> {
    let (input, _) = ws(tag("clear"))(input)?;
    let (input, _) = eof(input)?;
    Ok((
        input,
        Statement {
            verb: Keyword::Clear,
            key: String::default(),
            value: String::default(),
        },
    ))
}

fn parse_set(input: &str) -> IResult<&str, Statement> {
    let (input, _) = ws(tag("set"))(input)?;
    let (input, key) = ws(literal)(input)?;
    let (input, value) = ws(literal)(input)?;
    let (input, _) = eof(input)?;

    Ok((
        input,
        Statement {
            verb: Keyword::Set,
            key: key.to_string(),
            value: value.to_string(),
        },
    ))
}

fn parse_get(input: &str) -> IResult<&str, Statement> {
    let (input, _) = ws(tag("get"))(input)?;
    let (input, key) = ws(literal)(input)?;
    let (input, _) = eof(input)?;

    Ok((
        input,
        Statement {
            verb: Keyword::Get,
            key: key.to_string(),
            value: Default::default(),
        },
    ))
}

fn parse_attach_file(input: &str) -> IResult<&str, Statement> {
    let (input, _) = ws(tag("attach-to"))(input)?;
    let (input, file) = ws(literal)(input)?;
    let (input, _) = eof(input)?;

    Ok((
        input,
        Statement {
            verb: Keyword::AttachFile,
            key: file.to_string(),
            value: Default::default(),
        },
    ))
}

fn parse_move_file(input: &str) -> IResult<&str, Statement> {
    let (input, _) = ws(tag("mv-to"))(input)?;
    let (input, file) = ws(literal)(input)?;
    let (input, _) = eof(input)?;

    Ok((
        input,
        Statement {
            verb: Keyword::MoveFile,
            key: file.to_string(),
            value: Default::default(),
        },
    ))
}

/// match anything that is not space
pub fn literal(input: &str) -> IResult<&str, &str> {
    is_not(" \t\r\n")(input)
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test() {
        let (_, output) = parse_sql("set a 2").unwrap();
        assert_eq!(output.key, "a");
        assert_eq!(output.value, "2");
    }
}
