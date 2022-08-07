#[derive(Debug)]
pub(crate) enum Keyword {
    Reset,
    Set,
    Get,
}

#[derive(Debug)]
pub(crate) struct Statement {
    pub(crate) verb: Keyword,
    pub(crate) key: String,
    pub(crate) value: String,
}
