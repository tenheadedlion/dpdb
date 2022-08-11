#[derive(Debug)]
pub(crate) enum Keyword {
    Clear,
    MoveFile,
    AttachFile,
    Set,
    Get,
}

#[derive(Debug)]
pub(crate) struct Statement {
    pub(crate) verb: Keyword,
    pub(crate) key: String,
    pub(crate) value: String,
}
