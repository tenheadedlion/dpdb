pub enum Response {
    Record { key: Vec<u8>, value: Vec<u8> },
    Ok,
    Error { msg: String },
}