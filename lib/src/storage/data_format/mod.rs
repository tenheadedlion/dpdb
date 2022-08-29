pub fn encode(key: &[u8], value: &[u8]) -> Vec<u8> {
    let key_meta = key.len().to_be_bytes();
    let value_meta = value.len().to_be_bytes();
    [&key_meta, &value_meta, key, value].concat()
}

pub struct Record {
    pub klen: usize,
    pub vlen: usize,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
