use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Index {
    internal: HashMap<Vec<u8>, u64>,
}

impl Index {
    pub fn new() -> Self {
        Index {
            internal: HashMap::new(),
        }
    }

    pub fn insert(&mut self, k: &[u8], v: u64) -> Option<u64> {
        self.internal.insert(k.to_vec(), v)
    }

    pub fn get(&self, k: &[u8]) -> Option<&u64> {
        self.internal.get(k)
    }

    pub fn clear(&mut self) {
        self.internal.clear()
    }
}
