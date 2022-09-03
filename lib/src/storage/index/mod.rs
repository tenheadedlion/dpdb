use std::collections::BTreeMap;

#[derive(Debug)]
pub struct Node {
    pub(crate) offset: u64,
    pub(crate) segment: String,
}

#[derive(Debug)]
pub(crate) struct Index {
    pub(crate) internal: BTreeMap<Vec<u8>, Node>,
}

impl Index {
    pub fn new() -> Self {
        Index {
            internal: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, k: &[u8], segment: &str, offset: u64) -> Option<Node> {
        self.internal.insert(
            k.to_vec(),
            Node {
                segment: segment.to_string(),
                offset,
            },
        )
    }

    pub fn get(&self, k: &[u8]) -> Option<&Node> {
        self.internal.get(k)
    }

    pub fn clear(&mut self) {
        self.internal.clear()
    }
}
