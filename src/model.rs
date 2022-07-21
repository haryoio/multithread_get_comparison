use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct ImageValue {
    url: String,
    image: Vec<u8>,
}

impl ImageValue {
    pub fn new(url: String, image: Vec<u8>) -> Self {
        Self { url, image }
    }
}
