use std::sync::Arc;

use anyhow::{anyhow, Result};
use serde::{de::DeserializeOwned, Serialize};

use rocksdb::DB;

use crate::model::ImageValue;

pub trait RocksTable {
    const TABLE_NAME: &'static str;
    type RocksKey: AsRef<[u8]>;
    type RocksValue: Serialize + DeserializeOwned;
    fn get_db(&self) -> &DB;
    fn upsert(&self, key: &Self::RocksKey, value: &Self::RocksValue) -> Result<()> {
        let key = key.as_ref();
        let value = serde_json::to_string(value)?;
        let byte_key = value.as_bytes();
        let db = self.get_db();
        println!("saved images size {}", byte_key.len());

        match db.put(key, byte_key) {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!("{}", e)),
        }
    }

    fn read(&self, key: &Self::RocksKey) -> Result<Option<Self::RocksValue>> {
        let db = self.get_db();
        let byte_key = key.as_ref();

        match db.get(byte_key)? {
            Some(ivec) => {
                let string = String::from_utf8(ivec.to_vec())?;
                let value = serde_json::from_str::<Self::RocksValue>(&string)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }
    fn remove(&self, key: &Self::RocksKey) -> Result<()> {
        let db = self.get_db();
        let byte_key = key.as_ref();

        match db.delete(byte_key) {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!("{}", e)),
        }
    }
}

#[derive(Clone)]
pub struct ImageTable {
    db: Arc<DB>,
}

impl ImageTable {
    pub fn new(path: &str) -> Self {
        let db = DB::open_default(path).unwrap();
        Self { db: Arc::new(db) }
    }
}

impl<'a> RocksTable for ImageTable {
    const TABLE_NAME: &'static str = "Images";
    type RocksKey = String;
    type RocksValue = ImageValue;
    fn get_db(&self) -> &DB {
        &self.db
    }
}
