use std::sync::Arc;

use anyhow::Result;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use sled::Db;

use crate::model::ImageValue;

pub trait SledTable {
    const TABLE_NAME: &'static str;
    type SledKey: AsRef<[u8]>;
    type SledValue: Serialize + DeserializeOwned;
    fn get_db(&self) -> &Db;
    fn upsert(&self, key: &Self::SledKey, value: &Self::SledValue) -> Result<()> {
        let key = key.as_ref();
        let value = serde_json::to_string(value)?;
        let byte_key = value.as_bytes();
        let db = self.get_db();
        db.open_tree(Self::TABLE_NAME)?.insert(key, byte_key)?;
        Ok(())
    }

    fn read(&self, key: &Self::SledKey) -> Result<Option<Self::SledValue>> {
        let db = self.get_db();
        let byte_key = key.as_ref();
        let ret = db.open_tree(Self::TABLE_NAME)?.get(byte_key)?;
        match ret {
            Some(ivec) => {
                let string = String::from_utf8(ivec.to_vec())?;
                let value = serde_json::from_str::<Self::SledValue>(&string)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }
}

#[derive(Clone)]
pub struct ImageTable {
    db: Arc<sled::Db>,
}

impl ImageTable {
    pub fn new(path: &str) -> Self {
        let db = sled::open(path).unwrap();
        Self { db: Arc::new(db) }
    }
}

impl<'a> SledTable for ImageTable {
    const TABLE_NAME: &'static str = "Images";
    type SledKey = String;
    type SledValue = ImageValue;
    fn get_db(&self) -> &Db {
        &self.db
    }
}
