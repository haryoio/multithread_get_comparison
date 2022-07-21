use bytes::Bytes;
// use rocksdb::DB as RDB;
use extreme;
use sled;
use std::sync::Arc;
pub trait DB {
    fn init(file_path: &str) -> Self;
    fn save(&self, k: &str, v: &[u8]) -> bool;
    fn find(&self, k: &str) -> Option<String>;
    fn find_bytes(&self, k: &str) -> Option<Vec<u8>>;
    fn delete(&self, k: &str) -> bool;
}

// #[derive(Clone)]
// pub struct RocksDB {
//     db: Arc<RDB>,
// }

// impl DB for RocksDB {
//     fn init(file_path: &str) -> RocksDB {
//         RocksDB {
//             db: Arc::new(RDB::open_default(file_path).unwrap()),
//         }
//     }

//     fn save(&self, k: &str, v: &[u8]) -> bool {
//         self.db.put(k.as_bytes(), v).is_ok()
//     }

//     fn find_bytes(&self, k: &str) -> Option<Vec<u8>> {
//         let v = self.db.get(k.as_bytes());
//         if v.is_ok() {
//             v.unwrap().map(|v| v.to_vec())
//         } else {
//             None
//         }
//     }

//     fn find(&self, k: &str) -> Option<String> {
//         match self.db.get(k.as_bytes()) {
//             Ok(Some(v)) => {
//                 let result = String::from_utf8(v).unwrap();
//                 println!("Finding '{}' returns '{}'", k, result);
//                 Some(result)
//             }
//             Ok(None) => {
//                 println!("Finding '{}' returns None", k);
//                 None
//             }
//             Err(e) => {
//                 println!("Error retrieving value for {}: {}", k, e);
//                 None
//             }
//         }
//     }

//     fn delete(&self, k: &str) -> bool {
//         self.db.delete(k.as_bytes()).is_ok()
//     }
// }

#[derive(Clone)]
pub struct SledDB {
    db: Arc<sled::Db>,
}

impl DB for SledDB {
    fn init(file_path: &str) -> SledDB {
        let sled = sled::open(file_path).unwrap();
        let mut sub = sled.watch_prefix("");

        // extreme::run(async move {
        //     while let Some(event) = (&mut sub).await {
        //         println!("got event {:?}", event);
        //     }
        // });

        SledDB { db: Arc::new(sled) }
    }

    fn save(&self, k: &str, v: &[u8]) -> bool {
        self.db.insert(k, v).is_ok()
    }

    fn find_bytes(&self, k: &str) -> Option<Vec<u8>> {
        let v = self.db.get(k);
        if v.is_ok() {
            v.unwrap().map(|v| v.to_vec())
        } else {
            None
        }
    }

    fn find(&self, k: &str) -> Option<String> {
        match self.db.get(k) {
            Ok(Some(v)) => {
                let result = String::from_utf8(v.to_vec()).unwrap();
                println!("Finding '{}' returns '{}'", k, result);
                Some(result)
            }
            Ok(None) => {
                println!("Finding '{}' returns None", k);
                None
            }
            Err(e) => {
                println!("Error retrieving value for {}: {}", k, e);
                None
            }
        }
    }

    fn delete(&self, k: &str) -> bool {
        self.db.remove(k).is_ok()
    }
}
