mod model;
mod table {
    pub(crate) mod rocks;
    pub(crate) mod sled;
}
use std::{sync::Arc, thread};

// use crate::db::RocksDB;
use crate::{
    model::ImageValue,
    table::{
        rocks::{self as rocks_table, RocksTable},
        sled::{self as sled_table, SledTable},
    },
};
use anyhow::Result;
use bytes::Bytes;
use futures::future::join_all;
use image_print_rs::ImagePrinter;
use itertools::Itertools;
use reqwest;
use tokio::{self, sync::Mutex, task::JoinHandle};

const SLED_DB_PATH: &str = "/tmp/sled_db";
const ROCKS_DB_PATH: &str = "/tmp/rocks_db";
const ROCKS_MPSC_DB_PATH: &str = "/tmp/rocks_mpsc_db";

#[tokio::main]
async fn main() {
    let urls = vec![
        "https://cc0.photo/wp-content/uploads/2022/03/Rose-branches-in-the-sun-lantern-in-the-background-2048x1365.jpg",
        "https://cc0.photo/wp-content/uploads/2022/03/Backyard-view-of-orange-wall-and-ivy-2048x1536.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Close-up-of-branch-with-white-apple-blossoms-2048x1365.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Small-sailboats-covered-with-tarpaulin-on-the-jetty-2048x1536.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Hot-spring-in-cave-in-Iceland-2048x1360.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Sculpture-consisting-out-of-3-human-skulls-1371x2048.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Huge-glowing-sun-in-fog-and-behind-trees-2048x1365.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Half-timbered-house-overgrown-with-roses-in-Germany.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Icelandic-ram-on-green-pasture-2048x1360.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Red-CAT-excavator-demolishes-the-Odermark-building-in-Goslar-2048x1365.jpg",
        "https://cc0.photo/wp-content/uploads/2022/03/Rose-branches-in-the-sun-lantern-in-the-background-2048x1365.jpg",
        "https://cc0.photo/wp-content/uploads/2022/03/Backyard-view-of-orange-wall-and-ivy-2048x1536.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Close-up-of-branch-with-white-apple-blossoms-2048x1365.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Small-sailboats-covered-with-tarpaulin-on-the-jetty-2048x1536.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Hot-spring-in-cave-in-Iceland-2048x1360.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Sculpture-consisting-out-of-3-human-skulls-1371x2048.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Huge-glowing-sun-in-fog-and-behind-trees-2048x1365.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Half-timbered-house-overgrown-with-roses-in-Germany.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Icelandic-ram-on-green-pasture-2048x1360.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Red-CAT-excavator-demolishes-the-Odermark-building-in-Goslar-2048x1365.jpg",
        "https://cc0.photo/wp-content/uploads/2022/03/Rose-branches-in-the-sun-lantern-in-the-background-2048x1365.jpg",
        "https://cc0.photo/wp-content/uploads/2022/03/Backyard-view-of-orange-wall-and-ivy-2048x1536.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Close-up-of-branch-with-white-apple-blossoms-2048x1365.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Small-sailboats-covered-with-tarpaulin-on-the-jetty-2048x1536.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Hot-spring-in-cave-in-Iceland-2048x1360.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Sculpture-consisting-out-of-3-human-skulls-1371x2048.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Huge-glowing-sun-in-fog-and-behind-trees-2048x1365.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Half-timbered-house-overgrown-with-roses-in-Germany.jpg",
        "https://cc0.photo/wp-content/uploads/2022/02/Icelandic-ram-on-green-pasture-2048x1360.jpg",
    ];

    std::fs::remove_dir_all(SLED_DB_PATH).unwrap_or_default();
    std::fs::remove_dir_all(ROCKS_DB_PATH).unwrap_or_default();
    std::fs::remove_dir_all(ROCKS_MPSC_DB_PATH).unwrap_or_default();

    // let start = std::time::Instant::now();
    // let _ = get_image_single_thread(&urls).await;
    // let end = std::time::Instant::now();
    // let single = end.duration_since(start);

    let start = std::time::Instant::now();
    let _ = get_image_multi_thread(&urls).await;
    let end = std::time::Instant::now();
    let multi = end.duration_since(start);

    // let start = std::time::Instant::now();
    // let _ = get_image_mpsc(&urls).await;
    // let end = std::time::Instant::now();
    // let mp = end.duration_since(start);

    // let start = std::time::Instant::now();
    // let _ = get_image_multi_thread_sled(&urls).await;
    // let end = std::time::Instant::now();
    // let sled_cached = end.duration_since(start);

    // let start = std::time::Instant::now();
    // let _ = get_image_multi_thread_rocks(&urls).await;
    // let end = std::time::Instant::now();
    // let rocks_cached = end.duration_since(start);

    // let start = std::time::Instant::now();
    // let _ = get_image_mpsc_save_rocks(&urls).await;
    // let end = std::time::Instant::now();
    // let rocks_mpsc_cached = end.duration_since(start);

    // println!("Single thread: {:?}", single);
    println!("Multi thread: {:?}", multi);
    // println!("MPSC : {:?}", mp);
    // println!("Sled Cached: {:?}", sled_cached);
    // println!("Rocks Cached: {:?}", rocks_cached);
    // println!("MPSC Rocks Cached: {:?}", rocks_mpsc_cached);
}

// pub async fn get_image_multi_thread_sled(urls: &Vec<&str>) -> Result<()> {
//     println!("start get image mpsc");
//     let urls_count = urls.len();
//     let urls = urls.into_iter().unique().collect::<Vec<_>>();
//     let urls_count_dedup = urls.len();
//     println!("{} urls, {} deduped", urls_count, urls_count_dedup);

//     println!("start get image sled");
//     let db = sled_table::ImageTable::new(SLED_DB_PATH);

//     let mut tasks: Vec<JoinHandle<Result<ImageValue, String>>> = vec![];

//     for url in urls {
//         let db = db.clone();
//         let url = url.to_string();
//         tasks.push(tokio::spawn(async move {
//             let v = db.read(&url).unwrap();
//             if v.is_some() {
//                 println!("ALREADY EXISTS -- {} --", url);
//                 return Ok(v.unwrap());
//             }
//             println!("START --{}--", url);
//             match reqwest::get(url.to_string()).await {
//                 Ok(resp) => match resp.bytes().await {
//                     Ok(bytes) => {
//                         println!("END --{}--", url.clone());
//                         let res = &ImageValue::new(url.clone(), bytes.to_vec());
//                         db.upsert(&url, &res.clone()).unwrap();
//                         return Ok(res.clone());
//                     }
//                     Err(_) => return Err(format!("ERROR reading {}", &url)),
//                 },
//                 Err(_) => return Err(format!("ERROR downloading {}", &url)),
//             }
//         }));
//     }

//     println!("Started {} tasks. Waiting...", tasks.len());
//     let _ = join_all(tasks).await;
//     Ok(())
// }

// pub async fn get_image_multi_thread_rocks(urls: &Vec<&str>) -> Result<()> {
//     println!("start get image mpsc");
//     let urls_count = urls.len();
//     let urls = urls.into_iter().unique().collect::<Vec<_>>();
//     let urls_count_dedup = urls.len();
//     println!("{} urls, {} deduped", urls_count, urls_count_dedup);

//     println!("start get image rocks");
//     let db = rocks_table::ImageTable::new(ROCKS_DB_PATH);

//     let mut tasks: Vec<JoinHandle<Result<ImageValue, String>>> = vec![];

//     for url in urls {
//         let db = db.clone();
//         let url = url.to_string();
//         tasks.push(tokio::spawn(async move {
//             let v = db.read(&url).unwrap();
//             if v.is_some() {
//                 println!("ALREADY EXISTS -- {} --", url);
//                 return Ok(v.unwrap());
//             }
//             println!("START --{}--", url);
//             match reqwest::get(url.to_string()).await {
//                 Ok(resp) => match resp.bytes().await {
//                     Ok(bytes) => {
//                         println!("END --{}--", url.clone());
//                         let res = &ImageValue::new(url.clone(), bytes.to_vec());
//                         db.upsert(&url, &res.clone()).unwrap();
//                         println!("SAVED --{}--", url.clone());
//                         return Ok(res.clone());
//                     }
//                     Err(_) => return Err(format!("ERROR reading {}", &url)),
//                 },
//                 Err(_) => return Err(format!("ERROR downloading {}", &url)),
//             }
//         }));
//     }

//     println!("Started {} tasks. Waiting...", tasks.len());
//     let _ = join_all(tasks).await;

//     Ok(())
// }

pub async fn get_image_multi_thread(urls: &Vec<&str>) -> Result<()> {
    println!("start get image mpsc");
    let urls_count = urls.len();
    let urls = urls.into_iter().unique().collect::<Vec<_>>();
    let urls_count_dedup = urls.len();
    println!("{} urls, {} deduped", urls_count, urls_count_dedup);

    // println!("start get image multi thread");
    let mut tasks: Vec<JoinHandle<Result<(), String>>> = vec![];

    // let image_vec = Arc::new(Mutex::new(vec![]));

    for (i, url) in urls.iter().enumerate() {
        let url = url.to_string();
        let i = i.clone();
        // let image_vec = image_vec.clone();
        // println!("START --{}--", url);
        tasks.push(tokio::spawn(async move {
            match reqwest::get(url.to_string()).await {
                Ok(resp) => match resp.bytes().await {
                    Ok(bytes) => {
                        // let image_vec = image_vec.clone();
                        // image_vec.lock().await.push((i, bytes.to_vec()));

                        let a = tokio::spawn(async move {
                            let start = std::time::Instant::now();
                            let mut printer = ImagePrinter::new(10);
                            printer.offset(4, (i * 10 + 1) as i16);
                            let _ = printer.print(&bytes);
                            let end = std::time::Instant::now();
                            let single = end.duration_since(start);
                            println!("{:?}", single);
                        });
                        a.await.unwrap();
                        return Ok(());
                    }
                    Err(_) => return Err(format!("ERROR reading {}", &url)),
                },
                Err(_) => return Err(format!("ERROR downloading {}", &url)),
            }
        }));
    }

    println!("Started {} tasks. Waiting...", tasks.len());
    let res = join_all(tasks).await;

    Ok(())
}

// pub async fn get_image_single_thread(urls: &Vec<&str>) -> Result<()> {
//     for (_, url) in urls.iter().enumerate() {
//         let url = url.to_string();
//         println!("START --{}--", url);
//         tokio::spawn(async move {
//             match reqwest::get(url.to_string()).await {
//                 Ok(resp) => match resp.bytes().await {
//                     Ok(bytes) => {
//                         println!("END --{}--", url);
//                         Ok(bytes)
//                     }
//                     Err(_) => return Err(format!("ERROR reading {}", &url)),
//                 },
//                 Err(_) => return Err(format!("ERROR downloading {}", &url)),
//             }
//         });
//     }
//     Ok(())
// }

// async fn get_image_mpsc(urls: &Vec<&str>) -> Result<()> {
//     println!("start get image mpsc");
//     let urls_count = urls.len();
//     let urls = urls.into_iter().unique().collect::<Vec<_>>();
//     let urls_count_dedup = urls.len();
//     println!("{} urls, {} deduped", urls_count, urls_count_dedup);

//     let thread_num = 32;
//     let (mut main_tx, main_rx) = flume::bounded::<Bytes>(1);

//     for _ in 0..thread_num {
//         let (mut tx, rx) = flume::bounded(1);
//         std::mem::swap(&mut tx, &mut main_tx);

//         thread::spawn(move || {
//             for msg in rx.iter() {
//                 tx.send(msg).unwrap();
//             }
//         });
//     }

//     let urls = urls.clone();

//     for url in urls.iter() {
//         let main_tx = main_tx.clone();
//         let url = url.to_string();
//         println!("START --{}--", url);
//         tokio::spawn(async move {
//             match reqwest::get(url.to_string()).await {
//                 Ok(resp) => match resp.bytes().await {
//                     Ok(bytes) => {
//                         println!("END --{}--", url);
//                         main_tx.send(bytes).unwrap();
//                         Ok(())
//                     }
//                     Err(_) => return Err(format!("ERROR reading {}", &url)),
//                 },
//                 Err(_) => return Err(format!("ERROR downloading {}", &url)),
//             }
//         });
//     }

//     for _ in urls.iter() {
//         main_rx.recv().unwrap();
//     }
//     Ok(())
// }

async fn get_image_mpsc_save_rocks(urls: &Vec<&str>) -> Result<()> {
    println!("start get image mpsc");
    let urls_count = urls.len();
    let urls = urls.into_iter().unique().collect::<Vec<_>>();
    let urls_count_dedup = urls.len();
    println!("{} urls, {} deduped", urls_count, urls_count_dedup);

    println!("start get image mpsc with rocks");
    let db = rocks_table::ImageTable::new(ROCKS_MPSC_DB_PATH);
    let mut tasks: Vec<JoinHandle<Result<Bytes, String>>> = vec![];

    let thread_num = 32;
    let (mut main_tx, _) = flume::bounded::<(String, Bytes)>(1);

    for _ in 0..thread_num {
        let (mut tx, rx) = flume::bounded(1);
        std::mem::swap(&mut tx, &mut main_tx);
        let db = db.clone();
        thread::spawn(move || {
            for msg in rx.iter() {
                let (url, bytes) = msg;
                let image = ImageValue::new(url.clone(), bytes.to_vec());
                if let Err(e) = db.upsert(&url, &image) {
                    println!("ERROR saving {}: {}", url, e);
                    continue;
                }
                // println!("SAVED --{}--", url.clone());
            }
        });
    }

    let urls = urls.clone();
    for url in urls.iter() {
        let main_tx = main_tx.clone();
        let url = url.to_string();
        let v = db.read(&url).unwrap();
        if v.is_some() {
            // println!("ALREADY EXISTS -- {} --", &url);
            continue;
        }
        // println!("START --{}--", url);
        tasks.push(tokio::spawn(async move {
            match reqwest::get(url.to_string()).await {
                Ok(resp) => match resp.bytes().await {
                    Ok(bytes) => {
                        // println!("END --{}--", url);
                        main_tx.send((url, bytes.clone())).unwrap();
                        Ok(bytes)
                    }
                    Err(_) => return Err(format!("ERROR reading {}", &url)),
                },
                Err(_) => return Err(format!("ERROR downloading {}", &url)),
            }
        }));
    }

    // println!("Started {} tasks. Waiting...", tasks.len());
    let _ = join_all(tasks).await;
    let mut printer = ImagePrinter::new(10);

    // let _ = get_image_single_thread(&urls).await;

    for (i, url) in urls.iter().enumerate() {
        let start = std::time::Instant::now();
        let img = db.read(&url.to_string()).unwrap();
        if img.is_none() {
            println!("ERROR: {} not found", url);
            continue;
        }

        printer.offset(4, (i * 10 + 1) as i16);
        let _ = printer.print(&img.unwrap().image);
        let end = std::time::Instant::now();
        let single = end.duration_since(start);
        println!("{} {}", url, single.as_millis());
    }

    Ok(())
}
