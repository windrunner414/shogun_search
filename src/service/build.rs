use crate::analyzer::analyzer::Analyzer;
use crate::analyzer::char_filter::{CJKDocCharFilter, CharFilter};
use crate::analyzer::token_filter::{BasicTokenFilter, StopWordTokenFilter, TokenFilter};
use crate::analyzer::tokenizer::{JiebaTokenizer, Tokenizer};
use crate::store;
use crate::store::Document;
use core::future;
use futures::{Future, StreamExt};
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;
use std::thread::JoinHandle;
use std::time::SystemTime;

macro_rules! print_time_cost {
    ($str: expr, $time: expr) => {
        println!(
            "{} costs: {}ms",
            $str,
            SystemTime::now().duration_since($time).unwrap().as_millis()
        );
    };
}

pub fn start_builder_thread() -> (tokio::task::JoinHandle<()>, mpsc::Sender<BuildServiceTask>) {
    let (tx, rx): (
        mpsc::Sender<BuildServiceTask>,
        mpsc::Receiver<BuildServiceTask>,
    ) = mpsc::channel();

    let builder_thread = tokio::task::spawn_blocking(move || {
        let time = SystemTime::now();

        let mut stop_words_file = File::open("../../dict/stop_words.txt").unwrap();
        let title_analyzer = Analyzer::new(
            CJKDocCharFilter::new(),
            BasicTokenFilter::new(),
            JiebaTokenizer::new(),
        );
        let content_analyzer = Analyzer::new(
            CJKDocCharFilter::new(),
            StopWordTokenFilter::new(&mut stop_words_file).unwrap(),
            JiebaTokenizer::new(),
        );

        print_time_cost!("init analyzer", time);
        let time = SystemTime::now();

        let mut builder = store::Builder::new(
            title_analyzer,
            content_analyzer,
            store::Config::new(PathBuf::from("../../test_store/"), "test"),
        );

        for task in rx {
            match task.data {
                Some(data) => {
                    builder
                        .add_document(Document {
                            id: data.id,
                            title: data.title.as_str(),
                            content: data.content.as_str(),
                        })
                        .unwrap();
                    println!("add document({}) {}", data.id, data.title);
                }
                None => break,
            }
        }

        builder.finish().unwrap();

        print_time_cost!("build indexes", time);
    });

    (builder_thread, tx)
}

pub struct BuildService {
    pub tx: mpsc::Sender<BuildServiceTask>,
}

type SvcResponse = Response<Body>;
type SvcError = hyper::Error;
type SvcFuture = dyn Future<Output = Result<SvcResponse, SvcError>> + Send;

impl Service<Request<Body>> for BuildService {
    type Response = Response<Body>;
    type Error = hyper::Error;
    type Future = Pin<Box<SvcFuture>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let (parts, mut body) = req.into_parts();
        match (parts.method, parts.uri.path()) {
            (Method::POST, "/add") => {
                let tx = self.tx.clone();
                Box::pin(async move {
                    let body: serde_json::Result<AddPostReq> =
                        serde_json::from_slice(&body.next().await.unwrap().unwrap());

                    match body {
                        Ok(data) => {
                            tx.send(BuildServiceTask { data: Some(data) }).unwrap();
                            Ok(Response::builder()
                                .status(StatusCode::OK)
                                .body(Body::empty())
                                .unwrap())
                        }
                        Err(e) => {
                            eprintln!("bad request: {}", e);
                            Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::empty())
                                .unwrap())
                        }
                    }
                })
            }

            (Method::GET, "/finish") => {
                self.tx.send(BuildServiceTask { data: None }).unwrap();
                Box::pin(async {
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::empty())
                        .unwrap())
                })
            }

            _ => Box::pin(async {
                Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap())
            }),
        }
    }
}

pub struct BuildServiceTask {
    data: Option<AddPostReq>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AddPostReq {
    id: u32,
    title: String,
    content: String,
}
