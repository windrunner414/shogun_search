use crate::analyzer::analyzer::Analyzer;
use crate::analyzer::char_filter::{BasicCharFilter, CJKDocCharFilter};
use crate::analyzer::token_filter::{BasicTokenFilter, StopWordTokenFilter, TokenFilter};
use crate::analyzer::tokenizer::{JiebaTokenizer, Tokenizer};
use crate::query::Query;
use crate::service::build::{start_builder_thread, AddPostReq, BuildService, BuildServiceTask};
use crate::store::builder::{Builder, Config};
use crate::store::document::Document;
use clap::{App, Arg, SubCommand};
use core::future;
use fst::automaton::Levenshtein;
use futures::StreamExt;
use hyper::service::{make_service_fn, service_fn, Service};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use serde::{Deserialize, Serialize};
use std::fs::{read_dir, read_to_string, File};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;
use std::time::SystemTime;

mod analyzer;
mod query;
mod service;
mod store;

macro_rules! print_time_cost {
    ($str: expr, $time: expr) => {
        println!(
            "{} costs: {}ms",
            $str,
            SystemTime::now().duration_since($time).unwrap().as_millis()
        );
    };
}

#[tokio::main]
async fn main() {
    //test_query_single();

    let matches = App::new("Raiden Shogun Search")
        .version("0.0.1")
        .author("Yuxiang Liu <windrunner414@outlook.com>")
        .about("full text search")
        .arg(
            Arg::with_name("address")
                .short("a")
                .value_name("ADDRESS")
                .help("bind address")
                .required(true)
                .takes_value(true),
        )
        .subcommand(SubCommand::with_name("build").about("build indexes"))
        .get_matches();

    let address = SocketAddr::from_str(matches.value_of("address").unwrap()).unwrap();

    match matches.subcommand_matches("build") {
        Some(_) => run_build_server(address).await,
        None => run_query_server(address).await,
    };
}

struct MakeBuildService {
    tx: Sender<BuildServiceTask>,
}

impl<T> Service<T> for MakeBuildService {
    type Response = BuildService;
    type Error = std::io::Error;
    type Future = future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, _: T) -> Self::Future {
        future::ready(Ok(BuildService {
            tx: self.tx.clone(),
        }))
    }
}

async fn run_build_server(address: SocketAddr) {
    let (task, tx) = start_builder_thread();

    let make_svc = MakeBuildService { tx };

    let server = Server::bind(&address).serve(make_svc);

    let graceful =
        server.with_graceful_shutdown(async { task.await.expect("builder thread error") });

    if let Err(e) = graceful.await {
        eprintln!("server error: {}", e);
    }
}

async fn run_query_server(address: SocketAddr) {}

fn test_query_single() {
    let time = SystemTime::now();

    let analyzer = Analyzer::new(
        CJKDocCharFilter::new(),
        BasicTokenFilter::new(),
        JiebaTokenizer::new(),
    );

    print_time_cost!("init analyzer", time);

    let mut query = Query::new(
        analyzer,
        query::Config::new(PathBuf::from("../../test_store/"), "test", 3, 1),
    )
    .unwrap();

    let time = SystemTime::now();

    let results = query
        .query(
            "神里",
            &|w| Levenshtein::new(w, if w.chars().count() > 4 { 1 } else { 0 }).ok(),
            0..10,
        )
        .unwrap();

    let costs = SystemTime::now().duration_since(time).unwrap().as_millis();

    println!("{:?}", results);

    println!("search costs: {}ms, total: {}", costs, results.len());
}
