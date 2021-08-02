use crate::analyzer::analyzer::Analyzer;
use crate::analyzer::char_filter::BasicCharFilter;
use crate::analyzer::token_filter::{BasicTokenFilter, TokenFilter};
use crate::analyzer::tokenizer::{JiebaTokenizer, Tokenizer};
use crate::store::builder::{Builder, Config};
use crate::store::document::Document;
use std::path::PathBuf;
use std::fs::{read_dir, read_to_string};
use std::time::SystemTime;
use crate::query::query::Query;
use fst::automaton::{Levenshtein, AlwaysMatch};

mod analyzer;
mod store;
mod query;

macro_rules! print_time_cost {
    ($str: expr, $time: expr) => (
        println!("{} costs: {}ms", $str, SystemTime::now().duration_since($time).unwrap().as_millis());
    )
}

fn main() {
    //test_build_indexes();
    test_query_single();
}

fn test_build_indexes() {
    let time = SystemTime::now();

    let title_analyzer = Analyzer::new(
        BasicCharFilter::new(),
        BasicTokenFilter::new(false),
        JiebaTokenizer::new()
    );
    let content_analyzer = Analyzer::new(
        BasicCharFilter::new(),
        BasicTokenFilter::new(true),
        JiebaTokenizer::new()
    );

    print_time_cost!("init analyzer", time);
    let time = SystemTime::now();

    let mut builder = Builder::new(title_analyzer, content_analyzer, Config::new(PathBuf::from("./test_store/"), "test"));

    for entry in read_dir("/Users/yuxiang.liu/Downloads/test").unwrap() {
        let entry = entry.unwrap();
        let metadata = entry.metadata().unwrap();

        if metadata.is_file() {
            let filename = entry.file_name().into_string().unwrap();
            let filename = filename.split(".").collect::<Vec<&str>>();

            let title = filename[0];
            let content = read_to_string(entry.path()).unwrap();
            let id = filename[1].parse::<u32>().unwrap();

            builder.add_document(Document {id, title, content: content.as_str()}).unwrap();
        }
    }

    builder.finish().unwrap();

    print_time_cost!("build indexes", time);
}

fn test_query_single() {
    let time = SystemTime::now();

    let analyzer = Analyzer::new(
    BasicCharFilter::new(),
    BasicTokenFilter::new(false),
    JiebaTokenizer::new()
    );

    print_time_cost!("init analyzer", time);

    let mut query = Query::new(
        analyzer,
        query::Config::new(
        PathBuf::from("./test_store/"),
        "test",
        3,
        1,
        )
    ).unwrap();

    let time = SystemTime::now();

    let results = query.query("线下赛事", &|w| {
        Levenshtein::new(w, 0).ok()
    }).unwrap();

    let costs = SystemTime::now().duration_since(time).unwrap().as_millis();

    let mut string = String::new();
    for r in results.iter() {
        string.push('\n');
        string.push_str(read_to_string(format!("/Users/yuxiang.liu/Downloads/test/TT.{}", r)).unwrap().as_str());
    }
    println!("{}", string);

    println!("search costs: {}ms, total: {}", costs, results.len());
}
