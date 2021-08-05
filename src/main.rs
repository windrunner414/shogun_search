use crate::analyzer::analyzer::Analyzer;
use crate::analyzer::char_filter::{BasicCharFilter, CJKDocCharFilter};
use crate::analyzer::token_filter::{BasicTokenFilter, StopWordTokenFilter, TokenFilter};
use crate::analyzer::tokenizer::{JiebaTokenizer, Tokenizer};
use crate::query::Query;
use crate::store::builder::{Builder, Config};
use crate::store::document::Document;
use fst::automaton::{AlwaysMatch, Levenshtein};
use std::fs::{read_dir, read_to_string, File};
use std::path::PathBuf;
use std::time::SystemTime;

mod analyzer;
mod query;
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

fn main() {
    //test_build_indexes();
    test_query_single();
}

fn test_build_indexes() {
    let time = SystemTime::now();

    let mut stop_words_file = File::open("./dict/stop_words.txt").unwrap();
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

    let mut builder = Builder::new(
        title_analyzer,
        content_analyzer,
        Config::new(PathBuf::from("./test_store/"), "test"),
    );

    for entry in read_dir("/Users/yuxiang.liu/Downloads/test/").unwrap() {
        let entry = entry.unwrap();
        let metadata = entry.metadata().unwrap();

        if metadata.is_file() {
            let filename = entry.file_name().into_string().unwrap();
            let filename = filename.split(".").collect::<Vec<&str>>();

            let title = filename[0];
            let content = read_to_string(entry.path()).unwrap();
            let id = filename[1].parse::<u32>().unwrap();

            builder
                .add_document(Document {
                    id,
                    title,
                    content: content.as_str(),
                })
                .unwrap();
        }
    }

    builder.finish().unwrap();

    print_time_cost!("build indexes", time);
}

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
        query::Config::new(PathBuf::from("./test_store/"), "test", 3, 1),
    )
    .unwrap();

    let time = SystemTime::now();

    let results = query
        .query(
            "测试",
            &|w| Levenshtein::new(w, if w.chars().count() > 4 { 1 } else { 0 }).ok(),
            0..10,
        )
        .unwrap();

    let costs = SystemTime::now().duration_since(time).unwrap().as_millis();

    let mut string = String::new();
    for r in results.iter() {
        string.push('\n');
        string.push_str(
            read_to_string(format!("/Users/yuxiang.liu/Downloads/test/TT.{}", r))
                .unwrap()
                .as_str(),
        );
    }
    println!("{}", string);
    println!("{:?}", results);

    println!("search costs: {}ms, total: {}", costs, results.len());
}
