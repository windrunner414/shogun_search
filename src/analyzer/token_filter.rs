use crate::analyzer::Result;
use regex::Regex;
use std::collections::HashSet;
use std::fs::File;
use std::io::Read;

pub trait TokenFilter {
    fn filter<'a>(&self, token: &'a str) -> Option<&'a str>;
}

#[derive(Debug)]
pub struct BasicTokenFilter {}

impl BasicTokenFilter {
    pub fn new() -> Self {
        BasicTokenFilter {}
    }
}

impl TokenFilter for BasicTokenFilter {
    fn filter<'a>(&self, token: &'a str) -> Option<&'a str> {
        lazy_static::lazy_static! {
            static ref REGEX: Regex = Regex::new(r"\s+").unwrap();
        }

        if REGEX.is_match(token) {
            None
        } else {
            Some(token)
        }
    }
}

#[derive(Debug)]
pub struct StopWordTokenFilter {
    stop_words: HashSet<String>,
}

impl StopWordTokenFilter {
    pub fn new(dict_file: &mut File) -> Result<Self> {
        let mut buf = String::new();
        dict_file.read_to_string(&mut buf)?;

        let mut stop_words = HashSet::<String>::new();

        for i in buf.split('\n') {
            stop_words.insert(i.trim().to_string());
        }

        Ok(StopWordTokenFilter { stop_words })
    }
}

impl TokenFilter for StopWordTokenFilter {
    fn filter<'a>(&self, token: &'a str) -> Option<&'a str> {
        lazy_static::lazy_static! {
            static ref REGEX: Regex = Regex::new(r"\s+").unwrap();
        }

        if REGEX.is_match(token) {
            return None;
        }

        if self.stop_words.contains(token) {
            return None;
        }

        Some(token)
    }
}
