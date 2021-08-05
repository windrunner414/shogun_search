use regex::Regex;

pub trait CharFilter {
    fn filter(&self, text: &str) -> String;
}

#[derive(Debug)]
pub struct BasicCharFilter {}

impl BasicCharFilter {
    pub fn new() -> Self {
        BasicCharFilter {}
    }
}

impl CharFilter for BasicCharFilter {
    fn filter(&self, text: &str) -> String {
        text.to_string()
    }
}

#[derive(Debug)]
pub struct CJKDocCharFilter {}

impl CJKDocCharFilter {
    pub fn new() -> Self {
        CJKDocCharFilter {}
    }
}

impl CharFilter for CJKDocCharFilter {
    fn filter(&self, text: &str) -> String {
        lazy_static::lazy_static! {
            static ref REGEX: Regex = Regex::new(r"[\s\p{N}\p{P}a-zA-Z\u2E80-\uFE4F]+").unwrap();
        }

        let mut result = String::new();
        for capture in REGEX.captures_iter(text) {
            result.push_str(capture.get(0).unwrap().as_str());
        }

        result
    }
}
