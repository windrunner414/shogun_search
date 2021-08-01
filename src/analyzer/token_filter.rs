pub trait TokenFilter {
    fn filter<'a>(&self, token: &'a str) -> Option<&'a str>;
}

#[derive(Debug)]
pub struct BasicTokenFilter {
    filter_stop_words: bool
}

impl BasicTokenFilter {
    pub fn new(filter_stop_words: bool) -> Self {
        BasicTokenFilter {
            filter_stop_words
        }
    }

    fn is_stop_word(&self, token: &str) -> bool {
        token == "的"
    }
}

impl TokenFilter for BasicTokenFilter {
    fn filter<'a>(&self, token: &'a str) -> Option<&'a str> {
        if self.filter_stop_words && self.is_stop_word(token) {
            return None;
        }

        Some(token)
    }
}
