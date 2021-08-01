use crate::analyzer::char_filter::CharFilter;
use crate::analyzer::token_filter::TokenFilter;
use crate::analyzer::tokenizer::Tokenizer;
use crate::analyzer::error::{Result};

#[derive(Debug)]
pub struct Analyzer<C: CharFilter, T: TokenFilter, I: Tokenizer> {
    char_filter: C,
    token_filter: T,
    tokenizer: I,
}

impl<C, T, I> Analyzer<C, T, I>
    where C: CharFilter, T: TokenFilter, I: Tokenizer {
    pub fn new(char_filter: C, token_filter: T, tokenizer: I) -> Self {
        Analyzer {
            char_filter,
            token_filter,
            tokenizer,
        }
    }

    pub fn analyze<'a>(&self, text: &'a str) -> Result<Vec<&'a str>> {
        let text = self.char_filter.filter(text);
        let mut tokens = Vec::<&str>::new();

        for token in self.tokenizer.tokenize(text) {
            match self.token_filter.filter(token) {
                None => (),
                Some(t) => tokens.push(t),
            }
        }

        Ok(tokens)
    }
}