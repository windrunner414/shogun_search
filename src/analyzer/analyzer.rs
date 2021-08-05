use crate::analyzer::char_filter::CharFilter;
use crate::analyzer::error::Result;
use crate::analyzer::token_filter::TokenFilter;
use crate::analyzer::tokenizer::Tokenizer;

#[derive(Debug)]
pub struct Analyzer<C: CharFilter, T: TokenFilter, I: Tokenizer> {
    char_filter: C,
    token_filter: T,
    tokenizer: I,
}

impl<C, T, I> Analyzer<C, T, I>
where
    C: CharFilter,
    T: TokenFilter,
    I: Tokenizer,
{
    pub fn new(char_filter: C, token_filter: T, tokenizer: I) -> Self {
        Analyzer {
            char_filter,
            token_filter,
            tokenizer,
        }
    }

    pub fn analyze(&self, text: &str) -> Result<Vec<String>> {
        let text = self.char_filter.filter(text);
        let mut tokens = Vec::<String>::new();

        for token in self.tokenizer.tokenize(text.as_str()) {
            match self.token_filter.filter(token) {
                None => (),
                Some(t) => tokens.push(t.to_string()),
            }
        }

        Ok(tokens)
    }
}
