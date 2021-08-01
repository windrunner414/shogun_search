use jieba_rs::Jieba;

pub trait Tokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<&'a str>;
}

#[derive(Debug)]
pub struct JiebaTokenizer {
    jieba: Jieba
}

impl JiebaTokenizer {
    pub fn new() -> Self {
        JiebaTokenizer {
            jieba: Jieba::new()
        }
    }
}

impl Tokenizer for JiebaTokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<&'a str> {
        self.jieba.cut_for_search(text, true)
    }
}
