pub trait CharFilter {
    fn filter<'a>(&self, text: &'a str) -> &'a str;
}

#[derive(Debug)]
pub struct BasicCharFilter {
}

impl BasicCharFilter {
    pub fn new() -> Self {
        BasicCharFilter {}
    }
}

impl CharFilter for BasicCharFilter {
    fn filter<'a>(&self, text: &'a str) -> &'a str {
        text
    }
}
