pub struct Document<'a> {
    pub id: u64,
    pub title: &'a str,
    pub content: &'a str,
}
