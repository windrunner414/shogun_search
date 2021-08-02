pub struct Document<'a> {
    pub id: u32,
    pub title: &'a str,
    pub content: &'a str,
}
