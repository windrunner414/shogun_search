use crate::store::posting::{BuildingPostingData, BuildingPostingMap};
use crate::store::Document;
use std::collections::BTreeMap;

pub type BuildingTermDictionary = BTreeMap<String, BuildingTermData>;

#[derive(Debug)]
pub struct BuildingTermData {
    posting_map: BuildingPostingMap,
}

impl BuildingTermData {
    pub fn new() -> Self {
        BuildingTermData {
            posting_map: BuildingPostingMap::new(),
        }
    }

    pub fn add_posting(&mut self, doc: &Document, is_title: bool) {
        match self.posting_map.get_mut(&doc.id) {
            None => {
                let mut d = BuildingPostingData::new(doc);
                d.add_tf(is_title);
                self.posting_map.insert(doc.id, d);
            }
            Some(d) => d.add_tf(is_title),
        }
    }

    pub fn get_posting_map(&self) -> &BuildingPostingMap {
        &self.posting_map
    }
}
