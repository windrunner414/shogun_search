use std::collections::BTreeMap;
use crate::store::posting::{BuildingPostingMap, BuildingPostingData};

pub type BuildingTermDictionary = BTreeMap<String, BuildingTermData>;

#[derive(Debug)]
pub struct BuildingTermData {
    posting_map: BuildingPostingMap,
}

impl BuildingTermData {
    pub fn new(id: u32, is_title: bool) -> Self {
        let mut d = BuildingTermData {
            posting_map: BuildingPostingMap::new(),
        };
        d.add_posting(id, is_title);
        d
    }

    pub fn add_posting(&mut self, id: u32, is_title: bool) {
        match self.posting_map.get_mut(&id) {
            None => {
                self.posting_map.insert(id, BuildingPostingData::new(is_title));
            }
            Some(m) => m.add_tf(is_title)
        }
    }

    pub fn get_posting_map(&self) -> &BuildingPostingMap {
        &self.posting_map
    }
}
