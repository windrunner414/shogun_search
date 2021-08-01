use std::collections::BTreeMap;
use crate::store::Result;
use roaring::RoaringTreemap;
use byteorder::{WriteBytesExt, LittleEndian};

pub type BuildingPostingMap = BTreeMap<u64, BuildingPostingData>;

const TF_MAX: u16 = u16::MAX;

#[derive(Debug)]
pub struct BuildingPostingData {
    tf_content: u16,
    tf_title: u16,
}

impl BuildingPostingData {
    pub fn new(is_title: bool) -> Self {
        BuildingPostingData {
            tf_title: if is_title { 1 } else { 0 },
            tf_content: if is_title { 0 } else { 1 },
        }
    }

    #[inline]
    pub fn add_tf(&mut self, is_title: bool) {
        if is_title {
            if self.tf_title < TF_MAX { self.tf_title += 1; }
        } else {
            if self.tf_content < TF_MAX { self.tf_content += 1; }
        }
    }
}

#[derive(Debug)]
pub struct PostingListBuilder<'a, W: std::io::Write> {
    writer: W,
    map: &'a BuildingPostingMap
}

impl<'a, W: std::io::Write> PostingListBuilder<'a, W> {
    pub fn new(writer: W, map: &'a BuildingPostingMap) -> Self {
        PostingListBuilder { writer, map }
    }

    pub fn finish(&mut self) -> Result<u64> {
        let mut len = 0u64;

        let mut roaring = RoaringTreemap::new();

        for v in self.map.iter() {
            roaring.push(*v.0);
        }

        roaring.serialize_into(&mut self.writer)?;
        len += roaring.serialized_size() as u64;

        for v in self.map.iter() {
            self.writer.write_u16::<LittleEndian>(v.1.tf_title)?;
            self.writer.write_u16::<LittleEndian>(v.1.tf_content)?;

            len += (16 + 16) / 8;
        }

        Ok(len)
    }
}
