use std::collections::BTreeMap;
use crate::store::{Result, Error};
use roaring::RoaringTreemap;
use byteorder::{WriteBytesExt, LittleEndian, ByteOrder, ReadBytesExt};
use memmap2::{Mmap, MmapOptions};
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::cmp::Ordering;
use std::ops::Deref;

pub type BuildingPostingMap = BTreeMap<u32, BuildingPostingData>;

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
        } else if self.tf_content < TF_MAX { self.tf_content += 1; }
    }
}

const POSTING_SIZE: u32 = (32 + 16 + 16) / 8;
const INTERSECTION_PERFORMANCE_TIPPING_SIZE_DIFF: u32 = 50;

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
        self.writer.write_u32::<LittleEndian>(self.map.len() as u32)?;
        let mut len = 4u64;

        for v in self.map.iter() {
            self.writer.write_u32::<LittleEndian>(*v.0)?;
            self.writer.write_u16::<LittleEndian>(v.1.tf_title)?;
            self.writer.write_u16::<LittleEndian>(v.1.tf_content)?;

            len += POSTING_SIZE as u64;
        }

        Ok(len)
    }
}

pub fn posting_list_intersection<A: PostingList, B: PostingList>(a: &A, b: &B) -> Result<ScoredPostingList> {
    if a.len() < b.len() / INTERSECTION_PERFORMANCE_TIPPING_SIZE_DIFF {
        posting_list_intersection_search(a, b)
    } else if b.len() < a.len() / INTERSECTION_PERFORMANCE_TIPPING_SIZE_DIFF {
        posting_list_intersection_search(b, a)
    } else {
        posting_list_intersection_stitch(a, b)
    }
}

fn posting_list_intersection_search<A: PostingList, B: PostingList>(smaller: &A, larger: &B) -> Result<ScoredPostingList> {
    let mut result = ScoredPostingList::new();

    let mut min = 0u32;

    for i in 0..smaller.len() {
        let value = smaller.get(i)?;
        let mut max = larger.len();

        while min < max {
            let mid = min + ((max - min) >> 1);
            let c_value = larger.get(mid)?;

            if c_value < value {
                min = mid + 1;
            } else if c_value > value {
                max = mid;
            } else {
                result.add(value);
                min = mid + 1;
                break;
            }
        }

        if min >= larger.len() { break; }
    }

    Ok(result)
}

fn posting_list_intersection_stitch<A: PostingList, B: PostingList>(a: &A, b: &B) -> Result<ScoredPostingList> {
    let (mut i, mut j) = (0u32, 0u32);

    let mut result = ScoredPostingList::new();

    while i < a.len() && j < b.len() {
        let va = a.get(i)?;
        let vb = b.get(j)?;

        if va < vb {
            i += 1;
        } else if va > vb {
            j += 1;
        } else {
            result.add(va);
            i += 1;
            j += 1;
        }
    }

    Ok(result)
}

pub trait PostingList {
    fn len(&self) -> u32;
    fn get(&self, index: u32) -> Result<u32>;
}

#[derive(Debug)]
pub struct ScoredPostingList {
    postings: Vec<u32>,
}

impl PostingList for ScoredPostingList {
    #[inline(always)]
    fn len(&self) -> u32 { self.postings.len() as u32 }

    #[inline(always)]
    fn get(&self, index: u32) -> Result<u32> {
        if index >= self.len() {
            return Err(Error::OutOfRange);
        }

        Ok(self.postings[index as usize])
    }
}

impl ScoredPostingList {
    fn new() -> Self {
        ScoredPostingList { postings: Vec::<u32>::new() }
    }

    fn add(&mut self, id: u32) {
        self.postings.push(id);
    }
}

#[derive(Debug)]
pub struct RawPostingList {
    mmap: Mmap,
    len: u32,
}

impl PostingList for RawPostingList {
    #[inline(always)]
    fn len(&self) -> u32 { self.len }

    #[inline(always)]
    fn get(&self, index: u32) -> Result<u32> {
        if index >= self.len() {
            return Err(Error::OutOfRange);
        }

        Ok(LittleEndian::read_u32(&self.mmap[(index * POSTING_SIZE) as usize..]))
    }
}

impl RawPostingList {
    pub fn new(file: &mut File, seek_from: SeekFrom) -> Result<Self> {
        let offset = file.seek(seek_from)?;

        let len = file.read_u32::<LittleEndian>()?;

        if len == 0 {
            return Err(Error::OutOfRange);
        }

        let bytes = len * POSTING_SIZE;

        if file.metadata()?.len() < (offset + 4 + bytes as u64) {
            return Err(Error::OutOfRange);
        }

        let mmap = unsafe {
            MmapOptions::new().offset(offset + 4).len(bytes as usize).map(&*file)?
        };
        Ok(RawPostingList { mmap, len })
    }
}
