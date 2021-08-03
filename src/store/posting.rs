use crate::query::score::{calc_norm, calc_tf};
use crate::store::{Document, Error, Result};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use memmap2::{Mmap, MmapOptions};
use std::cmp::Ordering;
use std::collections::{BTreeMap, LinkedList};
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::ops::Deref;

pub type BuildingPostingMap = BTreeMap<u32, BuildingPostingData>;

/// 因为并没有存document的信息，无法通过doc id找到norm，所以norm只能每个term下都存一份
#[derive(Debug)]
pub struct BuildingPostingData {
    freq_content: u16,
    freq_title: u16,
    norm_content: u8,
    norm_title: u8,
}

impl BuildingPostingData {
    pub fn new(doc: &Document) -> Self {
        BuildingPostingData {
            freq_title: 0,
            freq_content: 0,
            norm_title: calc_norm(doc.title.chars().count()),
            norm_content: calc_norm(doc.content.chars().count()),
        }
    }

    #[inline]
    pub fn add_tf(&mut self, is_title: bool) {
        if is_title {
            if self.freq_title < u16::MAX {
                self.freq_title += 1;
            }
        } else if self.freq_content < u16::MAX {
            self.freq_content += 1;
        }
    }
}

/// doc_id(32bit) + norm_title(8bit) + norm_content(8bit) + tf_title(8bit) + tf_content(8bit)
const POSTING_SIZE: u32 = (32 + 8 + 8 + 8 + 8) / 8;
const INTERSECTION_PERFORMANCE_TIPPING_SIZE_DIFF: u32 = 50;

#[derive(Debug)]
pub struct PostingListBuilder<'a, W: std::io::Write> {
    writer: W,
    map: &'a BuildingPostingMap,
}

impl<'a, W: std::io::Write> PostingListBuilder<'a, W> {
    pub fn new(writer: W, map: &'a BuildingPostingMap) -> Self {
        PostingListBuilder { writer, map }
    }

    pub fn finish(&mut self) -> Result<u64> {
        self.writer
            .write_u32::<LittleEndian>(self.map.len() as u32)?;
        let mut len = 4u64;

        for v in self.map.iter() {
            self.writer.write_u32::<LittleEndian>(*v.0)?;
            self.writer.write_u8(calc_tf(v.1.freq_title))?;
            self.writer.write_u8(calc_tf(v.1.freq_content))?;
            self.writer.write_u8(v.1.norm_title)?;
            self.writer.write_u8(v.1.norm_content)?;

            len += POSTING_SIZE as u64;
        }

        Ok(len)
    }
}

#[derive(Debug, Clone)]
pub struct TermPriorityInfo {
    /// (tf_title, tf_content)
    pub tf: (u8, u8),
    /// (norm_title, norm_content)
    pub norm: (u8, u8),
}

impl TermPriorityInfo {
    pub fn new(tf: (u8, u8), norm: (u8, u8)) -> Self {
        TermPriorityInfo { tf, norm }
    }

    pub fn not_exist() -> Self {
        TermPriorityInfo::new((0u8, 0u8), (0u8, 0u8))
    }
}

#[derive(Debug)]
pub struct Posting {
    doc_id: u32,
    term_priority_info: Vec<TermPriorityInfo>,
}

impl Posting {
    fn new(doc_id: u32, before_term_num: u32) -> Self {
        Posting {
            doc_id,
            term_priority_info: vec![TermPriorityInfo::not_exist(); before_term_num as usize],
        }
    }

    fn add(&mut self, info: TermPriorityInfo) {
        self.term_priority_info.push(info);
    }

    pub fn get_doc_id(&self) -> u32 {
        self.doc_id
    }

    pub fn get_term_priority_info(&self) -> &Vec<TermPriorityInfo> {
        &self.term_priority_info
    }
}

#[derive(Debug)]
pub struct PostingListMerger {
    // TODO: benchmark一下是vec更快还是LinkedList？LinkedList会导致cache miss
    postings: Vec<Posting>,
    merged_num: u32,
}

impl PostingListMerger {
    pub fn new() -> Self {
        PostingListMerger {
            postings: Vec::new(),
            merged_num: 0,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> u32 {
        self.postings.len() as u32
    }

    #[inline(always)]
    pub fn get_postings(&self) -> &Vec<Posting> {
        &self.postings
    }

    #[inline(always)]
    pub fn mut_get_postings(&mut self) -> &mut Vec<Posting> {
        &mut self.postings
    }

    #[inline(always)]
    fn end_do_merge(&mut self) {
        self.merged_num += 1;
    }

    /// 应确保self比list的len要小，当差距足够大的时候性能可能会更好
    pub fn intersection(&mut self, list: &RawPostingList) -> Result<()> {
        if self.len() < list.len() / INTERSECTION_PERFORMANCE_TIPPING_SIZE_DIFF {
            self.intersection_by_search(list)
        } else {
            self.intersection_by_stitch(list)
        }
    }

    // TODO: 有没有什么更高效的办法批量删除元素？
    fn intersection_by_search(&mut self, list: &RawPostingList) -> Result<()> {
        let mut min = 0u32;

        let mut need_remove = Vec::<usize>::new();

        for i in 0..self.postings.len() {
            let mut max = list.len();

            if min >= max {
                self.postings.drain(i..self.postings.len());
                break;
            }

            let posting = unsafe { self.postings.get_unchecked_mut(i) };
            let value = posting.doc_id;

            let mut find = false;

            loop {
                let mid = min + ((max - min) >> 1);
                let c_value = list.get_doc_id(mid)?;

                if c_value < value {
                    min = mid + 1;
                } else if c_value > value {
                    max = mid;
                } else {
                    find = true;

                    posting.add(TermPriorityInfo::new(
                        list.get_tf(mid)?,
                        list.get_norm(mid)?,
                    ));

                    min = mid + 1;
                    break;
                }

                if min >= max {
                    break;
                }
            }

            if !find {
                need_remove.push(i);
            }
        }

        for i in need_remove {
            self.postings.remove(i);
        }

        self.end_do_merge();
        Ok(())
    }

    fn intersection_by_stitch(&mut self, list: &RawPostingList) -> Result<()> {
        let (mut i, mut j) = (0usize, 0u32);
        let mut need_remove = Vec::<usize>::new();

        while i < self.postings.len() && j < list.len() {
            let va = unsafe { self.postings.get_unchecked_mut(i) };
            let vb = list.get_doc_id(j)?;

            if va.doc_id < vb {
                need_remove.push(i);
                i += 1;
            } else if va.doc_id > vb {
                j += 1;
            } else {
                va.add(TermPriorityInfo::new(list.get_tf(j)?, list.get_norm(j)?));
                i += 1;
                j += 1;
            }
        }

        if i < self.postings.len() {
            self.postings.drain(i..self.postings.len());
        }

        for i in need_remove {
            self.postings.remove(i);
        }

        self.end_do_merge();
        Ok(())
    }

    pub fn union(&mut self, list: &RawPostingList) -> Result<()> {
        let (mut i, mut j) = (0usize, 0u32);

        let mut need_insert = Vec::<u32>::new();

        while i < self.postings.len() && j < list.len() {
            let va = unsafe { self.postings.get_unchecked_mut(i) };
            let vb = list.get_doc_id(j)?;

            if va.doc_id < vb {
                va.add(TermPriorityInfo::not_exist());
                i += 1;
            } else if va.doc_id > vb {
                need_insert.push(j);
                j += 1;
            } else {
                va.add(TermPriorityInfo::new(list.get_tf(j)?, list.get_norm(j)?));
                i += 1;
                j += 1;
            }
        }

        let mut insert = |i| -> Result<()> {
            let mut posting = Posting::new(list.get_doc_id(i)?, self.merged_num);
            posting.add(TermPriorityInfo::new(list.get_tf(i)?, list.get_norm(i)?));
            self.postings.push(posting);
            Ok(())
        };

        if j < list.len() {
            for i in j..list.len() {
                insert(i)?;
            }
        }

        for i in need_insert {
            insert(i)?;
        }
        // TODO: 已知前面一部分顺序都是排好的，只需要排新insert的部分就好了，并且新insert的部分也是有序的。merge num为0时union就不需要重新排序
        // insert时直接找到正确的位置insert会不会更快，LinkedList是否会更好？
        self.postings
            .sort_unstable_by(|a, b| a.doc_id.cmp(&b.doc_id));

        self.end_do_merge();
        Ok(())
    }
}

#[derive(Debug)]
pub struct RawPostingList {
    mmap: Mmap,
    len: u32,
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
            MmapOptions::new()
                .offset(offset + 4)
                .len(bytes as usize)
                .map(&*file)?
        };
        Ok(RawPostingList { mmap, len })
    }

    #[inline(always)]
    pub fn len(&self) -> u32 {
        self.len
    }

    #[inline(always)]
    pub fn get_doc_id(&self, index: u32) -> Result<u32> {
        if index >= self.len() {
            return Err(Error::OutOfRange);
        }

        Ok(LittleEndian::read_u32(
            &self.mmap[(index * POSTING_SIZE) as usize..],
        ))
    }

    #[inline(always)]
    pub fn get_tf(&self, index: u32) -> Result<(u8, u8)> {
        if index >= self.len() {
            return Err(Error::OutOfRange);
        }

        let offset = (index * POSTING_SIZE) as usize + 4;
        Ok((self.mmap[offset], self.mmap[offset + 1]))
    }

    #[inline(always)]
    pub fn get_norm(&self, index: u32) -> Result<(u8, u8)> {
        if index >= self.len() {
            return Err(Error::OutOfRange);
        }

        let offset = (index * POSTING_SIZE) as usize + 4 + 2;
        Ok((self.mmap[offset], self.mmap[offset + 1]))
    }
}
