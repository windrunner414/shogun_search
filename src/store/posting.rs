use std::collections::BTreeMap;
use crate::store::{Result, Error};
use roaring::RoaringTreemap;
use byteorder::{WriteBytesExt, LittleEndian, ByteOrder, ReadBytesExt};
use memmap2::{Mmap, MmapOptions};
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::cmp::Ordering;

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
        self.writer.write_u32::<LittleEndian>(self.map.len() as u32);
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

#[derive(Debug)]
pub struct PostingList {
    mmap: Mmap,
    len: u32,
}

impl PostingList {
    pub fn new(file: &File, seek: SeekFrom) -> Result<Self> {
        let mut reader = std::io::BufReader::new(file);
        let offset = reader.seek(seek)?;
        let len = reader.read_u32::<LittleEndian>()?;

        if len == 0 {
            return Err(Error::OutOfRange);
        }

        let bytes = len * POSTING_SIZE;

        if file.metadata()?.len() < (offset + 4 + bytes as u64) {
            return Err(Error::OutOfRange);
        }

        let mmap = unsafe {
            MmapOptions::new().offset(offset + 4).len(bytes as usize).map(file)?
        };
        Ok(PostingList { mmap, len })
    }

    #[inline(always)]
    pub fn len(&self) -> u32 { self.len }

    #[inline(always)]
    pub fn get(&self, index: u32) -> Result<u32> {
        if index >= self.len() {
            return Err(Error::OutOfRange);
        }

        Ok(LittleEndian::read_u32(&self.mmap[(index * POSTING_SIZE) as usize..]))
    }

    pub fn intersect(&self, other: &PostingList) -> Result<Vec<u32>> {
        let mut result = Vec::<u32>::new();

        let mut target = 2;
        let mut min1 = 0u32;
        let mut min2 = 0u32;
        let max1 = self.len();
        let max2 = other.len();

        let mut want = 0;

        while min1 < max1 && min2 < max2 {
            if target == 2 {
                want = self.get(min1)?;

                let mut min = min2;
                let mut max = max2;

                match other.get(max - 1)?.cmp(&want) {
                    Ordering::Equal => {
                        result.push(want);
                        min2 = max2;
                    },
                    Ordering::Less => {
                        min2 = max2;
                    },
                    _ => {
                        match other.get(min)?.cmp(&want) {
                            Ordering::Equal => {
                                result.push(want);
                                target = 1;
                                min2 = min + 1;
                            },
                            Ordering::Greater => {
                                min1 += 1;
                                target = 1;
                            },
                            _ =>  {
                                while min < max {
                                    let mid = (min + max) / 2;
                                    let v = other.get(mid)?;
                                    match v.cmp(&want) {
                                        Ordering::Equal => {
                                            result.push(want);
                                            target = 1;
                                            min2 = mid + 1;
                                            break;
                                        },
                                        Ordering::Less => {
                                            min = mid + 1;
                                        },
                                        Ordering::Greater => {
                                            max = mid;
                                        }
                                    }
                                }

                                if min == max {
                                    target = 1;
                                    min2 = max;
                                }
                            }
                        }
                    }
                }
            } else {
                want = self.get(min2)?;

                let mut min = min1;
                let mut max = max1;

                match self.get(max - 1)?.cmp(&want) {
                    Ordering::Equal => {
                        result.push(want);
                        min1 = max1;
                    },
                    Ordering::Less => {
                        min1 = max1;
                    },
                    _ => {
                        match self.get(min)?.cmp(&want) {
                            Ordering::Equal => {
                                result.push(want);
                                target = 2;
                                min1 = min + 1;
                            },
                            Ordering::Greater => {
                                min2 += 1;
                                target = 2;
                            },
                            _ =>  {
                                while min < max {
                                    let mid = (min + max) / 2;
                                    let v = self.get(mid)?;
                                    match v.cmp(&want) {
                                        Ordering::Equal => {
                                            result.push(want);
                                            target = 2;
                                            min1 = mid + 1;
                                            break;
                                        },
                                        Ordering::Less => {
                                            min = mid + 1;
                                        },
                                        Ordering::Greater => {
                                            max = mid;
                                        }
                                    }
                                }

                                if min == max {
                                    target = 2;
                                    min1 = max;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }
}
