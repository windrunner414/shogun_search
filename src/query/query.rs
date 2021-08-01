use crate::analyzer::char_filter::CharFilter;
use crate::analyzer::token_filter::TokenFilter;
use crate::analyzer::tokenizer::Tokenizer;
use crate::analyzer::analyzer::Analyzer;
use std::path::PathBuf;
use memmap2::{Mmap, MmapOptions};
use std::fs::File;
use crate::store::constants::{TERM_INDEX_FILE_SUFFIX, TERM_INDEX_MAGIC_NUMBER, VERSION, TERM_DICT_FILE_SUFFIX, TERM_DICT_MAGIC_NUMBER};
use crate::query::{Result, Error};
use byteorder::{LittleEndian, ByteOrder, ReadBytesExt};
use fst::automaton::Levenshtein;
use fst::IntoStreamer;
use std::collections::HashSet;
use roaring::RoaringTreemap;
use std::io::{Read, Seek, SeekFrom};

#[derive(Debug)]
pub struct Config<'a> {
    store_dir: PathBuf,
    identifier: &'a str,
    title_priority: u8,
    content_priority: u8,
    lev_distance: u8,
}

impl<'a> Config<'a> {
    pub fn new(
        store_dir: PathBuf,
        identifier: &'a str,
        title_priority: u8,
        content_priority: u8,
        lev_distance: u8
    ) -> Self {
        Config {
            store_dir,
            identifier,
            title_priority,
            content_priority,
            lev_distance
        }
    }

    #[inline]
    fn build_file_path(&self, suffix: &str) -> PathBuf {
        let mut buf = self.store_dir.clone();
        buf.push(String::from(self.identifier) + suffix);
        buf
    }
}

#[derive(Debug)]
pub struct Query<'a, C, T, I>
    where C: CharFilter, T: TokenFilter, I: Tokenizer {
    analyzer: Analyzer<C, T, I>,
    config: Config<'a>,
    term_index: fst::Map<Mmap>,
    term_dict: File,
}

impl<'a, C, T, I> Query<'a, C, T, I>
    where C: CharFilter, T: TokenFilter, I: Tokenizer {
    pub fn new(
        analyzer: Analyzer<C, T, I>,
        config: Config<'a>
    ) -> Result<Self> {
        let index_file = File::open(config.build_file_path(TERM_INDEX_FILE_SUFFIX)
            .to_str().unwrap())?;
        let index_offset = check_term_index(&index_file)?;

        let mmap = unsafe {
            MmapOptions::new().offset(index_offset as u64).map(&index_file)?
        };
        let fst = fst::Map::new(mmap)?;

        let dict_file = File::open(config.build_file_path(TERM_DICT_FILE_SUFFIX)
            .to_str().unwrap())?;
        check_term_dict(&dict_file);

        let query = Query {
            analyzer,
            config,
            term_index: fst,
            term_dict: dict_file
        };

        Ok(query)
    }

    fn query_posting_list(&mut self, offset: u64) -> Result<RoaringTreemap> {
        self.term_dict.seek(SeekFrom::Start(offset))?;
        let roaring = RoaringTreemap::deserialize_from(&self.term_dict)?;
        Ok(roaring)
    }

    pub fn query_single(&mut self, word: &str) -> Result<HashSet<u64>> {
        let results = match Levenshtein::new(word, self.config.lev_distance as u32) {
            Ok(lev) => self.term_index.search(lev).into_stream().into_str_vec()?,
            Err(e) => {
                println!("{}", e);
                match self.term_index.get(word) {
                    None => { vec![] }
                    Some(v) => { vec![(word.to_string(), v)] }
                }
            },
        };

        let mut set = HashSet::<u64>::new();
        for result in results {
            let posting_list = self.query_posting_list(result.1)?;
            for id in posting_list.iter() {
                set.insert(id);
            }
        }

        Ok(set)
    }
}

fn check_term_index(mut reader: impl std::io::Read) -> Result<usize> {
    if reader.read_u64::<LittleEndian>()? != TERM_INDEX_MAGIC_NUMBER
        || reader.read_u8()? != VERSION {
        return Err(Error::Incompatible);
    }

    Ok((64 + 8) / 8)
}

fn check_term_dict(mut reader: impl std::io::Read) -> Result<usize> {
    if reader.read_u64::<LittleEndian>()? != TERM_DICT_MAGIC_NUMBER
        || reader.read_u8()? != VERSION {
        return Err(Error::Incompatible);
    }

    Ok((64 + 8) / 8)
}
