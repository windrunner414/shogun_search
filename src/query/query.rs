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
use std::ops::Deref;

#[derive(Debug)]
pub struct Config<'a> {
    store_dir: PathBuf,
    identifier: &'a str,
    title_priority: u8,
    content_priority: u8
}

impl<'a> Config<'a> {
    pub fn new(
        store_dir: PathBuf,
        identifier: &'a str,
        title_priority: u8,
        content_priority: u8
    ) -> Self {
        Config {
            store_dir,
            identifier,
            title_priority,
            content_priority
        }
    }

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
    doc_num: u32
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

        let mut dict_file = File::open(config.build_file_path(TERM_DICT_FILE_SUFFIX)
            .to_str().unwrap())?;
        check_term_dict(&dict_file)?;
        let doc_num = dict_file.read_u32::<LittleEndian>()?;

        let query = Query {
            analyzer,
            config,
            term_index: fst,
            term_dict: dict_file,
            doc_num
        };

        Ok(query)
    }

    #[inline(always)]
    fn find_posting_list(&mut self, offset: u64) -> Result<RoaringTreemap> {
        self.term_dict.seek(SeekFrom::Start(offset))?;
        let roaring = RoaringTreemap::deserialize_from(&self.term_dict)?;
        Ok(roaring)
    }

    #[inline(always)]
    fn query_word_postings<A: fst::Automaton>(
        &mut self,
        word: &str,
        aut_builder: &impl Fn(&str) -> Option<A>
    ) -> Result<Vec<(String, RoaringTreemap)>> {
        let dict_indexes = match aut_builder(word) {
            None => self.term_index.get(word).map_or_else(
                Vec::new,
                |i| vec![(word.to_string(), i)]
            ),
            Some(aut) => self.term_index.search(aut).into_stream().into_str_vec()?
        };

        let mut result = Vec::<(String, RoaringTreemap)>::new();
        for index in dict_indexes {
            result.push((index.0, self.find_posting_list(index.1)?));
        }

        Ok(result)
    }

    pub fn query<A: fst::Automaton>(
        &mut self,
        sentence: &str,
        aut_builder: &impl Fn(&str) -> Option<A>
    ) -> Result<HashSet<u64>> {
        let mut postings = Vec::<(&str, Vec<(String, RoaringTreemap)>)>::new();

        for word in self.analyzer.analyze(sentence)? {
            postings.push((word, self.query_word_postings(word, aut_builder)?));
        }

        Ok(HashSet::<u64>::new())
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

struct PostingList {

}

impl PostingList {

}
