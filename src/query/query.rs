use crate::analyzer::analyzer::Analyzer;
use crate::analyzer::char_filter::CharFilter;
use crate::analyzer::token_filter::TokenFilter;
use crate::analyzer::tokenizer::Tokenizer;
use crate::query::score::{
    calc_cosine_unchecked, calc_norm, calc_tf, Score, TermPriorityCalculator,
    TfIdfTermPriorityCalculator,
};
use crate::query::{Error, Result};
use crate::store::constants::{
    TERM_DICT_FILE_SUFFIX, TERM_DICT_MAGIC_NUMBER, TERM_INDEX_FILE_SUFFIX, TERM_INDEX_MAGIC_NUMBER,
    VERSION,
};
use crate::store::posting::{PostingListMerger, RawPostingList};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use core::num::FpCategory::Nan;
use fst::automaton::Levenshtein;
use fst::{Automaton, IntoStreamer};
use memmap2::{Mmap, MmapOptions};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::{Deref, Range};
use std::path::PathBuf;

#[derive(Debug)]
pub struct Config<'a> {
    store_dir: PathBuf,
    identifier: &'a str,
    boost_title: u8,
    boost_content: u8,
}

impl<'a> Config<'a> {
    pub fn new(
        store_dir: PathBuf,
        identifier: &'a str,
        boost_title: u8,
        boost_content: u8,
    ) -> Self {
        Config {
            store_dir,
            identifier,
            boost_title,
            boost_content,
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
where
    C: CharFilter,
    T: TokenFilter,
    I: Tokenizer,
{
    analyzer: Analyzer<C, T, I>,
    config: Config<'a>,
    term_index: fst::Map<Mmap>,
    term_dict: File,
    doc_num: u32,
    term_priority_calculator: TfIdfTermPriorityCalculator,
}

impl<'a, C, T, I> Query<'a, C, T, I>
where
    C: CharFilter,
    T: TokenFilter,
    I: Tokenizer,
{
    pub fn new(analyzer: Analyzer<C, T, I>, config: Config<'a>) -> Result<Self> {
        let index_file = File::open(
            config
                .build_file_path(TERM_INDEX_FILE_SUFFIX)
                .to_str()
                .unwrap(),
        )?;
        let index_offset = check_term_index(&index_file)?;

        let mmap = unsafe {
            MmapOptions::new()
                .offset(index_offset as u64)
                .map(&index_file)?
        };
        let fst = fst::Map::new(mmap)?;

        let mut dict_file = File::open(
            config
                .build_file_path(TERM_DICT_FILE_SUFFIX)
                .to_str()
                .unwrap(),
        )?;
        check_term_dict(&dict_file)?;
        let doc_num = dict_file.read_u32::<LittleEndian>()?;

        let term_priority_calculator =
            TfIdfTermPriorityCalculator::new(doc_num, config.boost_title, config.boost_content);

        let query = Query {
            analyzer,
            config,
            term_index: fst,
            term_dict: dict_file,
            doc_num,
            term_priority_calculator,
        };

        Ok(query)
    }

    #[inline(always)]
    fn find_posting_list(&mut self, offset: u64) -> Result<RawPostingList> {
        Ok(RawPostingList::new(
            &mut self.term_dict,
            SeekFrom::Start(offset),
        )?)
    }

    #[inline(always)]
    fn query_term_postings<A: fst::Automaton>(
        &mut self,
        word: &str,
        aut_builder: &impl Fn(&str) -> Option<A>,
    ) -> Result<Option<RawPostingList>> {
        let dict_indexes = match aut_builder(word) {
            None => self
                .term_index
                .get(word)
                .map_or_else(Vec::new, |i| vec![(word.to_string(), i)]),
            Some(aut) => self.term_index.search(aut).into_stream().into_str_vec()?,
        };

        let mut other: Option<(String, u64)> = None;
        for index in dict_indexes.into_iter() {
            if index.0.as_str() == word {
                return Ok(Some(self.find_posting_list(index.1)?));
            } else {
                other = Some(index);
            }
        }

        other.map_or_else(
            || Ok(None),
            |index| Ok(Some(self.find_posting_list(index.1)?)),
        )
    }

    pub fn query<A: fst::Automaton>(
        &mut self,
        sentence: &str,
        aut_builder: &impl Fn(&str) -> Option<A>,
        range: Range<usize>,
    ) -> Result<Vec<u32>> {
        let sentence_ar = self.analyzer.analyze(sentence)?;

        let mut postings = Vec::<(&str, RawPostingList)>::new();

        let mut query_terms = HashMap::<&str, u16>::new();

        for word in sentence_ar.iter() {
            match query_terms.get_mut(word.as_str()) {
                None => {
                    query_terms.insert(word.as_str(), 1);
                }
                Some(i) => {
                    if *i < u16::MAX {
                        *i += 1;
                    }
                    continue;
                }
            }

            match self.query_term_postings(word.as_str(), aut_builder)? {
                None => (),
                Some(v) => {
                    postings.push((word.as_str(), v));
                }
            }
        }

        postings.sort_by(|a, b| a.1.len().cmp(&b.1.len()));

        println!("{:?}", query_terms);

        let mut df = Vec::<u32>::with_capacity(postings.len());
        let mut query_score = Vec::<f64>::with_capacity(postings.len());
        let mut merger = PostingListMerger::new();

        for p in postings.iter() {
            let list = &p.1;
            let query_term = query_terms.get(p.0).unwrap();
            let tf = calc_tf(*query_term);
            let norm = calc_norm(sentence.chars().count());
            query_score.push(
                self.term_priority_calculator
                    .calc(list.len(), tf, tf, norm, norm),
            );
            df.push(list.len());
            merger.union(&p.1)?;
        }

        let mut result = Vec::new();

        merger.mut_get_postings().sort_by_cached_key(|p| {
            let mut score = Vec::<f64>::with_capacity(postings.len());
            let terms = p.get_term_priority_info();
            for i in 0..terms.len() {
                let term = unsafe { terms.get_unchecked(i) };
                score.push(self.term_priority_calculator.calc(
                    *unsafe { df.get_unchecked(i) },
                    term.tf.0,
                    term.tf.1,
                    term.norm.0,
                    term.norm.1,
                ))
            }
            Score::new(&query_score, &score)
        });

        let pl = merger.get_postings();

        if range.start < pl.len() {
            let start = pl.len() - range.start;
            let end = if range.end <= pl.len() {
                pl.len() - range.end
            } else {
                0
            };

            for i in (end..start).rev() {
                result.push(unsafe { pl.get_unchecked(i) }.get_doc_id());
            }
        }

        Ok(result)
    }
}

fn check_term_index(mut reader: impl std::io::Read) -> Result<usize> {
    if reader.read_u64::<LittleEndian>()? != TERM_INDEX_MAGIC_NUMBER || reader.read_u8()? != VERSION
    {
        return Err(Error::Incompatible);
    }

    Ok((64 + 8) / 8)
}

fn check_term_dict(mut reader: impl std::io::Read) -> Result<usize> {
    if reader.read_u64::<LittleEndian>()? != TERM_DICT_MAGIC_NUMBER || reader.read_u8()? != VERSION
    {
        return Err(Error::Incompatible);
    }

    Ok((64 + 8) / 8)
}
