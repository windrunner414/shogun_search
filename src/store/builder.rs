use crate::analyzer::analyzer::Analyzer;
use crate::analyzer::char_filter::CharFilter;
use crate::analyzer::token_filter::TokenFilter;
use crate::analyzer::tokenizer::Tokenizer;
use crate::store::constants::{
    TERM_DICT_FILE_SUFFIX, TERM_DICT_MAGIC_NUMBER, TERM_INDEX_FILE_SUFFIX, TERM_INDEX_MAGIC_NUMBER,
    VERSION,
};
use crate::store::document::Document;
use crate::store::error::{Error, Result};
use crate::store::posting::PostingListBuilder;
use crate::store::term::{BuildingTermData, BuildingTermDictionary};
use byteorder::{LittleEndian, WriteBytesExt};
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug)]
pub struct Config<'a> {
    store_dir: PathBuf,
    identifier: &'a str,
}

impl<'a> Config<'a> {
    pub fn new(store_dir: PathBuf, identifier: &'a str) -> Self {
        Config {
            store_dir,
            identifier,
        }
    }

    fn build_file_path(&self, suffix: &str) -> PathBuf {
        let mut buf = self.store_dir.clone();
        buf.push(String::from(self.identifier) + suffix);
        buf
    }
}

// TODO: 这泛型太迷了，能简化吗？

#[derive(Debug)]
pub struct Builder<'a, C, T, I, C2, T2, I2>
where
    C: CharFilter,
    T: TokenFilter,
    I: Tokenizer,
    C2: CharFilter,
    T2: TokenFilter,
    I2: Tokenizer,
{
    title_analyzer: Analyzer<C, T, I>,
    content_analyzer: Analyzer<C2, T2, I2>,
    config: Config<'a>,

    dict: BuildingTermDictionary,
    doc_num: u32,
}

impl<'a, C, T, I, C2, T2, I2> Builder<'a, C, T, I, C2, T2, I2>
where
    C: CharFilter,
    T: TokenFilter,
    I: Tokenizer,
    C2: CharFilter,
    T2: TokenFilter,
    I2: Tokenizer,
{
    pub fn new(
        title_analyzer: Analyzer<C, T, I>,
        content_analyzer: Analyzer<C2, T2, I2>,
        config: Config<'a>,
    ) -> Self {
        Builder {
            title_analyzer,
            content_analyzer,
            config,
            dict: BuildingTermDictionary::new(),
            doc_num: 0,
        }
    }

    pub fn add_document(&mut self, doc: Document) -> Result<()> {
        self.doc_num += 1;

        for term in self.title_analyzer.analyze(doc.title)? {
            self.add_term(term.as_str(), &doc, true)?;
        }

        for term in self.content_analyzer.analyze(doc.content)? {
            self.add_term(term.as_str(), &doc, false)?;
        }

        Ok(())
    }

    #[inline]
    fn add_term(&mut self, term: &str, doc: &Document, is_title: bool) -> Result<()> {
        match self.dict.get_mut(term) {
            None => {
                let mut d = BuildingTermData::new();
                d.add_posting(doc, is_title);
                self.dict.insert(term.to_string(), d);
            }
            Some(d) => d.add_posting(doc, is_title),
        }

        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        let index_file = File::create(
            self.config
                .build_file_path(TERM_INDEX_FILE_SUFFIX)
                .to_str()
                .unwrap(),
        )?;
        let mut index_writer = std::io::BufWriter::new(index_file);

        let dict_file = File::create(
            self.config
                .build_file_path(TERM_DICT_FILE_SUFFIX)
                .to_str()
                .unwrap(),
        )?;
        let mut dict_writer = std::io::BufWriter::new(dict_file);
        let mut dict_offset = 0u64;

        self.write_index_header(&mut index_writer)?;
        dict_offset += self.write_dict_header(&mut dict_writer)?;

        let mut fst_builder = fst::raw::Builder::new(index_writer)?;

        for term in self.dict.iter() {
            fst_builder.insert(term.0, dict_offset)?;
            dict_offset += self.write_dict(&mut dict_writer, term.1)?;
        }

        fst_builder.finish()?;

        Ok(())
    }

    #[inline]
    fn write_index_header(&self, writer: &mut std::io::BufWriter<File>) -> Result<u64> {
        writer.write_u64::<LittleEndian>(TERM_INDEX_MAGIC_NUMBER)?;
        writer.write_u8(VERSION)?;

        Ok((64 + 8) / 8)
    }

    #[inline]
    fn write_dict_header(&self, writer: &mut std::io::BufWriter<File>) -> Result<u64> {
        writer.write_u64::<LittleEndian>(TERM_DICT_MAGIC_NUMBER)?;
        writer.write_u8(VERSION)?;
        writer.write_u32::<LittleEndian>(self.doc_num)?;

        Ok((64 + 8 + 32) / 8)
    }

    #[inline]
    fn write_dict(
        &self,
        writer: &mut std::io::BufWriter<File>,
        data: &BuildingTermData,
    ) -> Result<u64> {
        let mut len = 0u64;

        let mut builder = PostingListBuilder::new(writer, data.get_posting_map());
        len += builder.finish()?;

        Ok(len)
    }
}
