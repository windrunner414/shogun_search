use crate::query::Result;
use std::cmp::Ordering;

#[inline(always)]
pub fn calc_idf(df: u32, total_doc_num: u32) -> f64 {
    1f64 + f64::ln(total_doc_num as f64 / (df + 1) as f64)
}

#[inline(always)]
pub fn calc_tf(freq: u16) -> u8 {
    ((freq as f64).sqrt() * 8f64) as u8
}

#[inline(always)]
pub fn calc_norm(len: usize) -> u8 {
    (1f64 / (len as f64).sqrt() * 255f64) as u8
}

pub trait TermPriorityCalculator {
    fn calc(&self, df: u32, tf_title: u8, tf_content: u8, norm_title: u8, norm_content: u8) -> f64;
}

#[derive(Debug)]
pub struct TfIdfTermPriorityCalculator {
    total_doc_num: u32,
    boost_title: u8,
    boost_content: u8,
}

impl TfIdfTermPriorityCalculator {
    pub fn new(total_doc_num: u32, boost_title: u8, boost_content: u8) -> Self {
        TfIdfTermPriorityCalculator {
            total_doc_num,
            boost_title,
            boost_content,
        }
    }
}

impl TermPriorityCalculator for TfIdfTermPriorityCalculator {
    #[inline(always)]
    fn calc(&self, df: u32, tf_title: u8, tf_content: u8, norm_title: u8, norm_content: u8) -> f64 {
        calc_idf(df, self.total_doc_num)
            * (tf_title as f64 * norm_title as f64 * self.boost_title as f64
                + tf_content as f64 * norm_content as f64 * self.boost_content as f64)
    }
}

// TODO: 计算中会不会溢出
#[inline(always)]
pub unsafe fn calc_cosine_unchecked(a: &[f64], b: &[f64]) -> f64 {
    let (mut product, mut q_sum_a, mut q_sum_b) = (0f64, 0f64, 0f64);

    for i in 0..a.len() {
        let an = a.get_unchecked(i);
        let bn = b.get_unchecked(i);
        product += an * bn;
        q_sum_a += an * an;
        q_sum_b += bn * bn;
    }

    product / (q_sum_a.sqrt() * q_sum_b.sqrt())
}

#[derive(Debug)]
pub struct Score {
    cosine: f64,
}

impl Score {
    pub fn new(a: &[f64], b: &[f64]) -> Self {
        Score {
            cosine: unsafe { calc_cosine_unchecked(a, b) },
        }
    }
}

impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.cosine.eq(&other.cosine)
    }
}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.cosine.partial_cmp(&other.cosine)
    }
}

impl Eq for Score {}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.cosine > other.cosine {
            Ordering::Greater
        } else if self.cosine < other.cosine {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }
}
