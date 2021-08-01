pub mod document;
pub mod builder;
mod error;

pub use error::Error;
pub use error::Result;
pub use document::Document;
pub use builder::Builder;
pub use builder::Config;

pub(crate) mod term;
pub(crate) mod posting;
pub mod constants;