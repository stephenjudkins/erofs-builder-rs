//! Async EROFS image writer on tokio.
//!
//! File content is supplied via [`tokio::io::AsyncRead`] and the
//! serialized image is written to any [`tokio::io::AsyncWrite`].

pub mod layout;
pub mod writer;

pub use layout::{DataLayout, FileType};
pub use writer::{Builder, CreateOptions, InodeMeta, DEFAULT_BLOCK_SIZE};
