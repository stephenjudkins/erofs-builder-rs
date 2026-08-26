//! Sans-IO async EROFS image writer.
//!
//! The crate core is I/O-agnostic: file content enters via a [`Stream`] of
//! byte chunks and the serialized image leaves via a [`Sink`] of `Vec<u8>`
//! chunks, in order, from offset 0. With the `tokio` feature enabled,
//! adapters are provided for `tokio::io::AsyncRead` inputs and
//! `tokio::io::AsyncWrite` outputs.

pub mod layout;
pub mod sink;
pub mod source;
pub mod writer;

pub use futures_core::Stream;
pub use futures_sink::Sink;
pub use layout::{DataLayout, FileType};
pub use writer::{Builder, CreateOptions, InodeMeta, DEFAULT_BLOCK_SIZE};

#[cfg(feature = "tokio")]
pub use sink::AsyncWriteSink;
#[cfg(feature = "tokio")]
pub use source::{AsyncReadSource, StreamSource};
