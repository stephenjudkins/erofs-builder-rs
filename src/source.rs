//! Sans-IO byte source adapters for file content input.
//!
//! File content is consumed as a
//! `futures_core::Stream<Item = io::Result<Vec<u8>>>`; with the `tokio`
//! feature enabled, adapters are provided for common stream and reader
//! types.

#[cfg(feature = "tokio")]
mod tokio_impls {
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures_core::Stream;
    use tokio::io::AsyncRead;

    /// Adapter normalizing any `Stream<Item = Result<T, E>>`-like stream of
    /// byte chunks onto the `Stream<Item = io::Result<Vec<u8>>>` shape the
    /// writer consumes.
    ///
    /// Any item type convertible via `AsRef<[u8]>` works, which covers
    /// `bytes::Bytes`, `Vec<u8>` and similar containers.
    pub struct StreamSource<S> {
        inner: S,
        done: bool,
    }

    impl<S> StreamSource<S> {
        pub fn new(inner: S) -> Self {
            Self { inner, done: false }
        }
    }

    impl<S, T, E> Stream for StreamSource<S>
    where
        S: Stream<Item = Result<T, E>> + Send + Unpin,
        T: AsRef<[u8]>,
        E: Into<io::Error>,
    {
        type Item = io::Result<Vec<u8>>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.done {
                return Poll::Ready(None);
            }
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(item)) => match item {
                    Ok(v) => {
                        let v = v.as_ref().to_vec();
                        if v.is_empty() {
                            self.poll_next(cx)
                        } else {
                            Poll::Ready(Some(Ok(v)))
                        }
                    }
                    Err(e) => {
                        self.done = true;
                        Poll::Ready(Some(Err(e.into())))
                    }
                },
                Poll::Ready(None) => {
                    self.done = true;
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl<S> From<S> for StreamSource<S> {
        fn from(inner: S) -> Self {
            Self::new(inner)
        }
    }

    /// A [`Stream`] reading sequentially from a [`tokio::io::AsyncRead`].
    pub struct AsyncReadSource<R> {
        inner: R,
        buf: Vec<u8>,
    }

    impl<R: AsyncRead + Send + Unpin> AsyncReadSource<R> {
        pub fn new(inner: R) -> Self {
            Self {
                inner,
                buf: vec![0; 128 * 1024],
            }
        }
    }

    impl<R: AsyncRead + Send + Unpin> Stream for AsyncReadSource<R> {
        type Item = io::Result<Vec<u8>>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            use tokio::io::ReadBuf;
            let this = &mut *self;
            let mut rb = ReadBuf::new(&mut this.buf);
            match Pin::new(&mut this.inner).poll_read(cx, &mut rb) {
                Poll::Ready(Ok(())) if rb.filled().is_empty() => Poll::Ready(None),
                Poll::Ready(Ok(())) => Poll::Ready(Some(Ok(rb.filled().to_vec()))),
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

#[cfg(feature = "tokio")]
pub use tokio_impls::{AsyncReadSource, StreamSource};
