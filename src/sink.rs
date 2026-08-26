//! Output side of the writer: the image is emitted, region by region in
//! order, as items of a `futures_sink::Sink<Vec<u8>, Error = io::Error>`.

#[cfg(feature = "tokio")]
mod tokio_impls {
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures_sink::Sink;
    use tokio::io::AsyncWrite;

    /// `Sink<Vec<u8>>` adapter over any [`tokio::io::AsyncWrite`].
    ///
    /// `start_send` only stages the item (it has no waker and cannot do
    /// I/O); `poll_flush`/`poll_close` write it out, looping until the
    /// whole item has landed, partial writes included. After an error the
    /// sink stays poisoned, per the `Sink` contract.
    pub struct AsyncWriteSink<W> {
        inner: W,
        pending: Option<Vec<u8>>,
        written: usize,
        err: Option<(io::ErrorKind, String)>,
    }

    impl<W> AsyncWriteSink<W> {
        pub fn new(inner: W) -> Self {
            Self {
                inner,
                pending: None,
                written: 0,
                err: None,
            }
        }
    }

    impl<W: AsyncWrite + Unpin> AsyncWriteSink<W> {
        fn poison(&mut self, e: &io::Error) -> io::Error {
            self.err = Some((e.kind(), e.to_string()));
            io::Error::new(e.kind(), e.to_string())
        }

        fn poisoned(&self) -> Option<io::Error> {
            self.err
                .as_ref()
                .map(|(k, m)| io::Error::new(*k, m.clone()))
        }
    }

    impl<W: AsyncWrite + Unpin> Sink<Vec<u8>> for AsyncWriteSink<W> {
        type Error = io::Error;

        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
            if let Some(e) = self.poisoned() {
                return Poll::Ready(Err(e));
            }
            if self.pending.is_some() {
                return Sink::poll_flush(self, cx);
            }
            Poll::Ready(Ok(()))
        }

        fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), io::Error> {
            let this = &mut *self;
            if let Some(e) = this.poisoned() {
                return Err(e);
            }
            this.pending = Some(item);
            this.written = 0;
            Ok(())
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            let this = &mut *self;
            if let Some(e) = this.poisoned() {
                return Poll::Ready(Err(e));
            }
            loop {
                let Some(item) = this.pending.as_ref() else {
                    return Poll::Ready(Ok(()));
                };
                if this.written >= item.len() {
                    this.pending = None;
                    this.written = 0;
                    return Poll::Ready(Ok(()));
                }
                match Pin::new(&mut this.inner).poll_write(cx, &item[this.written..]) {
                    Poll::Ready(Ok(0)) => {
                        let e = io::Error::new(io::ErrorKind::WriteZero, "write wrote zero bytes");
                        return Poll::Ready(Err(this.poison(&e)));
                    }
                    Poll::Ready(Ok(n)) => {
                        this.written += n;
                        if this.written == item.len() {
                            this.pending = None;
                            this.written = 0;
                            return Poll::Ready(Ok(()));
                        }
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(this.poison(&e))),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        fn poll_close(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            match Sink::poll_flush(self.as_mut(), cx) {
                Poll::Ready(Ok(())) => Pin::new(&mut self.get_mut().inner).poll_shutdown(cx),
                other => other,
            }
        }
    }
}

#[cfg(feature = "tokio")]
pub use tokio_impls::AsyncWriteSink;
