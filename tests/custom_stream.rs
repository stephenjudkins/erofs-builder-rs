//! Callers can plug in any `futures_core::Stream<Item = io::Result<Vec<u8>>>`
//! as file content. This test hand-rolls one over a `Vec<u8>` — no tokio, no
//! runtime, just a minimal block-on with a noop waker — and checks the image
//! matches what `add_bytes` produces for the same data.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use erofs_rs::layout::{EROFS_SUPER_MAGIC_V1, EROFS_SUPER_OFFSET};
use erofs_rs::{Builder, CreateOptions, InodeMeta, Sink, Stream};

/// A hand-rolled `Stream` serving a `Vec<u8>` in fixed-size chunks.
struct VecStream {
    data: Vec<u8>,
    pos: usize,
}

impl VecStream {
    fn new(data: Vec<u8>) -> Self {
        Self { data, pos: 0 }
    }
}

impl Stream for VecStream {
    type Item = io::Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.pos >= this.data.len() {
            return Poll::Ready(None);
        }
        let end = (this.pos + 4096).min(this.data.len());
        let chunk = this.data[this.pos..end].to_vec();
        this.pos = end;
        Poll::Ready(Some(Ok(chunk)))
    }
}

/// A hand-rolled `Sink` collecting the image into a `Vec<u8>`.
struct VecSink(Vec<u8>);

impl Sink<Vec<u8>> for VecSink {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
        self.0.extend(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

fn build(data: Vec<u8>, streamed: bool) -> Vec<u8> {
    block_on(async {
        let mut b = Builder::new(CreateOptions::default()).unwrap();
        if streamed {
            b.add_file(
                "/file.bin",
                InodeMeta::reg(0o644),
                Box::pin(VecStream::new(data)),
            )
            .await
            .unwrap();
        } else {
            b.add_bytes("/file.bin", InodeMeta::reg(0o644), data)
                .await
                .unwrap();
        }
        let mut sink = VecSink(Vec::new());
        b.finish(Pin::new(&mut sink)).await.unwrap();
        sink.0
    })
}

#[test]
fn custom_vec_stream_matches_add_bytes() {
    let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let streamed = build(data.clone(), true);
    let inline = build(data.clone(), false);

    assert_eq!(streamed, inline, "streamed and inline images must match");

    assert!(streamed.len() > EROFS_SUPER_OFFSET + 4);
    let magic = EROFS_SUPER_MAGIC_V1.to_le_bytes();
    assert_eq!(
        &streamed[EROFS_SUPER_OFFSET..EROFS_SUPER_OFFSET + 4],
        &magic
    );
    assert_eq!(streamed.len() % 4096, 0);
    assert!(
        streamed.windows(4096).any(|w| w == &data[..4096]),
        "file data must appear verbatim in the image"
    );
}
