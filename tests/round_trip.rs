//! End-to-end round trip through the public tokio-native API: content in
//! via `AsyncRead` (a `Cursor`), image out via `AsyncWrite` (a `Vec<u8>`).

use std::io::Cursor;

use erofs_rs::layout::{EROFS_SUPER_MAGIC_V1, EROFS_SUPER_OFFSET};
use erofs_rs::{Builder, CreateOptions, InodeMeta};

async fn build(data: &[u8], streamed: bool) -> Vec<u8> {
    let mut b = Builder::new(CreateOptions::default()).unwrap();
    if streamed {
        b.add_file(
            "/file.bin",
            InodeMeta::reg(0o644),
            Cursor::new(data.to_vec()),
        )
        .await
        .unwrap();
    } else {
        b.add_bytes("/file.bin", InodeMeta::reg(0o644), data.to_vec())
            .await
            .unwrap();
    }
    let mut sink = Vec::new();
    b.finish(&mut sink).await.unwrap();
    sink
}

#[tokio::test]
async fn streamed_file_matches_add_bytes() {
    let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let streamed = build(&data, true).await;
    let inline = build(&data, false).await;

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
