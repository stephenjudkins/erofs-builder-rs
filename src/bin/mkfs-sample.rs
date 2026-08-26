use std::pin::Pin;

use clap::{Parser, ValueEnum};
use erofs_rs::{AsyncReadSource, AsyncWriteSink, Builder, CreateOptions, InodeMeta, StreamSource};

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum, Debug)]
enum Mode {
    /// Deterministic fixed fixture (used by the test script).
    Fixture,
    /// Pack a directory tree into an image.
    Dir,
}

#[derive(Parser, Debug)]
#[command(name = "mkfs-sample", about = "Create an EROFS image with erofs-rs")]
struct Args {
    output: String,
    #[arg(value_enum, default_value_t = Mode::Fixture)]
    mode: Mode,
    #[arg(long)]
    dir: Option<String>,
    #[arg(long, default_value_t = 4096)]
    block_size: usize,
    #[arg(long, default_value_t = 0)]
    build_time: u64,
    #[arg(long)]
    checksum: bool,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let opts = CreateOptions {
        block_size: args.block_size,
        build_time: args.build_time,
        build_time_nsec: 0,
        uuid: *uuid_from_seed(args.build_time),
        volume_name: "erofs-rs".into(),
        checksum: args.checksum,
    };
    let mut builder = Builder::new(opts)?;

    match args.mode {
        Mode::Fixture => build_fixture(&mut builder).await?,
        Mode::Dir => {
            let dir = args.dir.as_deref().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--dir required for dir mode",
                )
            })?;
            pack_dir(&mut builder, dir).await?;
        }
    }

    let file = tokio::fs::File::create(&args.output).await?;
    let sink = AsyncWriteSink::new(file);
    let mut sink = sink;
    builder.finish(Pin::new(&mut sink)).await?;
    Ok(())
}

fn uuid_from_seed(seed: u64) -> &'static [u8; 16] {
    use rand::{RngCore, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    Box::leak(Box::new({
        let mut u = [0u8; 16];
        rng.fill_bytes(&mut u);
        u
    }))
}

async fn build_fixture(builder: &mut Builder) -> std::io::Result<()> {
    const T: u64 = 1_700_000_000;
    let meta_dir = InodeMeta {
        mtime: T,
        ..InodeMeta::dir(0o755)
    };

    builder.mkdir("/etc", meta_dir.clone()).await?;

    let mut motd_meta = InodeMeta {
        mtime: T,
        ..InodeMeta::reg(0o644)
    };
    motd_meta
        .xattrs
        .insert("trusted.origin".to_string(), b"erofs-rs".to_vec());
    builder
        .add_bytes("/etc/motd", motd_meta, b"hello from erofs-rs\n".to_vec())
        .await?;

    // A larger streamed file spanning multiple blocks.
    let big: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let stream = StreamSource::new(tokio_stream::iter(
        big.chunks(7777)
            .map(|c| Ok::<_, std::io::Error>(c.to_vec()))
            .collect::<Vec<_>>(),
    ));
    builder
        .add_file(
            "/big.bin",
            InodeMeta {
                mtime: T,
                ..InodeMeta::reg(0o600)
            },
            Box::pin(stream),
        )
        .await?;

    builder
        .symlink(
            "/link",
            b"etc/motd",
            InodeMeta {
                mtime: T,
                ..InodeMeta::symlink()
            },
        )
        .await?;

    let mut zero_meta = InodeMeta {
        mtime: T,
        ..Default::default()
    };
    zero_meta.mode = 0o020000 | 0o666; // chrdev
    zero_meta.rdev = (0x01 << 8) | 0x05; // 1:5
    builder.mknod("/zero", zero_meta).await?;

    builder.mkdir("/empty", meta_dir).await?;
    builder
        .add_bytes(
            "/empty-file",
            InodeMeta {
                mtime: T,
                ..InodeMeta::reg(0o644)
            },
            Vec::new(),
        )
        .await?;
    Ok(())
}

async fn pack_dir(builder: &mut Builder, root: &str) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;

    fn meta_from(md: &std::fs::Metadata) -> InodeMeta {
        InodeMeta {
            mode: (md.mode() & 0xFFFF) as u16,
            uid: md.uid(),
            gid: md.gid(),
            mtime: md.mtime().max(0) as u64,
            mtime_nsec: md.mtime_nsec().max(0) as u32,
            nlink: Some(md.nlink() as u32),
            rdev: md.rdev() as u32,
            xattrs: Default::default(),
        }
    }

    async fn walk(builder: &mut Builder, fs_path: &str, img_path: &str) -> std::io::Result<()> {
        let mut rd = tokio::fs::read_dir(fs_path).await?;
        let mut entries = Vec::new();
        while let Some(e) = rd.next_entry().await? {
            entries.push(e);
        }
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.len() > 255 {
                continue;
            }
            let child_fs = format!("{}/{}", fs_path, name);
            let child_img = format!("{}/{}", img_path.trim_end_matches('/'), name);
            let md = entry.metadata().await?;
            let ft = md.file_type();
            if ft.is_dir() {
                builder.mkdir(&child_img, meta_from(&md)).await?;
                Box::pin(walk(builder, &child_fs, &child_img)).await?;
            } else if ft.is_symlink() {
                let target = tokio::fs::read_link(&child_fs).await?;
                builder
                    .symlink(
                        &child_img,
                        target.as_os_str().as_encoded_bytes(),
                        meta_from(&md),
                    )
                    .await?;
            } else if ft.is_file() {
                let f = tokio::fs::File::open(&child_fs).await?;
                builder
                    .add_file(
                        &child_img,
                        meta_from(&md),
                        Box::pin(AsyncReadSource::new(f)),
                    )
                    .await?;
            } else {
                builder.mknod(&child_img, meta_from(&md)).await?;
            }
        }
        Ok(())
    }

    let md = tokio::fs::metadata(root).await?;
    builder.mkdir("/", meta_from(&md)).await?;
    walk(builder, root, "/").await
}
