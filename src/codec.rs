use anyhow::{Result, bail};
use apache_avro::{Codec, XzSettings};

pub enum Compression {
    Avro(Codec),
    Deflate(u8),
    Zstandard(i32),
}

impl Compression {
    pub fn name(&self) -> &str {
        match self {
            Self::Avro(codec) => <&str>::from(*codec),
            Self::Deflate(_) => "deflate",
            Self::Zstandard(_) => "zstandard",
        }
    }

    pub fn compress(&self, data: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Avro(codec) => codec.compress(data)?,
            Self::Deflate(level) => *data = miniz_oxide::deflate::compress_to_vec(data, *level),
            Self::Zstandard(level) => *data = zstd::bulk::compress(data, *level)?,
        }
        Ok(())
    }
}

pub fn compression(name: &str, level: i32) -> Result<Compression> {
    Ok(match name {
        "null" => Compression::Avro(Codec::Null),
        "snappy" => Compression::Avro(Codec::Snappy),
        "bzip2" => Compression::Avro(Codec::Bzip2(Default::default())),
        "deflate" => {
            let level = if level == -1 { 6 } else { level };
            if !(0..=9).contains(&level) {
                bail!("deflate compression level must be -1 or 0..=9");
            }
            Compression::Deflate(level as u8)
        }
        "xz" => {
            let level = if level == -1 { 6 } else { level };
            if !(0..=9).contains(&level) {
                bail!("xz compression level must be -1 or 0..=9");
            }
            Compression::Avro(Codec::Xz(XzSettings::new(level as u8)))
        }
        "zstandard" => Compression::Zstandard(level),
        _ => {
            bail!("unknown codec {name:?}; expected null, deflate, snappy, bzip2, xz, or zstandard")
        }
    })
}
