use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Convert between Avro object container files and plain JSON"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Dump an Avro data file as JSON, one datum per line or pretty printed
    Tojson(ToJson),
    /// Read JSON values and write an Avro data file to stdout
    Fromjson(FromJson),
}

#[derive(Debug, Args)]
pub struct ToJson {
    /// Turn on pretty printing
    #[arg(long)]
    pub pretty: bool,
    /// Convert the first X records (10 when X is omitted)
    #[arg(long, num_args = 0..=1, default_missing_value = "10", allow_negative_numbers = true, value_name = "X")]
    pub head: Option<String>,
    /// Reader schema file (takes precedence over --reader-schema)
    #[arg(long, num_args = 0..=1, value_name = "FILE")]
    pub reader_schema_file: Option<PathBuf>,
    /// Inline reader schema
    #[arg(long, num_args = 0..=1, value_name = "SCHEMA")]
    pub reader_schema: Option<String>,
    /// Input Avro file, or - for stdin
    pub input: Option<PathBuf>,
}

impl ToJson {
    pub fn input_and_limit(&self) -> Result<(PathBuf, u64)> {
        let mut input = self.input.clone();
        let limit = match self.head.as_deref() {
            None => i64::MAX as u64,
            Some(value) => match value.parse::<i64>() {
                Ok(n) if n >= 0 => n as u64,
                Ok(_) => bail!("--head count must not be negative"),
                Err(_) => {
                    // jopt-simple consumes an optional argument even when it is the filename.
                    // Apache's tool puts a nonnumeric --head argument back among the inputs.
                    if input.is_some() {
                        bail!("expected exactly one input file");
                    }
                    input = Some(value.into());
                    10
                }
            },
        };
        Ok((
            input.context("expected an input file (use '-' for stdin)")?,
            limit,
        ))
    }
}

#[derive(Debug, Args)]
pub struct FromJson {
    /// Compression codec: null, deflate, snappy, bzip2, xz, zstandard
    #[arg(long, default_value = "null", value_name = "CODEC")]
    pub codec: String,
    /// Compression level for deflate, xz, or zstandard
    #[arg(long, default_value_t = -1, allow_negative_numbers = true)]
    pub level: i32,
    /// Schema file (takes precedence over --schema)
    #[arg(long, num_args = 0..=1, value_name = "FILE")]
    pub schema_file: Option<PathBuf>,
    /// Inline schema
    #[arg(long, num_args = 0..=1, value_name = "SCHEMA")]
    pub schema: Option<String>,
    /// Input JSON file, or - for stdin
    pub input: PathBuf,
}
