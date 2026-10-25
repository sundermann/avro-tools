mod cli;
mod codec;
mod container;
mod json;
mod resolution;

use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
    process::ExitCode,
};

use anyhow::{Context, Result};
use apache_avro::{Reader, Schema};
use clap::Parser;
use cli::{Cli, Command, FromJson, ToJson};
use container::ContainerWriter;
use json::AvroJson;
use resolution::Resolver;

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            if error.chain().any(|cause| {
                cause
                    .downcast_ref::<io::Error>()
                    .is_some_and(|e| e.kind() == io::ErrorKind::BrokenPipe)
            }) {
                return ExitCode::SUCCESS;
            }
            eprintln!("avro-tools: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<()> {
    let stdout = io::stdout();
    let mut output = BufWriter::new(stdout.lock());
    match cli.command {
        Command::Tojson(args) => to_json(args, &mut output)?,
        Command::Fromjson(args) => from_json(args, &mut output)?,
    }
    output.flush().context("flushing stdout")
}

fn open_input(path: &Path) -> Result<Box<dyn Read>> {
    if path == Path::new("-") {
        Ok(Box::new(BufReader::new(io::stdin())))
    } else {
        let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        Ok(Box::new(BufReader::new(file)))
    }
}

fn load_schema(inline: Option<&str>, file: Option<&Path>) -> Result<Option<Schema>> {
    let text = if let Some(file) = file {
        Some(
            std::fs::read_to_string(file)
                .with_context(|| format!("reading schema {}", file.display()))?,
        )
    } else {
        inline.map(str::to_owned)
    };
    text.map(|text| Schema::parse_str(&text).context("parsing Avro schema"))
        .transpose()
}

fn to_json(args: ToJson, output: &mut impl Write) -> Result<()> {
    let (path, limit) = args.input_and_limit()?;
    let schema = load_schema(
        args.reader_schema.as_deref(),
        args.reader_schema_file.as_deref(),
    )?;
    let input = open_input(&path)?;
    let reader = Reader::new(input).context("reading Avro header")?;
    let writer_schema = reader.writer_schema().clone();
    let resolver = schema
        .as_ref()
        .map(|schema| Resolver::new(&writer_schema, schema))
        .transpose()?;
    let output_schema = schema
        .as_ref()
        .unwrap_or_else(|| reader.writer_schema())
        .clone();
    let json = AvroJson::new(&output_schema)?;
    let mut count = 0_u64;
    // take() is deliberately before reading: --head=0 must not decode any records.
    for value in reader.take(usize::try_from(limit).unwrap_or(usize::MAX)) {
        count += 1;
        let value = value.with_context(|| format!("reading Avro datum {count}"))?;
        let value = if let Some(resolver) = &resolver {
            resolver
                .resolve(value)
                .with_context(|| format!("resolving datum {count} to the reader schema"))?
        } else {
            value
        };
        let value = json
            .encode(&value)
            .with_context(|| format!("encoding JSON datum {count}"))?;
        if args.pretty {
            serde_json::to_writer_pretty(&mut *output, &value)?;
        } else {
            serde_json::to_writer(&mut *output, &value)?;
        }
        writeln!(output)?;
    }
    if count == 0 {
        writeln!(output)?;
    }
    Ok(())
}

fn from_json(args: FromJson, output: &mut impl Write) -> Result<()> {
    let schema = load_schema(args.schema.as_deref(), args.schema_file.as_deref())?
        .context("need an input schema file (--schema-file) or inline schema (--schema)")?;
    let codec = codec::compression(&args.codec, args.level)?;
    let json = AvroJson::new(&schema)?;
    let input = open_input(&args.input)?;
    let mut writer = ContainerWriter::new(&schema, output, codec)?;
    // Stream values, not lines: pretty-printed and whitespace-separated JSON are valid too.
    for (index, value) in serde_json::Deserializer::from_reader(input)
        .into_iter::<serde_json::Value>()
        .enumerate()
    {
        let value = value.with_context(|| format!("parsing JSON datum {}", index + 1))?;
        let value = json
            .decode(&value)
            .with_context(|| format!("decoding JSON datum {}", index + 1))?;
        writer
            .append(&value)
            .with_context(|| format!("writing Avro datum {}", index + 1))?;
    }
    writer.finish().context("finishing Avro container")?;
    Ok(())
}
