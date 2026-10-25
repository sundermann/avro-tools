//! A streaming object-container envelope around Apache Avro's datum writer.
//!
//! apache-avro 0.22's DeflateSettings only exposes levels 0, 1, 6 and 9,
//! and its ZstandardSettings only accepts unsigned levels. Keeping this small
//! envelope here lets --level retain the Java tool's full compression range.
use std::{collections::HashMap, io::Write};

use anyhow::Result;
use apache_avro::{Schema, types::Value, writer::datum::GenericDatumWriter};

use crate::codec::Compression;

pub struct ContainerWriter<'a, W: Write> {
    output: W,
    datum: GenericDatumWriter<'a>,
    compression: Compression,
    marker: [u8; 16],
    buffer: Vec<u8>,
    count: i64,
}

impl<'a, W: Write> ContainerWriter<'a, W> {
    pub fn new(schema: &'a Schema, mut output: W, compression: Compression) -> Result<Self> {
        let mut marker = [0; 16];
        getrandom::fill(&mut marker)?;
        let metadata = Value::Map(HashMap::from([
            (
                "avro.schema".into(),
                Value::Bytes(serde_json::to_vec(schema)?),
            ),
            (
                "avro.codec".into(),
                Value::Bytes(compression.name().as_bytes().to_vec()),
            ),
        ]));
        let metadata_schema = Schema::parse_str(r#"{"type":"map","values":"bytes"}"#)?;
        output.write_all(b"Obj\x01")?;
        GenericDatumWriter::builder(&metadata_schema)
            .build()?
            .write_value(&mut output, metadata)?;
        output.write_all(&marker)?;
        Ok(Self {
            output,
            datum: GenericDatumWriter::builder(schema).build()?,
            compression,
            marker,
            buffer: Vec::with_capacity(64 * 1024),
            count: 0,
        })
    }

    pub fn append(&mut self, value: &Value) -> Result<()> {
        self.datum.write_value_ref(&mut self.buffer, value)?;
        self.count += 1;
        if self.buffer.len() >= 64 * 1024 || self.count >= 10_000 {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.count == 0 {
            return Ok(());
        }
        self.compression.compress(&mut self.buffer)?;
        let long = GenericDatumWriter::builder(&Schema::Long).build()?;
        long.write_value(&mut self.output, Value::Long(self.count))?;
        long.write_value(
            &mut self.output,
            Value::Long(i64::try_from(self.buffer.len())?),
        )?;
        self.output.write_all(&self.buffer)?;
        self.output.write_all(&self.marker)?;
        self.buffer.clear();
        self.count = 0;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.flush_block()?;
        self.output.flush()?;
        Ok(())
    }
}
