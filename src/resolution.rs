//! Reader-schema resolution retaining writer names and union branch identity.
//! Value::resolve alone has no writer schema, so it cannot disambiguate named
//! union branches or apply field aliases in the way Java's resolving decoder does.
use std::collections::HashMap;

use anyhow::{Context, Result, bail, ensure};
use apache_avro::{
    Schema, reader::datum::GenericDatumReader, schema::ResolvedSchema, types::Value,
    writer::datum::GenericDatumWriter,
};

use crate::json::{AvroJson, physical_schema};

pub struct Resolver<'a> {
    writer: &'a Schema,
    reader: &'a Schema,
    writer_names: ResolvedSchema<'a>,
    reader_names: ResolvedSchema<'a>,
    reader_json: AvroJson<'a>,
}

impl<'a> Resolver<'a> {
    pub fn new(writer: &'a Schema, reader: &'a Schema) -> Result<Self> {
        Ok(Self {
            writer,
            reader,
            writer_names: ResolvedSchema::new(writer)?,
            reader_names: ResolvedSchema::new(reader)?,
            reader_json: AvroJson::new(reader)?,
        })
    }

    pub fn resolve(&self, value: Value) -> Result<Value> {
        self.at(self.writer, self.reader, value, None, None)
    }

    fn at(
        &self,
        writer: &Schema,
        reader: &Schema,
        value: Value,
        wns: Option<&str>,
        rns: Option<&str>,
    ) -> Result<Value> {
        let writer = dereference(&self.writer_names, writer, wns)?;
        let reader = dereference(&self.reader_names, reader, rns)?;
        if let Schema::Union(union) = writer {
            let Value::Union(index, value) = value else {
                bail!("expected writer union value");
            };
            let branch = union
                .variants()
                .get(index as usize)
                .context("invalid writer union index")?;
            return self.at(branch, reader, *value, wns, rns);
        }
        if let Schema::Union(union) = reader {
            for (index, branch) in union.variants().iter().enumerate() {
                if self.compatible(writer, branch, wns, rns)? {
                    return Ok(Value::Union(
                        index as u32,
                        Box::new(self.at(writer, branch, value, wns, rns)?),
                    ));
                }
            }
            bail!("no reader union branch matches writer schema {writer:?}");
        }
        if let Some(physical) = physical_schema(writer) {
            return self.at(
                &physical,
                reader,
                transcode(writer, &physical, value)?,
                wns,
                rns,
            );
        }
        if let Some(physical) = physical_schema(reader) {
            let value = self.at(writer, &physical, value, wns, rns)?;
            return transcode(&physical, reader, value);
        }
        ensure!(
            self.compatible(writer, reader, wns, rns)?,
            "incompatible writer and reader schemas: {writer:?} -> {reader:?}"
        );
        Ok(match (writer, reader, value) {
            (Schema::Record(w), Schema::Record(r), Value::Record(values)) => {
                let mut values: HashMap<_, _> = values.into_iter().collect();
                let wns = w.name.namespace().or(wns);
                let rns = r.name.namespace().or(rns);
                Value::Record(
                    r.fields
                        .iter()
                        .map(|field| {
                            let source =
                                w.fields.iter().find(|f| f.name == field.name).or_else(|| {
                                    w.fields.iter().find(|f| field.aliases.contains(&f.name))
                                });
                            let value = if let Some(source) = source {
                                let value = values
                                    .remove(&source.name)
                                    .context("missing writer field")?;
                                self.at(&source.schema, &field.schema, value, wns, rns)
                                    .with_context(|| format!("resolving field {:?}", field.name))?
                            } else {
                                let default = field.default.as_ref().with_context(|| {
                                    format!(
                                        "reader field {:?} has no writer field or default",
                                        field.name
                                    )
                                })?;
                                self.reader_json
                                    .decode_default(&field.schema, default, rns)?
                            };
                            Ok((field.name.clone(), value))
                        })
                        .collect::<Result<_>>()?,
                )
            }
            (Schema::Enum(_), Schema::Enum(r), Value::Enum(_, symbol)) => {
                let symbol = if r.symbols.contains(&symbol) {
                    symbol
                } else {
                    r.default.clone().with_context(|| {
                        format!("reader enum has no symbol {symbol:?} or default")
                    })?
                };
                Value::Enum(
                    r.symbols
                        .iter()
                        .position(|s| s == &symbol)
                        .context("invalid reader enum default")? as u32,
                    symbol,
                )
            }
            (Schema::Array(w), Schema::Array(r), Value::Array(items)) => Value::Array(
                items
                    .into_iter()
                    .map(|v| self.at(&w.items, &r.items, v, wns, rns))
                    .collect::<Result<_>>()?,
            ),
            (Schema::Map(w), Schema::Map(r), Value::Map(items)) => Value::Map(
                items
                    .into_iter()
                    .map(|(key, value)| Ok((key, self.at(&w.types, &r.types, value, wns, rns)?)))
                    .collect::<Result<_>>()?,
            ),
            (Schema::Int, Schema::Long, Value::Int(n)) => Value::Long(i64::from(n)),
            (Schema::Int, Schema::Float, Value::Int(n)) => Value::Float(n as f32),
            (Schema::Int, Schema::Double, Value::Int(n)) => Value::Double(f64::from(n)),
            (Schema::Long, Schema::Float, Value::Long(n)) => Value::Float(n as f32),
            (Schema::Long, Schema::Double, Value::Long(n)) => Value::Double(n as f64),
            (Schema::Float, Schema::Double, Value::Float(n)) => Value::Double(f64::from(n)),
            (Schema::String, Schema::Bytes, Value::String(s)) => Value::Bytes(s.into_bytes()),
            (Schema::Bytes, Schema::String, Value::Bytes(b)) => {
                Value::String(String::from_utf8(b).context("bytes are not valid UTF-8")?)
            }
            (_, _, value) => value,
        })
    }

    fn compatible(
        &self,
        writer: &Schema,
        reader: &Schema,
        wns: Option<&str>,
        rns: Option<&str>,
    ) -> Result<bool> {
        let writer = dereference(&self.writer_names, writer, wns)?;
        let reader = dereference(&self.reader_names, reader, rns)?;
        if let Some(physical) = physical_schema(writer) {
            return self.compatible(&physical, reader, wns, rns);
        }
        if let Some(physical) = physical_schema(reader) {
            return self.compatible(writer, &physical, wns, rns);
        }
        let named_match = || {
            let wname = writer.name().unwrap().fullname(wns);
            let rname = reader.name().unwrap();
            wname == rname.fullname(rns)
                || reader.aliases().is_some_and(|aliases| {
                    aliases
                        .iter()
                        .any(|a| a.fullname(rname.namespace().or(rns)) == wname)
                })
        };
        Ok(match (writer, reader) {
            (Schema::Record(_), Schema::Record(_)) | (Schema::Enum(_), Schema::Enum(_)) => {
                named_match()
            }
            (Schema::Fixed(w), Schema::Fixed(r)) => w.size == r.size && named_match(),
            (Schema::Null, Schema::Null)
            | (Schema::Boolean, Schema::Boolean)
            | (Schema::Int, Schema::Int | Schema::Long | Schema::Float | Schema::Double)
            | (Schema::Long, Schema::Long | Schema::Float | Schema::Double)
            | (Schema::Float, Schema::Float | Schema::Double)
            | (Schema::Double, Schema::Double)
            | (Schema::String | Schema::Bytes, Schema::String | Schema::Bytes)
            | (Schema::Array(_), Schema::Array(_))
            | (Schema::Map(_), Schema::Map(_)) => true,
            _ => false,
        })
    }
}

fn dereference<'s>(
    names: &'s ResolvedSchema<'_>,
    schema: &'s Schema,
    namespace: Option<&str>,
) -> Result<&'s Schema> {
    if let Schema::Ref { name } = schema {
        let name = name.fully_qualified_name(namespace);
        names
            .get_names()
            .get(name.as_ref())
            .copied()
            .context("unresolved schema reference")
    } else {
        Ok(schema)
    }
}

fn transcode(from: &Schema, to: &Schema, value: Value) -> Result<Value> {
    let bytes = GenericDatumWriter::builder(from)
        .build()?
        .write_value_to_vec(value)?;
    Ok(GenericDatumReader::builder(to)
        .build()?
        .read_value(&mut bytes.as_slice())?)
}
