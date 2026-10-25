//! Plain JSON input, with Apache Avro handling scalar conversions and validation.
use anyhow::{Context, Result, bail, ensure};
use apache_avro::{
    Schema,
    reader::datum::GenericDatumReader,
    schema::{InnerDecimalSchema, ResolvedSchema, UuidSchema},
    types::Value,
    writer::datum::GenericDatumWriter,
};
use serde_json::Value as Json;

pub struct JsonDecoder<'a> {
    schema: &'a Schema,
    resolved: ResolvedSchema<'a>,
}

impl<'a> JsonDecoder<'a> {
    pub fn new(schema: &'a Schema) -> Result<Self> {
        Ok(Self {
            schema,
            resolved: ResolvedSchema::new(schema)?,
        })
    }

    pub fn decode(&self, json: &Json) -> Result<Value> {
        self.decode_at(self.schema, json, None, false)
    }

    pub(crate) fn decode_default(
        &self,
        schema: &Schema,
        json: &Json,
        namespace: Option<&str>,
    ) -> Result<Value> {
        self.decode_at(schema, json, namespace, true)
    }

    fn decode_at(
        &self,
        schema: &Schema,
        json: &Json,
        namespace: Option<&str>,
        default: bool,
    ) -> Result<Value> {
        if let Schema::Ref { name } = schema {
            let name = name.fully_qualified_name(namespace);
            let schema = self
                .resolved
                .get_names()
                .get(name.as_ref())
                .context("unresolved schema reference")?;
            return self.decode_at(schema, json, name.namespace(), default);
        }
        // The crate's JSON conversion represents every UUID as a string,
        // including UUIDs stored in a fixed field.
        if matches!(schema, Schema::Uuid(_))
            && let Some(uuid) = json.as_str()
        {
            return Ok(Value::Uuid(uuid.parse()?));
        }
        if let Some(physical) = physical_schema(schema) {
            let value = self.decode_at(&physical, json, namespace, default)?;
            let bytes = GenericDatumWriter::builder(&physical)
                .build()?
                .write_value_to_vec(value)?;
            return Ok(GenericDatumReader::builder(schema)
                .build()?
                .read_value(&mut bytes.as_slice())?);
        }
        let value = match schema {
            Schema::Record(record) => {
                let object = json.as_object().context("expected record object")?;
                let namespace = record.name.namespace().or(namespace);
                Value::Record(
                    record
                        .fields
                        .iter()
                        .map(|field| {
                            let (json, default) = match object.get(&field.name) {
                                Some(json) => (json, default),
                                None => (
                                    field.default.as_ref().with_context(|| {
                                        format!("missing record field {:?}", field.name)
                                    })?,
                                    true,
                                ),
                            };
                            Ok((
                                field.name.clone(),
                                self.decode_at(&field.schema, json, namespace, default)
                                    .with_context(|| format!("field {:?}", field.name))?,
                            ))
                        })
                        .collect::<Result<_>>()?,
                )
            }
            Schema::Union(union) => {
                // Plain JSON has no union tag: select the first branch that fits.
                for (index, branch) in union.variants().iter().enumerate() {
                    if let Ok(value) = self.decode_at(branch, json, namespace, default) {
                        return Ok(Value::Union(index as u32, Box::new(value)));
                    }
                }
                bail!("JSON value does not match any union branch");
            }
            Schema::Array(array) => Value::Array(
                json.as_array()
                    .context("expected array")?
                    .iter()
                    .map(|json| self.decode_at(&array.items, json, namespace, default))
                    .collect::<Result<_>>()?,
            ),
            Schema::Map(map) => Value::Map(
                json.as_object()
                    .context("expected map object")?
                    .iter()
                    .map(|(key, json)| {
                        Ok((
                            key.clone(),
                            self.decode_at(&map.types, json, namespace, default)?,
                        ))
                    })
                    .collect::<Result<_>>()?,
            ),
            Schema::Bytes | Schema::Fixed(_) => {
                // Schema defaults retain Avro's byte-string encoding. User JSON
                // uses serde_json's ordinary representation of Vec<u8>.
                let bytes = if default && let Some(string) = json.as_str() {
                    string
                        .chars()
                        .map(|c| u8::try_from(u32::from(c)).context("byte default exceeds U+00FF"))
                        .collect::<Result<Vec<_>>>()?
                } else {
                    serde_json::from_value::<Vec<u8>>(json.clone())
                        .context("expected an array of bytes (0..=255)")?
                };
                if let Schema::Fixed(fixed) = schema {
                    ensure!(
                        bytes.len() == fixed.size,
                        "fixed field requires {} bytes, got {}",
                        fixed.size,
                        bytes.len()
                    );
                    Value::Fixed(fixed.size, bytes)
                } else {
                    Value::Bytes(bytes)
                }
            }
            _ => Value::try_from(json.clone())?
                .resolve_with_names(schema, self.resolved.get_names())?,
        };
        match &value {
            Value::Float(number) => ensure!(number.is_finite(), "expected a finite JSON number"),
            Value::Double(number) => ensure!(number.is_finite(), "expected a finite JSON number"),
            _ => {}
        }
        Ok(value)
    }
}

pub(crate) fn physical_schema(schema: &Schema) -> Option<Schema> {
    Some(match schema {
        Schema::Date | Schema::TimeMillis => Schema::Int,
        Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => Schema::Long,
        Schema::Decimal(decimal) => match &decimal.inner {
            InnerDecimalSchema::Bytes => Schema::Bytes,
            InnerDecimalSchema::Fixed(fixed) => Schema::Fixed(fixed.clone()),
        },
        Schema::BigDecimal => Schema::Bytes,
        Schema::Uuid(uuid) => match uuid {
            UuidSchema::String => Schema::String,
            UuidSchema::Bytes => Schema::Bytes,
            UuidSchema::Fixed(fixed) => Schema::Fixed(fixed.clone()),
        },
        Schema::Duration(fixed) => Schema::Fixed(fixed.clone()),
        _ => return None,
    })
}
