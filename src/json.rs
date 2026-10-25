//! Schema-directed Avro JSON. Serde's representation of `apache_avro::Value`
//! is different from the JSON encoding specified by Avro (notably unions/bytes).
use anyhow::{Context, Result, bail, ensure};
use apache_avro::{
    Schema,
    reader::datum::GenericDatumReader,
    schema::{InnerDecimalSchema, ResolvedSchema, UuidSchema},
    types::Value,
    writer::datum::GenericDatumWriter,
};
use serde_json::{Map, Value as Json};

pub struct AvroJson<'a> {
    schema: &'a Schema,
    resolved: ResolvedSchema<'a>,
}

impl<'a> AvroJson<'a> {
    pub fn new(schema: &'a Schema) -> Result<Self> {
        Ok(Self {
            schema,
            resolved: ResolvedSchema::try_from(schema)?,
        })
    }

    pub fn decode(&self, json: &Json) -> Result<Value> {
        self.decode_at(self.schema, json, None)
    }

    pub fn encode(&self, value: &Value) -> Result<Json> {
        self.encode_at(self.schema, value, None)
    }

    // Schema defaults are JSON values without union tags. Nested record defaults
    // may themselves omit fields that have defaults.
    pub(crate) fn decode_default(
        &self,
        schema: &Schema,
        json: &Json,
        namespace: Option<&str>,
    ) -> Result<Value> {
        let schema = self.dereference(schema, namespace)?;
        Ok(match schema {
            Schema::Union(union) => {
                let mut selected = None;
                for (index, branch) in union.variants().iter().enumerate() {
                    if let Ok(value) = self.decode_default(branch, json, namespace) {
                        selected = Some(Value::Union(index as u32, Box::new(value)));
                        break;
                    }
                }
                selected.context("default does not match any union branch")?
            }
            Schema::Record(record) => {
                let object = json.as_object().context("expected record default object")?;
                let namespace = record.name.namespace().or(namespace);
                Value::Record(
                    record
                        .fields
                        .iter()
                        .map(|field| {
                            let json = object
                                .get(&field.name)
                                .or(field.default.as_ref())
                                .with_context(|| {
                                    format!("no default for field {:?}", field.name)
                                })?;
                            Ok((
                                field.name.clone(),
                                self.decode_default(&field.schema, json, namespace)?,
                            ))
                        })
                        .collect::<Result<_>>()?,
                )
            }
            Schema::Array(array) => Value::Array(
                json.as_array()
                    .context("expected array default")?
                    .iter()
                    .map(|json| self.decode_default(&array.items, json, namespace))
                    .collect::<Result<_>>()?,
            ),
            Schema::Map(map) => Value::Map(
                json.as_object()
                    .context("expected map default")?
                    .iter()
                    .map(|(key, json)| {
                        Ok((
                            key.clone(),
                            self.decode_default(&map.types, json, namespace)?,
                        ))
                    })
                    .collect::<Result<_>>()?,
            ),
            _ => self.decode_at(schema, json, namespace)?,
        })
    }

    fn dereference<'s>(
        &'s self,
        schema: &'s Schema,
        namespace: Option<&str>,
    ) -> Result<&'s Schema> {
        if let Schema::Ref { name } = schema {
            let name = name.fully_qualified_name(namespace);
            self.resolved
                .get_names()
                .get(name.as_ref())
                .copied()
                .with_context(|| format!("unresolved schema reference {name:?}"))
        } else {
            Ok(schema)
        }
    }

    pub(crate) fn decode_at(
        &self,
        schema: &Schema,
        json: &Json,
        namespace: Option<&str>,
    ) -> Result<Value> {
        let schema = self.dereference(schema, namespace)?;
        // Logical types use their underlying Avro type in JSON. Let the crate
        // perform the binary conversion to its logical Value variants.
        if let Some(physical) = physical_schema(schema) {
            let value = self.decode_at(&physical, json, namespace)?;
            let bytes = GenericDatumWriter::builder(&physical)
                .build()?
                .write_value_to_vec(value)?;
            return Ok(GenericDatumReader::builder(schema)
                .build()?
                .read_value(&mut bytes.as_slice())?);
        }
        Ok(match schema {
            Schema::Null => {
                ensure!(json.is_null(), "expected null");
                Value::Null
            }
            Schema::Boolean => Value::Boolean(json.as_bool().context("expected boolean")?),
            Schema::Int => {
                let n = json.as_i64().context("expected a 32-bit integer")?;
                Value::Int(i32::try_from(n).context("integer out of range for Avro int")?)
            }
            Schema::Long => Value::Long(json.as_i64().context("expected a 64-bit integer")?),
            Schema::Float => {
                let n = float(json)?;
                let f = n as f32;
                ensure!(
                    !n.is_finite() || f.is_finite(),
                    "number out of range for Avro float"
                );
                Value::Float(f)
            }
            Schema::Double => Value::Double(float(json)?),
            Schema::String => Value::String(json.as_str().context("expected string")?.to_owned()),
            Schema::Bytes => Value::Bytes(bytes(json)?),
            Schema::Fixed(fixed) => {
                let bytes = bytes(json)?;
                ensure!(
                    bytes.len() == fixed.size,
                    "fixed {} requires {} bytes, got {}",
                    fixed.name.fullname(namespace),
                    fixed.size,
                    bytes.len()
                );
                Value::Fixed(fixed.size, bytes)
            }
            Schema::Enum(enumeration) => {
                let symbol = json.as_str().context("expected enum symbol string")?;
                let index = enumeration
                    .symbols
                    .iter()
                    .position(|s| s == symbol)
                    .with_context(|| {
                        format!(
                            "unknown symbol {symbol:?} for enum {}",
                            enumeration.name.fullname(namespace)
                        )
                    })?;
                Value::Enum(index as u32, symbol.to_owned())
            }
            Schema::Array(array) => Value::Array(
                json.as_array()
                    .context("expected array")?
                    .iter()
                    .enumerate()
                    .map(|(i, item)| {
                        self.decode_at(&array.items, item, namespace)
                            .with_context(|| format!("array item {i}"))
                    })
                    .collect::<Result<_>>()?,
            ),
            Schema::Map(map) => Value::Map(
                json.as_object()
                    .context("expected map object")?
                    .iter()
                    .map(|(key, value)| {
                        Ok((
                            key.clone(),
                            self.decode_at(&map.types, value, namespace)
                                .with_context(|| format!("map key {key:?}"))?,
                        ))
                    })
                    .collect::<Result<_>>()?,
            ),
            Schema::Record(record) => {
                let object = json.as_object().context("expected record object")?;
                for key in object.keys() {
                    ensure!(
                        record.fields.iter().any(|f| &f.name == key),
                        "unknown record field {key:?}"
                    );
                }
                let namespace = record.name.namespace().or(namespace);
                Value::Record(
                    record
                        .fields
                        .iter()
                        .map(|field| {
                            let item = object.get(&field.name).with_context(|| {
                                format!("missing record field {:?}", field.name)
                            })?;
                            Ok((
                                field.name.clone(),
                                self.decode_at(&field.schema, item, namespace)
                                    .with_context(|| format!("field {:?}", field.name))?,
                            ))
                        })
                        .collect::<Result<_>>()?,
                )
            }
            Schema::Union(union) => {
                let variants = union.variants();
                if json.is_null() {
                    let index = variants
                        .iter()
                        .position(|s| matches!(s, Schema::Null))
                        .context("null is not a member of this union")?;
                    Value::Union(index as u32, Box::new(Value::Null))
                } else {
                    let object = json
                        .as_object()
                        .context("expected a union object with one type tag, or null")?;
                    ensure!(
                        object.len() == 1,
                        "union object must contain exactly one type tag"
                    );
                    let (tag, item) = object.iter().next().unwrap();
                    let mut selected = None;
                    for (index, variant) in variants.iter().enumerate() {
                        if !matches!(variant, Schema::Null) && self.tag(variant, namespace)? == *tag
                        {
                            selected = Some((index, variant));
                            break;
                        }
                    }
                    let (index, variant) =
                        selected.with_context(|| format!("unknown union tag {tag:?}"))?;
                    Value::Union(
                        index as u32,
                        Box::new(self.decode_at(variant, item, namespace)?),
                    )
                }
            }
            _ => bail!("unsupported JSON schema: {schema:?}"),
        })
    }

    fn encode_at(&self, schema: &Schema, value: &Value, namespace: Option<&str>) -> Result<Json> {
        let schema = self.dereference(schema, namespace)?;
        if let Some(physical) = physical_schema(schema) {
            let mut bytes = Vec::new();
            GenericDatumWriter::builder(schema)
                .build()?
                .write_value_ref(&mut bytes, value)?;
            let value = GenericDatumReader::builder(&physical)
                .build()?
                .read_value(&mut bytes.as_slice())?;
            return self.encode_at(&physical, &value, namespace);
        }
        Ok(match (schema, value) {
            (Schema::Null, Value::Null) => Json::Null,
            (Schema::Boolean, Value::Boolean(b)) => Json::Bool(*b),
            (Schema::Int, Value::Int(n)) => Json::from(*n),
            (Schema::Long, Value::Long(n)) => Json::from(*n),
            (Schema::Float, Value::Float(n)) => float_json(f64::from(*n)),
            (Schema::Double, Value::Double(n)) => float_json(*n),
            (Schema::String, Value::String(s)) => Json::String(s.clone()),
            (Schema::Bytes, Value::Bytes(b)) | (Schema::Fixed(_), Value::Fixed(_, b)) => {
                bytes_json(b)
            }
            (Schema::Enum(_), Value::Enum(_, symbol)) => Json::String(symbol.clone()),
            (Schema::Array(array), Value::Array(items)) => Json::Array(
                items
                    .iter()
                    .map(|v| self.encode_at(&array.items, v, namespace))
                    .collect::<Result<_>>()?,
            ),
            (Schema::Map(map), Value::Map(values)) => {
                let mut object = Map::new();
                for (key, value) in values {
                    object.insert(key.clone(), self.encode_at(&map.types, value, namespace)?);
                }
                Json::Object(object)
            }
            (Schema::Record(record), Value::Record(fields)) => {
                let namespace = record.name.namespace().or(namespace);
                let mut object = Map::new();
                for field in &record.fields {
                    let value = fields
                        .iter()
                        .find(|(key, _)| key == &field.name)
                        .with_context(|| format!("missing Avro field {:?}", field.name))?;
                    object.insert(
                        field.name.clone(),
                        self.encode_at(&field.schema, &value.1, namespace)?,
                    );
                }
                Json::Object(object)
            }
            (Schema::Union(union), Value::Union(index, value)) => {
                let variant = union
                    .variants()
                    .get(*index as usize)
                    .context("invalid Avro union index")?;
                let encoded = self.encode_at(variant, value, namespace)?;
                if matches!(variant, Schema::Null) {
                    Json::Null
                } else {
                    let mut object = Map::new();
                    object.insert(self.tag(variant, namespace)?, encoded);
                    Json::Object(object)
                }
            }
            _ => bail!("Avro value {value:?} does not match schema {schema:?}"),
        })
    }

    fn tag(&self, schema: &Schema, namespace: Option<&str>) -> Result<String> {
        let schema = self.dereference(schema, namespace)?;
        if let Some(name) = schema.name() {
            return Ok(name.fullname(namespace));
        }
        if let Some(physical) = physical_schema(schema) {
            return self.tag(&physical, namespace);
        }
        Ok(match schema {
            Schema::Null => "null",
            Schema::Boolean => "boolean",
            Schema::Int => "int",
            Schema::Long => "long",
            Schema::Float => "float",
            Schema::Double => "double",
            Schema::Bytes => "bytes",
            Schema::String => "string",
            Schema::Array(_) => "array",
            Schema::Map(_) => "map",
            _ => bail!("invalid union branch {schema:?}"),
        }
        .to_owned())
    }
}

fn bytes(json: &Json) -> Result<Vec<u8>> {
    json.as_str()
        .context("expected a byte string")?
        .chars()
        .map(|c| {
            u8::try_from(u32::from(c))
                .context("Avro byte strings only allow code points U+0000..U+00FF")
        })
        .collect()
}

fn bytes_json(bytes: &[u8]) -> Json {
    Json::String(bytes.iter().map(|&b| char::from(b)).collect())
}

fn float(json: &Json) -> Result<f64> {
    match json {
        Json::Number(n) => n.as_f64().context("expected a floating point number"),
        Json::String(s) if s == "NaN" => Ok(f64::NAN),
        Json::String(s) if s == "Infinity" => Ok(f64::INFINITY),
        Json::String(s) if s == "-Infinity" => Ok(f64::NEG_INFINITY),
        _ => bail!("expected a number, or \"NaN\", \"Infinity\", \"-Infinity\""),
    }
}

fn float_json(n: f64) -> Json {
    if n.is_nan() {
        Json::String("NaN".into())
    } else if n == f64::INFINITY {
        Json::String("Infinity".into())
    } else if n == f64::NEG_INFINITY {
        Json::String("-Infinity".into())
    } else {
        Json::from(n)
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
