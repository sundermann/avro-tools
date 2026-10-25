# avro-tools

A Rust command-line tool for converting Apache Avro object container files to
and from plain JSON, using [`apache-avro`](https://docs.rs/apache-avro/0.22.0/apache_avro/)
and `serde_json`. It keeps the `tojson` and `fromjson` option names from
[Java avro-tools 1.12.1](https://github.com/apache/avro/tree/release-1.12.1/lang/java/tools/src/main/java/org/apache/avro/tool).
Its JSON format uses ordinary values and byte arrays; it does not reproduce
Java's Avro JSON encoding.

## Build

Requires Rust 1.88 or newer and a C compiler for the native compression dependencies.

```sh
cargo build --release --locked
./target/release/avro-tools --help
```

To install the executable in Cargo's bin directory:

```sh
cargo install --path . --locked
```

## Commands

```text
avro-tools tojson [--pretty] [--head[=X]]
    [--reader-schema SCHEMA] [--reader-schema-file FILE] INPUT

avro-tools fromjson [--codec CODEC] [--level LEVEL]
    [--schema SCHEMA] [--schema-file FILE] INPUT
```

Both commands write to stdout. Use `-` as `INPUT` for stdin and shell redirection
to save the result. Input and schema files are local filesystem paths; Hadoop
filesystems and URI paths are outside this tool's scope.

```sh
avro-tools fromjson --schema-file event.avsc events.json > events.avro
avro-tools tojson events.avro > events.json
avro-tools tojson --pretty --head=3 events.avro
avro-tools tojson --head events.avro
avro-tools tojson --reader-schema-file projection.avsc events.avro
avro-tools fromjson --codec deflate --level 7 --schema-file event.avsc - < events.json > events.avro
```

An inline-schema example:

```sh
printf '%s\n' '{"id":1}' '{"id":2}' |
  avro-tools fromjson --schema '{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}' - > events.avro
avro-tools tojson events.avro
```

### tojson

| Option | Behavior |
| --- | --- |
| `--pretty` | Indent each JSON datum. Otherwise, write one datum per line. |
| `--head[=X]` | Read the first X datums. A bare `--head` means 10. Also accepts `--head X`. Zero emits no datums. |
| `--reader-schema SCHEMA` | Resolve records to this inline Avro schema. |
| `--reader-schema-file FILE` | Load the reader schema from a file; takes precedence over the inline schema. |

Without `--head`, all records are read. Without a reader schema, the container's
writer schema determines the JSON output. Reader-schema resolution supports
projection, numeric promotion, named and field aliases, defaults, and union
reordering. Empty output is a newline, as in Java avro-tools.

### fromjson

| Option | Behavior |
| --- | --- |
| `--schema SCHEMA` | Inline Avro schema. A schema is required. |
| `--schema-file FILE` | Load the schema from a file; takes precedence over the inline schema. |
| `--codec CODEC` | `null` (default), `deflate`, `snappy`, `bzip2`, `xz`, or `zstandard`. |
| `--level LEVEL` | Compression level; default `-1`. Applies to deflate, XZ, and Zstandard. Ignored by other codecs. |

Deflate accepts levels 0–9 and maps `-1` to 6. XZ accepts levels 0–9 and also maps
`-1` to 6; this deliberately avoids Java 1.12.1's `Unsupported preset: -1` failure.
Zstandard passes signed levels to its compressor, including negative fast levels.

Input is a stream of JSON values: newline-delimited, whitespace-separated, and
multiline values are accepted. Empty input creates a valid empty Avro container.
Records are processed incrementally; the entire input is not loaded into memory.

## Plain JSON

`tojson` uses the Apache Avro crate's conversion to `serde_json::Value`, then
`serde_json::to_writer` (or `to_writer_pretty`). `fromjson` streams values with
`serde_json::Deserializer` and uses the schema to resolve their Avro types.

- Unions are plain values: `"hello"`, `null`, or `{"id":1}`. There are no type tags.
  When several union branches accept a value, `fromjson` selects the first matching
  branch in schema order. Plain JSON cannot preserve that branch identity.
- Bytes and fixed values are integer arrays, for example `[0,128,255]`.
- Enums are symbol strings. Dates and timestamps are integers. Decimals and
  durations use byte arrays, and UUIDs are strings.
- Missing record fields use schema defaults when available; otherwise they are
  errors. Extra fields in input objects are ignored.
- Numbers must be finite. Avro NaN and infinity values produce an error because
  standard JSON cannot represent them.

For a field whose schema is `["null","string"]`, write `{"name":"Ada"}` instead
of Java's `{"name":{"string":"Ada"}}`. For a bytes field, write
`{"data":[65,66]}` instead of `{"data":"AB"}`.

The Avro container files remain interoperable with other Avro tools. The JSON
format differs from Java's, and an Avro → JSON → Avro round trip can change an
ambiguous union branch. Errors go to stderr and produce a nonzero exit status.

## Tests

```sh
cargo test --locked
cargo fmt --all -- --check
```

Optional interoperability tests use the official Java avro-tools jar:

```sh
AVRO_TOOLS_JAR=/path/to/avro-tools-1.12.1.jar cargo test --test java_compat -- --ignored
```

They verify Avro container interoperability in both directions for every codec,
while accounting for the different JSON formats, plus logical types, compression
levels, `--head`, pretty printing, and schema evolution.
