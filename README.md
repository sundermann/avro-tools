# avro-tools

A Rust command-line tool for Apache Avro object container files, using
[`apache-avro`](https://docs.rs/apache-avro/0.22.0/apache_avro/).
It implements the `tojson` and `fromjson` option set from
[Java avro-tools 1.12.1](https://github.com/apache/avro/tree/release-1.12.1/lang/java/tools/src/main/java/org/apache/avro/tool).

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

## Avro JSON encoding

This uses Avro's schema-directed JSON representation:

- A non-null union value has one type tag, for example `{"string":"hello"}`.
  Named branches use their fully qualified name, such as `{"example.Event":{...}}`.
  A null branch is simply `null`.
- Bytes and fixed values are JSON strings whose code points U+0000–U+00FF map
  directly to bytes. They are not base64 or integer arrays.
- Enums are symbol strings. Logical types use their underlying Avro encoding:
  dates/timestamps are integers, decimals are byte strings, and UUIDs are strings
  or fixed byte strings according to their underlying type.
- Every record field must be present in input, even if its schema defines a
  default. Defaults are used for reader-schema evolution.

Output whitespace, map key order, block sizes, and random container sync markers
may differ from Java's output. The data format is interoperable. Errors go to
stderr and produce a nonzero exit status.

## Tests

```sh
cargo test --locked
cargo fmt --all -- --check
```

Optional interoperability tests use the official Java avro-tools jar:

```sh
AVRO_TOOLS_JAR=/path/to/avro-tools-1.12.1.jar cargo test --test java_compat -- --ignored
```

They verify conversion in both directions for every codec, Avro JSON types,
logical types, compression levels, `--head`, pretty printing, and schema evolution.
