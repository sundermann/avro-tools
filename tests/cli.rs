use std::{
    io::Write,
    process::{Command, Output, Stdio},
};

use apache_avro::{Reader, Schema, Writer, types::Value};
use serde_json::{Value as Json, json};

fn invoke(args: &[&str], input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_avro-tools"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let input = input.to_vec();
    let feeder = std::thread::spawn(move || {
        let _ = stdin.write_all(&input);
    });
    let output = child.wait_with_output().unwrap();
    feeder.join().unwrap();
    output
}

fn success(args: &[&str], input: &[u8]) -> Vec<u8> {
    let output = invoke(args, input);
    assert!(
        output.status.success(),
        "args: {args:?}\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn from_json(schema: &str, values: &str) -> Vec<u8> {
    success(&["fromjson", "--schema", schema, "-"], values.as_bytes())
}

fn json_values(bytes: &[u8]) -> Vec<Json> {
    serde_json::Deserializer::from_slice(bytes)
        .into_iter::<Json>()
        .map(Result::unwrap)
        .collect()
}

fn roundtrip(schema: &str, values: &str) -> Vec<Json> {
    json_values(&success(&["tojson", "-"], &from_json(schema, values)))
}

#[test]
fn primitives_use_standard_json() {
    for (schema, values) in [
        (r#""null""#, "null\nnull"),
        (r#""boolean""#, "true false"),
        (r#""int""#, "-2147483648 2147483647"),
        (r#""long""#, "-9223372036854775808 9223372036854775807"),
        (r#""string""#, r#""hello" "世界\n😀""#),
        (r#""double""#, "-0.0 1.25"),
        (r#""float""#, "-0.0 1.25"),
    ] {
        assert_eq!(
            roundtrip(schema, values),
            json_values(values.as_bytes()),
            "schema: {schema}"
        );
    }
}

#[test]
fn rejects_nonfinite_avro_numbers_in_plain_json_output() {
    for number in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut writer = Writer::new(&Schema::Double, Vec::new()).unwrap();
        writer.append_value(Value::Double(number)).unwrap();
        let avro = writer.into_inner().unwrap();
        let output = invoke(&["tojson", "-"], &avro);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("encoding JSON"));
    }
}

#[test]
fn record_defaults_and_extra_fields_follow_serde_conventions() {
    let schema = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"name","type":["null","string"],"default":"guest"},{"name":"data","type":"bytes","default":"\u00ff"}]}"#;
    assert_eq!(
        roundtrip(schema, r#"{"id":1,"extra":true}"#),
        vec![json!({"id":1,"name":"guest","data":[255]})]
    );
}

#[test]
fn complex_types_use_plain_values_and_byte_arrays() {
    let schema = r#"{"type":"record","name":"example.Event","fields":[
        {"name":"id","type":"long"},
        {"name":"payload","type":"bytes"},
        {"name":"code","type":{"type":"fixed","name":"Code","size":2}},
        {"name":"status","type":{"type":"enum","name":"Status","symbols":["ON","OFF"]}},
        {"name":"tags","type":{"type":"array","items":"string"}},
        {"name":"counts","type":{"type":"map","values":"int"}},
        {"name":"choice","type":["null","string","Code","Status",{"type":"record","name":"Child","fields":[{"name":"n","type":"int"}]}]}
    ]}"#;
    let mut value = json!({"id":42,"payload":[0,128,255],"code":[65,66],"status":"ON","tags":["x","y"],"counts":{"a":1,"b":2},"choice":null});
    for choice in [
        Json::Null,
        json!("x"),
        json!([67, 68]),
        json!("OFF"),
        json!({"n":7}),
    ] {
        value["choice"] = choice;
        assert_eq!(roundtrip(schema, &value.to_string()), vec![value.clone()]);
    }
}

#[test]
fn recursive_references_and_ambiguous_unions() {
    let schema = r#"{"type":"record","name":"tree.Node","fields":[{"name":"value","type":["int","long","double"]},{"name":"next","type":["null","Node"]}]}"#;
    let input = json!({"value":7,"next":{"value":2.5,"next":null}});
    assert_eq!(roundtrip(schema, &input.to_string()), vec![input]);
    let schema = r#"[{"type":"record","name":"A","fields":[{"name":"n","type":"int"}]},{"type":"record","name":"B","fields":[{"name":"n","type":"int"}]}]"#;
    assert_eq!(roundtrip(schema, r#"{"n":1}"#), vec![json!({"n":1})]);
    // Plain JSON does not carry Avro branch names. The first match wins.
    let avro = from_json(schema, r#"{"n":1}"#);
    let value = Reader::new(avro.as_slice())
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert!(matches!(value, Value::Union(0, _)));
}

#[test]
fn logical_types_use_physical_json_encoding() {
    for (schema, values) in [
        (r#"{"type":"int","logicalType":"date"}"#, "-1 20000"),
        (r#"{"type":"int","logicalType":"time-millis"}"#, "12345"),
        (r#"{"type":"long","logicalType":"time-micros"}"#, "12345"),
        (
            r#"{"type":"long","logicalType":"timestamp-micros"}"#,
            "1720000000000000",
        ),
        (
            r#"{"type":"long","logicalType":"local-timestamp-nanos"}"#,
            "1720000000000000",
        ),
        (
            r#"{"type":"string","logicalType":"uuid"}"#,
            r#""550e8400-e29b-41d4-a716-446655440000""#,
        ),
        (
            r#"{"type":"fixed","name":"Id","size":16,"logicalType":"uuid"}"#,
            r#""550e8400-e29b-41d4-a716-446655440000""#,
        ),
        (
            r#"{"type":"bytes","logicalType":"decimal","precision":8,"scale":2}"#,
            "[4,210]",
        ),
        (
            r#"{"type":"fixed","name":"Money","size":4,"logicalType":"decimal","precision":8,"scale":2}"#,
            "[0,0,4,210]",
        ),
        (
            r#"{"type":"fixed","name":"Span","size":12,"logicalType":"duration"}"#,
            "[0,0,0,0,0,0,0,0,0,0,0,0]",
        ),
        (
            r#"["null",{"type":"long","logicalType":"timestamp-millis"}]"#,
            "null 12345",
        ),
    ] {
        assert_eq!(
            roundtrip(schema, values),
            json_values(values.as_bytes()),
            "schema: {schema}"
        );
    }
}

#[test]
fn streams_multiline_json_and_prints_record_fields_in_schema_order() {
    let schema = r#"{"type":"record","name":"R","fields":[{"name":"z","type":"int"},{"name":"a","type":"int"}]}"#;
    let input = b"{\n \"a\": 1, \"z\": 2\n}\n\n{\"z\":3,\"a\":4}";
    let avro = success(&["fromjson", "--schema", schema, "-"], input);
    assert_eq!(
        success(&["tojson", "-"], &avro),
        b"{\"z\":2,\"a\":1}\n{\"z\":3,\"a\":4}\n"
    );
    let pretty = success(&["tojson", "--pretty", "-"], &avro);
    assert!(String::from_utf8_lossy(&pretty).contains("\n  \"z\": 2"));
    assert_eq!(json_values(&pretty).len(), 2);
}

#[test]
fn reads_containers_created_with_apache_avro_writer() {
    let schema = Schema::parse_str(r#"["null","string","long"]"#).unwrap();
    let mut writer = Writer::new(&schema, Vec::new()).unwrap();
    writer
        .append_value(Value::Union(2, Box::new(Value::Long(12))))
        .unwrap();
    writer
        .append_value(Value::Union(0, Box::new(Value::Null)))
        .unwrap();
    let avro = writer.into_inner().unwrap();
    assert_eq!(
        json_values(&success(&["tojson", "-"], &avro)),
        vec![json!(12), Json::Null]
    );
}

#[test]
fn every_codec_and_compression_level_is_readable_by_apache_avro() {
    let values = "42 ".repeat(100);
    for codec in ["null", "deflate", "snappy", "bzip2", "xz", "zstandard"] {
        let levels = match codec {
            "deflate" | "xz" => vec![-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            "zstandard" => vec![-5, -1, 0, 1, 3, 9, 22],
            _ => vec![-1, 99], // Java ignores --level for these codecs.
        };
        for level in levels {
            let avro = success(
                &[
                    "fromjson",
                    "--schema",
                    "\"int\"",
                    "--codec",
                    codec,
                    "--level",
                    &level.to_string(),
                    "-",
                ],
                values.as_bytes(),
            );
            let reader = Reader::new(avro.as_slice()).unwrap();
            assert_eq!(
                reader.collect::<Result<Vec<_>, _>>().unwrap(),
                vec![Value::Int(42); 100],
                "{codec}, level {level}"
            );
        }
    }
}

#[test]
fn writes_multiple_blocks_including_zero_byte_null_values() {
    for (schema, values, expected) in [
        ("\"null\"", "null ".repeat(20001), Value::Null),
        (
            "\"string\"",
            format!("\"{}\" ", "x".repeat(100)).repeat(2001),
            Value::String("x".repeat(100)),
        ),
    ] {
        let avro = from_json(schema, &values);
        let actual = Reader::new(avro.as_slice())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            actual.len(),
            if schema == "\"null\"" { 20001 } else { 2001 }
        );
        assert!(actual.iter().all(|v| v == &expected));
    }
}

#[test]
fn head_defaults_explicit_values_and_zero() {
    let avro = from_json(
        "\"int\"",
        &(0..15).map(|n| format!("{n}\n")).collect::<String>(),
    );
    for (options, expected) in [
        (vec!["tojson", "-"], 15),
        (vec!["tojson", "--head", "-"], 10),
        (vec!["tojson", "--head=3", "-"], 3),
        (vec!["tojson", "--head", "3", "-"], 3),
        (vec!["tojson", "-", "--head"], 10),
        (vec!["tojson", "--head=0", "-"], 0),
    ] {
        assert_eq!(
            json_values(&success(&options, &avro)).len(),
            expected,
            "{options:?}"
        );
    }
    let output = invoke(&["tojson", "--head=-1", "-"], &avro);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("negative"));
}

#[test]
fn schema_files_and_input_files_and_schema_file_precedence() {
    let temp = tempfile::tempdir().unwrap();
    let schema = temp.path().join("data.avsc");
    let input = temp.path().join("data.json");
    let container = temp.path().join("data.avro");
    std::fs::write(&schema, "\"int\"").unwrap();
    std::fs::write(&input, "1 2 3").unwrap();
    let avro = success(
        &[
            "fromjson",
            "--schema",
            "invalid",
            "--schema-file",
            schema.to_str().unwrap(),
            input.to_str().unwrap(),
        ],
        b"",
    );
    std::fs::write(&container, &avro).unwrap();
    let output = success(
        &[
            "tojson",
            "--head",
            container.to_str().unwrap(),
            "--reader-schema",
            "invalid",
            "--reader-schema-file",
            schema.to_str().unwrap(),
        ],
        b"",
    );
    assert_eq!(json_values(&output), vec![json!(1), json!(2), json!(3)]);
}

#[test]
fn reader_schema_projects_promotes_and_adds_defaults() {
    let writer = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"int"},{"name":"remove","type":"string"}]}"#;
    let reader = r#"{"type":"record","name":"Event","fields":[{"name":"added","type":"string","default":"new"},{"name":"id","type":"long"}]}"#;
    let avro = from_json(writer, r#"{"id":1,"remove":"old"}"#);
    let output = success(&["tojson", "--reader-schema", reader, "-"], &avro);
    assert_eq!(json_values(&output), vec![json!({"added":"new","id":1})]);
    assert!(String::from_utf8_lossy(&output).starts_with("{\"added\""));
}

#[test]
fn reader_schema_handles_aliases_enum_defaults_and_reordered_named_unions() {
    let writer = include_str!("fixtures/evolution-writer.avsc");
    let reader = include_str!("fixtures/evolution-reader.avsc");
    let input = include_str!("fixtures/evolution-input.json");
    let expected = json_values(include_bytes!("fixtures/evolution-expected.json"));
    let avro = from_json(writer, input);
    assert_eq!(
        json_values(&success(&["tojson", "--reader-schema", reader, "-"], &avro)),
        expected
    );
}

#[test]
fn reader_schema_rejects_incompatible_names_and_numeric_demotion() {
    for (writer, reader, input) in [
        ("\"long\"", "\"int\"", "1"),
        (
            r#"{"type":"record","name":"A","fields":[]}"#,
            r#"{"type":"record","name":"B","fields":[]}"#,
            "{}",
        ),
        (
            r#"{"type":"record","name":"A","fields":[]}"#,
            r#"{"type":"record","name":"A","fields":[{"name":"missing","type":"int"}]}"#,
            "{}",
        ),
    ] {
        let avro = from_json(writer, input);
        assert!(
            !invoke(&["tojson", "--reader-schema", reader, "-"], &avro)
                .status
                .success()
        );
    }
}

#[test]
fn empty_input_produces_a_valid_empty_container_and_newline() {
    let avro = from_json("\"int\"", " \n\t");
    assert!(avro.starts_with(b"Obj\x01"));
    assert_eq!(Reader::new(avro.as_slice()).unwrap().count(), 0);
    assert_eq!(success(&["tojson", "-"], &avro), b"\n");
}

#[test]
fn rejects_malformed_json_and_values_that_do_not_match_schema() {
    for (schema, value) in [
        ("\"int\"", "2147483648"),
        ("\"int\"", "1.5"),
        ("\"long\"", "9223372036854775808"),
        ("\"boolean\"", "1"),
        ("\"bytes\"", "[256]"),
        ("\"bytes\"", "[-1]"),
        ("\"bytes\"", "[1.5]"),
        ("\"bytes\"", "\"😀\""),
        (r#"{"type":"fixed","name":"F","size":2}"#, "\"a\""),
        (r#"["null","string"]"#, r#"{"string":"tagged"}"#),
        (r#"["null","string"]"#, r#"{"int":1}"#),
        (r#"["null","string"]"#, r#"{"string":"x","null":null}"#),
        (r#"{"type":"enum","name":"E","symbols":["A"]}"#, "\"B\""),
        (
            r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}"#,
            "{}",
        ),
        ("\"double\"", "\"Infinity\""),
        ("\"float\"", "\"NaN\""),
        ("\"int\"", "1 {bad"),
    ] {
        let output = invoke(&["fromjson", "--schema", schema, "-"], value.as_bytes());
        assert!(!output.status.success(), "accepted {value} with {schema}");
        assert!(!output.stderr.is_empty());
    }
}

#[test]
fn rejects_bad_arguments_codecs_levels_and_corrupt_containers() {
    for args in [
        vec![],
        vec!["tojson"],
        vec!["fromjson", "-"],
        vec!["fromjson", "--schema", "bad", "-"],
        vec!["fromjson", "--schema", "\"int\"", "--codec", "bogus", "-"],
        vec![
            "fromjson",
            "--schema",
            "\"int\"",
            "--codec",
            "deflate",
            "--level=10",
            "-",
        ],
        vec![
            "fromjson",
            "--schema",
            "\"int\"",
            "--codec",
            "xz",
            "--level=-2",
            "-",
        ],
        vec!["tojson", "--unknown", "-"],
        vec!["tojson", "a", "b"],
    ] {
        let output = invoke(&args, b"");
        assert!(!output.status.success(), "accepted {args:?}");
        assert!(!output.stderr.is_empty());
    }
    let avro = from_json("\"int\"", "123");
    for input in [b"not avro".as_slice(), &avro[..avro.len() - 1]] {
        assert!(!invoke(&["tojson", "-"], input).status.success());
    }
}

#[test]
fn help_lists_the_full_parameter_set() {
    for (command, flags) in [
        (
            "tojson",
            vec![
                "--pretty",
                "--head",
                "--reader-schema",
                "--reader-schema-file",
            ],
        ),
        (
            "fromjson",
            vec!["--schema", "--schema-file", "--codec", "--level"],
        ),
    ] {
        let output = success(&[command, "--help"], b"");
        let text = String::from_utf8(output).unwrap();
        for flag in flags {
            assert!(text.contains(flag));
        }
    }
}
