//! Optional interoperability tests against the official Java avro-tools jar.
//! AVRO_TOOLS_JAR=/path/to/avro-tools-1.12.1.jar cargo test --test java_compat -- --ignored
use std::{
    io::Write,
    process::{Command, Stdio},
};

use serde_json::{Value, json};

fn run(java: bool, args: &[&str], input: &[u8]) -> Vec<u8> {
    let mut command = if java {
        let mut command = Command::new("java");
        command.args([
            "-jar",
            &std::env::var("AVRO_TOOLS_JAR")
                .expect("set AVRO_TOOLS_JAR to the official avro-tools jar"),
        ]);
        command
    } else {
        Command::new(env!("CARGO_BIN_EXE_avro-tools"))
    };
    let mut child = command
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
    assert!(
        output.status.success(),
        "java={java}, args={args:?}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn values(bytes: &[u8]) -> Vec<Value> {
    serde_json::Deserializer::from_slice(bytes)
        .into_iter::<Value>()
        .map(Result::unwrap)
        .collect()
}

#[test]
#[ignore = "requires Java and AVRO_TOOLS_JAR"]
fn containers_interoperate_with_different_json_formats_for_every_codec() {
    let schema = r#"{"type":"record","name":"Event","fields":[
        {"name":"data","type":"bytes"},
        {"name":"code","type":{"type":"fixed","name":"Code","size":2}},
        {"name":"choice","type":["null","string"]},
        {"name":"kind","type":{"type":"enum","name":"Kind","symbols":["ON","OFF"]}}
    ]}"#;
    let plain = json!({"data":[0,128,255],"code":[65,66],"choice":"hello","kind":"ON"});
    let java =
        json!({"data":"\u{0}\u{80}\u{ff}","code":"AB","choice":{"string":"hello"},"kind":"ON"});
    for codec in ["null", "deflate", "snappy", "bzip2", "xz", "zstandard"] {
        for producer_java in [false, true] {
            let input = if producer_java { &java } else { &plain };
            let expected = if producer_java { &plain } else { &java };
            let avro = run(
                producer_java,
                &[
                    "fromjson", "--schema", schema, "--codec", codec, "--level", "6", "-",
                ],
                input.to_string().as_bytes(),
            );
            assert_eq!(
                values(&run(!producer_java, &["tojson", "-"], &avro)),
                vec![expected.clone()],
                "codec={codec}, producer_java={producer_java}"
            );
        }
    }
}

#[test]
#[ignore = "requires Java and AVRO_TOOLS_JAR"]
fn logical_types_use_plain_json_with_interoperable_containers() {
    let schema = r#"{"type":"record","name":"R","fields":[
        {"name":"date","type":{"type":"int","logicalType":"date"}},
        {"name":"decimal","type":{"type":"bytes","logicalType":"decimal","precision":8,"scale":2}},
        {"name":"uuid","type":{"type":"string","logicalType":"uuid"}},
        {"name":"value","type":["int","long","double"]}
    ]}"#;
    let plain = json!({"date":20000,"decimal":[4,210],"uuid":"550e8400-e29b-41d4-a716-446655440000","value":7});
    let java = json!({"date":20000,"decimal":"\u{4}Ò","uuid":"550e8400-e29b-41d4-a716-446655440000","value":{"long":7}});
    let avro = run(
        true,
        &["fromjson", "--schema", schema, "-"],
        java.to_string().as_bytes(),
    );
    assert_eq!(
        values(&run(false, &["tojson", "-"], &avro)),
        vec![plain.clone()]
    );
    let avro = run(
        false,
        &["fromjson", "--schema", schema, "-"],
        plain.to_string().as_bytes(),
    );
    let mut expected = java;
    expected["value"] = json!({"int":7}); // Plain JSON selects the first matching branch.
    assert_eq!(values(&run(true, &["tojson", "-"], &avro)), vec![expected]);
}

#[test]
#[ignore = "requires Java and AVRO_TOOLS_JAR"]
fn reads_java_containers_with_schema_evolution() {
    let writer = include_str!("fixtures/evolution-writer.avsc");
    let reader = include_str!("fixtures/evolution-reader.avsc");
    let mut input: Value =
        serde_json::from_str(include_str!("fixtures/evolution-input.json")).unwrap();
    input["choice"] = json!({"example.B":{"n":7}});
    input["token"] = json!("AB");
    let expected = values(include_bytes!("fixtures/evolution-expected.json"));
    let avro = run(
        true,
        &["fromjson", "--schema", writer, "-"],
        input.to_string().as_bytes(),
    );
    assert_eq!(
        values(&run(
            false,
            &["tojson", "--reader-schema", reader, "-"],
            &avro
        )),
        expected
    );
}

#[test]
#[ignore = "requires Java and AVRO_TOOLS_JAR"]
fn compression_levels_head_pretty_and_reader_schema() {
    let input = (0..15).map(|n| format!("{n}\n")).collect::<String>();
    for (codec, level) in [("deflate", "2"), ("xz", "0"), ("zstandard", "-3")] {
        for producer_java in [false, true] {
            let avro = run(
                producer_java,
                &[
                    "fromjson", "--schema", "\"int\"", "--codec", codec, "--level", level, "-",
                ],
                input.as_bytes(),
            );
            for args in [
                vec!["tojson", "--head", "-"],
                vec!["tojson", "--head=3", "--pretty", "-"],
                vec!["tojson", "--head=0", "-"],
                vec!["tojson", "--reader-schema", "\"double\"", "-"],
            ] {
                assert_eq!(
                    values(&run(false, &args, &avro)),
                    values(&run(true, &args, &avro))
                );
            }
        }
    }
}
