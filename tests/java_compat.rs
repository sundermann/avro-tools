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
fn all_codecs_work_in_both_directions_with_avro_json_types() {
    let schema = r#"{"type":"record","name":"demo.Event","fields":[
        {"name":"id","type":"long"},
        {"name":"flag","type":"boolean"},
        {"name":"float","type":"float"},
        {"name":"double","type":"double"},
        {"name":"bytes","type":"bytes"},
        {"name":"fixed","type":{"type":"fixed","name":"Code","size":2}},
        {"name":"enum","type":{"type":"enum","name":"Kind","symbols":["A","B"]}},
        {"name":"array","type":{"type":"array","items":"int"}},
        {"name":"map","type":{"type":"map","values":"string"}},
        {"name":"choice","type":["null","string","Code","Kind"]},
        {"name":"next","type":["null","Event"]}
    ]}"#;
    let first = json!({"id":9223372036854775807_i64,"flag":true,"float":0.1,"double":1.25,"bytes":"\u{0}\u{80}\u{ff}","fixed":"AB","enum":"A","array":[1,2],"map":{"k":"世界😀"},"choice":{"demo.Code":"CD"},"next":null});
    let mut second = first.clone();
    second["choice"] = json!({"demo.Kind":"B"});
    second["next"] = json!({"demo.Event":first});
    let input = format!("{first}\n{second}");
    let mut expected = vec![first, second];
    // JsonEncoder writes a float through its double overload.
    for value in &mut expected {
        value["float"] = json!(f64::from(0.1_f32));
    }
    expected[1]["next"]["demo.Event"]["float"] = json!(f64::from(0.1_f32));
    for codec in ["null", "deflate", "snappy", "bzip2", "xz", "zstandard"] {
        for producer_java in [false, true] {
            // Java 1.12.1 passes its default -1 to XZ, which rejects it.
            let level = if codec == "xz" { "6" } else { "-1" };
            let avro = run(
                producer_java,
                &[
                    "fromjson", "--schema", schema, "--codec", codec, "--level", level, "-",
                ],
                input.as_bytes(),
            );
            let actual = run(!producer_java, &["tojson", "-"], &avro);
            assert_eq!(
                values(&actual),
                expected,
                "codec={codec}, producer_java={producer_java}"
            );
        }
    }
}

#[test]
#[ignore = "requires Java and AVRO_TOOLS_JAR"]
fn aliases_enum_defaults_and_reordered_named_unions_match_java() {
    let writer = include_str!("fixtures/evolution-writer.avsc");
    let reader = include_str!("fixtures/evolution-reader.avsc");
    let input = include_bytes!("fixtures/evolution-input.json");
    let avro = run(true, &["fromjson", "--schema", writer, "-"], input);
    let args = ["tojson", "--reader-schema", reader, "-"];
    assert_eq!(
        values(&run(false, &args, &avro)),
        values(&run(true, &args, &avro))
    );
}

#[test]
#[ignore = "requires Java and AVRO_TOOLS_JAR"]
fn logical_types_and_numeric_union_tags_match_java() {
    let schema = r#"{"type":"record","name":"R","fields":[
        {"name":"date","type":{"type":"int","logicalType":"date"}},
        {"name":"time","type":{"type":"long","logicalType":"time-micros"}},
        {"name":"timestamp","type":{"type":"long","logicalType":"timestamp-millis"}},
        {"name":"decimal","type":{"type":"bytes","logicalType":"decimal","precision":8,"scale":2}},
        {"name":"money","type":{"type":"fixed","name":"Money","size":4,"logicalType":"decimal","precision":8,"scale":2}},
        {"name":"uuid","type":{"type":"string","logicalType":"uuid"}},
        {"name":"value","type":["int","long","double"]}
    ]}"#;
    let input = json!({"date":20000,"time":1234567,"timestamp":1720000000000_i64,"decimal":"\u{4}Ò","money":"\u{0}\u{0}\u{4}Ò","uuid":"550e8400-e29b-41d4-a716-446655440000","value":{"long":7}});
    for producer_java in [false, true] {
        let avro = run(
            producer_java,
            &["fromjson", "--schema", schema, "-"],
            input.to_string().as_bytes(),
        );
        assert_eq!(
            values(&run(!producer_java, &["tojson", "-"], &avro)),
            vec![input.clone()]
        );
    }
}

#[test]
#[ignore = "requires Java and AVRO_TOOLS_JAR"]
fn compression_levels_head_pretty_and_reader_schema_match_java() {
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
                    values(&run(true, &args, &avro)),
                    "{args:?}, {codec}, producer_java={producer_java}"
                );
            }
        }
    }
    let writer = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"int"},{"name":"remove","type":"string"}]}"#;
    let reader = r#"{"type":"record","name":"Event","fields":[{"name":"added","type":"string","default":"new"},{"name":"id","type":"long"}]}"#;
    let avro = run(
        true,
        &["fromjson", "--schema", writer, "-"],
        br#"{"id":1,"remove":"old"}"#,
    );
    let args = ["tojson", "--reader-schema", reader, "-"];
    assert_eq!(
        values(&run(false, &args, &avro)),
        values(&run(true, &args, &avro))
    );
}
