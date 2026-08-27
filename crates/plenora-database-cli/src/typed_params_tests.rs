use super::*;
use serde_json::json;

#[test]
fn parse_value_type_covers_all_scalars() {
    assert!(matches!(
        parse_value_type("42:int").unwrap(),
        ParameterValue::I32(42)
    ));
    assert!(matches!(
        parse_value_type("9999999999:bigint").unwrap(),
        ParameterValue::I64(9_999_999_999)
    ));
    assert!(matches!(
        parse_value_type("3.14:float").unwrap(),
        ParameterValue::F64(_)
    ));
    assert!(matches!(
        parse_value_type("true:bool").unwrap(),
        ParameterValue::Bool(true)
    ));
    assert!(matches!(
        parse_value_type("false:bool").unwrap(),
        ParameterValue::Bool(false)
    ));
    assert!(
        matches!(parse_value_type("hello:string").unwrap(), ParameterValue::String(s) if s == "hello")
    );
    assert!(matches!(
        parse_value_type("hello:text").unwrap(),
        ParameterValue::String(_)
    ));
    assert!(matches!(
        parse_value_type("11111111-2222-3333-4444-555555555555:uuid").unwrap(),
        ParameterValue::Uuid(_)
    ));
    assert!(matches!(
        parse_value_type(r#"{"k":"v"}:json"#).unwrap(),
        ParameterValue::Json(_)
    ));
    assert!(matches!(
        parse_value_type("2026-08-12:date").unwrap(),
        ParameterValue::Date(_)
    ));
    assert!(matches!(
        parse_value_type("2026-08-12T10:00:00:timestamp").unwrap(),
        ParameterValue::Timestamp(_)
    ));
    assert!(matches!(
        parse_value_type("2026-08-12T10:00:00Z:timestamptz").unwrap(),
        ParameterValue::TimestampTz(_)
    ));
    let bytes = parse_value_type("deadbeef:bytes-hex").unwrap();
    assert!(matches!(bytes, ParameterValue::Bytes(ref b) if b == &vec![0xde, 0xad, 0xbe, 0xef]));
}

#[test]
fn parse_null_uses_null_prefix() {
    assert!(parse_value_type("null:").is_err());
    let n = parse_value_type("null:uuid").unwrap();
    assert!(matches!(n, ParameterValue::Null { type_name } if type_name == "uuid"));
    let m = parse_value_type("null:jsonb").unwrap();
    assert!(matches!(m, ParameterValue::Null { type_name } if type_name == "jsonb"));
}

#[test]
fn parse_value_type_rejects_unknown() {
    assert!(parse_value_type("42:blob").is_err());
    assert!(parse_value_type("42").is_err()); // missing :TYPE
    assert!(parse_value_type("not-a-bool:bool").is_err());
    assert!(parse_value_type("not-a-number:int").is_err());
}

#[test]
fn parse_named_value_type_splits_on_first_equal() {
    let (n, v) = parse_named_value_type("app.tenant_id=t42:string").unwrap();
    assert_eq!(n, "app.tenant_id");
    assert!(matches!(v, ParameterValue::String(s) if s == "t42"));
    let (n2, v2) = parse_named_value_type(r#"app.filter={"a":1}:json"#).unwrap();
    assert_eq!(n2, "app.filter");
    assert!(matches!(v2, ParameterValue::Json(ref j) if j == &json!({"a": 1})));
}

#[test]
fn strip_bind_params_preserves_positional_order() {
    let (rest, params) = strip_bind_params(vec![
        "DSN_ENV".into(),
        "--param".into(),
        "42:int".into(),
        "--param".into(),
        "hello:string".into(),
        "SELECT $1, $2".into(),
    ])
    .unwrap();
    assert_eq!(rest, vec!["DSN_ENV", "SELECT $1, $2"]);
    assert_eq!(params.0.len(), 2);
    assert!(matches!(params.0[0], ParameterValue::I32(42)));
    assert!(matches!(params.0[1], ParameterValue::String(ref s) if s == "hello"));
}

// ---- strip_matching_outer_quotes ---------------------------------------

#[test]
fn matched_outer_double_quotes_are_stripped() {
    // Shell può aver conservato le quote: user ha scritto "hello".
    let v = parse_value_type(r#""hello":string"#).unwrap();
    assert!(matches!(v, ParameterValue::String(s) if s == "hello"));
}

#[test]
fn matched_outer_single_quotes_are_stripped() {
    let v = parse_value_type("'hello':string").unwrap();
    assert!(matches!(v, ParameterValue::String(s) if s == "hello"));
}

#[test]
fn asymmetric_quotes_are_preserved() {
    // Soltanto coppie di quote corrispondenti vengono rimosse.
    let v = parse_value_type(r#""hello':string"#).unwrap();
    assert!(matches!(v, ParameterValue::String(s) if s == r#""hello'"#));
}

#[test]
fn internal_quotes_are_preserved() {
    // Il valore contiene realmente virgolette interne, non wrapping.
    let v = parse_value_type(r#"he said "hi":string"#).unwrap();
    assert!(matches!(v, ParameterValue::String(s) if s == r#"he said "hi""#));
}

#[test]
fn json_string_top_level_is_not_stripped() {
    // Una stringa JSON top-level conserva le quote necessarie alla sintassi.
    let v = parse_value_type(r#""foo":json"#).unwrap();
    assert!(matches!(v, ParameterValue::Json(ref j) if j == &json!("foo")));
}

#[test]
fn bytes_hex_never_strips() {
    let v = parse_value_type("deadbeef:bytes-hex").unwrap();
    assert!(matches!(v, ParameterValue::Bytes(ref b) if b == &vec![0xde, 0xad, 0xbe, 0xef]));
}

#[test]
fn strip_bind_params_handles_short_flag() {
    let (rest, params) =
        strip_bind_params(vec!["-p".into(), "true:bool".into(), "sql".into()]).unwrap();
    assert_eq!(rest, vec!["sql"]);
    assert!(matches!(params.0[0], ParameterValue::Bool(true)));
}

/// Nessun errore di parsing rimette il valore nel messaggio.
///
/// Il marcatore e un segreto plausibile: se compare nell'errore, quel
/// percorso lo avrebbe scritto in stderr JSON, nei log e nella telemetria.
/// La guardia enumera le forme, cosi un tipo nuovo che ricade
/// nell'abitudine `format!("... {value}")` si ferma qui.
#[test]
fn no_parse_error_carries_the_value() {
    const MARKER: &str = "hunter2SEGRETO";

    let specs = [
        format!("{MARKER}:int"),
        format!("{MARKER}:bigint"),
        format!("{MARKER}:float"),
        format!("{MARKER}:bool"),
        format!("{MARKER}:uuid"),
        format!("{MARKER}:json"),
        format!("{MARKER}:bytes-hex"),
        format!("{MARKER}:tipo-inesistente"),
        // Senza separatore: lo spec intero era finito nel messaggio.
        MARKER.to_owned(),
    ];

    for spec in specs {
        let error = parse_value_type(&spec).expect_err(&format!("{spec} deve fallire"));
        let rendered = format!("{error:?}");
        assert!(
            !rendered.contains(MARKER),
            "il valore e finito nell'errore di `{spec}`: {rendered}"
        );
    }

    // E la variante con nome, dove a perdersi era lo spec completo.
    let named = parse_named_value_type(&format!("segreto{MARKER}"))
        .expect_err("param senza '=' deve fallire");
    assert!(!format!("{named:?}").contains(MARKER), "{named:?}");
}

/// Un hex dispari o con caratteri non validi non ristampa i caratteri.
///
/// I marcatori sono maiuscoli e improbabili per non coincidere con parole
/// legittime del messaggio.
#[test]
fn hex_errors_report_shape_not_content() {
    for (spec, marker) in [
        ("ABCDEFXQ:bytes-hex", "ABCDEFXQ"),
        ("KWKW:bytes-hex", "KWKW"),
    ] {
        let error = parse_value_type(spec).expect_err("hex non valido");
        let rendered = format!("{error:?}");
        assert!(!rendered.contains(marker), "{rendered}");
    }
}
