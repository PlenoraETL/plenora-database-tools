#![allow(clippy::doc_markdown, clippy::manual_is_multiple_of, clippy::match_same_arms)]
//! Parser per parametri tipizzati passati via CLI: `NAME=VALUE:TYPE` o
//! `VALUE:TYPE` (per parametri posizionali senza nome).
//!
//! Usato da execute-sql (--param), execute-scalar (--param), conditional-update
//! (--set-param), portable-execute (nel JSON), --session-context (globale).

use crate::CliResult;
use plenora_database_core::provider::ParameterValue;

/// Elenco di parametri parsed da CLI, in ordine posizionale ($1, $2, ...).
#[derive(Debug, Default, Clone)]
pub(crate) struct TypedParams(pub Vec<ParameterValue>);

impl TypedParams {
    pub(crate) fn into_inner(self) -> Vec<ParameterValue> {
        self.0
    }
}

/// Parsa `VALUE:TYPE` (senza nome) → `ParameterValue`.
///
/// Types supportati:
///   - `bool`  (true/false)
///   - `int`   (i32)
///   - `bigint`|`long` (i64)
///   - `float`|`double` (f64)
///   - `string`|`text` (String)
///   - `bytes-hex` (Vec<u8> da hex)
///   - `uuid`  (String 36 chars con dash)
///   - `json`  (parsed serde_json::Value)
///   - `date`  (YYYY-MM-DD)
///   - `timestamp` (ISO-8601 senza tz)
///   - `timestamptz` (RFC-3339)
///   - `null:<type>` (bool/int/bigint/text/uuid/json/date/timestamp/timestamptz/bytea)
pub(crate) fn parse_value_type(spec: &str) -> CliResult<ParameterValue> {
    // Caso speciale: `null:<sub-type>` — l'intero spec è la dichiarazione.
    // Nessun valore concreto perché è NULL.
    if let Some(sub) = spec.strip_prefix("null:") {
        if sub.is_empty() {
            return Err(
                "type 'null' richiede sotto-tipo: null:bool|int|bigint|text|uuid|json|date|timestamp|timestamptz|bytea"
                    .into(),
            );
        }
        return Ok(ParameterValue::Null {
            type_name: sub.to_owned(),
        });
    }
    let (raw_value, ty) = spec
        .rsplit_once(':')
        .ok_or_else(|| format!("param senza separatore ':': {spec}"))?;
    // Fix review #15: prima usavamo `trim_matches('"' | '\'')` che
    // eliminava quote iniziali/finali indiscriminatamente,
    // corrompendo:
    // 1. Stringhe che contengono realmente virgolette (es. testo
    //    citato `"he said \"hi\""` → perdeva le virgolette esterne).
    // 2. Valori JSON stringa top-level `"foo"` → parsed come `foo`
    //    che non è valid JSON.
    // 3. Stringhe asimmetriche `"foo'` → strip di caratteri validi.
    //
    // Ora: strip solo una coppia matched (stesso quote ad entrambi
    // gli estremi) — è il pattern comune in cui la shell ha
    // conservato le quote, non un valore che le contiene realmente.
    // Per JSON e bytes-hex non strippo mai (i loro parser non
    // dipendono dallo strip esterno).
    let value = strip_matching_outer_quotes(raw_value, ty);
    match ty {
        "null" => Err(
            "sintassi null: usa direttamente 'null:<sub-type>' senza valore, es 'null:uuid'"
                .into(),
        ),
        "bool" => match value {
            "true" => Ok(ParameterValue::Bool(true)),
            "false" => Ok(ParameterValue::Bool(false)),
            _ => Err(format!("bool non riconosciuto (usa true|false): {value}").into()),
        },
        "int" => value
            .parse::<i32>()
            .map(ParameterValue::I32)
            .map_err(|_| format!("int non valido: {value}").into()),
        "bigint" | "long" => value
            .parse::<i64>()
            .map(ParameterValue::I64)
            .map_err(|_| format!("bigint non valido: {value}").into()),
        "float" | "double" => value
            .parse::<f64>()
            .map(ParameterValue::F64)
            .map_err(|_| format!("float non valido: {value}").into()),
        "string" | "text" => Ok(ParameterValue::String(value.to_owned())),
        "uuid" => {
            if value.len() != 36 {
                return Err(format!("uuid deve avere lunghezza 36: {value}").into());
            }
            Ok(ParameterValue::Uuid(value.to_owned()))
        }
        "json" => serde_json::from_str::<serde_json::Value>(value)
            .map(ParameterValue::Json)
            .map_err(|e| format!("json non parsabile: {e}").into()),
        "date" => Ok(ParameterValue::Date(value.to_owned())),
        "timestamp" => Ok(ParameterValue::Timestamp(value.to_owned())),
        "timestamptz" => Ok(ParameterValue::TimestampTz(value.to_owned())),
        "bytes-hex" | "bytea" => decode_hex(value)
            .map(ParameterValue::Bytes)
            .map_err(std::convert::Into::into),
        other => Err(format!(
            "type sconosciuto: {other} \
             (bool|int|bigint|float|string|uuid|json|date|timestamp|timestamptz|bytes-hex|null:<type>)"
        )
        .into()),
    }
}

/// Parsa `NAME=VALUE:TYPE` → `(name, ParameterValue)`. NAME è usato dal
/// `--session-context` (dove serve nome del setting); nel caso di `--param`
/// per bind position, il nome è ignorato dal consumer.
pub(crate) fn parse_named_value_type(spec: &str) -> CliResult<(String, ParameterValue)> {
    let (name, rest) = spec
        .split_once('=')
        .ok_or_else(|| format!("param senza '=': {spec}"))?;
    if name.is_empty() {
        return Err(format!("param con name vuoto: {spec}").into());
    }
    let value = parse_value_type(rest)?;
    Ok((name.to_owned(), value))
}

/// Rimuove al massimo una coppia matched di quote esterne
/// (`"..."` o `'...'`) da un valore CLI, senza corrompere valori
/// che contengono realmente virgolette. Fix review #15.
///
/// Politica per tipo:
/// - `json`, `bytes-hex`, `bytea`: **mai** strip — il parser JSON
///   richiede quote esplicite per stringhe top-level; hex non ha
///   quote di suo.
/// - Altri tipi: strip solo se stringa lunga ≥ 2 e inizia+finisce con
///   lo stesso quote character.
fn strip_matching_outer_quotes<'a>(raw: &'a str, ty: &str) -> &'a str {
    match ty {
        "json" | "bytes-hex" | "bytea" => return raw,
        _ => {}
    }
    let bytes = raw.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            // Safe slice: entrambi i quote sono ASCII single-byte,
            // quindi 1 e len-1 sono char boundary validi.
            return &raw[1..raw.len() - 1];
        }
    }
    raw
}

fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    let s = s.trim_start_matches("\\x").trim_start_matches("0x");
    if s.len() % 2 != 0 {
        return Err(format!("hex length dispari: {s}"));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(format!("hex nibble non valido: {}", b as char)),
    }
}

/// Estrae tutti i `--param <spec>` (o `-p <spec>`) presenti in `args`
/// in ordine, li rimuove, restituisce il resto + i parametri parsati.
///
/// L'ordine dei `--param` è preservato: `$1` == primo `--param`, ecc.
pub(crate) fn strip_bind_params(args: Vec<String>) -> CliResult<(Vec<String>, TypedParams)> {
    let mut out = Vec::with_capacity(args.len());
    let mut params = Vec::new();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        if arg == "--param" || arg == "-p" {
            let spec = iter
                .next()
                .ok_or_else(|| format!("{arg} richiede un argomento NAME=VALUE:TYPE o VALUE:TYPE"))?;
            // NAME opzionale per bind position: se manca '=', trattiamo tutto come VALUE:TYPE.
            let value = if spec.contains('=') && !spec.starts_with('{') {
                // '=' presente, potrebbe essere un JSON (evita quel falso positivo).
                let (_name, v) = parse_named_value_type(&spec)?;
                v
            } else {
                parse_value_type(&spec)?
            };
            params.push(value);
        } else {
            out.push(arg);
        }
    }
    Ok((out, TypedParams(params)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_value_type_covers_all_scalars() {
        assert!(matches!(parse_value_type("42:int").unwrap(), ParameterValue::I32(42)));
        assert!(matches!(parse_value_type("9999999999:bigint").unwrap(), ParameterValue::I64(9_999_999_999)));
        assert!(matches!(parse_value_type("3.14:float").unwrap(), ParameterValue::F64(_)));
        assert!(matches!(parse_value_type("true:bool").unwrap(), ParameterValue::Bool(true)));
        assert!(matches!(parse_value_type("false:bool").unwrap(), ParameterValue::Bool(false)));
        assert!(matches!(parse_value_type("hello:string").unwrap(), ParameterValue::String(s) if s == "hello"));
        assert!(matches!(parse_value_type("hello:text").unwrap(), ParameterValue::String(_)));
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

    // ---- Fix review #15: strip_matching_outer_quotes -----------------------

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
        // Pre-fix rimuoveva sia `"` iniziale che `'` finale — corrompendo
        // il valore. Ora solo coppie matched sono strippate.
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
        // JSON top-level: `"foo"` è una JSON string valida. Pre-fix
        // veniva strippato a `foo` che non è JSON valido.
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
}
