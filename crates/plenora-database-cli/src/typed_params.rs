#![allow(
    clippy::doc_markdown,
    clippy::manual_is_multiple_of,
    clippy::match_same_arms
)]
// Il modulo e provider-neutral e non conosce nessun adapter: legge
// `NAME=VALUE:TYPE` e produce `ParameterValue` del core. Oggi lo chiamano
// solo i comandi PostgreSQL, quindi senza quella feature il compilatore lo
// vede inutilizzato. La risposta **non** e metterlo dietro `postgres`: e
// esattamente l'accoppiamento che rendeva impossibile costruire il binario
// con il solo adapter MySQL. Il giorno che un comando MySQL bindera
// parametri tipizzati, questa riga sparisce da sola.
#![cfg_attr(not(feature = "postgres"), allow(dead_code))]
//! Parser per parametri tipizzati passati via CLI: `NAME=VALUE:TYPE` o
//! `VALUE:TYPE` (per parametri posizionali senza nome).
//!
//! Usato da execute-sql (--param), execute-scalar (--param), conditional-update
//! (--set-param), portable-execute (nel JSON), --session-context (globale).
//!
//! # I messaggi d'errore non riportano il valore
//!
//! Un parametro CLI **e** un payload: e il dato che sta per essere bindato.
//! Gli errori di parsing includevano lo spec o il valore per intero, quindi
//! una password passata con il tipo sbagliato — `pwd:hunter2:int` — finiva
//! integralmente in stderr, che qui e JSON, e da li nei log e nella
//! telemetria. Contraddiceva il divieto che
//! `plenora_database_core::DatabaseError` dichiara sul proprio `message`:
//! contesto operativo, mai payload.
//!
//! Ora un errore dice **dove** e **cosa si aspettava**, mai **cosa ha letto**.
//! Chi deve vedere il valore ce l'ha gia: l'ha scritto lui sulla riga di
//! comando.

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
        .ok_or("param senza separatore ':' (atteso VALUE:TYPE)")?;
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
            "sintassi null: usa direttamente 'null:<sub-type>' senza valore, es 'null:uuid'".into(),
        ),
        "bool" => match value {
            "true" => Ok(ParameterValue::Bool(true)),
            "false" => Ok(ParameterValue::Bool(false)),
            _ => Err("bool non riconosciuto (usa true|false)".into()),
        },
        "int" => value
            .parse::<i32>()
            .map(ParameterValue::I32)
            .map_err(|_| "valore non interpretabile come int (i32)".into()),
        "bigint" | "long" => value
            .parse::<i64>()
            .map(ParameterValue::I64)
            .map_err(|_| "valore non interpretabile come bigint (i64)".into()),
        "float" | "double" => value
            .parse::<f64>()
            .map(ParameterValue::F64)
            .map_err(|_| "valore non interpretabile come float (f64)".into()),
        "string" | "text" => Ok(ParameterValue::String(value.to_owned())),
        "uuid" => {
            if value.len() != 36 {
                return Err(format!(
                    "uuid deve avere lunghezza 36, ricevuti {} caratteri",
                    value.chars().count()
                )
                .into());
            }
            Ok(ParameterValue::Uuid(value.to_owned()))
        }
        "json" => serde_json::from_str::<serde_json::Value>(value)
            .map(ParameterValue::Json)
            // Solo riga e colonna: il `Display` di `serde_json::Error`
            // include il frammento che non ha saputo leggere, cioe una parte
            // del valore.
            .map_err(|e| {
                format!(
                    "json non parsabile a riga {}, colonna {}",
                    e.line(),
                    e.column()
                )
                .into()
            }),
        "date" => Ok(ParameterValue::Date(value.to_owned())),
        "timestamp" => Ok(ParameterValue::Timestamp(value.to_owned())),
        "timestamptz" => Ok(ParameterValue::TimestampTz(value.to_owned())),
        "bytes-hex" | "bytea" => decode_hex(value)
            .map(ParameterValue::Bytes)
            .map_err(std::convert::Into::into),
        _ => Err(
            "type sconosciuto: ammessi bool, int, bigint, float, string, uuid, json, \
             date, timestamp, timestamptz, bytes-hex, null:<type>"
                .into(),
        ),
    }
}

/// Parsa `NAME=VALUE:TYPE` → `(name, ParameterValue)`. NAME è usato dal
/// `--session-context` (dove serve nome del setting); nel caso di `--param`
/// per bind position, il nome è ignorato dal consumer.
pub(crate) fn parse_named_value_type(spec: &str) -> CliResult<(String, ParameterValue)> {
    // Lo spec intero contiene il valore: nessuno dei due errori qui lo cita.
    let (name, rest) = spec
        .split_once('=')
        .ok_or("param senza '=' (atteso NAME=VALUE:TYPE)")?;
    if name.is_empty() {
        return Err("param con name vuoto (atteso NAME=VALUE:TYPE)".into());
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
        return Err(format!("hex di lunghezza dispari ({} caratteri)", s.len()));
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
        // Nemmeno un singolo carattere: su un valore corto e gia una parte
        // consistente del segreto.
        _ => Err("carattere non esadecimale nel valore".to_owned()),
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
            let spec = iter.next().ok_or_else(|| {
                format!("{arg} richiede un argomento NAME=VALUE:TYPE o VALUE:TYPE")
            })?;
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
        assert!(
            matches!(bytes, ParameterValue::Bytes(ref b) if b == &vec![0xde, 0xad, 0xbe, 0xef])
        );
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
    /// I marcatori sono maiuscoli e improbabili di proposito: la prima
    /// stesura cercava `"zz"`, che compare dentro «lunghe**zz**a» del
    /// messaggio, e il test falliva su se stesso invece che sul difetto.
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
}
