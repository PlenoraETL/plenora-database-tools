#![allow(
    clippy::doc_markdown,
    clippy::manual_is_multiple_of,
    clippy::match_same_arms
)]
// Il modulo e provider-neutral e non conosce nessun adapter: legge
// `NAME=VALUE:TYPE` e produce `ParameterValue` del core. I chiamanti attuali
// sono nei comandi PostgreSQL, quindi senza quella feature il compilatore lo
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
//! Un errore dice **dove** e **cosa si aspettava**, mai **cosa ha letto**.
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
    // Rimuove soltanto una coppia di quote corrispondenti, senza corrompere:
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
/// che contengono realmente virgolette.
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
#[path = "typed_params_tests.rs"]
mod tests;
