//! Prova di fedelta dei parametri bindati, comune ai provider: un oracolo
//! indipendente dal codice che prova.
//!
//! La regola che misura (`plenora_database_core::parameter_fidelity` e
//! `operand_admission`): ogni variante di `ParameterValue`, su ogni provider
//! e percorso, o ha un binding esatto — il server riceve proprio quel valore,
//! lo conserva e lo ritrova con `col = ?`, e nessun valore vicino lo ritrova —
//! oppure e rifiutata in modo esplicito prima dell'invio.
//!
//! L'oracolo non si fida dei decoder del repository:
//!
//! - **ogni** caso esatto dichiara la resa canonica che il **server** fa del
//!   valore conservato (`NativeSql::Raw`: esadecimale dei byte o dei bit di un
//!   double, testo a precisione piena di decimali e temporali), letta con uno
//!   statement nativo dopo la scrittura di ciascun percorso;
//! - ogni percorso rilegge: il nativo con la query della transazione, il
//!   portable con la resa del server dopo la propria scrittura, la lettura
//!   plan-based proiettando la colonna del valore, cosi che i decoder Arrow
//!   girino e si confrontino con la forma canonica attesa;
//! - i valori vicini non li sceglie il caso: si generano per famiglia
//!   ([`neighbours`]) — ±1 ulp, ±1 attorno a 2^53, la cifra oltre la scala,
//!   ±1 microsecondo, data contro timestamp, l'espansione decimale troncata di
//!   un double — e nessuno deve ritrovare la riga. Le varianti di spazi finali
//!   e di maiuscole dipendono dalla collation: si registrano e non decidono.
//!
//! Il modulo non panica: restituisce un resoconto, e il test live del
//! provider lo trasforma in un verdetto.

use chrono::{Duration, NaiveDate, NaiveDateTime};
use plenora_database_core::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use plenora_database_core::arrow::{DataType, RecordBatch};
use plenora_database_core::facade::{execute_portable, execute_portable_returning};
use plenora_database_core::plan::{FilterExpression, ObjectRef, ReadOperation};
use plenora_database_core::portable::{
    and, eq, Expression, InsertStatement, PortableStatement, Projection, SelectStatement, TableRef,
};
use plenora_database_core::provider::{ParameterBag, ParameterValue, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions, TransactionScope};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Row};
use std::collections::BTreeMap;
use std::fmt::Write as _;

/// Lo statement nativo che il provider scrive per la tabella sonda.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeSql {
    /// `INSERT (ID, <col>)` con due segnaposto: id e valore.
    Insert,
    /// La colonna della riga `ID = ?`.
    Select,
    /// `COUNT(*)` delle righe con `ID = ?` e `<col> = ?`.
    Count,
    /// La resa canonica che il server fa del valore conservato della riga
    /// `ID = ?`, come un solo testo: l'oracolo indipendente dai decoder.
    Raw,
    /// `DELETE` della riga `ID = ?`, per il percorso di lettura.
    Delete,
}

/// Il percorso misurato.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FidelityPath {
    Native,
    Portable,
    Read,
}

/// Cio che il caso deve provare su un percorso.
#[derive(Debug, Clone, PartialEq)]
pub enum Expect {
    /// Conservato come `raw` (la resa del server), riletto come `read`,
    /// ritrovato da `col = ?` con lo stesso valore, mai da un vicino.
    Exact {
        read: ParameterValue,
        raw: String,
        distinct: Vec<ParameterValue>,
    },
    /// Conservato e riletto come dichiarato, senza confronti: il limite
    /// dichiarato dello SQL del chiamante, in cui il server assegna il valore
    /// esatto a una colonna piu stretta (`operand_admission`).
    Assigned { read: ParameterValue, raw: String },
    /// Rifiutato con questa categoria prima dell'invio; sul percorso di
    /// lettura basta il rifiuto prima dello stream (`Prepare` o `Validate`).
    Refused(ErrorCategory),
}

impl Expect {
    #[must_use]
    pub fn exact(read: ParameterValue, raw: impl Into<String>) -> Self {
        Self::Exact {
            read,
            raw: raw.into(),
            distinct: Vec::new(),
        }
    }

    #[must_use]
    pub fn distinct_from(self, value: ParameterValue) -> Self {
        match self {
            Self::Exact {
                read,
                raw,
                mut distinct,
            } => {
                distinct.push(value);
                Self::Exact {
                    read,
                    raw,
                    distinct,
                }
            }
            other => other,
        }
    }
}

/// Un caso: una variante, una colonna, un esito atteso, i percorsi su cui
/// vale e gli esiti che un percorso dichiara diversi.
#[derive(Debug, Clone)]
pub struct Case {
    pub label: &'static str,
    pub column: &'static str,
    pub value: ParameterValue,
    pub expect: Expect,
    pub paths: Vec<FidelityPath>,
    pub overrides: Vec<(FidelityPath, Expect)>,
}

impl Case {
    #[must_use]
    pub fn new(
        label: &'static str,
        column: &'static str,
        value: ParameterValue,
        expect: Expect,
    ) -> Self {
        Self {
            label,
            column,
            value,
            expect,
            paths: vec![
                FidelityPath::Native,
                FidelityPath::Portable,
                FidelityPath::Read,
            ],
            overrides: Vec::new(),
        }
    }

    #[must_use]
    pub fn only(mut self, paths: &[FidelityPath]) -> Self {
        self.paths = paths.to_vec();
        self
    }

    /// Un esito diverso su un percorso: tipicamente il portable, che legge il
    /// catalogo, rifiuta cio che lo SQL del chiamante ammette come limite
    /// dichiarato.
    #[must_use]
    pub fn on(mut self, path: FidelityPath, expect: Expect) -> Self {
        self.overrides.push((path, expect));
        self
    }

    fn expect_on(&self, path: FidelityPath) -> &Expect {
        self.overrides
            .iter()
            .find(|(candidate, _)| *candidate == path)
            .map_or(&self.expect, |(_, expect)| expect)
    }
}

/// Esito di un caso su un percorso; `Err` descrive la discrepanza. Le note
/// registrano i confronti che dipendono dalla collation.
#[derive(Debug, Clone)]
pub struct Outcome {
    pub label: &'static str,
    pub path: FidelityPath,
    pub result: std::result::Result<(), String>,
    pub notes: Vec<String>,
}

/// Il bersaglio della prova: provider, tabella sonda, statement nativi.
pub struct Target<'a> {
    pub provider: &'a dyn Provider,
    pub secret: &'a SecretString,
    /// I limiti di ogni percorso: ciascuno riceve un budget nuovo, perche
    /// la durata di un budget unico scadrebbe a meta della prova.
    pub limits: ResourceLimits,
    /// Tabella con una colonna intera `ID` e una colonna per tipo.
    pub table: ObjectRef,
    /// Il nome della colonna `ID` come la scrive il dialetto nel portable e
    /// nel filtro della lettura.
    pub id_column: &'static str,
    pub native: &'a (dyn Fn(NativeSql, &str) -> String + Sync),
    /// Lo SQL del chiamante di questo provider non conosce l'operando
    /// (limite dichiarato in `operand_admission`): i vicini di un'altra
    /// famiglia numerica o temporale si registrano invece di decidere.
    pub native_cross_type_declared: bool,
}

/// Esegue tutti i casi su tutti i percorsi dichiarati e ne raccoglie gli
/// esiti, senza fermarsi al primo.
pub async fn run(target: &Target<'_>, cases: &[Case]) -> Vec<Outcome> {
    let mut outcomes = Vec::new();
    for (index, case) in cases.iter().enumerate() {
        let id = i32::try_from(index).unwrap_or(i32::MAX).saturating_add(1);
        for path in &case.paths {
            let mut notes = Vec::new();
            let result = match path {
                FidelityPath::Native => native(target, case, id, &mut notes).await,
                FidelityPath::Portable => portable(target, case, id, &mut notes).await,
                FidelityPath::Read => read(target, case, id, &mut notes).await,
            };
            outcomes.push(Outcome {
                label: case.label,
                path: *path,
                result,
                notes,
            });
        }
    }
    outcomes
}

/// Le righe del resoconto, una per esito, e l'elenco delle discrepanze.
#[must_use]
pub fn report(outcomes: &[Outcome]) -> (String, Vec<String>) {
    let mut text = String::new();
    let mut failures = Vec::new();
    for outcome in outcomes {
        match &outcome.result {
            Ok(()) => {
                let _ = writeln!(text, "ok    {:?} {}", outcome.path, outcome.label);
            }
            Err(reason) => {
                failures.push(format!("{:?} {}: {reason}", outcome.path, outcome.label));
                let _ = writeln!(text, "FAIL  {:?} {}: {reason}", outcome.path, outcome.label);
            }
        }
        for note in &outcome.notes {
            let _ = writeln!(text, "      nota: {note}");
        }
    }
    (text, failures)
}

type Probe = std::result::Result<(), String>;

// ---------------------------------------------------------------------------
// Forma canonica e vicini
// ---------------------------------------------------------------------------

fn normalized_decimal(text: &str) -> String {
    let negative = text.starts_with('-');
    let unsigned = text.trim_start_matches(['-', '+']);
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let integer = integer.trim_start_matches('0');
    let fraction = fraction.trim_end_matches('0');
    if integer.is_empty() && fraction.is_empty() {
        return "0".to_owned();
    }
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    out.push_str(if integer.is_empty() { "0" } else { integer });
    if !fraction.is_empty() {
        out.push('.');
        out.push_str(fraction);
    }
    out
}

fn parse_timestamp(text: &str) -> Option<NaiveDateTime> {
    [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d-%H.%M.%S%.f",
    ]
    .iter()
    .find_map(|format| NaiveDateTime::parse_from_str(text, format).ok())
}

fn canonical_timestamp(value: NaiveDateTime) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
}

fn canonical_instant(text: &str) -> Option<String> {
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|value| canonical_timestamp(value.naive_utc()) + "Z")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02X}");
        out
    })
}

/// La forma canonica di un valore per il confronto con il decoder: interi e
/// decimali per valore, double per bit, temporali al microsecondo,
/// `TimestampTz` come istante UTC.
#[must_use]
pub fn canon(value: &ParameterValue) -> String {
    match value {
        ParameterValue::Null { .. } => "null".to_owned(),
        ParameterValue::Bool(flag) => format!("bool:{flag}"),
        ParameterValue::I32(number) => format!("num:{number}"),
        ParameterValue::I64(number) => format!("num:{number}"),
        ParameterValue::Decimal(text) => format!("num:{}", normalized_decimal(text)),
        ParameterValue::F64(number) => format!("f64:{:016x}", number.to_bits()),
        ParameterValue::String(text) | ParameterValue::Enum { label: text, .. } => {
            format!("text:{text}")
        }
        ParameterValue::Uuid(text) => format!("text:{}", text.to_ascii_lowercase()),
        ParameterValue::Bytes(bytes) | ParameterValue::Wkb { bytes, .. } => {
            format!("bytes:{}", hex(bytes))
        }
        ParameterValue::Date(text) => format!("date:{text}"),
        ParameterValue::Timestamp(text) => parse_timestamp(text).map_or_else(
            || format!("ts?:{text}"),
            |value| format!("ts:{}", canonical_timestamp(value)),
        ),
        ParameterValue::TimestampTz(text) => canonical_instant(text)
            .map_or_else(|| format!("tstz?:{text}"), |value| format!("tstz:{value}")),
        ParameterValue::Json(document) => format!("json:{document}"),
    }
}

fn arrow_text(text: &str, expected: &ParameterValue) -> String {
    match expected {
        ParameterValue::Json(_) => serde_json::from_str::<serde_json::Value>(text).map_or_else(
            |_| format!("text:{text}"),
            |document| format!("json:{document}"),
        ),
        ParameterValue::TimestampTz(_) => canonical_instant(text)
            .map_or_else(|| format!("text:{text}"), |value| format!("tstz:{value}")),
        ParameterValue::Uuid(_) => format!("text:{}", text.to_ascii_lowercase()),
        _ => format!("text:{text}"),
    }
}

/// La forma canonica di una cella Arrow, interpretata con il tipo atteso dove
/// Arrow porta testo (JSON, istanti, UUID).
fn arrow_canon(
    array: &dyn Array,
    row: usize,
    expected: &ParameterValue,
) -> std::result::Result<String, String> {
    if array.is_null(row) {
        return Ok("null".to_owned());
    }
    macro_rules! cell {
        ($kind:ty) => {
            array
                .as_any()
                .downcast_ref::<$kind>()
                .ok_or_else(|| "array Arrow di tipo inatteso".to_owned())?
                .value(row)
        };
    }
    Ok(match array.data_type() {
        DataType::Boolean => format!("bool:{}", cell!(BooleanArray)),
        DataType::Int8 => format!("num:{}", cell!(Int8Array)),
        DataType::Int16 => format!("num:{}", cell!(Int16Array)),
        DataType::Int32 => format!("num:{}", cell!(Int32Array)),
        DataType::Int64 => format!("num:{}", cell!(Int64Array)),
        DataType::UInt8 => format!("num:{}", cell!(UInt8Array)),
        DataType::UInt16 => format!("num:{}", cell!(UInt16Array)),
        DataType::UInt32 => format!("num:{}", cell!(UInt32Array)),
        DataType::UInt64 => format!("num:{}", cell!(UInt64Array)),
        DataType::Float32 => format!("f64:{:016x}", f64::from(cell!(Float32Array)).to_bits()),
        DataType::Float64 => format!("f64:{:016x}", cell!(Float64Array).to_bits()),
        DataType::Decimal128(_, scale) => {
            let value = cell!(Decimal128Array);
            let digits = value.unsigned_abs().to_string();
            let scale = usize::try_from(*scale).map_err(|_| "scala Arrow negativa".to_owned())?;
            let padded = format!("{digits:0>width$}", width = scale + 1);
            let (integer, fraction) = padded.split_at(padded.len() - scale);
            let sign = if value < 0 { "-" } else { "" };
            format!(
                "num:{}",
                normalized_decimal(&format!("{sign}{integer}.{fraction}"))
            )
        }
        DataType::Utf8 => arrow_text(cell!(StringArray), expected),
        DataType::LargeUtf8 => arrow_text(cell!(LargeStringArray), expected),
        DataType::Binary => format!("bytes:{}", hex(cell!(BinaryArray))),
        DataType::LargeBinary => format!("bytes:{}", hex(cell!(LargeBinaryArray))),
        DataType::Date32 => {
            let days = cell!(Date32Array);
            NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_signed(Duration::days(i64::from(days))))
                .map(|date| format!("date:{}", date.format("%Y-%m-%d")))
                .ok_or_else(|| "data Arrow fuori dominio".to_owned())?
        }
        DataType::Timestamp(_, zone) => {
            let micros = cell!(TimestampMicrosecondArray);
            let value = chrono::DateTime::from_timestamp_micros(micros)
                .ok_or_else(|| "timestamp Arrow fuori dominio".to_owned())?
                .naive_utc();
            if zone.is_some() {
                format!("tstz:{}Z", canonical_timestamp(value))
            } else {
                format!("ts:{}", canonical_timestamp(value))
            }
        }
        other => {
            return Err(format!(
                "tipo Arrow {other:?} senza forma canonica nell'oracolo"
            ))
        }
    })
}

/// La natura di un valore vicino.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Neighbour {
    /// Stessa variante: non deve mai ritrovare la riga.
    SameType,
    /// Un'altra famiglia numerica o temporale: non deve ritrovare la riga,
    /// salvo il limite dichiarato dello SQL del chiamante.
    CrossType,
    /// Spazi finali, maiuscole e zeri finali dei binari: semantica della
    /// collation e del padding del server, registrata.
    Collation,
}

/// I vicini di un valore, generati per famiglia.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn neighbours(value: &ParameterValue) -> Vec<(ParameterValue, Neighbour)> {
    use Neighbour::{Collation, CrossType, SameType};
    let mut out = Vec::new();
    match value {
        ParameterValue::F64(number) if number.is_finite() => {
            let bits = number.to_bits();
            for next in [bits.wrapping_add(1), bits.wrapping_sub(1)] {
                let candidate = f64::from_bits(next);
                if candidate.is_finite() && candidate.to_bits() != bits {
                    out.push((ParameterValue::F64(candidate), SameType));
                }
            }
            if let Some(exact) =
                plenora_database_core::operand_admission::exact_decimal_of_f64(*number)
            {
                let text = exact.text();
                let digits = text.trim_start_matches('-').replace('.', "");
                if digits.trim_start_matches('0').len() > 34 {
                    // L'espansione troncata: un decimale che arrotonda a
                    // questo double ma non e lui.
                    let cut = text.len() - (digits.trim_start_matches('0').len() - 34);
                    out.push((ParameterValue::Decimal(text[..cut].to_owned()), CrossType));
                }
            }
            if number.fract() == 0.0 && number.abs() < 9.2e18 {
                #[allow(clippy::cast_possible_truncation)]
                let integral = *number as i64;
                for delta in [1_i64, -1] {
                    if let Some(candidate) = integral.checked_add(delta) {
                        out.push((ParameterValue::I64(candidate), CrossType));
                    }
                }
            }
        }
        ParameterValue::I32(number) => {
            for delta in [1_i32, -1] {
                if let Some(candidate) = number.checked_add(delta) {
                    out.push((ParameterValue::I32(candidate), SameType));
                }
            }
        }
        ParameterValue::I64(number) => {
            for delta in [1_i64, -1] {
                if let Some(candidate) = number.checked_add(delta) {
                    out.push((ParameterValue::I64(candidate), SameType));
                }
            }
            #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
            let rounded = *number as f64;
            #[allow(clippy::cast_possible_truncation)]
            if rounded.is_finite() && (rounded as i128) != i128::from(*number) {
                // Il double piu vicino: un altro valore.
                out.push((ParameterValue::F64(rounded), CrossType));
            }
            out.push((
                ParameterValue::Decimal(format!("{number}.000000000000000000001")),
                CrossType,
            ));
        }
        ParameterValue::Decimal(text) => {
            let normalized = normalized_decimal(text);
            let tail = if normalized.contains('.') {
                "000001"
            } else {
                ".000001"
            };
            out.push((
                ParameterValue::Decimal(format!("{normalized}{tail}")),
                SameType,
            ));
            out.push((
                ParameterValue::Decimal(format!(
                    "{normalized}{}0000000000000000000001",
                    if normalized.contains('.') { "" } else { "." }
                )),
                SameType,
            ));
            if let Ok(parsed) = normalized.parse::<f64>() {
                let exact = plenora_database_core::operand_admission::exact_decimal_of_f64(parsed)
                    .map(|exact| exact.text());
                if exact.as_deref() != Some(normalized.as_str()) {
                    out.push((ParameterValue::F64(parsed), CrossType));
                }
            }
        }
        ParameterValue::Timestamp(text) => {
            if let Some(value) = parse_timestamp(text) {
                for delta in [
                    Duration::microseconds(1),
                    Duration::microseconds(-1),
                    Duration::seconds(1),
                ] {
                    if let Some(candidate) = value.checked_add_signed(delta) {
                        out.push((
                            ParameterValue::Timestamp(canonical_timestamp(candidate)),
                            SameType,
                        ));
                    }
                }
                if value.time() != chrono::NaiveTime::MIN {
                    out.push((
                        ParameterValue::Date(value.date().format("%Y-%m-%d").to_string()),
                        CrossType,
                    ));
                }
            }
        }
        ParameterValue::Date(text) => {
            if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
                if let Some(next) = date.succ_opt() {
                    out.push((
                        ParameterValue::Date(next.format("%Y-%m-%d").to_string()),
                        SameType,
                    ));
                }
                out.push((
                    ParameterValue::Timestamp(format!("{text}T00:00:00.000001")),
                    CrossType,
                ));
            }
        }
        ParameterValue::TimestampTz(text) => {
            if let Ok(value) = chrono::DateTime::parse_from_rfc3339(text) {
                if let Some(candidate) = value.checked_add_signed(Duration::microseconds(1)) {
                    out.push((
                        ParameterValue::TimestampTz(
                            candidate.to_rfc3339_opts(chrono::SecondsFormat::Micros, false),
                        ),
                        SameType,
                    ));
                }
                // Lo stesso orologio con un altro offset: un altro istante.
                let wall = value.naive_local();
                out.push((
                    ParameterValue::TimestampTz(format!("{}+00:00", canonical_timestamp(wall))),
                    SameType,
                ));
            }
        }
        ParameterValue::String(text) if !text.is_empty() => {
            out.push((ParameterValue::String(format!("{text}x")), SameType));
            out.push((ParameterValue::String(format!("{text} ")), Collation));
            let upper = text.to_uppercase();
            if &upper != text {
                out.push((ParameterValue::String(upper), Collation));
            }
        }
        ParameterValue::Bytes(bytes) => {
            // Uno zero finale: SQL Server confronta i binari completando il
            // piu corto con zeri (padding, come gli spazi di un testo).
            let mut padded = bytes.clone();
            padded.push(0);
            out.push((ParameterValue::Bytes(padded), Collation));
            let mut longer = bytes.clone();
            longer.push(0x5A);
            out.push((ParameterValue::Bytes(longer), SameType));
            let reversed = bytes.iter().rev().copied().collect::<Vec<_>>();
            if &reversed != bytes {
                out.push((ParameterValue::Bytes(reversed), SameType));
            }
        }
        ParameterValue::Bool(flag) => out.push((ParameterValue::Bool(!flag), SameType)),
        ParameterValue::Uuid(text) if text.len() == 36 => {
            let last = if text.ends_with('0') { '1' } else { '0' };
            out.push((
                ParameterValue::Uuid(format!("{}{last}", &text[..35])),
                SameType,
            ));
        }
        _ => {}
    }
    out
}

// ---------------------------------------------------------------------------
// Percorsi
// ---------------------------------------------------------------------------

fn budget(target: &Target<'_>) -> std::result::Result<ResourceBudget, String> {
    ResourceBudget::new(target.limits.clone())
        .map_err(|error| format!("budget: {}", describe(&error)))
}

async fn begin(target: &Target<'_>) -> std::result::Result<Box<dyn TransactionScope>, String> {
    let budget = budget(target)?;
    target
        .provider
        .begin_transaction(
            target.secret,
            &TransactionOptions::default(),
            &budget,
            &CancellationToken::new(),
        )
        .await
        .map_err(|error| format!("apertura transazione: {}", describe(&error)))
}

fn describe(error: &DatabaseError) -> String {
    format!("{:?}/{:?}: {}", error.category, error.phase, error.message)
}

fn refused(result: &std::result::Result<u64, DatabaseError>, category: ErrorCategory) -> Probe {
    match result {
        Err(error) if error.category == category && error.phase == ErrorPhase::Prepare => Ok(()),
        Err(error) => Err(format!(
            "rifiuto atteso {category:?}/Prepare, ottenuto {}",
            describe(error)
        )),
        Ok(_) => Err(format!(
            "rifiuto atteso {category:?}, il valore e stato scritto"
        )),
    }
}

fn count_of(rows: &[Row]) -> std::result::Result<u64, String> {
    let value = rows
        .first()
        .and_then(|row| row.values().first())
        .ok_or_else(|| "conteggio senza righe".to_owned())?;
    match value {
        ParameterValue::I32(count) => {
            u64::try_from(*count).map_err(|_| "conteggio negativo".into())
        }
        ParameterValue::I64(count) => {
            u64::try_from(*count).map_err(|_| "conteggio negativo".into())
        }
        ParameterValue::Decimal(text) => text
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("conteggio non intero: {text}")),
        other => Err(format!("conteggio di tipo inatteso: {other:?}")),
    }
}

async fn query_first(
    scope: &mut dyn TransactionScope,
    statement: Statement,
) -> std::result::Result<ParameterValue, String> {
    let rows = scope
        .query(&statement, &CancellationToken::new())
        .await
        .map_err(|error| format!("rilettura: {}", describe(&error)))?;
    rows.first()
        .and_then(|row| row.values().first())
        .cloned()
        .ok_or_else(|| "rilettura senza righe".to_owned())
}

/// La resa del server della riga `id`, confrontata con quella attesa.
async fn check_raw(
    target: &Target<'_>,
    scope: &mut dyn TransactionScope,
    case: &Case,
    id: i32,
    raw: &str,
) -> Probe {
    let stored = query_first(
        scope,
        Statement::new((target.native)(NativeSql::Raw, case.column))
            .with_params(vec![ParameterValue::I32(id)]),
    )
    .await?;
    match stored {
        ParameterValue::String(text) if text == raw => Ok(()),
        other => Err(format!(
            "conservato (resa del server) {other:?}, atteso {raw:?}"
        )),
    }
}

/// Un confronto atteso: il valore stesso ritrova la riga (un `NULL` no, per
/// la logica a tre valori), un vicino no.
struct Comparison {
    value: ParameterValue,
    expected: usize,
    kind: Neighbour,
}

fn comparisons(case: &Case, distinct: &[ParameterValue]) -> Vec<Comparison> {
    let own = usize::from(!matches!(case.value, ParameterValue::Null { .. }));
    let mut out = vec![Comparison {
        value: case.value.clone(),
        expected: own,
        kind: Neighbour::SameType,
    }];
    out.extend(distinct.iter().map(|value| Comparison {
        value: value.clone(),
        expected: 0,
        kind: Neighbour::SameType,
    }));
    out.extend(
        neighbours(&case.value)
            .into_iter()
            .map(|(value, kind)| Comparison {
                value,
                expected: 0,
                kind,
            }),
    );
    out
}

/// Il giudizio di un confronto: `Ok` se torna; i confronti di collation e
/// quelli coperti dal limite dichiarato diventano note.
fn judge(
    comparison: &Comparison,
    matched: Option<u64>,
    declared_cross_type: bool,
    notes: &mut Vec<String>,
    what: &str,
) -> Probe {
    let ok = matched.map_or(comparison.expected == 0, |matched| {
        usize::try_from(matched).ok() == Some(comparison.expected)
    });
    if ok {
        return Ok(());
    }
    let message = format!(
        "{what} con {:?}: {matched:?} righe, attese {}",
        comparison.value, comparison.expected
    );
    match comparison.kind {
        Neighbour::Collation => {
            notes.push(format!("collation: {message}"));
            Ok(())
        }
        Neighbour::CrossType if declared_cross_type => {
            notes.push(format!("limite dichiarato (tipi diversi): {message}"));
            Ok(())
        }
        _ => Err(message),
    }
}

async fn native(target: &Target<'_>, case: &Case, id: i32, notes: &mut Vec<String>) -> Probe {
    let mut scope = begin(target).await?;
    let probe = native_steps(target, scope.as_mut(), case, id, notes).await;
    let rollback = scope.rollback(&CancellationToken::new()).await;
    probe?;
    rollback.map_err(|error| format!("rollback: {}", describe(&error)))
}

async fn native_steps(
    target: &Target<'_>,
    scope: &mut dyn TransactionScope,
    case: &Case,
    id: i32,
    notes: &mut Vec<String>,
) -> Probe {
    let sql = |kind| (target.native)(kind, case.column);
    let inserted = scope
        .execute(
            &Statement::new(sql(NativeSql::Insert))
                .with_params(vec![ParameterValue::I32(id), case.value.clone()]),
            &CancellationToken::new(),
        )
        .await;
    let (read, raw, distinct, compare) = match case.expect_on(FidelityPath::Native) {
        Expect::Refused(category) => return refused(&inserted, *category),
        Expect::Assigned { read, raw } => (read, raw, &[][..], false),
        Expect::Exact {
            read,
            raw,
            distinct,
        } => (read, raw, distinct.as_slice(), true),
    };
    inserted.map_err(|error| format!("scrittura: {}", describe(&error)))?;
    let stored = query_first(
        scope,
        Statement::new(sql(NativeSql::Select)).with_params(vec![ParameterValue::I32(id)]),
    )
    .await?;
    // Il confronto e per valore (`canon`): la grafia del decoder non conta,
    // i bit di un double si.
    if canon(&stored) != canon(read) {
        return Err(format!("riletto {stored:?}, atteso {read:?}"));
    }
    check_raw(target, scope, case, id, raw).await?;
    if !compare {
        return Ok(());
    }
    for comparison in comparisons(case, distinct) {
        let matched = match scope
            .query(
                &Statement::new(sql(NativeSql::Count))
                    .with_params(vec![ParameterValue::I32(id), comparison.value.clone()]),
                &CancellationToken::new(),
            )
            .await
        {
            Ok(rows) => Some(count_of(&rows)?),
            Err(error) if error.phase == ErrorPhase::Prepare => None,
            Err(error) => {
                // Un errore dopo l'invio interrompe la transazione su alcuni
                // server: si registra e si chiude il percorso.
                return Err(format!(
                    "confronto con {:?}: {}",
                    comparison.value,
                    describe(&error)
                ));
            }
        };
        judge(
            &comparison,
            matched,
            target.native_cross_type_declared,
            notes,
            "col = ?",
        )?;
    }
    Ok(())
}

fn portable_table(target: &Target<'_>) -> TableRef {
    TableRef {
        schema: target.table.schema.clone(),
        name: target.table.object.clone(),
    }
}

async fn portable(target: &Target<'_>, case: &Case, id: i32, notes: &mut Vec<String>) -> Probe {
    let mut scope = begin(target).await?;
    let probe = portable_steps(target, scope.as_mut(), case, id, notes).await;
    let rollback = scope.rollback(&CancellationToken::new()).await;
    probe?;
    rollback.map_err(|error| format!("rollback: {}", describe(&error)))
}

async fn portable_steps(
    target: &Target<'_>,
    scope: &mut dyn TransactionScope,
    case: &Case,
    id: i32,
    notes: &mut Vec<String>,
) -> Probe {
    let insert = PortableStatement::Insert(InsertStatement {
        table: portable_table(target),
        columns: vec![target.id_column.to_owned(), case.column.to_owned()],
        values: vec![vec![
            Expression::literal(ParameterValue::I32(id)),
            Expression::literal(case.value.clone()),
        ]],
        returning: Vec::new(),
    });
    let inserted = execute_portable(scope, &insert, &CancellationToken::new()).await;
    let (raw, distinct) = match case.expect_on(FidelityPath::Portable) {
        Expect::Refused(category) => return refused(&inserted, *category),
        Expect::Exact { raw, distinct, .. } => (raw, distinct),
        Expect::Assigned { .. } => {
            return Err("Assigned vale soltanto sul percorso nativo".into());
        }
    };
    inserted.map_err(|error| format!("scrittura portable: {}", describe(&error)))?;
    check_raw(target, scope, case, id, raw).await?;
    for comparison in comparisons(case, distinct) {
        let select = PortableStatement::Select(SelectStatement {
            table: portable_table(target),
            projection: Projection::Columns(vec![target.id_column.to_owned()]),
            filter: Some(and(vec![
                eq(target.id_column, ParameterValue::I32(id)),
                eq(case.column, comparison.value.clone()),
            ])),
            order_by: Vec::new(),
            limit: None,
        });
        let matched =
            match execute_portable_returning(scope, &select, &CancellationToken::new()).await {
                Ok(rows) => Some(u64::try_from(rows.len()).unwrap_or(u64::MAX)),
                Err(error) if error.phase == ErrorPhase::Prepare => None,
                Err(error) => {
                    return Err(format!(
                        "confronto portable con {:?}: {}",
                        comparison.value,
                        describe(&error)
                    ))
                }
            };
        judge(&comparison, matched, false, notes, "eq portable")?;
    }
    Ok(())
}

async fn read(target: &Target<'_>, case: &Case, id: i32, notes: &mut Vec<String>) -> Probe {
    let (read_value, distinct) = match case.expect_on(FidelityPath::Read) {
        Expect::Exact { read, distinct, .. } => (read, distinct),
        Expect::Refused(category) => return read_refused(target, case, *category).await,
        Expect::Assigned { .. } => {
            return Err("Assigned vale soltanto sul percorso nativo".into());
        }
    };
    let mut scope = begin(target).await?;
    let mut inserted = scope
        .execute(
            &Statement::new((target.native)(NativeSql::Insert, case.column))
                .with_params(vec![ParameterValue::I32(id), case.value.clone()]),
            &CancellationToken::new(),
        )
        .await;
    // Un valore che lo SQL del chiamante rifiuta prima dell'invio (il limite
    // dichiarato) si scrive con il portable, che legge il catalogo.
    if matches!(&inserted, Err(error) if error.phase == ErrorPhase::Prepare) {
        let insert = PortableStatement::Insert(InsertStatement {
            table: portable_table(target),
            columns: vec![target.id_column.to_owned(), case.column.to_owned()],
            values: vec![vec![
                Expression::literal(ParameterValue::I32(id)),
                Expression::literal(case.value.clone()),
            ]],
            returning: Vec::new(),
        });
        inserted = execute_portable(scope.as_mut(), &insert, &CancellationToken::new()).await;
    }
    if let Err(error) = inserted {
        let _ = scope.rollback(&CancellationToken::new()).await;
        return Err(format!("scrittura: {}", describe(&error)));
    }
    scope
        .commit(&CancellationToken::new())
        .await
        .map_err(|error| format!("commit: {}", describe(&error)))?;
    let probe = read_steps(target, case, id, read_value, distinct, notes).await;
    let cleanup = delete(target, case.column, id).await;
    probe.and(cleanup)
}

async fn read_steps(
    target: &Target<'_>,
    case: &Case,
    id: i32,
    read_value: &ParameterValue,
    distinct: &[ParameterValue],
    notes: &mut Vec<String>,
) -> Probe {
    // Il decoder Arrow della colonna, senza filtro sul valore.
    let batches = read_rows(target, case.column, id, None).await?;
    let mut cells = Vec::new();
    for batch in &batches {
        let column = batch
            .column_by_name(case.column)
            .ok_or_else(|| "la lettura non rende la colonna del valore".to_owned())?;
        for row in 0..batch.num_rows() {
            cells.push(arrow_canon(column.as_ref(), row, read_value)?);
        }
    }
    let expected = canon(read_value);
    if cells != [expected.clone()] {
        return Err(format!("lettura Arrow {cells:?}, attesa [{expected:?}]"));
    }
    for comparison in comparisons(case, distinct) {
        let matched = match read_rows(target, case.column, id, Some(&comparison.value)).await {
            Ok(batches) => Some(
                batches
                    .iter()
                    .map(|batch| u64::try_from(batch.num_rows()).unwrap_or(u64::MAX))
                    .sum(),
            ),
            Err(reason) if reason.contains("/Prepare:") || reason.contains("/Validate:") => None,
            Err(reason) => return Err(reason),
        };
        judge(&comparison, matched, false, notes, "filtro di lettura")?;
    }
    Ok(())
}

async fn read_refused(target: &Target<'_>, case: &Case, category: ErrorCategory) -> Probe {
    // La lettura rifiuta prima di aprire lo stream; la categoria puo essere
    // quella del proprio validatore, la fase no.
    match read_rows(target, case.column, 0, Some(&case.value)).await {
        Err(reason) if reason.contains("/Prepare:") || reason.contains("/Validate:") => Ok(()),
        Err(reason) => Err(format!(
            "rifiuto atteso {category:?} in lettura, ottenuto {reason}"
        )),
        Ok(_) => Err(format!(
            "rifiuto atteso {category:?} in lettura, filtro eseguito"
        )),
    }
}

async fn read_rows(
    target: &Target<'_>,
    column: &str,
    id: i32,
    value: Option<&ParameterValue>,
) -> std::result::Result<Vec<RecordBatch>, String> {
    let id_filter = FilterExpression::Eq {
        field: target.id_column.to_owned(),
        parameter: "id".to_owned(),
    };
    let mut parameters = BTreeMap::from([("id".to_owned(), ParameterValue::I32(id))]);
    let filter = match value {
        Some(value) => {
            parameters.insert("value".to_owned(), value.clone());
            FilterExpression::And {
                args: vec![
                    id_filter,
                    FilterExpression::Eq {
                        field: column.to_owned(),
                        parameter: "value".to_owned(),
                    },
                ],
            }
        }
        None => id_filter,
    };
    let operation = ReadOperation {
        source: target.table.clone(),
        projection: vec![target.id_column.to_owned(), column.to_owned()],
        order_by: Vec::new(),
        row_limit: None,
        row_offset: None,
        filter: Some(filter),
        declared_crs: Vec::new(),
    };
    let parameters = ParameterBag::new(parameters);
    let cancellation = CancellationToken::new();
    let budget = budget(target)?;
    let mut stream = target
        .provider
        .read(
            target.secret,
            &operation,
            &parameters,
            &budget,
            &cancellation,
        )
        .await
        .map_err(|error| describe(&error))?;
    let mut batches = Vec::new();
    while let Some(batch) = stream
        .next_batch(&cancellation)
        .await
        .map_err(|error| describe(&error))?
    {
        batches.push(batch);
    }
    Ok(batches)
}

async fn delete(target: &Target<'_>, column: &str, id: i32) -> Probe {
    let mut scope = begin(target).await?;
    let deleted = scope
        .execute(
            &Statement::new((target.native)(NativeSql::Delete, column))
                .with_params(vec![ParameterValue::I32(id)]),
            &CancellationToken::new(),
        )
        .await;
    if let Err(error) = deleted {
        let _ = scope.rollback(&CancellationToken::new()).await;
        return Err(format!("pulizia: {}", describe(&error)));
    }
    scope
        .commit(&CancellationToken::new())
        .await
        .map(|_| ())
        .map_err(|error| format!("commit pulizia: {}", describe(&error)))
}

#[cfg(test)]
#[path = "parameter_fidelity_tests.rs"]
mod tests;
