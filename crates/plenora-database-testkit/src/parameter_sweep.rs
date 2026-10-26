//! Censimento **generato** della fedelta dei parametri: l'oracolo dei valori.
//!
//! Il censimento a casi ([`crate::parameter_fidelity`]) prova punti scelti a
//! mano, uno per famiglia; il terzo giro della #135 ha mostrato che cosi la
//! prova non vede le classi che stanno fra i punti (i decimali con |x| < 1 e
//! un numero dispari di zeri iniziali, la scala di `FLOAT(M, D)`, il testo
//! oltre la lunghezza o fuori dal charset). Qui i valori si **generano** per
//! famiglia — grandezze, segni, numero di cifre, zeri iniziali e finali,
//! confini di scala, ulp vicini, lunghezze da 0 a n + 1, spazi finali,
//! caratteri fuori dal piano di base e fuori dai charset non UTF-8, secondi
//! frazionari a ogni precisione — per **ogni** parametrizzazione di colonna
//! che il provider dichiara (precisione, scala, lunghezza, unita, charset),
//! su **ogni** percorso (SQL del chiamante, AST portable, lettura plan-based).
//!
//! L'oracolo non ha esiti scritti a mano e non usa la regola che prova
//! (`operand_admission`): un modello indipendente del tipo della colonna
//! ([`Kind`]) dice, per ogni valore, che cosa la colonna conserverebbe se il
//! valore vi entrasse esatto, oppure che non vi entra. Poi:
//!
//! - un valore **accettato** deve essere conservato come dice il modello,
//!   letto dalla **resa del server** ([`Render`]: esadecimale dei byte e dei
//!   bit, testo a precisione piena), non dai decoder del repository;
//! - ogni decoder del repository (la riga della transazione, la cella Arrow
//!   della lettura) deve rendere lo stesso valore che rende il server;
//! - `col = w` deve ritrovare la riga soltanto per i `w` che il modello
//!   conserva uguali (i vicini generati e i valori adiacenti dell'insieme);
//! - un valore **rifiutato** e sempre ammesso dall'oracolo (un errore non e
//!   silenzioso); il rifiuto di un valore che il modello conserva esatto si
//!   registra come nota (rifiuto per eccesso), non come guasto.
//!
//! I fatti del server che il repository dichiara invece di giudicare
//! ([`ServerFacts`]: il testo vuoto `NULL` di Oracle, il `-0.0` normalizzato,
//! il limite dello SQL del chiamante) diventano note, mai silenzio.

use crate::parameter_fidelity::{neighbours, FidelityPath, NativeSql, Neighbour, Target};
use chrono::{Duration, NaiveDate, NaiveDateTime};
use plenora_database_core::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use plenora_database_core::arrow::DataType;
use plenora_database_core::facade::{execute_portable, execute_portable_returning};
use plenora_database_core::plan::{FilterExpression, ReadOperation};
use plenora_database_core::portable::{
    and, eq, Expression, InsertStatement, PortableStatement, Projection, SelectStatement, TableRef,
};
use plenora_database_core::provider::{ParameterBag, ParameterValue as V};
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::transaction::{Statement, TransactionOptions, TransactionScope};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorPhase};
use std::collections::BTreeMap;
use std::fmt::Write as _;

// ---------------------------------------------------------------------------
// Il modello della colonna
// ---------------------------------------------------------------------------

/// L'unita in cui la colonna conta la lunghezza di un testo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    Characters,
    /// I byte del charset della colonna (UTF-8 per [`Set::Unicode`]).
    Bytes,
    Utf16,
}

/// I caratteri che la colonna conserva.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Set {
    Unicode,
    /// Il piano multilingue di base (`utf8mb3`).
    Bmp,
    Windows1252,
}

/// Il tipo di una colonna sonda, come lo dichiara il test e non come lo
/// legge il repository.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Boolean,
    Integer {
        min: i128,
        max: i128,
    },
    /// IEEE a 24 o 53 bit di mantissa.
    Float {
        bits: u32,
    },
    /// `FLOAT(M, D)` / `DOUBLE(M, D)`: il server arrotonda a `scale` cifre.
    ScaledFloat {
        bits: u32,
        precision: u32,
        scale: u32,
    },
    /// Precisione e scala; la scala puo essere negativa.
    Decimal {
        precision: u32,
        scale: i32,
    },
    /// Al piu `digits` cifre significative.
    UnboundedDecimal {
        digits: u32,
    },
    Text {
        length: Option<u32>,
        padded: bool,
        unit: Unit,
        set: Set,
    },
    Binary {
        length: Option<u32>,
        padded: bool,
    },
    Date,
    Timestamp {
        fraction: u32,
    },
    TimestampTz {
        fraction: u32,
    },
}

/// La resa del valore conservato che il server fa con lo SQL di
/// [`SweepTarget::raw`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Render {
    /// Testo decimale (`.05`, `-1.2300`, `1E+30`).
    Decimal,
    /// I bit del floating point in esadecimale.
    FloatHex {
        bits: u32,
        little_endian: bool,
    },
    /// Il testo piu corto che rilegge lo stesso floating point.
    FloatText {
        bits: u32,
    },
    /// Il testo come esadecimale dei suoi byte UTF-8.
    Utf8Hex,
    /// Il testo come esadecimale delle sue unita UTF-16 little-endian.
    Utf16LeHex,
    BytesHex,
    /// `YYYY-MM-DD`.
    Date,
    /// Data e ora con le cifre frazionarie della colonna.
    Timestamp,
    /// L'istante in UTC, come [`Render::Timestamp`].
    Instant,
    /// `true`/`false` o `1`/`0`.
    Boolean,
}

/// Una colonna della tabella sonda.
#[derive(Debug, Clone, Copy)]
pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
    pub render: Render,
}

/// I fatti del server che il repository dichiara e non giudica.
#[derive(Debug, Clone, Copy, Default)]
#[allow(clippy::struct_excessive_bools)]
pub struct ServerFacts {
    /// Il testo e il binario vuoti si conservano `NULL` (Oracle).
    pub empty_is_null: bool,
    /// `-0.0` si conserva `0.0` (Db2, `MariaDB`, Oracle `FLOAT`).
    pub normalizes_negative_zero: bool,
    /// Il server non conserva i subnormali (Db2).
    pub refuses_subnormals: bool,
    /// Il server rifiuta il carattere NUL (PostgreSQL).
    pub refuses_nul: bool,
    /// Lo spazio di riempimento di `CHAR(n)` non si vede nella resa
    /// (PostgreSQL e `MySQL` lo tolgono).
    pub hides_padding: bool,
    /// Lo SQL del chiamante non conosce l'operando: l'assegnazione a una
    /// colonna piu stretta e il confronto fra tipi diversi seguono il server
    /// (limite dichiarato in `operand_admission`).
    pub native_declared: bool,
    /// La lettura scrive tutti i valori in una transazione sola (Oracle: una
    /// transazione per valore esaurisce i cursori del fork, ORA-01000).
    pub single_transaction: bool,
}

/// Il bersaglio del censimento generato.
pub struct SweepTarget<'a> {
    pub target: Target<'a>,
    /// `SELECT <resa> FROM <tabella> WHERE ID = ?` per la colonna, con
    /// `NULL` per un valore nullo.
    pub raw: &'a (dyn Fn(&str) -> String + Sync),
    pub facts: ServerFacts,
    /// Soltanto i valori dell'insieme con questi indici (una sessione nuova
    /// per parte: il limite dei cursori del fork Oracle); `None` per tutti.
    /// Le parti si riuniscono con [`merge`].
    pub part: Option<std::ops::Range<usize>>,
}

impl SweepTarget<'_> {
    fn includes(&self, index: usize) -> bool {
        self.part.as_ref().is_none_or(|part| part.contains(&index))
    }
}

// ---------------------------------------------------------------------------
// Il valore conservato
// ---------------------------------------------------------------------------

/// Un numero esatto in forma normale: segno, cifre intere senza zeri
/// iniziali, cifre frazionarie senza zeri finali.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Num {
    negative: bool,
    integer: String,
    fraction: String,
}

impl Num {
    const fn zero() -> Self {
        Self {
            negative: false,
            integer: String::new(),
            fraction: String::new(),
        }
    }

    fn new(negative: bool, integer: &str, fraction: &str) -> Self {
        let integer = integer.trim_start_matches('0').to_owned();
        let fraction = fraction.trim_end_matches('0').to_owned();
        let zero = integer.is_empty() && fraction.is_empty();
        Self {
            negative: negative && !zero,
            integer,
            fraction,
        }
    }

    /// Un testo decimale con segno, punto e esponente facoltativi.
    fn parse(text: &str) -> Option<Self> {
        let text = text.trim();
        let (negative, unsigned) = match text.as_bytes().first()? {
            b'-' => (true, &text[1..]),
            b'+' => (false, &text[1..]),
            _ => (false, text),
        };
        let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
            Some(index) => (
                &unsigned[..index],
                unsigned[index + 1..].parse::<i64>().ok()?,
            ),
            None => (unsigned, 0),
        };
        let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        if integer.is_empty() && fraction.is_empty()
            || !integer
                .bytes()
                .chain(fraction.bytes())
                .all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let digits = format!("{integer}{fraction}");
        let point = i64::try_from(integer.len()).ok()? + exponent;
        if !(-400..=400).contains(&point) {
            return None;
        }
        let (integer, fraction) = if point <= 0 {
            (
                String::new(),
                "0".repeat(usize::try_from(-point).ok()?) + &digits,
            )
        } else {
            let point = usize::try_from(point).ok()?;
            if point >= digits.len() {
                (
                    digits.clone() + &"0".repeat(point - digits.len()),
                    String::new(),
                )
            } else {
                (digits[..point].to_owned(), digits[point..].to_owned())
            }
        };
        Some(Self::new(negative, &integer, &fraction))
    }

    fn of_i128(value: i128) -> Self {
        Self::new(value < 0, &value.unsigned_abs().to_string(), "")
    }

    fn text(&self) -> String {
        let mut out = String::new();
        if self.negative {
            out.push('-');
        }
        out.push_str(if self.integer.is_empty() {
            "0"
        } else {
            &self.integer
        });
        if !self.fraction.is_empty() {
            out.push('.');
            out.push_str(&self.fraction);
        }
        out
    }

    fn integer_digits(&self) -> u32 {
        u32::try_from(self.integer.len()).unwrap_or(u32::MAX)
    }

    fn fraction_digits(&self) -> u32 {
        u32::try_from(self.fraction.len()).unwrap_or(u32::MAX)
    }

    fn significant_digits(&self) -> u32 {
        let digits = format!("{}{}", self.integer, self.fraction);
        let trimmed = digits.trim_start_matches('0').trim_end_matches('0');
        u32::try_from(trimmed.len()).unwrap_or(u32::MAX)
    }

    fn as_i128(&self) -> Option<i128> {
        if !self.fraction.is_empty() {
            return None;
        }
        let magnitude = if self.integer.is_empty() {
            0
        } else {
            self.integer.parse::<i128>().ok()?
        };
        Some(if self.negative { -magnitude } else { magnitude })
    }
}

/// L'espansione decimale esatta di un double: `m * 2^e` in base 10, con
/// aritmetica intera a limbi di 10^9 (indipendente da quella del core).
#[must_use]
fn exact_f64(value: f64) -> Option<Num> {
    if !value.is_finite() {
        return None;
    }
    let bits = value.to_bits();
    let negative = bits >> 63 == 1;
    let exponent = i64::try_from((bits >> 52) & 0x7FF).ok()?;
    let fraction = bits & ((1_u64 << 52) - 1);
    let (mantissa, exponent) = if exponent == 0 {
        (fraction, -1074)
    } else {
        (fraction | (1_u64 << 52), exponent - 1075)
    };
    if mantissa == 0 {
        return Some(Num::zero());
    }
    // limbi little-endian in base 10^9
    let mut limbs: Vec<u64> = vec![mantissa % 1_000_000_000, mantissa / 1_000_000_000];
    let multiply = |limbs: &mut Vec<u64>, factor: u64| {
        let mut carry = 0_u64;
        for limb in limbs.iter_mut() {
            let product = *limb * factor + carry;
            *limb = product % 1_000_000_000;
            carry = product / 1_000_000_000;
        }
        while carry > 0 {
            limbs.push(carry % 1_000_000_000);
            carry /= 1_000_000_000;
        }
    };
    let (shift, point) = if exponent >= 0 {
        let mut remaining = exponent;
        while remaining > 0 {
            let step = remaining.min(29);
            multiply(&mut limbs, 1_u64 << step);
            remaining -= step;
        }
        (0_usize, 0_usize)
    } else {
        let powers = usize::try_from(-exponent).ok()?;
        let mut remaining = powers;
        while remaining > 0 {
            let step = remaining.min(13);
            multiply(&mut limbs, 5_u64.pow(u32::try_from(step).ok()?));
            remaining -= step;
        }
        (powers, powers)
    };
    let _ = shift;
    let mut digits = String::new();
    for (index, limb) in limbs.iter().rev().enumerate() {
        if index == 0 {
            let _ = write!(digits, "{limb}");
        } else {
            let _ = write!(digits, "{limb:09}");
        }
    }
    let digits = digits.trim_start_matches('0').to_owned();
    let (integer, fraction) = if point >= digits.len() {
        (String::new(), "0".repeat(point - digits.len()) + &digits)
    } else {
        (
            digits[..digits.len() - point].to_owned(),
            digits[digits.len() - point..].to_owned(),
        )
    };
    Some(Num::new(negative, &integer, &fraction))
}

/// Il valore che la colonna conserva.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stored {
    Null,
    Bool(bool),
    /// Il valore esatto e, per un floating point, i bit del double (un `f32`
    /// allargato): il segno di zero conta.
    Number(Num, Option<u64>),
    Text(String),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Instant(NaiveDateTime),
    /// Un infinito o un NaN che il server ha conservato (i bit del double).
    NonFinite(u64),
}

impl Stored {
    fn float(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return Some(Self::NonFinite(value.to_bits()));
        }
        Some(Self::Number(exact_f64(value)?, Some(value.to_bits())))
    }

    fn describe(&self) -> String {
        match self {
            Self::Null => "NULL".to_owned(),
            Self::Bool(flag) => flag.to_string(),
            Self::Number(number, Some(bits)) => format!("{} ({bits:016x})", number.text()),
            Self::Number(number, None) => number.text(),
            Self::Text(text) => format!("{text:?}"),
            Self::Bytes(bytes) => format!("0x{}", hex(bytes)),
            Self::Date(date) => date.to_string(),
            Self::Timestamp(value) => value.format("%Y-%m-%dT%H:%M:%S%.9f").to_string(),
            Self::Instant(value) => value.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string(),
            Self::NonFinite(bits) => format!("{} ({bits:016x})", f64::from_bits(*bits)),
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02X}");
        out
    })
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    let text = text.trim();
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(text.get(index..index + 2)?, 16).ok())
        .collect()
}

/// Lo stesso valore conservato, con la semantica del padding di `CHAR(n)`
/// quando il server lo toglie dalla resa.
fn same(left: &Stored, right: &Stored, kind: Kind, facts: ServerFacts) -> bool {
    match (left, right, kind) {
        (Stored::Text(left), Stored::Text(right), Kind::Text { padded: true, .. })
            if facts.hides_padding =>
        {
            left.trim_end_matches(' ') == right.trim_end_matches(' ')
        }
        (Stored::Number(left, Some(a)), Stored::Number(right, Some(b)), _) => {
            left == right && a == b
        }
        (Stored::Number(left, _), Stored::Number(right, _), _) => left == right,
        _ => left == right,
    }
}

// ---------------------------------------------------------------------------
// Il modello: che cosa la colonna conserverebbe
// ---------------------------------------------------------------------------

/// Perche un valore non entra esatto nella colonna, o che cosa vi diventa.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Model {
    /// Conservato esatto come questo valore.
    Exact(Stored),
    /// Non entra esatto: accettarlo e un guasto (o un limite dichiarato).
    Inexact,
    /// Un fatto dichiarato del server: accettarlo e una nota.
    Declared(&'static str),
}

fn exact_number(value: &V) -> Option<(Num, Option<f64>)> {
    match value {
        V::I32(number) => Some((Num::of_i128(i128::from(*number)), None)),
        V::I64(number) => Some((Num::of_i128(i128::from(*number)), None)),
        V::F64(number) => Some((exact_f64(*number)?, Some(*number))),
        V::Decimal(text) => Some((Num::parse(text)?, None)),
        _ => None,
    }
}

/// Il floating point a `bits` bit che rappresenta esatto `number`.
fn float_of(number: &Num, bits: u32) -> Option<f64> {
    let text = number.text();
    let value = if bits <= 24 {
        f64::from(text.parse::<f32>().ok()?)
    } else {
        text.parse::<f64>().ok()?
    };
    (exact_f64(value).as_ref() == Some(number)).then_some(value)
}

fn text_length(text: &str, unit: Unit, set: Set) -> usize {
    match (unit, set) {
        (Unit::Characters, _) | (Unit::Bytes, Set::Windows1252) => text.chars().count(),
        (Unit::Utf16, _) => text.encode_utf16().count(),
        (Unit::Bytes, _) => text.len(),
    }
}

const WINDOWS_1252: [char; 27] = [
    '\u{20AC}', '\u{201A}', '\u{0192}', '\u{201E}', '\u{2026}', '\u{2020}', '\u{2021}', '\u{02C6}',
    '\u{2030}', '\u{0160}', '\u{2039}', '\u{0152}', '\u{017D}', '\u{2018}', '\u{2019}', '\u{201C}',
    '\u{201D}', '\u{2022}', '\u{2013}', '\u{2014}', '\u{02DC}', '\u{2122}', '\u{0161}', '\u{203A}',
    '\u{0153}', '\u{017E}', '\u{0178}',
];

fn holds(set: Set, character: char) -> bool {
    match set {
        Set::Unicode => true,
        Set::Bmp => u32::from(character) <= 0xFFFF,
        Set::Windows1252 => {
            u32::from(character) < 0x80
                || (0xA0..=0xFF).contains(&u32::from(character))
                || WINDOWS_1252.contains(&character)
        }
    }
}

fn parse_timestamp(text: &str) -> Option<NaiveDateTime> {
    let text = text.trim();
    // Db2 rende fino a 12 cifre: oltre le nove devono essere zeri.
    let text = match text.split_once('.') {
        Some((head, fraction)) if fraction.len() > 9 => {
            if !fraction[9..].bytes().all(|b| b == b'0') {
                return None;
            }
            format!("{head}.{}", &fraction[..9])
        }
        _ => text.to_owned(),
    };
    [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d-%H.%M.%S%.f",
        "%Y-%m-%d-%H.%M.%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ]
    .iter()
    .find_map(|format| NaiveDateTime::parse_from_str(&text, format).ok())
}

fn fraction_digits(text: &str) -> u32 {
    text.split_once('.').map_or(0, |(_, rest)| {
        let digits = rest
            .bytes()
            .take_while(u8::is_ascii_digit)
            .collect::<Vec<_>>();
        let significant = digits.iter().rposition(|b| *b != b'0').map_or(0, |i| i + 1);
        u32::try_from(significant).unwrap_or(u32::MAX)
    })
}

/// Il modello: che cosa conserva `kind` di `value`.
#[allow(clippy::too_many_lines)]
fn model(value: &V, kind: Kind, facts: ServerFacts) -> Model {
    if matches!(value, V::Null { .. }) {
        return Model::Exact(Stored::Null);
    }
    match kind {
        Kind::Boolean => match value {
            V::Bool(flag) => Model::Exact(Stored::Bool(*flag)),
            _ => Model::Inexact,
        },
        Kind::Integer { min, max } => match exact_number(value) {
            Some((number, _)) => match number.as_i128() {
                Some(integer) if (min..=max).contains(&integer) => {
                    Model::Exact(Stored::Number(number, None))
                }
                _ => Model::Inexact,
            },
            None => Model::Inexact,
        },
        Kind::Float { bits } => {
            let Some((number, own)) = exact_number(value) else {
                return Model::Inexact;
            };
            if let Some(own) = own {
                if own == 0.0 && own.is_sign_negative() {
                    return if facts.normalizes_negative_zero {
                        Model::Declared("-0.0 normalizzato dal server")
                    } else {
                        Stored::float(own).map_or(Model::Inexact, Model::Exact)
                    };
                }
                if own != 0.0 && own.abs() < f64::MIN_POSITIVE && facts.refuses_subnormals {
                    return Model::Declared("subnormale non conservato dal server");
                }
            }
            match float_of(&number, bits) {
                Some(float)
                    if bits <= 24
                        && float.abs() < f64::from(f32::MIN_POSITIVE)
                        && float != 0.0
                        && facts.refuses_subnormals =>
                {
                    Model::Declared("subnormale non conservato dal server")
                }
                Some(float) => float_model(float, own.is_some()),
                None => Model::Inexact,
            }
        }
        Kind::ScaledFloat {
            bits,
            precision,
            scale,
        } => {
            let Some((number, _)) = exact_number(value) else {
                return Model::Inexact;
            };
            let Some(float) = float_of(&number, bits) else {
                return Model::Inexact;
            };
            // Il server arrotonda a `scale` cifre con l'aritmetica dei double:
            // il valore resta lui soltanto se e esatto con `scale` cifre e
            // `x * 10^scale` e un intero esatto (al piu 15 cifre). Misurato:
            // `-0.1` in `DOUBLE(10, 2)` diventava `-0.09999999999999998`.
            let exact_float = exact_f64(float).unwrap_or_else(Num::zero);
            if exact_float.fraction_digits() <= scale
                && exact_float.integer_digits() <= precision.saturating_sub(scale)
                && exact_float.integer_digits() + scale <= 15
            {
                if float == 0.0 && float.is_sign_negative() && facts.normalizes_negative_zero {
                    return Model::Declared("-0.0 normalizzato dal server");
                }
                float_model(float, matches!(value, V::F64(_)))
            } else {
                Model::Inexact
            }
        }
        Kind::Decimal { precision, scale } => {
            let Some((number, _)) = exact_number(value) else {
                return Model::Inexact;
            };
            let room = i64::from(precision) - i64::from(scale);
            let fits = if scale >= 0 {
                number.fraction_digits() <= scale.unsigned_abs()
                    && i64::from(number.integer_digits()) <= room
            } else {
                let zeros = usize::try_from(scale.unsigned_abs()).unwrap_or(usize::MAX);
                number.fraction.is_empty()
                    && (number.integer.is_empty()
                        || (number.integer.len() > zeros
                            && number.integer.bytes().rev().take(zeros).all(|b| b == b'0')))
                    && i64::from(number.integer_digits()) <= room
            };
            if fits {
                Model::Exact(Stored::Number(number, None))
            } else {
                Model::Inexact
            }
        }
        Kind::UnboundedDecimal { digits } => match exact_number(value) {
            Some((number, _)) if number.significant_digits() <= digits => {
                Model::Exact(Stored::Number(number, None))
            }
            _ => Model::Inexact,
        },
        Kind::Text {
            length,
            padded,
            unit,
            set,
        } => {
            let text = match value {
                V::String(text)
                | V::Enum { label: text, .. }
                | V::Uuid(text)
                | V::Decimal(text)
                | V::Date(text)
                | V::Timestamp(text) => text.clone(),
                _ => return Model::Inexact,
            };
            if text.is_empty() && facts.empty_is_null {
                return Model::Declared("testo vuoto conservato NULL dal server");
            }
            if text.contains('\0') && facts.refuses_nul {
                return Model::Declared("NUL rifiutato dal server");
            }
            if !text.chars().all(|character| holds(set, character)) {
                return Model::Inexact;
            }
            let count = text_length(&text, unit, set);
            let length = length.map(|length| usize::try_from(length).unwrap_or(usize::MAX));
            if length.is_some_and(|length| count > length) {
                return Model::Inexact;
            }
            let stored = match length {
                Some(length) if padded => {
                    let pad = match unit {
                        Unit::Characters | Unit::Utf16 | Unit::Bytes => length - count,
                    };
                    format!("{text}{}", " ".repeat(pad))
                }
                _ => text,
            };
            Model::Exact(Stored::Text(stored))
        }
        Kind::Binary { length, padded } => {
            let V::Bytes(bytes) = value else {
                return Model::Inexact;
            };
            if bytes.is_empty() && facts.empty_is_null {
                return Model::Declared("binario vuoto conservato NULL dal server");
            }
            let length = length.map(|length| usize::try_from(length).unwrap_or(usize::MAX));
            if length.is_some_and(|length| bytes.len() > length) {
                return Model::Inexact;
            }
            let mut stored = bytes.clone();
            if let (Some(length), true) = (length, padded) {
                stored.resize(length, 0);
            }
            Model::Exact(Stored::Bytes(stored))
        }
        Kind::Date => match value {
            V::Date(text) => NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .map_or(Model::Inexact, |date| Model::Exact(Stored::Date(date))),
            V::Timestamp(text) => match parse_timestamp(text) {
                Some(stamp) if stamp.time() == chrono::NaiveTime::MIN => {
                    Model::Exact(Stored::Date(stamp.date()))
                }
                _ => Model::Inexact,
            },
            _ => Model::Inexact,
        },
        Kind::Timestamp { fraction } => match value {
            V::Timestamp(text) => match parse_timestamp(text) {
                // Il repository lega al microsecondo.
                Some(stamp) if fraction_digits(text) <= fraction.min(6) => {
                    Model::Exact(Stored::Timestamp(stamp))
                }
                _ => Model::Inexact,
            },
            V::Date(text) => NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .ok()
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .map_or(Model::Inexact, |stamp| {
                    Model::Exact(Stored::Timestamp(stamp))
                }),
            _ => Model::Inexact,
        },
        Kind::TimestampTz { fraction } => match value {
            V::TimestampTz(text) => match chrono::DateTime::parse_from_rfc3339(text) {
                Ok(instant) if fraction_digits(text) <= fraction.min(6) => {
                    Model::Exact(Stored::Instant(instant.naive_utc()))
                }
                _ => Model::Inexact,
            },
            _ => Model::Inexact,
        },
    }
}

/// Il floating point conservato: con i bit (il segno di zero) se il valore
/// legato e un double, per valore se e un intero o un decimale, in cui lo
/// zero non ha segno.
fn float_model(float: f64, bits: bool) -> Model {
    match (Stored::float(float), bits) {
        (Some(stored), true) => Model::Exact(stored),
        (Some(Stored::Number(number, _)), false) => Model::Exact(Stored::Number(number, None)),
        _ => Model::Inexact,
    }
}

/// Il floating point piu vicino a un decimale (l'arrotondamento del server).
fn float_of_nearest(number: &Num, bits: u32) -> Option<f64> {
    let text = number.text();
    if bits <= 24 {
        text.parse::<f32>().ok().map(f64::from)
    } else {
        text.parse::<f64>().ok()
    }
}

// ---------------------------------------------------------------------------
// I valori generati
// ---------------------------------------------------------------------------

const fn pow10(exponent: u32) -> i128 {
    10_i128.pow(exponent)
}

fn decimal_text(digits: i128, scale: u32) -> String {
    let negative = digits < 0;
    let digits = digits.unsigned_abs().to_string();
    let scale = usize::try_from(scale).unwrap_or(0);
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (integer, fraction) = padded.split_at(padded.len() - scale);
    let sign = if negative { "-" } else { "" };
    if fraction.is_empty() {
        format!("{sign}{integer}")
    } else {
        format!("{sign}{integer}.{fraction}")
    }
}

/// I valori numerici: interi, double e decimali per grandezza, segno,
/// cifre, zeri iniziali (pari e dispari) e finali, confini della colonna.
#[allow(clippy::too_many_lines)]
fn numeric_values(kind: Kind) -> Vec<V> {
    let mut out = Vec::new();
    for number in [0_i64, 1, -1, 7, 10, -10, 99, 100, 12_345, -98_765] {
        out.push(V::I64(number));
    }
    for number in [i32::MAX, i32::MIN, 1_000_000] {
        out.push(V::I32(number));
    }
    for number in [
        1_i64 << 31,
        1_i64 << 53,
        (1_i64 << 53) + 1,
        1_i64 << 60,
        i64::MAX,
        i64::MIN,
        16_777_217,
    ] {
        out.push(V::I64(number));
    }
    for float in [
        0.0,
        -0.0,
        0.1,
        -0.1,
        0.5,
        0.25,
        1.25,
        2.75,
        1.5,
        -2.5,
        1.0 / 3.0,
        1e-7,
        123_456.789,
        9_007_199_254_740_992.0,
        16_777_217.0,
        f64::from(f32::MAX),
        f64::from(0.1_f32),
        1e300,
        5e-324,
        f64::MAX,
        f64::from_bits(0.1_f64.to_bits() + 1),
    ] {
        out.push(V::F64(float));
    }
    // Zeri iniziali pari e dispari, positivi e negativi; zeri finali.
    for text in [
        "0.5",
        "0.05",
        "0.005",
        "0.0005",
        "0.00005",
        "0.000005",
        "-0.05",
        "-0.005",
        "-0.0005",
        "0.15",
        "0.015",
        "0.0105",
        "1.2300",
        "100.00",
        "0.0",
        "-0",
        "2.5",
        "1.25",
        "2.75",
        "0.1",
        "12345.6789",
        "12345.67891",
        "9007199254740993",
        "16777217",
        "1e2",
        "1.5E-3",
    ] {
        out.push(V::Decimal(text.to_owned()));
    }
    // Grandezze, da 1e-12 a 1e30.
    for exponent in (-12_i32..=30).step_by(3) {
        out.push(V::Decimal(if exponent >= 0 {
            format!("1{}", "0".repeat(usize::try_from(exponent).unwrap_or(0)))
        } else {
            format!(
                "0.{}1",
                "0".repeat(usize::try_from(-exponent - 1).unwrap_or(0))
            )
        }));
    }
    match kind {
        Kind::Integer { min, max } => {
            for bound in [min, max, min - 1, max + 1] {
                if let Ok(bound) = i64::try_from(bound) {
                    out.push(V::I64(bound));
                } else {
                    out.push(V::Decimal(bound.to_string()));
                }
            }
        }
        Kind::Decimal { precision, scale } if scale >= 0 => {
            let scale = scale.unsigned_abs();
            let largest = pow10(precision) - 1;
            for digits in [largest, -largest, largest + 1, 1, -1] {
                out.push(V::Decimal(decimal_text(digits, scale)));
            }
            // Una cifra oltre la scala, e la scala piena.
            out.push(V::Decimal(decimal_text(12_345, scale + 1)));
            out.push(V::Decimal(decimal_text(12_345, scale)));
            if scale > 0 {
                out.push(V::Decimal(format!(
                    "0.{}",
                    "0".repeat(usize::try_from(scale - 1).unwrap_or(0)) + "1"
                )));
                out.push(V::Decimal(format!(
                    "0.{}",
                    "0".repeat(usize::try_from(scale).unwrap_or(0)) + "1"
                )));
            }
        }
        Kind::Decimal { precision, scale } => {
            let rounding = scale.unsigned_abs();
            let unit = pow10(rounding);
            for multiple in [1_i128, 123, -45, pow10(precision) - 1, pow10(precision)] {
                out.push(V::Decimal((multiple * unit).to_string()));
            }
            out.push(V::Decimal((unit + 1).to_string()));
            out.push(V::Decimal((unit / 2).to_string()));
        }
        Kind::UnboundedDecimal { digits } => {
            let full = "9".repeat(usize::try_from(digits).unwrap_or(0));
            out.push(V::Decimal(format!("0.{full}")));
            out.push(V::Decimal(format!("0.{full}9")));
            out.push(V::Decimal(format!("{full}.5")));
        }
        Kind::ScaledFloat { scale, .. } => {
            for digits in [15_i128, 125, 1_234_567, 25] {
                out.push(V::Decimal(decimal_text(digits, scale)));
                out.push(V::Decimal(decimal_text(digits, scale + 1)));
            }
        }
        _ => {}
    }
    out
}

fn text_values(kind: Kind) -> Vec<V> {
    let mut out: Vec<V> = [
        "",
        "a",
        "ab",
        "abc",
        "abcd",
        "abc ",
        "abc   ",
        " ab",
        "AbC",
        "\u{E8}",
        "a\u{E8}",
        "\u{20AC}",
        "\u{142}",
        "\u{65E5}",
        "\u{1F600}",
        "a\u{1F600}",
        "e\u{301}",
        "\u{142}\u{119}\u{65E5}x",
        "a\0b",
        "caff\u{E8}",
    ]
    .iter()
    .map(|text| V::String((*text).to_owned()))
    .collect();
    if let Kind::Text {
        length: Some(length),
        ..
    } = kind
    {
        let length = usize::try_from(length).unwrap_or(0);
        for count in [length.saturating_sub(1), length, length + 1] {
            out.push(V::String("x".repeat(count)));
            out.push(V::String(format!("{} ", "x".repeat(count))));
            out.push(V::String("\u{E8}".repeat(count)));
        }
    }
    out.push(V::Decimal("1.50".to_owned()));
    out.push(V::Date("2024-01-01".to_owned()));
    out
}

fn binary_values(kind: Kind) -> Vec<V> {
    let mut values: Vec<Vec<u8>> = vec![
        Vec::new(),
        vec![0],
        vec![0xAB, 0x01],
        vec![0xAB, 0x01, 0x00],
        vec![0x00, 0xFF, 0x41, 0x7F, 0x80],
    ];
    if let Kind::Binary {
        length: Some(length),
        ..
    } = kind
    {
        let length = usize::try_from(length).unwrap_or(0);
        for count in [length.saturating_sub(1), length, length + 1] {
            values.push(vec![0x5A; count]);
        }
    }
    values.into_iter().map(V::Bytes).collect()
}

fn temporal_values(kind: Kind) -> Vec<V> {
    let mut out = Vec::new();
    for date in [
        "2024-02-29",
        "1970-01-01",
        "1999-12-31",
        "2000-01-01",
        "9999-12-31",
    ] {
        out.push(V::Date(date.to_owned()));
    }
    let fractions = [
        "",
        ".1",
        ".12",
        ".123",
        ".1234",
        ".12345",
        ".123456",
        ".1234567",
        ".12345678",
        ".123456789",
        ".100",
        ".000001",
        ".5",
    ];
    for fraction in fractions {
        out.push(V::Timestamp(format!("2024-01-01T10:20:30{fraction}")));
    }
    for stamp in [
        "2024-02-29T00:00:00",
        "1999-12-31T23:59:59.999999",
        "2024-01-01T00:00:00.000001",
    ] {
        out.push(V::Timestamp(stamp.to_owned()));
    }
    if matches!(kind, Kind::TimestampTz { .. }) {
        for fraction in ["", ".123", ".123456", ".1234567"] {
            for offset in ["+02:00", "-05:30", "+00:00"] {
                out.push(V::TimestampTz(format!(
                    "2024-01-01T10:20:30{fraction}{offset}"
                )));
            }
        }
    }
    out
}

/// L'insieme generato per la colonna.
#[must_use]
pub fn values(kind: Kind) -> Vec<V> {
    match kind {
        Kind::Boolean => vec![V::Bool(true), V::Bool(false)],
        Kind::Integer { .. }
        | Kind::Float { .. }
        | Kind::ScaledFloat { .. }
        | Kind::Decimal { .. }
        | Kind::UnboundedDecimal { .. } => numeric_values(kind),
        Kind::Text { .. } => text_values(kind),
        Kind::Binary { .. } => binary_values(kind),
        Kind::Date | Kind::Timestamp { .. } | Kind::TimestampTz { .. } => temporal_values(kind),
    }
}

// ---------------------------------------------------------------------------
// Le rese: server, riga della transazione, cella Arrow
// ---------------------------------------------------------------------------

fn parse_float(text: &str, bits: u32) -> Option<Stored> {
    let text = text.trim();
    let value = if bits <= 24 {
        f64::from(text.parse::<f32>().ok()?)
    } else {
        text.parse::<f64>().ok()?
    };
    Stored::float(value)
}

/// La resa del server, letta nel dominio della colonna.
fn parse_render(value: &V, render: Render) -> Option<Stored> {
    let text = match value {
        V::Null { .. } => return Some(Stored::Null),
        V::String(text) | V::Decimal(text) => text.clone(),
        V::I32(number) => number.to_string(),
        V::I64(number) => number.to_string(),
        V::Bool(flag) => return Some(Stored::Bool(*flag)),
        _ => return None,
    };
    match render {
        Render::Decimal => Num::parse(&text).map(|number| Stored::Number(number, None)),
        Render::FloatHex {
            bits,
            little_endian,
        } => {
            let mut bytes = unhex(&text)?;
            if little_endian {
                bytes.reverse();
            }
            if bits <= 24 {
                let raw: [u8; 4] = bytes.try_into().ok()?;
                Stored::float(f64::from(f32::from_be_bytes(raw)))
            } else {
                let raw: [u8; 8] = bytes.try_into().ok()?;
                Stored::float(f64::from_be_bytes(raw))
            }
        }
        Render::FloatText { bits } => parse_float(&text, bits),
        Render::Utf8Hex => String::from_utf8(unhex(&text)?).ok().map(Stored::Text),
        Render::Utf16LeHex => {
            let bytes = unhex(&text)?;
            let (pairs, rest) = bytes.as_chunks::<2>();
            if !rest.is_empty() {
                return None;
            }
            char::decode_utf16(pairs.iter().map(|pair| u16::from_le_bytes(*pair)))
                .collect::<Result<String, _>>()
                .ok()
                .map(Stored::Text)
        }
        Render::BytesHex => unhex(&text).map(Stored::Bytes),
        Render::Date => NaiveDate::parse_from_str(text.trim(), "%Y-%m-%d")
            .ok()
            .map(Stored::Date),
        Render::Timestamp => parse_timestamp(&text).map(Stored::Timestamp),
        Render::Instant => parse_timestamp(&text).map(Stored::Instant),
        Render::Boolean => match text.trim() {
            "true" | "1" | "t" | "TRUE" => Some(Stored::Bool(true)),
            "false" | "0" | "f" | "FALSE" => Some(Stored::Bool(false)),
            _ => None,
        },
    }
}

/// Il valore di una riga della transazione (il decoder del repository).
fn stored_of_value(value: &V, kind: Kind) -> Option<Stored> {
    let numeric = |number: Num| match kind {
        Kind::Float { bits } | Kind::ScaledFloat { bits, .. } => {
            float_of_nearest(&number, bits).and_then(Stored::float)
        }
        _ => Some(Stored::Number(number, None)),
    };
    match (value, kind) {
        (V::Null { .. }, _) => Some(Stored::Null),
        (V::Bool(flag), _) => Some(Stored::Bool(*flag)),
        (V::I32(number), Kind::Boolean) => Some(Stored::Bool(*number != 0)),
        (V::I64(number), Kind::Boolean) => Some(Stored::Bool(*number != 0)),
        (V::I32(number), _) => numeric(Num::of_i128(i128::from(*number))),
        (V::I64(number), _) => numeric(Num::of_i128(i128::from(*number))),
        (V::F64(number), _) => Stored::float(*number),
        (V::Decimal(text), _)
        | (
            V::String(text),
            Kind::Integer { .. } | Kind::Decimal { .. } | Kind::UnboundedDecimal { .. },
        ) => numeric(Num::parse(text)?),
        // I temporali che il decoder rende come testo (il `datetimeoffset` di
        // SQL Server nella lettura Arrow).
        (V::String(text), Kind::TimestampTz { .. }) => chrono::DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|instant| Stored::Instant(instant.naive_utc())),
        (V::String(text), Kind::Timestamp { .. }) => parse_timestamp(text).map(Stored::Timestamp),
        (V::String(text), Kind::Date) => NaiveDate::parse_from_str(text.trim(), "%Y-%m-%d")
            .ok()
            .map(Stored::Date),
        (V::String(text) | V::Uuid(text) | V::Enum { label: text, .. }, _) => {
            Some(Stored::Text(text.clone()))
        }
        (V::Bytes(bytes), _) => Some(Stored::Bytes(bytes.clone())),
        (V::Date(text), Kind::Date) => NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .ok()
            .map(Stored::Date),
        (V::Date(text), _) => NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .ok()
            .and_then(|date| date.and_hms_opt(0, 0, 0))
            .map(Stored::Timestamp),
        (V::Timestamp(text), Kind::Date) => {
            parse_timestamp(text).map(|stamp| Stored::Date(stamp.date()))
        }
        (V::Timestamp(text), _) => parse_timestamp(text).map(Stored::Timestamp),
        (V::TimestampTz(text), _) => chrono::DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|instant| Stored::Instant(instant.naive_utc())),
        _ => None,
    }
}

/// La cella Arrow della lettura plan-based (il decoder Arrow).
fn stored_of_arrow(array: &dyn Array, row: usize, kind: Kind) -> Result<Stored, String> {
    if array.is_null(row) {
        return Ok(Stored::Null);
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
    let integer = |value: i128| Stored::Number(Num::of_i128(value), None);
    let text = |text: &str| -> Result<Stored, String> {
        stored_of_value(&V::String(text.to_owned()), kind)
            .ok_or_else(|| format!("testo Arrow non leggibile come {kind:?}: {text:?}"))
    };
    Ok(match array.data_type() {
        DataType::Boolean => Stored::Bool(cell!(BooleanArray)),
        DataType::Int8 => integer(i128::from(cell!(Int8Array))),
        DataType::Int16 => integer(i128::from(cell!(Int16Array))),
        DataType::Int32 => integer(i128::from(cell!(Int32Array))),
        DataType::Int64 => integer(i128::from(cell!(Int64Array))),
        DataType::UInt8 => integer(i128::from(cell!(UInt8Array))),
        DataType::UInt16 => integer(i128::from(cell!(UInt16Array))),
        DataType::UInt32 => integer(i128::from(cell!(UInt32Array))),
        DataType::UInt64 => integer(i128::from(cell!(UInt64Array))),
        DataType::Float32 => Stored::float(f64::from(cell!(Float32Array)))
            .ok_or_else(|| "float Arrow non finito".to_owned())?,
        DataType::Float64 => Stored::float(cell!(Float64Array))
            .ok_or_else(|| "double Arrow non finito".to_owned())?,
        DataType::Decimal128(_, scale) => {
            let value = cell!(Decimal128Array);
            // Una scala negativa e un esponente: `123` con scala -2 e 12300.
            let text = u32::try_from(*scale).map_or_else(
                |_| format!("{value}E{}", -i32::from(*scale)),
                |scale| decimal_text(value, scale),
            );
            Stored::Number(
                Num::parse(&text).ok_or_else(|| "decimale Arrow".to_owned())?,
                None,
            )
        }
        DataType::Utf8 => text(cell!(StringArray))?,
        DataType::LargeUtf8 => text(cell!(LargeStringArray))?,
        DataType::Binary => Stored::Bytes(cell!(BinaryArray).to_vec()),
        DataType::LargeBinary => Stored::Bytes(cell!(LargeBinaryArray).to_vec()),
        DataType::Date32 => {
            let days = cell!(Date32Array);
            Stored::Date(
                NaiveDate::from_ymd_opt(1970, 1, 1)
                    .and_then(|epoch| epoch.checked_add_signed(Duration::days(i64::from(days))))
                    .ok_or_else(|| "data Arrow fuori dominio".to_owned())?,
            )
        }
        DataType::Timestamp(_, zone) => {
            let micros = cell!(TimestampMicrosecondArray);
            let value = chrono::DateTime::from_timestamp_micros(micros)
                .ok_or_else(|| "timestamp Arrow fuori dominio".to_owned())?
                .naive_utc();
            match (zone, kind) {
                (_, Kind::Date) => Stored::Date(value.date()),
                (Some(_), _) => Stored::Instant(value),
                (None, _) => Stored::Timestamp(value),
            }
        }
        other => return Err(format!("tipo Arrow {other:?} senza resa nell'oracolo")),
    })
}

// ---------------------------------------------------------------------------
// L'esecuzione
// ---------------------------------------------------------------------------

/// L'esito di una colonna su un percorso: i guasti e le note.
#[derive(Debug, Clone, Default)]
pub struct SweepOutcome {
    pub column: &'static str,
    pub path: Option<FidelityPath>,
    pub accepted: usize,
    pub refused: usize,
    pub failures: Vec<String>,
    pub notes: Vec<String>,
}

/// Il resoconto e i guasti di tutte le colonne.
#[must_use]
pub fn sweep_report(outcomes: &[SweepOutcome]) -> (String, Vec<String>) {
    let mut text = String::new();
    let mut failures = Vec::new();
    for outcome in outcomes {
        let _ = writeln!(
            text,
            "{:<5} {:?} {}: {} accettati, {} rifiutati, {} guasti, {} note",
            if outcome.failures.is_empty() {
                "ok"
            } else {
                "FAIL"
            },
            outcome.path,
            outcome.column,
            outcome.accepted,
            outcome.refused,
            outcome.failures.len(),
            outcome.notes.len()
        );
        for failure in &outcome.failures {
            let _ = writeln!(text, "      guasto: {failure}");
            failures.push(format!("{:?} {}: {failure}", outcome.path, outcome.column));
        }
        for note in &outcome.notes {
            let _ = writeln!(text, "      nota: {note}");
        }
    }
    (text, failures)
}

fn describe(error: &DatabaseError) -> String {
    format!("{:?}/{:?}: {}", error.category, error.phase, error.message)
}

fn budget(target: &Target<'_>) -> Result<ResourceBudget, String> {
    ResourceBudget::new(target.limits.clone()).map_err(|error| describe(&error))
}

async fn begin(target: &Target<'_>) -> Result<Box<dyn TransactionScope>, String> {
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

/// Una istruzione dentro un savepoint: un errore del server non interrompe
/// la transazione dei valori successivi.
async fn guarded<T>(
    scope: &mut dyn TransactionScope,
    run: impl for<'s> FnOnce(
        &'s mut dyn TransactionScope,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<T, DatabaseError>> + Send + 's>,
    >,
) -> Result<Result<T, DatabaseError>, String> {
    let cancel = CancellationToken::new();
    scope
        .savepoint("plenora_sweep", &cancel)
        .await
        .map_err(|error| format!("savepoint: {}", describe(&error)))?;
    let outcome = run(scope).await;
    let closed = if outcome.is_ok() {
        // T-SQL non ha `RELEASE SAVEPOINT`: il savepoint resta fino al commit.
        match scope.release_savepoint("plenora_sweep", &cancel).await {
            Err(error) if error.category == plenora_database_core::ErrorCategory::Unsupported => {
                Ok(())
            }
            other => other,
        }
    } else {
        scope.rollback_to_savepoint("plenora_sweep", &cancel).await
    };
    closed.map_err(|error| format!("savepoint: {}", describe(&error)))?;
    Ok(outcome)
}

fn portable_table(target: &Target<'_>) -> TableRef {
    TableRef {
        schema: target.table.schema.clone(),
        name: target.table.object.clone(),
    }
}

fn insert_statement(target: &Target<'_>, column: &str, id: i32, value: &V) -> PortableStatement {
    PortableStatement::Insert(InsertStatement {
        table: portable_table(target),
        columns: vec![target.id_column.to_owned(), column.to_owned()],
        values: vec![vec![
            Expression::literal(V::I32(id)),
            Expression::literal(value.clone()),
        ]],
        returning: Vec::new(),
    })
}

fn count_of(rows: &[plenora_database_core::Row]) -> Result<u64, String> {
    match rows.first().and_then(|row| row.values().first()) {
        Some(V::I32(count)) => u64::try_from(*count).map_err(|_| "conteggio".to_owned()),
        Some(V::I64(count)) => u64::try_from(*count).map_err(|_| "conteggio".to_owned()),
        Some(V::Decimal(text)) => text.trim().parse().map_err(|_| "conteggio".to_owned()),
        other => Err(format!("conteggio inatteso: {other:?}")),
    }
}

async fn first_value(scope: &mut dyn TransactionScope, statement: Statement) -> Result<V, String> {
    let rows = scope
        .query(&statement, &CancellationToken::new())
        .await
        .map_err(|error| format!("rilettura: {}", describe(&error)))?;
    rows.first()
        .and_then(|row| row.values().first())
        .cloned()
        .ok_or_else(|| "rilettura senza righe".to_owned())
}

/// I confronti di un valore: i vicini generati per famiglia e i valori
/// adiacenti dell'insieme.
fn comparisons(set: &[V], index: usize) -> Vec<(V, Neighbour)> {
    let mut out = neighbours(&set[index]);
    for other in [index.wrapping_sub(1), index + 1] {
        if let Some(value) = set.get(other) {
            out.push((value.clone(), Neighbour::SameType));
        }
    }
    out
}

/// Il giudizio di un valore scritto: la resa del server contro il modello,
/// il decoder contro la resa.
struct Written {
    stored: Stored,
}

/// Se il valore e della famiglia della colonna.
const fn same_family(value: &V, kind: Kind) -> bool {
    matches!(
        (value, kind),
        (V::Bool(_), Kind::Boolean)
            | (
                V::I32(_) | V::I64(_) | V::Decimal(_),
                Kind::Integer { .. } | Kind::Decimal { .. } | Kind::UnboundedDecimal { .. }
            )
            | (V::F64(_), Kind::Float { .. } | Kind::ScaledFloat { .. })
            | (V::String(_), Kind::Text { .. })
            | (V::Bytes(_), Kind::Binary { .. })
            | (V::Date(_), Kind::Date)
            | (V::Timestamp(_), Kind::Timestamp { .. })
            | (V::TimestampTz(_), Kind::TimestampTz { .. })
    )
}

fn judge_write(
    column: &Column,
    value: &V,
    stored: &Stored,
    facts: ServerFacts,
    native: bool,
    outcome: &mut SweepOutcome,
) -> bool {
    match model(value, column.kind, facts) {
        Model::Exact(expected) => {
            if same(&expected, stored, column.kind, facts) {
                true
            } else if native && facts.native_declared && !same_family(value, column.kind) {
                // Lo SQL del chiamante non conosce l'operando: un valore di
                // un'altra famiglia (un decimale in un testo) il server lo
                // converte con le sue regole (limite dichiarato).
                outcome.notes.push(format!(
                    "{value:?}: convertito dal server come {} (limite dichiarato dello SQL del chiamante)",
                    stored.describe()
                ));
                false
            } else {
                outcome.failures.push(format!(
                    "{value:?} accettato e conservato {} (resa del server), atteso {}",
                    stored.describe(),
                    expected.describe()
                ));
                false
            }
        }
        Model::Declared(reason) => {
            outcome.notes.push(format!(
                "{value:?}: {reason} (conservato {})",
                stored.describe()
            ));
            false
        }
        Model::Inexact if native && facts.native_declared => {
            outcome.notes.push(format!(
                "{value:?}: assegnato dal server come {} (limite dichiarato dello SQL del chiamante)",
                stored.describe()
            ));
            false
        }
        Model::Inexact => {
            outcome.failures.push(format!(
                "{value:?} non entra esatto nella colonna ma e stato accettato: conservato {}",
                stored.describe()
            ));
            false
        }
    }
}

fn judge_refusal(
    column: &Column,
    value: &V,
    error: &DatabaseError,
    facts: ServerFacts,
    outcome: &mut SweepOutcome,
) {
    outcome.refused += 1;
    if let Model::Exact(expected) = model(value, column.kind, facts) {
        outcome.notes.push(format!(
            "rifiuto per eccesso: {value:?} sarebbe conservato {} ({})",
            expected.describe(),
            describe(error)
        ));
    }
}

/// Se il confronto `col = w` deve ritrovare la riga che conserva `stored`.
/// Il confronto e per valore: `-0.0 = 0.0` e vero in SQL come in IEEE, e
/// la scrittura ha gia giudicato il segno di zero conservato.
fn should_match(stored: &Stored, w: &V, column: &Column, facts: ServerFacts) -> Option<bool> {
    let by_value = |stored: &Stored| match stored {
        Stored::Number(number, _) => Stored::Number(number.clone(), None),
        other => other.clone(),
    };
    match model(w, column.kind, facts) {
        Model::Exact(expected) => Some(same(
            &by_value(&expected),
            &by_value(stored),
            column.kind,
            facts,
        )),
        Model::Inexact => Some(false),
        Model::Declared(_) => None,
    }
}

/// Dove si confronta: la colonna, i fatti del server, il percorso.
struct Context<'c> {
    column: &'c Column,
    facts: ServerFacts,
    native: bool,
    what: &'static str,
}

/// I byte senza gli zeri finali.
fn trailing_zeros_trimmed(bytes: &[u8]) -> &[u8] {
    &bytes[..bytes
        .iter()
        .rposition(|byte| *byte != 0)
        .map_or(0, |index| index + 1)]
}

fn judge_match(
    context: &Context<'_>,
    stored: &Stored,
    w: &V,
    kind: Neighbour,
    matched: Option<u64>,
    outcome: &mut SweepOutcome,
) {
    let Context {
        column,
        facts,
        native,
        what,
    } = *context;
    let Some(matched) = matched else {
        return;
    };
    let Some(expected) = should_match(stored, w, column, facts) else {
        return;
    };
    if (matched == 1) == expected {
        return;
    }
    let message = format!(
        "{what} con {w:?}: {matched} righe, {} su {}",
        if expected { "attesa 1" } else { "attese 0" },
        stored.describe()
    );
    // Gli spazi finali e le maiuscole del testo, gli zeri finali dei binari
    // (SQL Server confronta completando il piu corto): semantica di
    // confronto del server, dichiarata.
    let collation = matches!(kind, Neighbour::Collation)
        || matches!((stored, w), (Stored::Text(left), V::String(right))
            if left.trim_end().to_lowercase() == right.trim_end().to_lowercase())
        || matches!((stored, w), (Stored::Bytes(left), V::Bytes(right))
            if trailing_zeros_trimmed(left) == trailing_zeros_trimmed(right));
    if collation {
        outcome.notes.push(format!("collation: {message}"));
    } else if native && facts.native_declared {
        outcome
            .notes
            .push(format!("limite dichiarato (SQL del chiamante): {message}"));
    } else {
        outcome.failures.push(message);
    }
}

/// Il censimento di una colonna su un percorso.
async fn sweep_column(
    sweep: &SweepTarget<'_>,
    column: &Column,
    path: FidelityPath,
    base: i32,
) -> SweepOutcome {
    let mut outcome = SweepOutcome {
        column: column.name,
        path: Some(path),
        ..SweepOutcome::default()
    };
    let result = match path {
        FidelityPath::Native | FidelityPath::Portable => {
            write_and_compare(sweep, column, path, base, &mut outcome).await
        }
        FidelityPath::Read => read_path(sweep, column, base, &mut outcome).await,
    };
    if let Err(reason) = result {
        outcome.failures.push(reason);
    }
    if sweep.part.is_none() {
        vacuity(&mut outcome);
    }
    outcome
}

/// Una prova che non accetta e non rifiuta nulla in modo esplicito e vuota.
fn vacuity(outcome: &mut SweepOutcome) {
    if outcome.accepted == 0 && outcome.refused == 0 {
        outcome
            .failures
            .push("nessun valore accettato ne rifiutato: la prova sarebbe vuota".to_owned());
    }
}

/// Le parti di un censimento ([`SweepTarget::part`]) riunite per colonna e
/// percorso, con la prova di non vacuita sull'insieme.
#[must_use]
pub fn merge(parts: Vec<SweepOutcome>) -> Vec<SweepOutcome> {
    let mut merged: Vec<SweepOutcome> = Vec::new();
    for part in parts {
        match merged
            .iter_mut()
            .find(|outcome| outcome.column == part.column && outcome.path == part.path)
        {
            Some(outcome) => {
                outcome.accepted += part.accepted;
                outcome.refused += part.refused;
                outcome.failures.extend(part.failures);
                outcome.notes.extend(part.notes);
            }
            None => merged.push(part),
        }
    }
    for outcome in &mut merged {
        vacuity(outcome);
    }
    merged
}

async fn write_value(
    sweep: &SweepTarget<'_>,
    scope: &mut dyn TransactionScope,
    column: &Column,
    path: FidelityPath,
    id: i32,
    value: &V,
) -> Result<u64, DatabaseError> {
    let target = &sweep.target;
    if path == FidelityPath::Portable {
        let insert = insert_statement(target, column.name, id, value);
        execute_portable(scope, &insert, &CancellationToken::new()).await
    } else {
        let statement = Statement::new((target.native)(NativeSql::Insert, column.name))
            .with_params(vec![V::I32(id), value.clone()]);
        scope.execute(&statement, &CancellationToken::new()).await
    }
}

/// La resa del server e il decoder della transazione per la riga `id`.
async fn stored_row(
    sweep: &SweepTarget<'_>,
    scope: &mut dyn TransactionScope,
    column: &Column,
    id: i32,
    outcome: &mut SweepOutcome,
) -> Result<Written, String> {
    let raw = first_value(
        scope,
        Statement::new((sweep.raw)(column.name)).with_params(vec![V::I32(id)]),
    )
    .await?;
    let stored = parse_render(&raw, column.render)
        .ok_or_else(|| format!("resa del server non leggibile: {raw:?}"))?;
    let decoded = match first_value(
        scope,
        Statement::new((sweep.target.native)(NativeSql::Select, column.name))
            .with_params(vec![V::I32(id)]),
    )
    .await
    {
        Ok(decoded) => decoded,
        // Il decoder rifiuta il valore conservato: un errore, non un
        // silenzio (un infinito in un `BINARY_FLOAT` di Oracle).
        Err(reason) => {
            outcome.notes.push(format!(
                "decoder della transazione rifiuta {}: {reason}",
                stored.describe()
            ));
            return Ok(Written { stored });
        }
    };
    match stored_of_value(&decoded, column.kind) {
        Some(value) if same(&value, &stored, column.kind, sweep.facts) => {}
        other => outcome.failures.push(format!(
            "decoder della transazione {decoded:?} ({}) contro la resa del server {}",
            other.map_or_else(|| "illeggibile".to_owned(), |value| value.describe()),
            stored.describe()
        )),
    }
    Ok(Written { stored })
}

/// Un confronto `col = w` nella transazione del valore, dentro un savepoint:
/// `None` se il confronto e rifiutato prima dell'invio o fallisce sul server.
async fn compare(
    sweep: &SweepTarget<'_>,
    scope: &mut dyn TransactionScope,
    column: &Column,
    native: bool,
    id: i32,
    w: &V,
    outcome: &mut SweepOutcome,
) -> Result<Option<u64>, String> {
    let target = &sweep.target;
    let rows = if native {
        let statement = Statement::new((target.native)(NativeSql::Count, column.name))
            .with_params(vec![V::I32(id), w.clone()]);
        guarded(scope, move |scope| {
            Box::pin(async move { scope.query(&statement, &CancellationToken::new()).await })
        })
        .await?
        .map(|rows| count_of(&rows))
    } else {
        let select = PortableStatement::Select(SelectStatement {
            table: portable_table(target),
            projection: Projection::Columns(vec![target.id_column.to_owned()]),
            filter: Some(and(vec![
                eq(target.id_column, V::I32(id)),
                eq(column.name, w.clone()),
            ])),
            order_by: Vec::new(),
            limit: None,
        });
        guarded(scope, move |scope| {
            Box::pin(async move {
                execute_portable_returning(scope, &select, &CancellationToken::new()).await
            })
        })
        .await?
        .map(|rows| Ok(u64::try_from(rows.len()).unwrap_or(u64::MAX)))
    };
    match rows {
        Ok(count) => count.map(Some),
        Err(error) if error.phase == ErrorPhase::Prepare => Ok(None),
        Err(error) => {
            outcome
                .notes
                .push(format!("confronto con {w:?}: {}", describe(&error)));
            Ok(None)
        }
    }
}

/// I percorsi di scrittura: una transazione per valore (un errore del server
/// la interrompe su PostgreSQL e la condanna su SQL Server), annullata alla
/// fine.
async fn write_and_compare(
    sweep: &SweepTarget<'_>,
    column: &Column,
    path: FidelityPath,
    base: i32,
    outcome: &mut SweepOutcome,
) -> Result<(), String> {
    let target = &sweep.target;
    let native = path == FidelityPath::Native;
    let set = values(column.kind);
    for (index, value) in set.iter().enumerate() {
        if !sweep.includes(index) {
            continue;
        }
        let id = base + i32::try_from(index).unwrap_or(0);
        let mut scope = begin(target).await?;
        let result: Result<(), String> = async {
            if let Err(error) = write_value(sweep, scope.as_mut(), column, path, id, value).await {
                judge_refusal(column, value, &error, sweep.facts, outcome);
                return Ok(());
            }
            outcome.accepted += 1;
            let written = stored_row(sweep, scope.as_mut(), column, id, outcome).await?;
            if !judge_write(column, value, &written.stored, sweep.facts, native, outcome) {
                return Ok(());
            }
            let mut probes = vec![(value.clone(), Neighbour::SameType)];
            probes.extend(comparisons(&set, index));
            let context = Context {
                column,
                facts: sweep.facts,
                native,
                what: if native { "col = ?" } else { "eq portable" },
            };
            for (w, kind) in probes {
                match compare(sweep, scope.as_mut(), column, native, id, &w, outcome).await {
                    Ok(matched) => {
                        judge_match(&context, &written.stored, &w, kind, matched, outcome);
                    }
                    // Il savepoint non si ripristina (SQL Server condanna la
                    // transazione dopo un errore): i confronti restanti di
                    // questo valore non si possono fare.
                    Err(reason) => {
                        outcome
                            .notes
                            .push(format!("confronti di {value:?} interrotti: {reason}"));
                        break;
                    }
                }
            }
            Ok(())
        }
        .await;
        let _ = scope.rollback(&CancellationToken::new()).await;
        result?;
    }
    Ok(())
}

async fn read_batches(
    sweep: &SweepTarget<'_>,
    column: &Column,
    filter: FilterExpression,
    parameters: BTreeMap<String, V>,
) -> Result<Vec<plenora_database_core::arrow::RecordBatch>, DatabaseError> {
    let target = &sweep.target;
    let operation = ReadOperation {
        source: target.table.clone(),
        projection: vec![target.id_column.to_owned(), column.name.to_owned()],
        order_by: Vec::new(),
        row_limit: None,
        row_offset: None,
        filter: Some(filter),
        declared_crs: Vec::new(),
    };
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(target.limits.clone())?;
    let mut stream = target
        .provider
        .read(
            target.secret,
            &operation,
            &ParameterBag::new(parameters),
            &budget,
            &cancellation,
        )
        .await?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next_batch(&cancellation).await? {
        batches.push(batch);
    }
    Ok(batches)
}

/// Il percorso di lettura: i valori si scrivono con lo SQL del chiamante (o
/// con il portable, se lo SQL del chiamante li rifiuta), si leggono tutti con
/// una lettura plan-based (il decoder Arrow contro la resa del server), e il
/// filtro della lettura confronta ogni valore e il suo primo vicino.
#[allow(clippy::too_many_lines)]
async fn read_path(
    sweep: &SweepTarget<'_>,
    column: &Column,
    base: i32,
    outcome: &mut SweepOutcome,
) -> Result<(), String> {
    let target = &sweep.target;
    let set = values(column.kind);
    let mut written: BTreeMap<i32, (usize, Stored)> = BTreeMap::new();
    // Le righe che un giro interrotto avesse lasciato.
    let ids = (0..set.len())
        .map(|index| base + i32::try_from(index).unwrap_or(0))
        .collect::<Vec<_>>();
    delete_rows(target, column, &ids).await?;
    if sweep.facts.single_transaction {
        written = write_all_in_one(sweep, column, &set, base, outcome).await?;
    }
    for (index, value) in set.iter().enumerate() {
        if !sweep.includes(index) {
            continue;
        }
        if sweep.facts.single_transaction {
            break;
        }
        let id = base + i32::try_from(index).unwrap_or(0);
        let mut scope = begin(target).await?;
        let mut inserted = write_value(
            sweep,
            scope.as_mut(),
            column,
            FidelityPath::Native,
            id,
            value,
        )
        .await;
        if matches!(&inserted, Err(error) if error.phase == ErrorPhase::Prepare) {
            inserted = write_value(
                sweep,
                scope.as_mut(),
                column,
                FidelityPath::Portable,
                id,
                value,
            )
            .await;
        }
        let exact = if inserted.is_ok() {
            let row = stored_row(sweep, scope.as_mut(), column, id, outcome).await;
            match row {
                // Soltanto i valori conservati esatti entrano nella lettura:
                // gli altri li giudicano gli altri percorsi.
                Ok(row) => match model(value, column.kind, sweep.facts) {
                    Model::Exact(expected)
                        if same(&expected, &row.stored, column.kind, sweep.facts) =>
                    {
                        Some(row.stored)
                    }
                    _ => None,
                },
                Err(reason) => {
                    let _ = scope.rollback(&CancellationToken::new()).await;
                    return Err(reason);
                }
            }
        } else {
            None
        };
        if let Some(stored) = exact {
            scope
                .commit(&CancellationToken::new())
                .await
                .map_err(|error| format!("commit: {}", describe(&error)))?;
            written.insert(id, (index, stored));
        } else {
            let _ = scope.rollback(&CancellationToken::new()).await;
        }
    }
    let probe: Result<(), String> = async {
        let last = base + i32::try_from(set.len()).unwrap_or(0);
        let range = FilterExpression::And {
            args: vec![
                FilterExpression::Gte {
                    field: target.id_column.to_owned(),
                    parameter: "low".to_owned(),
                },
                FilterExpression::Lt {
                    field: target.id_column.to_owned(),
                    parameter: "high".to_owned(),
                },
            ],
        };
        let parameters = BTreeMap::from([
            ("low".to_owned(), V::I32(base)),
            ("high".to_owned(), V::I32(last)),
        ]);
        // Un errore rifiuta la lettura intera: si legge riga per riga, e
        // ogni valore che la lettura rifiuta e una nota (un rifiuto, non
        // un silenzio).
        let batches = match read_batches(sweep, column, range, parameters).await {
            Ok(batches) => batches,
            // La lettura della colonna rifiutata prima dello stream: un
            // rifiuto dichiarato del tipo (i binari di Db2), non un silenzio.
            Err(error) if matches!(error.phase, ErrorPhase::Prepare | ErrorPhase::Validate) => {
                outcome.notes.push(format!(
                    "lettura della colonna rifiutata: {}",
                    describe(&error)
                ));
                outcome.accepted = outcome.accepted.max(1);
                return Ok(());
            }
            Err(_) => {
                let mut batches = Vec::new();
                let ids = written.keys().copied().collect::<Vec<_>>();
                for id in ids {
                    let filter = FilterExpression::Eq {
                        field: target.id_column.to_owned(),
                        parameter: "id".to_owned(),
                    };
                    let parameters = BTreeMap::from([("id".to_owned(), V::I32(id))]);
                    match read_batches(sweep, column, filter, parameters).await {
                        Ok(rows) => batches.extend(rows),
                        Err(error) => {
                            if let Some((index, stored)) = written.remove(&id) {
                                outcome.refused += 1;
                                outcome.notes.push(format!(
                                    "lettura rifiutata per {:?} (conservato {}): {}",
                                    set[index],
                                    stored.describe(),
                                    describe(&error)
                                ));
                            }
                        }
                    }
                }
                batches
            }
        };
        let mut seen = 0_usize;
        let mut read_ids = std::collections::BTreeSet::new();
        for batch in &batches {
            let ids = batch
                .column_by_name(target.id_column)
                .ok_or_else(|| "lettura senza colonna ID".to_owned())?;
            let cells = batch
                .column_by_name(column.name)
                .ok_or_else(|| "lettura senza la colonna del valore".to_owned())?;
            for row in 0..batch.num_rows() {
                let id = match stored_of_arrow(
                    ids.as_ref(),
                    row,
                    Kind::Integer {
                        min: i128::MIN,
                        max: i128::MAX,
                    },
                )? {
                    Stored::Number(number, _) => number
                        .as_i128()
                        .and_then(|id| i32::try_from(id).ok())
                        .ok_or_else(|| "ID Arrow".to_owned())?,
                    _ => return Err("ID Arrow".to_owned()),
                };
                let Some((index, stored)) = written.get(&id) else {
                    return Err(format!("la lettura rende la riga {id}, non scritta"));
                };
                seen += 1;
                read_ids.insert(id);
                let cell = stored_of_arrow(cells.as_ref(), row, column.kind)?;
                if !same(&cell, stored, column.kind, sweep.facts) {
                    outcome.failures.push(format!(
                        "decoder Arrow {} contro la resa del server {} per {:?}",
                        cell.describe(),
                        stored.describe(),
                        set[*index]
                    ));
                }
            }
        }
        if seen != written.len() {
            outcome.failures.push(format!(
                "la lettura rende {seen} righe, scritte {}: mancano {:?}",
                written.len(),
                written
                    .iter()
                    .filter(|(id, _)| !read_ids.contains(id))
                    .map(|(_, (index, stored))| format!(
                        "{:?} ({})",
                        set[*index],
                        stored.describe()
                    ))
                    .collect::<Vec<_>>()
            ));
        }
        outcome.accepted = written
            .len()
            .max(usize::from(seen == 0 && !batches.is_empty()));
        for (id, (index, stored)) in &written {
            let mut probes = vec![(set[*index].clone(), Neighbour::SameType)];
            probes.extend(comparisons(&set, *index).into_iter().take(1));
            for (w, kind) in probes {
                let filter = FilterExpression::And {
                    args: vec![
                        FilterExpression::Eq {
                            field: target.id_column.to_owned(),
                            parameter: "id".to_owned(),
                        },
                        FilterExpression::Eq {
                            field: column.name.to_owned(),
                            parameter: "value".to_owned(),
                        },
                    ],
                };
                let parameters = BTreeMap::from([
                    ("id".to_owned(), V::I32(*id)),
                    ("value".to_owned(), w.clone()),
                ]);
                let matched = match read_batches(sweep, column, filter, parameters).await {
                    Ok(batches) => Some(
                        batches
                            .iter()
                            .map(|batch| u64::try_from(batch.num_rows()).unwrap_or(u64::MAX))
                            .sum(),
                    ),
                    Err(error)
                        if matches!(error.phase, ErrorPhase::Prepare | ErrorPhase::Validate) =>
                    {
                        None
                    }
                    // Un errore dopo l'invio (il limite dei cursori del fork
                    // Oracle, una connessione persa) e un errore, non un
                    // silenzio: si registra e il confronto non decide.
                    Err(error) => {
                        outcome
                            .notes
                            .push(format!("filtro con {w:?} fallito: {}", describe(&error)));
                        None
                    }
                };
                let context = Context {
                    column,
                    facts: sweep.facts,
                    native: false,
                    what: "filtro di lettura",
                };
                judge_match(&context, stored, &w, kind, matched, outcome);
            }
        }
        Ok(())
    }
    .await;
    let mut cleanup = begin(target).await?;
    for id in written.keys() {
        let statement = Statement::new((target.native)(NativeSql::Delete, column.name))
            .with_params(vec![V::I32(*id)]);
        if let Err(error) = cleanup.execute(&statement, &CancellationToken::new()).await {
            let _ = cleanup.rollback(&CancellationToken::new()).await;
            return Err(format!("pulizia: {}", describe(&error)));
        }
    }
    cleanup
        .commit(&CancellationToken::new())
        .await
        .map_err(|error| format!("commit pulizia: {}", describe(&error)))?;
    probe
}

/// Cancella le righe `ids` in una transazione.
async fn delete_rows(target: &Target<'_>, column: &Column, ids: &[i32]) -> Result<(), String> {
    let mut cleanup = begin(target).await?;
    for id in ids {
        let statement = Statement::new((target.native)(NativeSql::Delete, column.name))
            .with_params(vec![V::I32(*id)]);
        if let Err(error) = cleanup.execute(&statement, &CancellationToken::new()).await {
            let _ = cleanup.rollback(&CancellationToken::new()).await;
            return Err(format!("pulizia: {}", describe(&error)));
        }
    }
    cleanup
        .commit(&CancellationToken::new())
        .await
        .map(|_| ())
        .map_err(|error| format!("commit pulizia: {}", describe(&error)))
}

/// La scrittura della lettura in una transazione sola, ogni valore nel suo
/// savepoint (un server che non condanna la transazione dopo un errore).
async fn write_all_in_one(
    sweep: &SweepTarget<'_>,
    column: &Column,
    set: &[V],
    base: i32,
    outcome: &mut SweepOutcome,
) -> Result<BTreeMap<i32, (usize, Stored)>, String> {
    let target = &sweep.target;
    let mut written = BTreeMap::new();
    let mut scope = begin(target).await?;
    let result: Result<(), String> = async {
        for (index, value) in set.iter().enumerate() {
            if !sweep.includes(index) {
                continue;
            }
            let id = base + i32::try_from(index).unwrap_or(0);
            let native = Statement::new((target.native)(NativeSql::Insert, column.name))
                .with_params(vec![V::I32(id), value.clone()]);
            let mut inserted = guarded(scope.as_mut(), move |scope| {
                Box::pin(async move { scope.execute(&native, &CancellationToken::new()).await })
            })
            .await?;
            if matches!(&inserted, Err(error) if error.phase == ErrorPhase::Prepare) {
                let insert = insert_statement(target, column.name, id, value);
                inserted = guarded(scope.as_mut(), move |scope| {
                    Box::pin(async move {
                        execute_portable(scope, &insert, &CancellationToken::new()).await
                    })
                })
                .await?;
            }
            if inserted.is_err() {
                continue;
            }
            let row = stored_row(sweep, scope.as_mut(), column, id, outcome).await?;
            match model(value, column.kind, sweep.facts) {
                Model::Exact(expected)
                    if same(&expected, &row.stored, column.kind, sweep.facts) =>
                {
                    written.insert(id, (index, row.stored));
                }
                _ => {
                    let statement = Statement::new((target.native)(NativeSql::Delete, column.name))
                        .with_params(vec![V::I32(id)]);
                    scope
                        .execute(&statement, &CancellationToken::new())
                        .await
                        .map_err(|error| format!("pulizia: {}", describe(&error)))?;
                }
            }
        }
        Ok(())
    }
    .await;
    if let Err(reason) = result {
        let _ = scope.rollback(&CancellationToken::new()).await;
        return Err(reason);
    }
    scope
        .commit(&CancellationToken::new())
        .await
        .map_err(|error| format!("commit: {}", describe(&error)))?;
    Ok(written)
}

/// Il censimento generato di tutte le colonne su tutti i percorsi.
pub async fn sweep(
    target: &SweepTarget<'_>,
    columns: &[Column],
    paths: &[FidelityPath],
) -> Vec<SweepOutcome> {
    let mut outcomes = Vec::new();
    for column in columns {
        // Una base di ID per colonna, stabile: le colonne di una tabella non
        // si sovrappongono anche quando si provano una alla volta.
        let hash = column.name.bytes().fold(2_166_136_261_u32, |hash, byte| {
            (hash ^ u32::from(byte)).wrapping_mul(16_777_619)
        });
        let base = 100_000 + i32::try_from(hash % 900).unwrap_or(0) * 1_000;
        for path in paths {
            outcomes.push(sweep_column(target, column, *path, base).await);
        }
    }
    outcomes
}

#[cfg(test)]
#[path = "parameter_sweep_tests.rs"]
mod tests;
