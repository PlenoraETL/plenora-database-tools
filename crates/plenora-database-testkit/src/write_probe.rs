//! La sonda della regola A (`plenora_database_core::write_admission`).
//!
//! Per ogni chiave d'ammissione, una batteria fissa di valori limite scritta
//! con il writer oppure con l'oracolo diretto del provider e rollback.
//! L'oracolo diretto separa la misura del server dall'ammissione della libreria:
//! una coppia chiusa dal preflight deve comunque essere misurabile.
//!
//! Il driver e comune ai provider: crea la tabella `ID` + `V` con il tipo
//! nativo della chiave, legge dal provider la chiave d'ammissione che il
//! preflight calcolerebbe per quella colonna ([`ProbeHooks`]), scrive la
//! batteria in Append `Strict` (tutta insieme, e valore per valore se il
//! server ne rifiuta qualcuno), rilegge e confronta valore per valore
//! ([`same_value`]). Le riscritture si spiegano con le classi della regola S
//! ([`classify`]); quelle che nessuna classe spiega diventano `ogni`.
//!
//! Il provider, in `cfg(test)`, non applica la regola A alla tabella della
//! sonda ([`PROBE_TABLE`]): e la misura che la regola leggera.

use plenora_database_core::arrow::array::cast::AsArray;
use plenora_database_core::arrow::array::types::{
    Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type,
    UInt8Type,
};
use plenora_database_core::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::outcome::WriteStatus;
use plenora_database_core::plan::{
    ObjectRef, OrderBy, ReadOperation, SortDirection, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::provider::{
    BatchStream, ParameterBag, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::store_rewrite::StoreRewrite;
use plenora_database_core::write_admission::{
    arrow_key, instantiate, AdmissionFixture, AdmissionKey, MeasuredKey, MeasuredOutcome,
    MeasuredRewrite, ServerMeasurement, TargetDimensions, BATTERY_VERSION,
};
use plenora_database_core::CancellationToken;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

/// La tabella della sonda: il provider, in `cfg(test)`, non le applica la
/// regola A.
pub const PROBE_TABLE: &str = "PLN_SONDA_W";

/// Se `object` e la tabella della sonda (il confronto ignora le maiuscole:
/// Oracle e Db2 la catalogano in maiuscolo, PostgreSQL in minuscolo).
#[must_use]
pub const fn is_probe_table(object: &str) -> bool {
    object.eq_ignore_ascii_case(PROBE_TABLE)
}

/// Una chiave da misurare: il tipo nativo come si dichiara nel DDL, il tipo
/// Arrow dell'ingresso, la lunghezza se il tipo ne ha una simbolica, e se la
/// colonna e un documento JSON.
#[derive(Debug, Clone)]
pub struct ProbeSpec {
    pub native_ddl: String,
    pub arrow: DataType,
    pub dimension: Option<u64>,
    pub document: bool,
    /// Il dominio del testo, per i tipi nativi che ricevono testo e ne
    /// conservano un valore tipizzato ([`TextDomain`]).
    pub text_domain: TextDomain,
}

/// Il dominio di un testo verso un tipo nativo che ne conserva un valore
/// tipizzato, non il testo: la batteria e il confronto del dominio.
///
/// # Limite dichiarato: i testi tipizzati
///
/// - **Regola**: un istante con offset rientra esatto se e lo stesso istante
///   con lo stesso offset (`Z` e `+00:00` non distinguono, ne `T` e lo
///   spazio, ne gli zeri finali della frazione); un UUID se e lo stesso UUID
///   (le maiuscole, le graffe e i trattini non distinguono).
/// - **Ambito**: le chiavi misurate con [`TextDomain::OffsetInstant`] e
///   [`TextDomain::Uuid`] (il `datetimeoffset` e l'`uniqueidentifier` di SQL
///   Server).
/// - **Hazard**: chi legge il testo byte per byte vede una forma diversa da
///   quella scritta (`+00:00` riletto `Z`, le maiuscole rilette minuscole).
/// - **Rientro**: il confronto byte per byte anche per questi tipi.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TextDomain {
    /// Testo libero: la batteria dei testi, confronto byte per byte.
    #[default]
    Free,
    /// Un istante RFC 3339 con offset.
    OffsetInstant,
    /// Un UUID.
    Uuid,
}

impl ProbeSpec {
    #[must_use]
    pub fn new(native_ddl: impl Into<String>, arrow: DataType) -> Self {
        Self {
            native_ddl: native_ddl.into(),
            arrow,
            dimension: None,
            document: false,
            text_domain: TextDomain::Free,
        }
    }

    /// Il testo si misura nel dominio `domain` ([`TextDomain`]).
    #[must_use]
    pub const fn with_text_domain(mut self, domain: TextDomain) -> Self {
        self.text_domain = domain;
        self
    }

    #[must_use]
    pub const fn with_dimension(mut self, dimension: u64) -> Self {
        self.dimension = Some(dimension);
        self
    }

    #[must_use]
    pub const fn document(mut self) -> Self {
        self.document = true;
        self
    }
}

/// Cio che la sonda chiede al provider.
pub trait ProbeHooks: Send + Sync {
    /// Il provider.
    fn provider(&self) -> &dyn Provider;
    /// Il segreto della connessione.
    fn secret(&self) -> &SecretString;
    /// Lo schema della tabella della sonda.
    fn schema(&self) -> Option<String>;
    /// Il nome della colonna `ID` e il prefisso delle colonne misurate
    /// (`V0`, `V1`, ...), come il catalogo li registra.
    fn columns(&self) -> (&'static str, &'static str);
    /// Gli statement che ricreano la tabella della sonda: `ID` intero a 64
    /// bit chiave primaria e una colonna `{prefisso}{i}` per ogni tipo di
    /// `natives`, nell'ordine.
    fn create_sql(&self, natives: &[String]) -> Vec<String>;
    /// Lo statement che la elimina, se c'e (gli errori si ignorano).
    fn drop_sql(&self) -> String;
    /// Lo statement che la svuota.
    fn truncate_sql(&self) -> String;
    /// Oracolo diretto con rollback, indipendente dai tipi intermedi.
    fn direct_probe<'a>(
        &'a self,
        _spec: &'a ProbeSpec,
        _values: &'a dyn Array,
        _row: usize,
    ) -> Option<ProviderFuture<'a, DirectObservation>> {
        None
    }

    /// Le chiavi d'ammissione e le dimensioni che il preflight calcolerebbe
    /// per ogni colonna `(nome, tipo Arrow dell'ingresso)`, dal catalogo e
    /// nello stesso ordine.
    fn admission_keys<'a>(
        &'a self,
        columns: &'a [(String, DataType)],
    ) -> ProviderFuture<'a, Vec<(AdmissionKey, TargetDimensions)>>;
}

#[path = "write_probe_direct.rs"]
mod direct;
pub use direct::{direct_error, direct_value, DirectObservation};

/// Un valore della batteria e il suo esito.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueOutcome {
    Exact,
    Rewritten,
    Refused,
}

/// La batteria di un tipo Arrow: i nomi e i valori, nello stesso ordine.
pub struct Battery {
    pub names: Vec<String>,
    pub values: ArrayRef,
}

const DAY_NS: i128 = 86_400_000_000_000;

#[allow(clippy::too_many_lines)]
fn text_values(dimension: Option<u64>) -> Vec<(String, Option<String>)> {
    let mut values: Vec<(&str, String)> = vec![
        ("vuoto", String::new()),
        ("spazio", " ".to_owned()),
        ("a", "a".to_owned()),
        ("spazio_finale", "a ".to_owned()),
        ("spazi_finali", "a   ".to_owned()),
        ("spazio_iniziale", " a".to_owned()),
        ("nul_interno", "a\0b".to_owned()),
        ("solo_nul", "\0".to_owned()),
        ("controllo", "a\tb\r\nc".to_owned()),
        ("latino", "àéîõü".to_owned()),
        ("fuori_latin1", "ΩЖł€".to_owned()),
        ("non_bmp", "😀𝄞".to_owned()),
        ("combinante", "e\u{301}".to_owned()),
        ("precomposto", "\u{e9}".to_owned()),
        ("bom", "\u{feff}a".to_owned()),
        ("sostituzione", "\u{fffd}".to_owned()),
        ("separatore_riga", "a\u{2028}b".to_owned()),
        ("barra_inversa", "a\\b'c\"d".to_owned()),
    ];
    if let Some(length) = dimension {
        let length = usize::try_from(length.min(4000)).unwrap_or(4000);
        values.push(("pieno", "x".repeat(length)));
        if length > 1 {
            values.push(("pieno_meno_uno", "x".repeat(length - 1)));
            values.push((
                "pieno_spazio_finale",
                format!("{} ", "x".repeat(length - 1)),
            ));
        }
        values.push(("oltre_con_spazio", format!("{} ", "x".repeat(length))));
        values.push(("oltre_con_spazi", format!("{}   ", "x".repeat(length))));
        values.push(("oltre", "x".repeat(length + 1)));
        values.push(("pieno_multibyte", "é".repeat(length)));
        values.push(("pieno_non_bmp", "😀".repeat(length)));
    } else {
        values.push(("lungo", "x".repeat(40_000)));
        values.push(("lungo_multibyte", "é".repeat(20_000)));
    }
    let mut named = values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), Some(value)))
        .collect::<Vec<_>>();
    named.push(("nullo".to_owned(), None));
    named
}

fn document_values() -> Vec<(String, Option<String>)> {
    let values = [
        ("oggetto", r#"{"a":1}"#),
        ("uno_virgola_zero", r#"{"a":1.0}"#),
        ("esponente", r#"{"a":1e2}"#),
        ("fuori_double", r#"{"a":1e400}"#),
        (
            "cinquanta_cifre",
            r#"{"a":12345678901234567890123456789012345678901234567890}"#,
        ),
        ("zero_negativo", r#"{"a":-0}"#),
        ("frazione", r#"{"a":1.5}"#),
        ("diciassette_cifre", r#"{"a":0.30000000000000004}"#),
        ("intero_breve", r#"{"a":123456789012345}"#),
        ("ordine", r#"{"b":1,"a":2}"#),
        ("chiave_ripetuta", r#"{"a":1,"a":2}"#),
        ("spazi", " { \"a\" : [ 1 , 2 ] } "),
        ("escape", r#"{"a":"é\n"}"#),
        ("non_bmp", r#"{"a":"😀"}"#),
        ("annidato", r#"{"a":{"b":[true,false,null]}}"#),
        ("lista", r#"[1,"x",null]"#),
        ("scalare_stringa", r#""x""#),
        ("scalare_null", "null"),
        ("oggetto_vuoto", "{}"),
    ];
    let mut named = values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), Some(value.to_owned())))
        .collect::<Vec<_>>();
    named.push(("nullo".to_owned(), None));
    named
}

fn offset_instant_values() -> Vec<(String, Option<String>)> {
    let values = [
        ("utc_z", "2026-01-01T10:00:00Z"),
        ("utc_z_minuscole", "2026-01-01t10:00:00z"),
        ("utc_piu_zero", "2026-01-01T10:00:00+00:00"),
        ("utc_meno_zero", "2026-01-01T10:00:00-00:00"),
        ("separatore_spazio", "2026-01-01 10:00:00+00:00"),
        ("piu_due", "2026-01-01T10:00:00+02:00"),
        ("meno_cinque_e_mezzo", "2026-01-01T10:00:00-05:30"),
        ("piu_cinque_e_tre_quarti", "2026-01-01T10:00:00+05:45"),
        ("piu_quattordici", "2026-01-01T10:00:00+14:00"),
        ("meno_quattordici", "2026-01-01T10:00:00-14:00"),
        ("oltre_quattordici", "2026-01-01T10:00:00+14:01"),
        ("cifre_1", "2026-01-01T10:00:00.1+01:00"),
        ("cifre_2", "2026-01-01T10:00:00.12+01:00"),
        ("cifre_3", "2026-01-01T10:00:00.123+01:00"),
        ("cifre_4", "2026-01-01T10:00:00.1234+01:00"),
        ("cifre_5", "2026-01-01T10:00:00.12345+01:00"),
        ("cifre_6", "2026-01-01T10:00:00.123456+01:00"),
        ("cifre_7", "2026-01-01T10:00:00.1234567+01:00"),
        ("cifre_8", "2026-01-01T10:00:00.12345678+01:00"),
        ("cifre_9", "2026-01-01T10:00:00.123456789+01:00"),
        ("cifre_10", "2026-01-01T10:00:00.1234567891+01:00"),
        ("zeri_finali", "2026-01-01T10:00:00.1000000+01:00"),
        ("quasi_secondo", "2026-01-01T10:00:00.9999999-03:00"),
        ("epoca", "1970-01-01T00:00:00Z"),
        ("prima_epoca", "1969-12-31T23:59:59.999999-03:00"),
        ("bisestile", "2000-02-29T12:00:00+01:00"),
        ("anno_1", "0001-01-01T00:00:00+00:00"),
        ("anno_1_offset_positivo", "0001-01-01T00:00:00+01:00"),
        ("anno_9999_fine", "9999-12-31T23:59:59.9999999+00:00"),
        ("anno_9999_offset_negativo", "9999-12-31T23:59:59-01:00"),
        ("secondo_intercalare", "2016-12-31T23:59:60Z"),
        ("data_non_valida", "2026-02-30T10:00:00Z"),
        ("senza_offset", "2026-01-01T10:00:00"),
    ];
    let mut named = values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), Some(value.to_owned())))
        .collect::<Vec<_>>();
    named.push(("nullo".to_owned(), None));
    named
}

fn uuid_values() -> Vec<(String, Option<String>)> {
    let values = [
        ("minuscole", "6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        ("maiuscole", "6F9619FF-8B86-D011-B42D-00C04FC964FF"),
        ("graffe", "{6f9619ff-8b86-d011-b42d-00c04fc964ff}"),
        ("zero", "00000000-0000-0000-0000-000000000000"),
        ("massimo", "ffffffff-ffff-ffff-ffff-ffffffffffff"),
        ("senza_trattini", "6f9619ff8b86d011b42d00c04fc964ff"),
        ("urn", "urn:uuid:6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        ("coda", "6f9619ff-8b86-d011-b42d-00c04fc964ffxyz"),
        ("coda_trattino", "6f9619ff-8b86-d011-b42d-00c04fc964ff-0000"),
        ("corto", "6f9619ff-8b86-d011-b42d-00c04fc964f"),
        ("non_esadecimale", "6f9619ff-8b86-d011-b42d-00c04fc964fg"),
        ("spazi", " 6f9619ff-8b86-d011-b42d-00c04fc964ff "),
        ("vuoto", ""),
    ];
    let mut named = values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), Some(value.to_owned())))
        .collect::<Vec<_>>();
    named.push(("nullo".to_owned(), None));
    named
}

/// I microsecondi del giorno della batteria di `Time64`.
fn time_values() -> Vec<(&'static str, i64)> {
    vec![
        ("mezzanotte", 0),
        ("un_micro", 1),
        ("un_secondo", 1_000_000),
        ("mezzogiorno", 43_200_000_000),
        ("cifre_1", 36_000_100_000),
        ("cifre_2", 36_000_120_000),
        ("cifre_3", 36_000_123_000),
        ("cifre_4", 36_000_123_400),
        ("cifre_5", 36_000_123_450),
        ("cifre_6", 36_000_123_456),
        ("quasi_mezzanotte", 86_399_999_999),
        ("mezzanotte_dopo", 86_400_000_000),
        ("negativo", -1),
    ]
}

/// Gli intervalli della batteria di `Interval(MonthDayNano)`: mesi, giorni e
/// nanosecondi.
fn interval_values() -> Vec<(&'static str, (i32, i32, i64))> {
    vec![
        ("zero", (0, 0, 0)),
        ("un_mese", (1, 0, 0)),
        ("meno_un_giorno", (0, -1, 0)),
        ("un_micro", (0, 0, 1_000)),
        ("un_nano", (0, 0, 1)),
        ("segni_misti", (1, -1, 1_000)),
        ("venticinque_ore", (0, 0, 90_000_000_000_000)),
        ("secondo_e_mezzo", (0, 0, 1_500_000_000)),
        ("cento_anni_in_mesi", (1_200, 0, 0)),
        ("cento_anni_in_giorni", (0, 36_500, 0)),
        ("tutto_negativo", (-1, -1, -1_000)),
    ]
}

fn binary_values(dimension: Option<u64>) -> Vec<(String, Option<Vec<u8>>)> {
    let mut values: Vec<(&str, Vec<u8>)> = vec![
        ("vuoto", Vec::new()),
        ("zero", vec![0]),
        ("uno_zero", vec![1, 0]),
        ("zero_uno", vec![0, 1]),
        ("zeri_finali", vec![1, 0, 0]),
        ("ff", vec![0xff]),
        ("spazio", vec![0x20]),
        ("ogni_byte", (0..=255).collect()),
    ];
    match dimension {
        Some(length) => {
            let length = usize::try_from(length.min(4000)).unwrap_or(4000);
            values.push(("pieno", vec![0xab; length]));
            if length > 1 {
                values.push(("pieno_meno_uno", vec![0xab; length - 1]));
            }
            values.push(("oltre", vec![0xab; length + 1]));
        }
        None => values.push(("lungo", vec![0xab; 40_000])),
    }
    let mut named = values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), Some(value)))
        .collect::<Vec<_>>();
    named.push(("nullo".to_owned(), None));
    named
}

fn float64_values() -> Vec<(&'static str, f64)> {
    vec![
        ("zero", 0.0),
        ("zero_negativo", -0.0),
        ("uno", 1.0),
        ("meno_uno_e_mezzo", -1.5),
        ("decimo", 0.1),
        ("diciassette_cifre", 0.1 + 0.2),
        ("terzo", 1.0 / 3.0),
        ("pi_greco", std::f64::consts::PI),
        ("intero_2_53", 9_007_199_254_740_992.0),
        ("massimo", f64::MAX),
        ("minimo", f64::MIN),
        ("normale_minimo", f64::MIN_POSITIVE),
        ("subnormale_minimo", 5e-324),
        ("subnormale", 1e-310),
        ("e_meno_300", 1e-300),
        ("e_meno_200", 1e-200),
        ("e_meno_150", 1e-150),
        ("e_meno_131", 1e-131),
        ("e_meno_130", 1e-130),
        ("e_meno_129", 1e-129),
        ("e_meno_100", 1e-100),
        ("e_meno_50", 1e-50),
        ("e_meno_38", 1e-38),
        ("e_meno_10", 1e-10),
        ("e_100", 1e100),
        ("e_125", 1e125),
        ("e_126", 1e126),
        ("e_200", 1e200),
        ("meno_e_meno_200", -1e-200),
        ("infinito", f64::INFINITY),
        ("meno_infinito", f64::NEG_INFINITY),
        ("nan", f64::NAN),
    ]
}

fn float32_values() -> Vec<(&'static str, f32)> {
    vec![
        ("zero", 0.0),
        ("zero_negativo", -0.0),
        ("uno", 1.0),
        ("meno_uno_e_mezzo", -1.5),
        ("decimo", 0.1),
        ("terzo", 1.0 / 3.0),
        ("sette_cifre", 1.234_567_9),
        ("intero_2_24", 16_777_216.0),
        ("massimo", f32::MAX),
        ("minimo", f32::MIN),
        ("normale_minimo", f32::MIN_POSITIVE),
        ("subnormale_minimo", 1e-45),
        ("subnormale", 1e-40),
        ("e_meno_30", 1e-30),
        ("e_meno_10", 1e-10),
        ("e_30", 1e30),
        ("infinito", f32::INFINITY),
        ("meno_infinito", f32::NEG_INFINITY),
        ("nan", f32::NAN),
    ]
}

/// Gli istanti della batteria, in nanosecondi dall'epoca UTC.
fn instant_values() -> Vec<(&'static str, i128)> {
    let second: i128 = 1_000_000_000;
    let base = 1_767_225_600 * second; // 2026-01-01T00:00:00Z
    vec![
        ("epoca", 0),
        ("intero", base),
        ("mezzo", base + second / 2),
        ("sette_decimi", base + 7 * second / 10),
        ("cifre_1", base + 100_000_000),
        ("cifre_2", base + 120_000_000),
        ("cifre_3", base + 123_000_000),
        ("cifre_4", base + 123_400_000),
        ("cifre_5", base + 123_450_000),
        ("cifre_6", base + 123_456_000),
        ("cifre_7", base + 123_456_700),
        ("cifre_8", base + 123_456_780),
        ("cifre_9", base + 123_456_789),
        ("quasi_secondo", base + 999_999_000),
        ("un_micro", base + 1_000),
        ("prima_epoca_micro", -1_000),
        ("prima_epoca_mezzo", -second / 2),
        ("anno_1", -62_135_596_800 * second),
        ("anno_0", -62_167_219_200 * second),
        ("anno_1582_giuliano", -12_219_379_200 * second),
        ("anno_1582_gregoriano", -12_219_292_800 * second),
        ("anno_1900", -2_203_891_200 * second),
        ("anno_9999_fine", 253_402_300_799 * second + 999_999_000),
        ("bisestile", 951_825_600 * second),
        ("anno_2038", 2_147_483_648 * second),
        ("dst_roma_salto", 1_774_747_800 * second),
        ("dst_roma_ritorno_prima", 1_792_888_200 * second),
        ("dst_roma_ritorno_dopo", 1_792_891_800 * second),
    ]
}

fn date_values() -> Vec<(&'static str, i32)> {
    vec![
        ("epoca", 0),
        ("meno_uno", -1),
        ("uno", 1),
        ("bisestile", 11_016),
        ("anno_1900_marzo", -25_508),
        ("gregoriano", -141_427),
        ("giuliano", -141_438),
        ("anno_1", -719_162),
        ("anno_0", -719_528),
        ("prima_dell_anno_0", -720_000),
        ("buco_1582", -141_432),
        ("anno_9999_fine", 2_932_896),
        ("anno_2038", 24_855),
    ]
}

fn with_null<T>(values: Vec<(&'static str, T)>) -> (Vec<String>, Vec<Option<T>>) {
    let mut names = values
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .collect::<Vec<_>>();
    let mut items = values
        .into_iter()
        .map(|(_, value)| Some(value))
        .collect::<Vec<_>>();
    names.push("nullo".to_owned());
    items.push(None);
    (names, items)
}

/// La batteria di `spec`, o `None` se il tipo Arrow non ne ha una (la
/// chiave non si misura e resta rifiutata).
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn battery(spec: &ProbeSpec) -> Option<Battery> {
    let (names, values): (Vec<String>, ArrayRef) = match &spec.arrow {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let named = if spec.document {
                document_values()
            } else {
                match spec.text_domain {
                    TextDomain::Free => text_values(spec.dimension),
                    TextDomain::OffsetInstant => offset_instant_values(),
                    TextDomain::Uuid => uuid_values(),
                }
            };
            let labels = named.iter().map(|(name, _)| name.clone()).collect();
            let values = named.into_iter().map(|(_, value)| value);
            let array: ArrayRef = if spec.arrow == DataType::Utf8 {
                Arc::new(values.collect::<StringArray>())
            } else {
                Arc::new(values.collect::<LargeStringArray>())
            };
            (labels, array)
        }
        DataType::Binary | DataType::LargeBinary => {
            let named = binary_values(spec.dimension);
            let labels = named.iter().map(|(name, _)| name.clone()).collect();
            let values = named
                .iter()
                .map(|(_, value)| value.as_deref())
                .collect::<Vec<_>>();
            let array: ArrayRef = if spec.arrow == DataType::Binary {
                Arc::new(BinaryArray::from(values))
            } else {
                Arc::new(LargeBinaryArray::from(values))
            };
            (labels, array)
        }
        DataType::Boolean => {
            let (names, items) = with_null(vec![("vero", true), ("falso", false)]);
            (names, Arc::new(BooleanArray::from(items)))
        }
        DataType::Int16 => {
            let (names, items) = with_null(vec![
                ("zero", 0_i16),
                ("uno", 1),
                ("meno_uno", -1),
                ("minimo", i16::MIN),
                ("massimo", i16::MAX),
            ]);
            (names, Arc::new(Int16Array::from(items)))
        }
        DataType::Int32 => {
            let (names, items) = with_null(vec![
                ("zero", 0_i32),
                ("uno", 1),
                ("meno_uno", -1),
                ("minimo", i32::MIN),
                ("massimo", i32::MAX),
            ]);
            (names, Arc::new(Int32Array::from(items)))
        }
        DataType::Int64 => {
            let (names, items) = with_null(vec![
                ("zero", 0_i64),
                ("uno", 1),
                ("meno_uno", -1),
                ("minimo", i64::MIN),
                ("massimo", i64::MAX),
                ("oltre_2_53", 9_007_199_254_740_993),
            ]);
            (names, Arc::new(Int64Array::from(items)))
        }
        DataType::Float64 => {
            let (names, items) = with_null(float64_values());
            (names, Arc::new(Float64Array::from(items)))
        }
        DataType::Float32 => {
            let (names, items) = with_null(float32_values());
            (names, Arc::new(Float32Array::from(items)))
        }
        DataType::Decimal128(precision, scale) => {
            let precision = u32::from(*precision);
            let scale_digits = u32::try_from(*scale).ok()?;
            let max = 10_i128.pow(precision) - 1;
            let mut values = vec![
                ("zero", 0_i128),
                ("unita", 1),
                ("meno_unita", -1),
                ("massimo", max),
                ("minimo", -max),
                ("cifre_alterne", alternating(precision)),
            ];
            if scale_digits > 0 {
                values.push(("mezzo", 5 * 10_i128.pow(scale_digits - 1)));
            }
            if precision > scale_digits {
                values.push(("intero", 10_i128.pow(scale_digits)));
            }
            let (names, items) = with_null(values);
            let array = Decimal128Array::from(items)
                .with_precision_and_scale(*u8::try_from(precision).ok().as_ref()?, *scale)
                .ok()?;
            (names, Arc::new(array))
        }
        DataType::Date32 => {
            let (names, items) = with_null(date_values());
            (names, Arc::new(Date32Array::from(items)))
        }
        DataType::UInt8 => {
            let (names, items) = with_null(vec![
                ("zero", 0_u8),
                ("uno", 1),
                ("meta", 127),
                ("oltre_meta", 128),
                ("massimo", u8::MAX),
            ]);
            (names, Arc::new(UInt8Array::from(items)))
        }
        // Gli interi stretti e senza segno di `MySQL` e `MariaDB` (`TINYINT`,
        // `SMALLINT UNSIGNED`, `INT UNSIGNED`, `BIGINT UNSIGNED`).
        DataType::Int8 => {
            let (names, items) = with_null(vec![
                ("zero", 0_i8),
                ("uno", 1),
                ("meno_uno", -1),
                ("minimo", i8::MIN),
                ("massimo", i8::MAX),
            ]);
            (
                names,
                Arc::new(plenora_database_core::arrow::array::Int8Array::from(items)),
            )
        }
        DataType::UInt16 => {
            let (names, items) = with_null(vec![
                ("zero", 0_u16),
                ("uno", 1),
                ("oltre_meta", 32_768),
                ("massimo", u16::MAX),
            ]);
            (
                names,
                Arc::new(plenora_database_core::arrow::array::UInt16Array::from(
                    items,
                )),
            )
        }
        DataType::UInt32 => {
            let (names, items) = with_null(vec![
                ("zero", 0_u32),
                ("uno", 1),
                ("oltre_meta", 2_147_483_648),
                ("massimo", u32::MAX),
            ]);
            (
                names,
                Arc::new(plenora_database_core::arrow::array::UInt32Array::from(
                    items,
                )),
            )
        }
        DataType::UInt64 => {
            let (names, items) = with_null(vec![
                ("zero", 0_u64),
                ("uno", 1),
                ("oltre_2_53", 9_007_199_254_740_993),
                ("oltre_meta", 9_223_372_036_854_775_808),
                ("massimo", u64::MAX),
            ]);
            (
                names,
                Arc::new(plenora_database_core::arrow::array::UInt64Array::from(
                    items,
                )),
            )
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let (names, items) = with_null(time_values());
            (names, Arc::new(Time64MicrosecondArray::from(items)))
        }
        // L'`interval` di PostgreSQL.
        DataType::Interval(plenora_database_core::arrow::schema::IntervalUnit::MonthDayNano) => {
            let values = interval_values()
                .into_iter()
                .map(|(name, (months, days, nanoseconds))| {
                    (
                        name,
                        plenora_database_core::arrow::array::types::IntervalMonthDayNanoType::make_value(
                            months,
                            days,
                            nanoseconds,
                        ),
                    )
                })
                .collect();
            let (names, items) = with_null(values);
            (
                names,
                Arc::new(
                    plenora_database_core::arrow::array::IntervalMonthDayNanoArray::from(items),
                ),
            )
        }
        DataType::Timestamp(unit, zone) => {
            let per_unit: i128 = match unit {
                TimeUnit::Second => 1_000_000_000,
                TimeUnit::Millisecond => 1_000_000,
                TimeUnit::Microsecond => 1_000,
                TimeUnit::Nanosecond => 1,
            };
            let representable = instant_values()
                .into_iter()
                .filter(|(_, ns)| ns % per_unit == 0)
                .filter_map(|(name, ns)| i64::try_from(ns / per_unit).ok().map(|raw| (name, raw)))
                .collect::<Vec<_>>();
            let (names, items) = with_null(representable);
            let array: ArrayRef = match unit {
                TimeUnit::Second => {
                    Arc::new(TimestampSecondArray::from(items).with_timezone_opt(zone.clone()))
                }
                TimeUnit::Millisecond => {
                    Arc::new(TimestampMillisecondArray::from(items).with_timezone_opt(zone.clone()))
                }
                TimeUnit::Microsecond => {
                    Arc::new(TimestampMicrosecondArray::from(items).with_timezone_opt(zone.clone()))
                }
                TimeUnit::Nanosecond => {
                    Arc::new(TimestampNanosecondArray::from(items).with_timezone_opt(zone.clone()))
                }
            };
            (names, array)
        }
        _ => return None,
    };
    Some(Battery { names, values })
}

fn alternating(digits: u32) -> i128 {
    (0..digits).fold(0_i128, |acc, index| {
        acc * 10 + i128::from(index % 2 == 0) * 9
    })
}

/// Il valore canonico di una cella per il confronto della sonda.
#[derive(Debug, Clone, PartialEq)]
enum Canon {
    Null,
    Bool(bool),
    /// Un numero esatto: il valore non scalato e la scala, ridotti.
    Exact(i128, i32),
    /// I bit di un float (un `f32` si allarga a `f64`, esattamente).
    Float(u64),
    NaN,
    Text(Vec<u8>),
    Bytes(Vec<u8>),
    InstantNs(i128),
    TimeNs(i128),
    Json(Json),
    /// Un istante con offset: i nanosecondi UTC dall'epoca, l'offset in
    /// secondi e se il testo nomina il secondo intercalare (`:60`).
    OffsetInstant(i128, i32, bool),
    Uuid(u128),
    /// Un intervallo: mesi, giorni e nanosecondi, campo per campo.
    Interval(i32, i32, i64),
    Unknown,
}

/// Il valore canonico di una cella nel dominio di `spec`: un testo del
/// dominio che non si legge resta testo, e non e uguale a nessun valore del
/// dominio.
fn canon_in(spec: &ProbeSpec, array: &dyn Array, row: usize) -> Canon {
    let plain = canon(array, row, spec.document);
    let Canon::Text(bytes) = &plain else {
        return plain;
    };
    let Ok(text) = std::str::from_utf8(bytes) else {
        return plain;
    };
    let typed = match spec.text_domain {
        TextDomain::Free => None,
        TextDomain::OffsetInstant => parse_offset_instant(text),
        TextDomain::Uuid => parse_uuid(text).map(Canon::Uuid),
    };
    typed.unwrap_or(plain)
}

fn digits(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// I giorni dal 1970-01-01 della data civile (calendario gregoriano
/// prolettico).
const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_index = (month + 9) % 12;
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Un istante RFC 3339 (`YYYY-MM-DD`, `T`, `t` o spazio, `HH:MM:SS`, una
/// frazione fino a nove cifre, `Z`, `z` o `±HH:MM`).
fn parse_offset_instant(text: &str) -> Option<Canon> {
    let bytes = text.as_bytes();
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b't' | b' ')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year = digits(&bytes[0..4])?;
    let month = digits(&bytes[5..7])?;
    let day = digits(&bytes[8..10])?;
    let hour = digits(&bytes[11..13])?;
    let minute = digits(&bytes[14..16])?;
    let second = digits(&bytes[17..19])?;
    let leap_year = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let month_days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap_year => 29,
        2 => 28,
        _ => return None,
    };
    if day < 1 || day > month_days || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let mut rest = &bytes[19..];
    let mut nanos: i128 = 0;
    if rest.first() == Some(&b'.') {
        let fraction = rest[1..]
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if fraction == 0 || fraction > 9 {
            return None;
        }
        let value = i128::from(digits(&rest[1..=fraction])?);
        nanos = value * 10_i128.pow(9 - u32::try_from(fraction).ok()?);
        rest = &rest[1 + fraction..];
    }
    let offset = match rest {
        [b'Z' | b'z'] => 0,
        [sign @ (b'+' | b'-'), h1, h2, b':', m1, m2] => {
            let hours = digits(&[*h1, *h2])?;
            let minutes = digits(&[*m1, *m2])?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            let seconds = i32::try_from(hours * 3_600 + minutes * 60).ok()?;
            if *sign == b'-' {
                -seconds
            } else {
                seconds
            }
        }
        _ => return None,
    };
    let local = i128::from(days_from_civil(year, month, day)) * DAY_NS
        + i128::from(hour * 3_600 + minute * 60 + second) * 1_000_000_000
        + nanos;
    Some(Canon::OffsetInstant(
        local - i128::from(offset) * 1_000_000_000,
        offset,
        second == 60,
    ))
}

/// Un UUID nelle forme con i trattini (anche fra graffe o dopo `urn:uuid:`) o
/// senza, in maiuscole o minuscole.
fn parse_uuid(text: &str) -> Option<u128> {
    let lowered = text.to_ascii_lowercase();
    let body = lowered
        .strip_prefix("urn:uuid:")
        .or_else(|| {
            lowered
                .strip_prefix('{')
                .and_then(|inner| inner.strip_suffix('}'))
        })
        .unwrap_or(&lowered);
    let hex = match body.len() {
        32 => body.to_owned(),
        36 => {
            let parts = body.split('-').map(str::len).collect::<Vec<_>>();
            if parts != [8, 4, 4, 4, 12] {
                return None;
            }
            body.replace('-', "")
        }
        _ => return None,
    };
    if !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    u128::from_str_radix(&hex, 16).ok()
}

const fn exact(mut unscaled: i128, mut scale: i32) -> Canon {
    while scale > 0 && unscaled % 10 == 0 {
        unscaled /= 10;
        scale -= 1;
    }
    if unscaled == 0 {
        scale = 0;
    }
    Canon::Exact(unscaled, scale)
}

const fn float(value: f64) -> Canon {
    if value.is_nan() {
        Canon::NaN
    } else {
        Canon::Float(value.to_bits())
    }
}

#[allow(clippy::too_many_lines)]
fn canon(array: &dyn Array, row: usize, document: bool) -> Canon {
    if array.is_null(row) {
        return Canon::Null;
    }
    let instant = |raw: i64, per_second: i128| {
        Canon::InstantNs(i128::from(raw) * (1_000_000_000 / per_second))
    };
    match array.data_type() {
        DataType::Boolean => Canon::Bool(array.as_boolean().value(row)),
        DataType::Int8 => exact(i128::from(array.as_primitive::<Int8Type>().value(row)), 0),
        DataType::Int16 => exact(i128::from(array.as_primitive::<Int16Type>().value(row)), 0),
        DataType::Int32 => exact(i128::from(array.as_primitive::<Int32Type>().value(row)), 0),
        DataType::Int64 => exact(i128::from(array.as_primitive::<Int64Type>().value(row)), 0),
        DataType::UInt8 => exact(i128::from(array.as_primitive::<UInt8Type>().value(row)), 0),
        DataType::UInt16 => exact(i128::from(array.as_primitive::<UInt16Type>().value(row)), 0),
        DataType::UInt32 => exact(i128::from(array.as_primitive::<UInt32Type>().value(row)), 0),
        DataType::UInt64 => exact(
            i128::from(
                array
                    .as_primitive::<plenora_database_core::arrow::array::types::UInt64Type>()
                    .value(row),
            ),
            0,
        ),
        DataType::Decimal128(_, scale) => exact(
            array.as_primitive::<Decimal128Type>().value(row),
            i32::from(*scale),
        ),
        DataType::Float32 => float(f64::from(array.as_primitive::<Float32Type>().value(row))),
        DataType::Float64 => float(array.as_primitive::<Float64Type>().value(row)),
        DataType::Utf8 | DataType::LargeUtf8 => {
            let text = if array.data_type() == &DataType::Utf8 {
                array.as_string::<i32>().value(row)
            } else {
                array.as_string::<i64>().value(row)
            };
            if document {
                if let Some(json) = parse_json(text) {
                    return Canon::Json(json);
                }
            }
            Canon::Text(text.as_bytes().to_vec())
        }
        DataType::Binary => Canon::Bytes(array.as_binary::<i32>().value(row).to_vec()),
        DataType::LargeBinary => Canon::Bytes(array.as_binary::<i64>().value(row).to_vec()),
        DataType::FixedSizeBinary(_) => {
            Canon::Bytes(array.as_fixed_size_binary().value(row).to_vec())
        }
        DataType::Date32 => {
            Canon::InstantNs(i128::from(array.as_primitive::<Date32Type>().value(row)) * DAY_NS)
        }
        DataType::Date64 => instant(array.as_primitive::<Date64Type>().value(row), 1_000),
        DataType::Timestamp(TimeUnit::Second, _) => {
            instant(array.as_primitive::<TimestampSecondType>().value(row), 1)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => instant(
            array.as_primitive::<TimestampMillisecondType>().value(row),
            1_000,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => instant(
            array.as_primitive::<TimestampMicrosecondType>().value(row),
            1_000_000,
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => instant(
            array.as_primitive::<TimestampNanosecondType>().value(row),
            1_000_000_000,
        ),
        DataType::Time64(TimeUnit::Microsecond) => Canon::TimeNs(
            i128::from(array.as_primitive::<Time64MicrosecondType>().value(row)) * 1_000,
        ),
        DataType::Time64(TimeUnit::Nanosecond) => Canon::TimeNs(i128::from(
            array.as_primitive::<Time64NanosecondType>().value(row),
        )),
        DataType::Interval(plenora_database_core::arrow::schema::IntervalUnit::MonthDayNano) => {
            let value = array
                .as_primitive::<plenora_database_core::arrow::array::types::IntervalMonthDayNanoType>()
                .value(row);
            Canon::Interval(value.months, value.days, value.nanoseconds)
        }
        _ => Canon::Unknown,
    }
}

/// Se la cella `read_row` di `read` rilegge esattamente la cella
/// `written_row` di `written`. Un tipo che il confronto non conosce non e
/// mai uguale.
#[must_use]
pub fn same_value(
    written: &dyn Array,
    written_row: usize,
    read: &dyn Array,
    read_row: usize,
    document: bool,
) -> bool {
    let left = canon(written, written_row, document);
    let right = canon(read, read_row, document);
    left != Canon::Unknown && left == right
}

/// Come [`same_value`], nel dominio del testo di `spec` ([`TextDomain`]).
#[must_use]
pub fn same_value_in(
    spec: &ProbeSpec,
    written: &dyn Array,
    written_row: usize,
    read: &dyn Array,
    read_row: usize,
) -> bool {
    let left = canon_in(spec, written, written_row);
    let right = canon_in(spec, read, read_row);
    left != Canon::Unknown && left == right
}

/// Un documento JSON per il confronto: i membri in ordine di chiave (le
/// chiavi ripetute restano), le stringhe decodificate, i numeri per forma.
#[derive(Debug, Clone, PartialEq)]
enum Json {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<Self>),
    Object(Vec<(String, Self)>),
}

fn parse_json(text: &str) -> Option<Json> {
    let mut parser = JsonParser {
        bytes: text.as_bytes(),
        index: 0,
    };
    let value = parser.value()?;
    parser.whitespace();
    (parser.index == parser.bytes.len()).then_some(value)
}

struct JsonParser<'a> {
    bytes: &'a [u8],
    index: usize,
}

impl JsonParser<'_> {
    fn whitespace(&mut self) {
        while self
            .bytes
            .get(self.index)
            .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
        {
            self.index += 1;
        }
    }

    fn literal(&mut self, word: &str, value: Json) -> Option<Json> {
        self.bytes[self.index..]
            .starts_with(word.as_bytes())
            .then(|| {
                self.index += word.len();
                value
            })
    }

    fn value(&mut self) -> Option<Json> {
        self.whitespace();
        match self.bytes.get(self.index)? {
            b'n' => self.literal("null", Json::Null),
            b't' => self.literal("true", Json::Bool(true)),
            b'f' => self.literal("false", Json::Bool(false)),
            b'"' => self.string().map(Json::String),
            b'[' => {
                self.index += 1;
                let mut items = Vec::new();
                self.whitespace();
                if self.bytes.get(self.index) == Some(&b']') {
                    self.index += 1;
                    return Some(Json::Array(items));
                }
                loop {
                    items.push(self.value()?);
                    self.whitespace();
                    match self.bytes.get(self.index)? {
                        b',' => self.index += 1,
                        b']' => {
                            self.index += 1;
                            return Some(Json::Array(items));
                        }
                        _ => return None,
                    }
                }
            }
            b'{' => {
                self.index += 1;
                let mut members = Vec::new();
                self.whitespace();
                if self.bytes.get(self.index) == Some(&b'}') {
                    self.index += 1;
                    return Some(Json::Object(members));
                }
                loop {
                    self.whitespace();
                    let key = self.string()?;
                    self.whitespace();
                    if self.bytes.get(self.index) != Some(&b':') {
                        return None;
                    }
                    self.index += 1;
                    members.push((key, self.value()?));
                    self.whitespace();
                    match self.bytes.get(self.index)? {
                        b',' => self.index += 1,
                        b'}' => {
                            self.index += 1;
                            members.sort_by(|left, right| left.0.cmp(&right.0));
                            return Some(Json::Object(members));
                        }
                        _ => return None,
                    }
                }
            }
            b'-' | b'0'..=b'9' => {
                let start = self.index;
                while self.bytes.get(self.index).is_some_and(|byte| {
                    matches!(byte, b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                }) {
                    self.index += 1;
                }
                Some(Json::Number(
                    String::from_utf8(self.bytes[start..self.index].to_vec()).ok()?,
                ))
            }
            _ => None,
        }
    }

    fn string(&mut self) -> Option<String> {
        if self.bytes.get(self.index) != Some(&b'"') {
            return None;
        }
        let start = self.index;
        self.index += 1;
        while *self.bytes.get(self.index)? != b'"' {
            self.index += if self.bytes[self.index] == b'\\' {
                2
            } else {
                1
            };
        }
        self.index += 1;
        serde_json::from_slice::<String>(&self.bytes[start..self.index]).ok()
    }
}

/// Le classi candidate per spiegare le riscritture di `spec`, in ordine.
fn candidates(
    spec: &ProbeSpec,
    written: &dyn Array,
    outcomes: &[ValueOutcome],
) -> Vec<MeasuredRewrite> {
    match &spec.arrow {
        DataType::Utf8 | DataType::LargeUtf8 if spec.document => {
            vec![MeasuredRewrite::VuotoANull, MeasuredRewrite::JsonNumeri]
        }
        DataType::Utf8 | DataType::LargeUtf8 => vec![
            MeasuredRewrite::VuotoANull,
            MeasuredRewrite::RiempieTesto,
            MeasuredRewrite::SpaziOltre,
            MeasuredRewrite::ToglieFinali,
            MeasuredRewrite::NonAscii,
        ],
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => vec![
            MeasuredRewrite::VuotoANull,
            MeasuredRewrite::RiempieBinario,
            MeasuredRewrite::ToglieFinali,
        ],
        DataType::Float32 | DataType::Float64 => {
            let mut list = vec![MeasuredRewrite::ZeroNegativo];
            if let Some(min_abs) = underflow_threshold(written, outcomes) {
                list.push(MeasuredRewrite::Underflow { min_abs });
            }
            list
        }
        DataType::Timestamp(_, _) => fractional_digits(written, outcomes)
            .map(|cifre| vec![MeasuredRewrite::FrazioneSecondi { cifre }])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn float_at(array: &dyn Array, row: usize) -> Option<f64> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Float32 => Some(f64::from(array.as_primitive::<Float32Type>().value(row))),
        DataType::Float64 => Some(array.as_primitive::<Float64Type>().value(row)),
        _ => None,
    }
}

/// Il minimo assoluto esatto: il piu piccolo modulo finito non nullo riletto
/// uguale, se ogni valore piu piccolo e riscritto.
fn underflow_threshold(written: &dyn Array, outcomes: &[ValueOutcome]) -> Option<f64> {
    let tiny = |outcome: ValueOutcome| {
        (0..written.len())
            .filter(|row| outcomes[*row] == outcome)
            .filter_map(|row| float_at(written, row))
            .filter(|value| value.is_finite() && *value != 0.0)
            .map(f64::abs)
            .collect::<Vec<_>>()
    };
    let exact_min = tiny(ValueOutcome::Exact)
        .into_iter()
        .fold(f64::INFINITY, f64::min);
    let rewritten = tiny(ValueOutcome::Rewritten);
    (exact_min.is_finite() && rewritten.iter().any(|value| *value < exact_min)).then_some(exact_min)
}

fn subsecond_ns(array: &dyn Array, row: usize) -> Option<i128> {
    match canon(array, row, false) {
        Canon::InstantNs(ns) => Some(ns.rem_euclid(1_000_000_000)),
        _ => None,
    }
}

/// Le cifre frazionarie conservate: la piu grande `d` per cui ogni istante
/// riscritto ha cifre oltre la `d`-esima.
fn fractional_digits(written: &dyn Array, outcomes: &[ValueOutcome]) -> Option<u8> {
    let rewritten = (0..written.len())
        .filter(|row| outcomes[*row] == ValueOutcome::Rewritten)
        .map(|row| subsecond_ns(written, row))
        .collect::<Option<Vec<_>>>()?;
    if rewritten.is_empty() {
        return None;
    }
    (0..=8_u8).rev().find(|digits| {
        let unit = 10_i128.pow(9 - u32::from(*digits));
        rewritten.iter().all(|ns| ns % unit != 0)
    })
}

/// Le classi che spiegano le riscritture misurate e i valori trattati per
/// eccesso, senza sapere quali valori sono diventati `NULL` (si veda
/// [`classify_with`]).
#[must_use]
pub fn classify(
    spec: &ProbeSpec,
    dimensions: TargetDimensions,
    written: &dyn Array,
    outcomes: &[ValueOutcome],
) -> (Vec<MeasuredRewrite>, Vec<usize>) {
    classify_with(
        spec,
        dimensions,
        written,
        outcomes,
        &vec![false; outcomes.len()],
    )
}

/// Le classi che spiegano le riscritture misurate e i valori trattati per
/// eccesso.
///
/// La scelta e avida: a ogni passo entra la classe che spiega piu valori
/// riscritti non ancora spiegati senza predire un valore riletto uguale; se
/// nessuna lo fa, una classe per eccesso (`riempie_testo`, che conta i
/// caratteri anche dove la colonna conta i byte, `non_ascii`, `json_numeri`,
/// `frazione_secondi`) con il minimo di valori trattati per eccesso, che si
/// registrano. A parita vale l'ordine delle candidate. `vuoto_a_null` spiega
/// soltanto i valori riletti come `NULL` (`became_null`), e le altre classi
/// soltanto quelli che non lo sono: un testo vuoto che `char(n)` completa con
/// spazi e un riempimento, non un `NULL`. Se le classi non coprono ogni
/// riscrittura, la chiave e `ogni`.
#[must_use]
pub fn classify_with(
    spec: &ProbeSpec,
    dimensions: TargetDimensions,
    written: &dyn Array,
    outcomes: &[ValueOutcome],
    became_null: &[bool],
) -> (Vec<MeasuredRewrite>, Vec<usize>) {
    let rewritten = (0..written.len())
        .filter(|row| outcomes[*row] == ValueOutcome::Rewritten)
        .collect::<Vec<_>>();
    let exact = (0..written.len())
        .filter(|row| outcomes[*row] == ValueOutcome::Exact)
        .collect::<Vec<_>>();
    if rewritten.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let predicts = |rewrite: StoreRewrite, row: usize| {
        !written.is_null(row)
            && rewrite.admits(written.data_type())
            && rewrite.rewrites(written, row)
    };
    let pool = candidates(spec, written, outcomes)
        .into_iter()
        .filter_map(|candidate| {
            instantiate(
                plenora_database_core::plan::ProviderKind::Postgres,
                candidate,
                dimensions,
            )
            .ok()
            .map(|rewrite| (candidate, rewrite))
        })
        .collect::<Vec<_>>();
    let mut selected: Vec<MeasuredRewrite> = Vec::new();
    let mut covered = std::collections::BTreeSet::new();
    let mut excess = std::collections::BTreeSet::new();
    loop {
        let uncovered = rewritten
            .iter()
            .copied()
            .filter(|row| !covered.contains(row))
            .collect::<Vec<_>>();
        if uncovered.is_empty() {
            break;
        }
        let mut best: Option<(usize, Vec<usize>, Vec<usize>)> = None;
        for (index, (candidate, rewrite)) in pool.iter().enumerate() {
            if selected.contains(candidate) {
                continue;
            }
            let to_null = *candidate == MeasuredRewrite::VuotoANull;
            let hits = uncovered
                .iter()
                .copied()
                .filter(|row| became_null[*row] == to_null && predicts(*rewrite, *row))
                .collect::<Vec<_>>();
            if hits.is_empty() {
                continue;
            }
            let false_positives = exact
                .iter()
                .copied()
                .filter(|row| predicts(*rewrite, *row))
                .collect::<Vec<_>>();
            let over_approximating = matches!(
                candidate,
                MeasuredRewrite::RiempieTesto
                    | MeasuredRewrite::NonAscii
                    | MeasuredRewrite::JsonNumeri
                    | MeasuredRewrite::FrazioneSecondi { .. }
            );
            if !false_positives.is_empty() && !over_approximating {
                continue;
            }
            let better = best.as_ref().is_none_or(|(_, best_hits, best_fp)| {
                (false_positives.len(), std::cmp::Reverse(hits.len()))
                    < (best_fp.len(), std::cmp::Reverse(best_hits.len()))
            });
            if better {
                best = Some((index, hits, false_positives));
            }
        }
        let Some((index, hits, false_positives)) = best else {
            return (vec![MeasuredRewrite::Ogni], exact);
        };
        covered.extend(hits);
        excess.extend(false_positives);
        selected.push(pool[index].0);
    }
    (selected, excess.into_iter().collect())
}

struct Batches {
    schema: SchemaRef,
    queue: VecDeque<RecordBatch>,
    rows: u64,
}

impl BatchStream for Batches {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move { Ok(self.queue.pop_front()) })
    }

    fn declared_input_rows(&self) -> Option<u64> {
        Some(self.rows)
    }
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget della sonda")
}

/// Il nome della colonna misurata `index`.
fn value_column(hooks: &dyn ProbeHooks, index: usize) -> String {
    format!("{}{index}", hooks.columns().1)
}

/// Il batch della sonda: le colonne di `batteries`, ogni riga (`row`
/// assente) o la sola riga `row`, con `ID` uguale all'indice nella batteria.
fn probe_batch(hooks: &dyn ProbeHooks, batteries: &[&Battery], row: Option<usize>) -> RecordBatch {
    let mut fields = vec![Field::new(hooks.columns().0, DataType::Int64, false)];
    fields.extend(batteries.iter().enumerate().map(|(index, battery)| {
        Field::new(
            value_column(hooks, index),
            battery.values.data_type().clone(),
            true,
        )
    }));
    let schema =
        plenora_database_core::protocol::contract_schema(fields).expect("schema della sonda");
    let rows = batteries.first().map_or(0, |battery| battery.values.len());
    let ids: Vec<i64> = row.map_or_else(
        || {
            (0..rows)
                .map(|index| i64::try_from(index).expect("indice"))
                .collect()
        },
        |row| vec![i64::try_from(row).expect("indice")],
    );
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(ids))];
    columns.extend(batteries.iter().map(|battery| {
        row.map_or_else(
            || Arc::clone(&battery.values),
            |row| battery.values.slice(row, 1),
        )
    }));
    RecordBatch::try_new(schema, columns).expect("batch della sonda")
}

fn operation(hooks: &dyn ProbeHooks) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: hooks.schema(),
            object: PROBE_TABLE.to_owned(),
        },
        mode: WriteMode::Append,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

async fn write_rows(hooks: &dyn ProbeHooks, batteries: &[&Battery], row: Option<usize>) -> bool {
    let cancellation = CancellationToken::new();
    let budget = budget();
    let batch = probe_batch(hooks, batteries, row);
    let operation = operation(hooks);
    let prepared = match hooks
        .provider()
        .prepare_write(
            hooks.secret(),
            &operation,
            batch.schema(),
            &budget,
            &cancellation,
        )
        .await
    {
        Ok(prepared) => prepared,
        Err(error) => {
            trace("preflight", &error);
            return false;
        }
    };
    let input = Batches {
        schema: batch.schema(),
        rows: batch.num_rows() as u64,
        queue: VecDeque::from([batch]),
    };
    match hooks
        .provider()
        .write(
            hooks.secret(),
            prepared,
            Box::new(input),
            &budget,
            &cancellation,
        )
        .await
    {
        Ok(outcome) => outcome.status == WriteStatus::Committed,
        Err(error) => {
            trace("scrittura", &error);
            false
        }
    }
}

/// Con `PLENORA_SONDE_SCRITTURE_TRACCIA=1` la sonda stampa categoria e
/// messaggio degli errori (senza dati: e la regola dei messaggi pubblici).
///
/// Un errore di trasporto, di connessione o di tempo non e una misura: il
/// server o la rete non hanno risposto, e la sonda si ferma invece di
/// registrare la chiave come rifiutata.
fn trace(phase: &str, error: &plenora_database_core::DatabaseError) {
    assert!(
        !matches!(
            error.category,
            plenora_database_core::ErrorCategory::Io
                | plenora_database_core::ErrorCategory::Timeout
                | plenora_database_core::ErrorCategory::Transient
        ),
        "la sonda non misura: errore di trasporto ({phase}): {:?} {}",
        error.category,
        error.message
    );
    if std::env::var("PLENORA_SONDE_SCRITTURE_TRACCIA").as_deref() == Ok("1") {
        eprintln!(
            "sonda {phase}: {:?} {:?} {}",
            error.category, error.phase, error.message
        );
    }
}

/// Le righe rilette: per ogni `ID`, il batch e la riga; per ogni batch, le
/// colonne misurate.
type ReadBack = (BTreeMap<i64, (usize, usize)>, Vec<Vec<ArrayRef>>);

async fn read_back(hooks: &dyn ProbeHooks, columns: usize) -> Option<ReadBack> {
    let id = hooks.columns().0;
    let cancellation = CancellationToken::new();
    let budget = budget();
    let mut projection = vec![id.to_owned()];
    projection.extend((0..columns).map(|index| value_column(hooks, index)));
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: hooks.schema(),
            object: PROBE_TABLE.to_owned(),
        },
        projection,
        order_by: vec![OrderBy {
            field: id.to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    let mut stream = hooks
        .provider()
        .read(
            hooks.secret(),
            &operation,
            &ParameterBag::new(BTreeMap::new()),
            &budget,
            &cancellation,
        )
        .await
        .map_err(|error| trace("lettura", &error))
        .ok()?;
    let mut positions = BTreeMap::new();
    let mut batches = Vec::new();
    while let Some(batch) = stream
        .next_batch(&cancellation)
        .await
        .map_err(|error| trace("lettura", &error))
        .ok()?
    {
        let column = batch.column(0);
        for row in 0..batch.num_rows() {
            let id = match canon(column.as_ref(), row, false) {
                Canon::Exact(value, 0) => i64::try_from(value).ok()?,
                _ => return None,
            };
            if positions.insert(id, (batches.len(), row)).is_some() {
                return None;
            }
        }
        batches.push(batch.columns()[1..].to_vec());
    }
    Some((positions, batches))
}

async fn ddl(hooks: &dyn ProbeHooks, sql: &str) -> bool {
    hooks
        .provider()
        .execute_ddl(hooks.secret(), sql, &CancellationToken::new())
        .await
        .is_ok()
}

/// Le chiavi che si misurano insieme, una colonna ciascuna: lo stesso tipo
/// Arrow e la stessa batteria (gli stessi nomi), al piu `GROUP` colonne.
const GROUP: usize = 120;

fn same_shape(left: &(ProbeSpec, Battery), right: &(ProbeSpec, Battery)) -> bool {
    left.0.arrow == right.0.arrow
        && left.0.document == right.0.document
        && left.0.text_domain == right.0.text_domain
        && left.1.names == right.1.names
}

/// Misura un gruppo di chiavi in una tabella sola. Con piu di una chiave, se
/// la scrittura dell'intera batteria non riesce si restituisce `None` e le
/// chiavi si misurano una per una; con una sola, si riscrive valore per
/// valore.
///
/// # Panics
///
/// Se la tabella della sonda non si crea o le chiavi d'ammissione non si
/// leggono: la misura non si puo fare, e il gate fallisce.
async fn measure_group(
    hooks: &dyn ProbeHooks,
    group: &[&(ProbeSpec, Battery)],
) -> Option<Vec<MeasuredKey>> {
    let _ = ddl(hooks, &hooks.drop_sql()).await;
    let natives = group
        .iter()
        .map(|(spec, _)| spec.native_ddl.clone())
        .collect::<Vec<_>>();
    for sql in hooks.create_sql(&natives) {
        if let Err(error) = hooks
            .provider()
            .execute_ddl(hooks.secret(), &sql, &CancellationToken::new())
            .await
        {
            panic!("la tabella della sonda non si crea: {natives:?}: {error:?}");
        }
    }
    let columns = group
        .iter()
        .enumerate()
        .map(|(index, (spec, _))| (value_column(hooks, index), spec.arrow.clone()))
        .collect::<Vec<_>>();
    let keys = hooks
        .admission_keys(&columns)
        .await
        .unwrap_or_else(|error| panic!("chiavi d'ammissione di {natives:?}: {error:?}"));
    assert_eq!(keys.len(), group.len(), "{natives:?}");
    for ((spec, _), (key, _)) in group.iter().zip(&keys) {
        assert_eq!(key.arrow, arrow_key(&spec.arrow), "{}", spec.native_ddl);
        assert_eq!(key.dimensione, spec.dimension, "{}", spec.native_ddl);
    }
    if group.iter().any(|(spec, battery)| {
        hooks
            .direct_probe(spec, battery.values.as_ref(), 0)
            .is_some()
    }) {
        if group.len() > 1 {
            assert!(ddl(hooks, &hooks.drop_sql()).await, "pulizia oracolo");
            return None;
        }
        let (spec, battery) = group[0];
        let (key, dimensions) = keys.into_iter().next().expect("chiave");
        return Some(vec![
            direct::measure(hooks, spec, battery, key, dimensions).await,
        ]);
    }
    let batteries = group.iter().map(|(_, battery)| battery).collect::<Vec<_>>();
    let rows = batteries[0].names.len();
    let mut refused = vec![false; rows];
    if !write_rows(hooks, &batteries, None).await {
        if group.len() > 1 {
            let _ = ddl(hooks, &hooks.drop_sql()).await;
            return None;
        }
        let _ = ddl(hooks, &hooks.truncate_sql()).await;
        for (row, refusal) in refused.iter_mut().enumerate() {
            *refusal = !write_rows(hooks, &batteries, Some(row)).await;
        }
    }
    let read = read_back(hooks, group.len()).await;
    // `PLENORA_SONDE_SCRITTURE_CONSERVA=1` lascia la tabella per la diagnosi.
    if std::env::var("PLENORA_SONDE_SCRITTURE_CONSERVA").as_deref() != Ok("1") {
        let _ = ddl(hooks, &hooks.drop_sql()).await;
    }
    Some(
        group
            .iter()
            .zip(keys)
            .enumerate()
            .map(|(column, ((spec, battery), (key, dimensions)))| {
                let measured = verdict(
                    spec,
                    battery,
                    key,
                    dimensions,
                    &refused,
                    read.as_ref(),
                    column,
                );
                eprintln!(
                    "sonda {} {}: {:?}",
                    spec.native_ddl, measured.arrow, measured.esito
                );
                measured
            })
            .collect(),
    )
}

/// L'esito di una colonna misurata.
fn verdict(
    spec: &ProbeSpec,
    battery: &Battery,
    key: AdmissionKey,
    dimensions: TargetDimensions,
    refused: &[bool],
    read: Option<&ReadBack>,
    column: usize,
) -> MeasuredKey {
    let rows = refused.len();
    let mut outcomes = vec![ValueOutcome::Refused; rows];
    let mut became_null = vec![false; rows];
    if let Some((positions, batches)) = read {
        for row in 0..rows {
            if refused[row] {
                continue;
            }
            let id = i64::try_from(row).expect("indice");
            if let Some((batch, position)) = positions.get(&id) {
                became_null[row] =
                    !battery.values.is_null(row) && batches[*batch][column].is_null(*position);
            }
            outcomes[row] = match positions.get(&id) {
                Some((batch, position))
                    if same_value_in(
                        spec,
                        battery.values.as_ref(),
                        row,
                        batches[*batch][column].as_ref(),
                        *position,
                    ) =>
                {
                    ValueOutcome::Exact
                }
                _ => ValueOutcome::Rewritten,
            };
        }
    }
    let readable = read.is_some();
    let non_null_written = (0..rows)
        .filter(|row| !battery.values.is_null(*row) && !refused[*row])
        .count();
    let esito = if !readable && refused.iter().any(|value| !value) {
        MeasuredOutcome::Illeggibile
    } else if non_null_written == 0 {
        MeasuredOutcome::Rifiuta
    } else if outcomes.contains(&ValueOutcome::Rewritten) {
        MeasuredOutcome::Riscrive
    } else {
        MeasuredOutcome::Esatto
    };
    let names = |selected: &[usize]| {
        selected
            .iter()
            .map(|row| battery.names[*row].clone())
            .collect::<Vec<_>>()
    };
    let (riscritture, per_eccesso, riscritti) = if esito == MeasuredOutcome::Riscrive {
        let (classes, excess) = classify_with(
            spec,
            dimensions,
            battery.values.as_ref(),
            &outcomes,
            &became_null,
        );
        let rewritten = (0..rows)
            .filter(|row| outcomes[*row] == ValueOutcome::Rewritten)
            .collect::<Vec<_>>();
        (classes, names(&excess), names(&rewritten))
    } else {
        (Vec::new(), Vec::new(), Vec::new())
    };
    let rifiutati = (0..rows)
        .filter(|row| refused[*row] || !readable)
        .collect::<Vec<_>>();
    MeasuredKey {
        arrow: key.arrow,
        nativo: key.nativo,
        dimensione: key.dimensione,
        esito,
        riscritture,
        riscritti,
        rifiutati: names(&rifiutati),
        per_eccesso,
    }
}

/// Misura una chiave da sola.
///
/// # Panics
///
/// Come [`measure_server`].
pub async fn measure_key(hooks: &dyn ProbeHooks, spec: &ProbeSpec) -> MeasuredKey {
    let Some(battery) = battery(spec) else {
        panic!("nessuna batteria per {}", spec.native_ddl);
    };
    let pair = (spec.clone(), battery);
    measure_group(hooks, &[&pair])
        .await
        .and_then(|mut keys| keys.pop())
        .expect("una chiave da sola si misura sempre")
}

/// Misura ogni chiave e restituisce le misure del server.
///
/// Le chiavi con lo stesso tipo Arrow e la stessa batteria si misurano insieme, una colonna
/// ciascuna, e una per una se la batteria intera non si scrive.
///
/// # Panics
///
/// Se una tabella della sonda non si crea, una chiave d'ammissione non si
/// legge o un tipo Arrow non ha una batteria: la misura non si puo fare, e
/// il gate fallisce.
pub async fn measure_server(
    hooks: &dyn ProbeHooks,
    specs: &[ProbeSpec],
    server_major: &str,
    server_version: &str,
) -> ServerMeasurement {
    // `PLENORA_SONDE_SCRITTURE_FILTRO` (soltanto con la misura, mai con la
    // verifica: un fixture si confronta per intero) restringe le chiavi a
    // quelle il cui tipo nativo contiene il testo dato, per la diagnosi.
    let filter = std::env::var("PLENORA_SONDE_SCRITTURE_FILTRO").ok();
    assert!(
        filter.is_none() || std::env::var("PLENORA_SONDE_SCRITTURE_MISURA").as_deref() == Ok("1"),
        "il filtro vale soltanto con la misura"
    );
    let prepared = specs
        .iter()
        .filter(|spec| {
            filter
                .as_deref()
                .is_none_or(|filter| spec.native_ddl.contains(filter))
        })
        .map(|spec| {
            let Some(battery) = battery(spec) else {
                panic!("nessuna batteria per {}", spec.native_ddl);
            };
            (spec.clone(), battery)
        })
        .collect::<Vec<_>>();
    let mut chiavi = Vec::with_capacity(specs.len());
    let mut start = 0;
    while start < prepared.len() {
        let mut end = start + 1;
        while end < prepared.len()
            && end - start < GROUP
            && same_shape(&prepared[start], &prepared[end])
        {
            end += 1;
        }
        let group = prepared[start..end].iter().collect::<Vec<_>>();
        if let Some(keys) = measure_group(hooks, &group).await {
            chiavi.extend(keys);
        } else {
            for pair in group {
                chiavi.extend(
                    measure_group(hooks, &[pair])
                        .await
                        .expect("una chiave da sola si misura sempre"),
                );
            }
        }
        start = end;
    }
    ServerMeasurement {
        server_major: server_major.to_owned(),
        server_version: server_version.to_owned(),
        immagine: std::env::var("PLENORA_SONDE_SCRITTURE_IMMAGINE")
            .unwrap_or_else(|_| "non dichiarata".to_owned()),
        batteria: BATTERY_VERSION.to_owned(),
        chiavi,
    }
}

/// Scrive il referto e verifica la misura contro il fixture.
///
/// Il referto va in `PLENORA_SONDE_SCRITTURE_REFERTO`; salvo
/// `PLENORA_SONDE_SCRITTURE_MISURA=1` la misura deve coincidere con il
/// fixture incorporato per la stessa major: e l'oracolo di regressione.
///
/// # Panics
///
/// Se il referto non si scrive, o la misura non coincide con il fixture.
pub fn report_and_check(measured: &ServerMeasurement, fixture: &[u8]) {
    if let Ok(path) = std::env::var("PLENORA_SONDE_SCRITTURE_REFERTO") {
        let document = serde_json::to_string_pretty(measured).expect("referto");
        std::fs::write(&path, document + "\n").expect("scrittura del referto");
    }
    if std::env::var("PLENORA_SONDE_SCRITTURE_MISURA").as_deref() == Ok("1") {
        return;
    }
    let fixture: AdmissionFixture =
        plenora_database_core::strict_json::from_slice(fixture).expect("fixture della regola A");
    let Some(expected) = fixture
        .misure
        .iter()
        .find(|measurement| measurement.server_major == measured.server_major)
    else {
        panic!(
            "il fixture non misura la major {}: si rigenera con la sonda",
            measured.server_major
        );
    };
    let differences = differences(expected, measured);
    assert!(
        differences.is_empty(),
        "la misura del server (non l'ammissione della libreria) non coincide con il fixture:\n{}",
        differences.join("\n")
    );
}

/// Le differenze fra due misure della stessa major, chiave per chiave.
#[must_use]
pub fn differences(expected: &ServerMeasurement, measured: &ServerMeasurement) -> Vec<String> {
    let index = |measurement: &ServerMeasurement| {
        measurement
            .chiavi
            .iter()
            .map(|key| {
                (
                    (key.arrow.clone(), key.nativo.clone(), key.dimensione),
                    key.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    let (left, right) = (index(expected), index(measured));
    let mut out = Vec::new();
    for (key, value) in &left {
        match right.get(key) {
            None => out.push(format!("assente nella misura: {key:?}")),
            Some(other) if other != value => {
                out.push(format!(
                    "diversa: {key:?}\n  fixture {value:?}\n  misura  {other:?}"
                ));
            }
            Some(_) => {}
        }
    }
    for key in right.keys() {
        if !left.contains_key(key) {
            out.push(format!("assente nel fixture: {key:?}"));
        }
    }
    out
}

#[cfg(test)]
#[path = "write_probe_tests.rs"]
mod tests;
