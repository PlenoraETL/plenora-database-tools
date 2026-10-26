//! La regola unica dei parametri che incontrano un operando.
//!
//! **Regola.** Un parametro si lega sul tipo dell'operando che incontra — la
//! colonna in cui si scrive o con cui si confronta, come la descrive il
//! catalogo, **con** precisione, scala, lunghezza e precisione temporale — e
//! si ammette soltanto se il valore e rappresentabile esatto in quel tipo.
//! Il valore ammesso e il valore riscritto nel tipo dell'operando, cosi che il
//! server confronti o assegni fra valori dello stesso tipo e non converta
//! nessuno dei due: un `F64(2^53)` contro un `BIGINT` diventa `I64(2^53)`, un
//! `Decimal("0.5")` contro un `DOUBLE` diventa `F64(0.5)`, un `Timestamp` a
//! mezzanotte contro una `DATE` diventa `Date`. Intero, floating point e
//! decimale si giudicano per rappresentabilita esatta, non per famiglia. Ogni
//! altro caso si rifiuta prima dell'invio, `DataMapping` in `Prepare`: un
//! intero oltre 2^53 contro un `DOUBLE`, un `F64(0.1)` contro un
//! `DECIMAL(12, 4)`, un `Decimal` con piu cifre della scala, un `Timestamp`
//! con una frazione oltre la precisione della colonna, una data con un'ora
//! diversa da mezzanotte. Un operando il cui tipo non si conosce esatto
//! ([`OperandType::Unknown`]) rifiuta ogni valore che non sia `NULL`.
//!
//! La regola vale dove l'operando e noto: l'AST portable (il catalogo della
//! transazione, `TransactionScope::column_types`), il DML relazionale dello
//! SDK, il filtro della lettura plan-based (il catalogo dell'oggetto letto),
//! il parameter-array Db2 e lo SQL del chiamante su Db2 (il tipo che
//! `SQLDescribeParam` da al segnaposto). La applica soltanto il cancello
//! [`crate::admission_gate`], con l'elenco dei punti d'ingresso.
//!
//! Il terzo giro della #135 l'ha estesa a tre classi che il censimento a punti
//! non vedeva, trovate dal censimento generato
//! (`plenora_database_testkit::parameter_sweep`):
//!
//! - **testo**: la lunghezza nell'unita della colonna ([`LengthUnit`]:
//!   caratteri, byte del charset, unita UTF-16) e ogni carattere nel suo
//!   charset ([`Charset`]); altrimenti il server tronca (`'abc   '` in un
//!   `varchar(3)` di PostgreSQL e Db2 diventava `'abc'`) o sostituisce
//!   (`'łę日x'` in un `varchar` CP1252 di SQL Server diventava `'l?x'`);
//! - **`FLOAT(M, D)` e `DOUBLE(M, D)` di `MySQL` e `MariaDB`**
//!   ([`OperandType::ScaledFloat`]): il server arrotonda con l'aritmetica dei
//!   double, e soltanto un valore esatto con `D` cifre e al piu quindici cifre
//!   resta lui (`1.25` in `FLOAT(7, 1)` diventava `1.2`, `-0.1` in `DOUBLE(10,
//!   2)` diventava `-0.09999999999999998`);
//! - **decimali a scala negativa** ([`OperandType::RoundedDecimal`]).
//!
//! # Limite dichiarato: lo SQL del chiamante
//!
//! - **Regola deviata.** Sullo SQL scritto dal chiamante (`execute`, `query`
//!   della transazione) di PostgreSQL, `MySQL`, `MariaDB`, SQL Server e
//!   Oracle l'operando di un segnaposto non si conosce con il suo tipo
//!   completo: `MySQL`, `MariaDB` e Oracle non descrivono i segnaposto,
//!   PostgreSQL ne descrive il tipo senza typmod, SQL Server non li descrive
//!   sul protocollo usato. Rifiutarli tutti chiuderebbe il percorso.
//! - **Ambito.** Il valore arriva al server esatto nel **proprio** tipo
//!   (`parameter_fidelity`); l'assegnazione a una colonna piu stretta (un
//!   `Decimal` in un `numeric(12, 4)`, una frazione in un `timestamp(0)`) e il
//!   confronto fra tipi diversi (un `BIGINT` con un `DOUBLE`) seguono le
//!   regole del server, che arrotonda o converte.
//! - **Hazard.** Il valore conservato puo differire da quello legato, e un
//!   confronto fra tipi diversi puo ritrovare un vicino.
//! - **Rientro.** La descrizione dei segnaposto con il tipo completo:
//!   `sp_describe_undeclared_parameters` su SQL Server, il typmod del
//!   bersaglio su PostgreSQL. Chi vuole la garanzia usa l'AST portable, che
//!   legge il catalogo.

use crate::plan::{FilterExpression, ProviderKind};
use crate::portable::{Expression, PortableStatement, Predicate};
use crate::provider::{ParameterBag, ParameterValue};
use crate::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::BTreeMap;
use std::fmt::Write as _;

/// Il tipo di una colonna come lo dice il catalogo del provider, prima della
/// classificazione in [`OperandType`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogType {
    /// Il nome del tipo nel catalogo del provider (`int4`, `decimal`,
    /// `TIMESTAMP(6)`, `DECIMAL`).
    pub name: String,
    /// La dichiarazione completa quando porta piu del nome (`COLUMN_TYPE` di
    /// `MySQL`: `int unsigned`).
    pub declaration: Option<String>,
    /// Lunghezza in caratteri per i tipi testuali, in byte per i binari.
    pub length: Option<u64>,
    pub precision: Option<u64>,
    pub scale: Option<i64>,
    /// Cifre frazionarie dei tipi temporali, quando il catalogo le tiene a
    /// parte dalla scala.
    pub datetime_precision: Option<u64>,
    /// L'unita in cui il catalogo conta la lunghezza di un tipo testuale.
    pub length_unit: LengthUnit,
    /// I caratteri che la colonna testuale sa conservare.
    pub charset: Charset,
}

/// L'unita della lunghezza di un tipo testuale.
///
/// I caratteri (PostgreSQL, `MySQL`, `VARCHAR2(n CHAR)`), i byte del charset della colonna
/// (`varchar(n)` di SQL Server, `VARCHAR2(n BYTE)`, `VARCHAR(n)` di Db2), le
/// unita UTF-16 (`nvarchar(n)`, `NVARCHAR2(n)` in AL16UTF16, `GRAPHIC`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LengthUnit {
    #[default]
    Characters,
    Bytes,
    Utf16,
}

/// I caratteri che una colonna testuale conserva.
///
/// Un carattere fuori dal
/// charset il server lo sostituisce (`'日'` in un `varchar` CP1252 di SQL
/// Server diventava `'?'`, `'ł'` diventava `'l'`): si rifiuta prima.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Charset {
    /// UTF-8 o UTF-16: ogni carattere.
    #[default]
    Unicode,
    /// UTF-8 a tre byte (`utf8mb3` di `MySQL`): il piano multilingue di base.
    UnicodeBmp,
    /// Windows-1252 (`latin1` di `MySQL`, le collation CP1252 di SQL Server,
    /// `WE8MSWIN1252` di Oracle), un byte per carattere.
    Windows1252,
    /// Un charset che il repository non tabula: soltanto ASCII e certo, un
    /// byte per carattere.
    Ascii,
}

impl Charset {
    /// Se il charset conserva `character`.
    #[must_use]
    pub fn holds(self, character: char) -> bool {
        match self {
            Self::Unicode => true,
            Self::UnicodeBmp => u32::from(character) <= 0xFFFF,
            Self::Ascii => character.is_ascii(),
            Self::Windows1252 => {
                character.is_ascii()
                    || ('\u{A0}'..='\u{FF}').contains(&character)
                    || WINDOWS_1252_HIGH.contains(&character)
            }
        }
    }

    /// I byte di `text` nel charset (soltanto per un testo che il charset
    /// conserva).
    fn bytes(self, text: &str) -> usize {
        match self {
            Self::Unicode | Self::UnicodeBmp => text.len(),
            Self::Windows1252 | Self::Ascii => text.chars().count(),
        }
    }
}

/// I caratteri di Windows-1252 fra `0x80` e `0x9F` (i cinque byte non
/// definiti esclusi).
const WINDOWS_1252_HIGH: [char; 27] = [
    '\u{20AC}', '\u{201A}', '\u{0192}', '\u{201E}', '\u{2026}', '\u{2020}', '\u{2021}', '\u{02C6}',
    '\u{2030}', '\u{0160}', '\u{2039}', '\u{0152}', '\u{017D}', '\u{2018}', '\u{2019}', '\u{201C}',
    '\u{201D}', '\u{2022}', '\u{2013}', '\u{2014}', '\u{02DC}', '\u{2122}', '\u{0161}', '\u{203A}',
    '\u{0153}', '\u{017E}', '\u{0178}',
];

/// La lunghezza di `text` nell'unita della colonna.
fn text_length(text: &str, unit: LengthUnit, charset: Charset) -> usize {
    match unit {
        LengthUnit::Characters => text.chars().count(),
        LengthUnit::Bytes => charset.bytes(text),
        LengthUnit::Utf16 => text.encode_utf16().count(),
    }
}

/// Il tipo di un operando, con cio che serve a giudicare l'esattezza.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperandType {
    Boolean,
    Integer {
        min: i128,
        max: i128,
    },
    /// Floating point IEEE con `mantissa_bits` bit di mantissa (24 o 53).
    Float {
        mantissa_bits: u32,
    },
    /// `FLOAT(M, D)` e `DOUBLE(M, D)` di `MySQL` e `MariaDB`: il server
    /// arrotonda il valore a `scale` cifre frazionarie e `precision` cifre in
    /// tutto prima di salvarlo come floating point (`1.25` in `FLOAT(7, 1)`
    /// diventava `1.2`).
    ScaledFloat {
        mantissa_bits: u32,
        precision: u32,
        scale: u32,
    },
    /// Decimale a precisione e scala fisse.
    Decimal {
        precision: u32,
        scale: u32,
    },
    /// Decimale a scala negativa (`numeric(p, -r)` PostgreSQL, `NUMBER(p, -r)`
    /// Oracle): un intero multiplo di `10^rounding` con al piu `precision`
    /// cifre prima degli zeri arrotondati.
    RoundedDecimal {
        precision: u32,
        rounding: u32,
    },
    /// Decimale senza scala fissa, al piu `digits` cifre significative.
    UnboundedDecimal {
        digits: u32,
    },
    /// Testo; `padded` per i tipi a lunghezza fissa completati con spazi.
    /// La lunghezza si conta in `unit`, e la colonna conserva i caratteri di
    /// `charset`.
    Text {
        length: Option<u64>,
        padded: bool,
        unit: LengthUnit,
        charset: Charset,
    },
    /// Binario; `padded` per i tipi a lunghezza fissa completati con zeri.
    Binary {
        length: Option<u64>,
        padded: bool,
    },
    Date,
    /// Data e ora con `fraction_digits` cifre frazionarie (0..=6 portabili).
    Timestamp {
        fraction_digits: u32,
    },
    TimestampTz {
        fraction_digits: u32,
    },
    Uuid,
    Json,
    /// Un tipo enumerato dell'utente: ammette la propria etichetta.
    Enumeration,
    /// Un tipo che il repository non sa giudicare esatto (temporali legacy,
    /// LOB, intervalli, tipi dell'utente): ammette soltanto `NULL`.
    Unknown,
}

const I8: (i128, i128) = (i8::MIN as i128, i8::MAX as i128);
const I16: (i128, i128) = (i16::MIN as i128, i16::MAX as i128);
const I24: (i128, i128) = (-8_388_608, 8_388_607);
const I32: (i128, i128) = (i32::MIN as i128, i32::MAX as i128);
const I64: (i128, i128) = (i64::MIN as i128, i64::MAX as i128);

const fn integer(range: (i128, i128)) -> OperandType {
    OperandType::Integer {
        min: range.0,
        max: range.1,
    }
}

const fn unsigned(bits: u32) -> OperandType {
    OperandType::Integer {
        min: 0,
        max: (1_i128 << bits) - 1,
    }
}

fn decimal(precision: Option<u64>, scale: Option<i64>, unbounded_digits: u32) -> OperandType {
    match (precision, scale) {
        (Some(precision), Some(scale)) if scale >= 0 => OperandType::Decimal {
            precision: u32::try_from(precision).unwrap_or(u32::MAX),
            scale: u32::try_from(scale).unwrap_or(u32::MAX),
        },
        (Some(precision), None) => OperandType::Decimal {
            precision: u32::try_from(precision).unwrap_or(u32::MAX),
            scale: 0,
        },
        (Some(precision), Some(scale)) => OperandType::RoundedDecimal {
            precision: u32::try_from(precision).unwrap_or(u32::MAX),
            rounding: u32::try_from(scale.unsigned_abs()).unwrap_or(u32::MAX),
        },
        // Una scala senza precisione (`NUMBER(*, s)` di Oracle): la
        // precisione e quella del tipo, la scala resta. Seconda lettura del
        // terzo giro: `NUMBER(*, -2)` ammetteva `123` e conservava `100`.
        (None, Some(scale)) if scale > 0 => OperandType::Decimal {
            precision: unbounded_digits,
            scale: u32::try_from(scale).unwrap_or(u32::MAX),
        },
        (None, Some(scale)) if scale < 0 => OperandType::RoundedDecimal {
            precision: unbounded_digits,
            rounding: u32::try_from(scale.unsigned_abs()).unwrap_or(u32::MAX),
        },
        (None, _) => OperandType::UnboundedDecimal {
            digits: unbounded_digits,
        },
    }
}

/// Un tipo testuale del catalogo, con l'unita della sua lunghezza e il suo
/// charset.
const fn text_of(length: Option<u64>, padded: bool, catalog: &CatalogType) -> OperandType {
    OperandType::Text {
        length,
        padded,
        unit: catalog.length_unit,
        charset: catalog.charset,
    }
}

/// Un testo senza lunghezza, in un charset Unicode.
const UNBOUNDED_TEXT: OperandType = OperandType::Text {
    length: None,
    padded: false,
    unit: LengthUnit::Characters,
    charset: Charset::Unicode,
};

fn fraction(digits: Option<u64>, default: u32) -> u32 {
    digits.map_or(default, |digits| u32::try_from(digits).unwrap_or(u32::MAX))
}

/// La classificazione di un tipo del catalogo, una tabella per provider.
#[must_use]
pub fn operand_type(provider: ProviderKind, catalog: &CatalogType) -> OperandType {
    let name = catalog.name.to_ascii_lowercase();
    match provider {
        ProviderKind::Postgres => postgres(&name, catalog),
        ProviderKind::Mysql | ProviderKind::Mariadb => mysql(&name, catalog),
        ProviderKind::Sqlserver => sqlserver(&name, catalog),
        ProviderKind::Oracle => oracle(&name, catalog),
        ProviderKind::Db2 => db2(&name, catalog),
        _ => OperandType::Unknown,
    }
}

fn postgres(name: &str, catalog: &CatalogType) -> OperandType {
    match name {
        "bool" | "boolean" => OperandType::Boolean,
        "int2" | "smallint" => integer(I16),
        "int4" | "integer" => integer(I32),
        "int8" | "bigint" => integer(I64),
        "float4" | "real" => OperandType::Float { mantissa_bits: 24 },
        "float8" | "double precision" => OperandType::Float { mantissa_bits: 53 },
        // `numeric` senza typmod: precisione arbitraria, ogni decimale e
        // esatto (il limite di 131072 cifre del server e un errore).
        "numeric" | "decimal" => decimal(catalog.precision, catalog.scale, u32::MAX),
        "text" | "name" => text_of(None, false, catalog),
        "varchar" | "character varying" => text_of(catalog.length, false, catalog),
        "bpchar" | "character" => text_of(catalog.length, true, catalog),
        "bytea" => OperandType::Binary {
            length: None,
            padded: false,
        },
        "date" => OperandType::Date,
        "timestamp" | "timestamp without time zone" => OperandType::Timestamp {
            fraction_digits: fraction(catalog.datetime_precision, 6),
        },
        "timestamptz" | "timestamp with time zone" => OperandType::TimestampTz {
            fraction_digits: fraction(catalog.datetime_precision, 6),
        },
        "uuid" => OperandType::Uuid,
        "json" | "jsonb" => OperandType::Json,
        "enum" => OperandType::Enumeration,
        _ => OperandType::Unknown,
    }
}

fn mysql(name: &str, catalog: &CatalogType) -> OperandType {
    let unsigned_column = catalog
        .declaration
        .as_deref()
        .is_some_and(|declaration| declaration.to_ascii_lowercase().contains("unsigned"));
    let text = |padded| text_of(catalog.length, padded, catalog);
    match (name, unsigned_column) {
        ("tinyint", false) => integer(I8),
        ("tinyint", true) => unsigned(8),
        ("smallint", false) => integer(I16),
        ("smallint", true) => unsigned(16),
        ("mediumint", false) => integer(I24),
        ("mediumint", true) => unsigned(24),
        ("int" | "integer", false) => integer(I32),
        ("int" | "integer", true) => unsigned(32),
        ("bigint", false) => integer(I64),
        ("bigint", true) => unsigned(64),
        ("float", _) => scaled_float(24, catalog),
        ("double" | "real", _) => scaled_float(53, catalog),
        ("decimal" | "numeric", _) => decimal(catalog.precision, catalog.scale, 0),
        ("char", _) => text(true),
        ("varchar" | "tinytext" | "text" | "mediumtext" | "longtext", _) => text(false),
        ("binary", _) => OperandType::Binary {
            length: catalog.length,
            padded: true,
        },
        ("varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob", _) => OperandType::Binary {
            length: catalog.length,
            padded: false,
        },
        ("date", _) => OperandType::Date,
        // `TIMESTAMP` si legge e si scrive nel fuso della sessione: e un
        // timestamp senza fuso per chi lo lega.
        ("datetime" | "timestamp", _) => OperandType::Timestamp {
            fraction_digits: fraction(catalog.datetime_precision, 0),
        },
        ("json", _) => OperandType::Json,
        _ => OperandType::Unknown,
    }
}

/// Le cifre oltre le quali `x * 10^D` non e piu un intero esatto in un double
/// (`10^15 < 2^53`).
const SCALED_FLOAT_DIGITS: u32 = 15;

/// Un valore che l'arrotondamento di `FLOAT(M, D)` lascia com'e qualunque sia
/// l'aritmetica del server: il valore esatto ha al piu `D` cifre frazionarie
/// e al piu quindici cifre in tutto, cosi `x * 10^D` e un intero esatto e
/// tornare indietro e esatto. Non basta che la forma piu corta abbia `D`
/// cifre: misurato su `MySQL`, `-0.1` in un `DOUBLE(10, 2)` diventava
/// `-0.09999999999999998`, e `-0.05` diventava `-0.050000000000000044`.
fn scaled_float_exact(exact: &ExactDecimal, precision: u32, scale: u32) -> bool {
    let integer = u32::try_from(exact.integer.len()).unwrap_or(u32::MAX);
    fits_decimal(exact, precision, scale) && integer.saturating_add(scale) <= SCALED_FLOAT_DIGITS
}

/// `FLOAT`, `DOUBLE` o, con la scala dichiarata (`float(7,1)` nella
/// `COLUMN_TYPE`), `FLOAT(M, D)` e `DOUBLE(M, D)`.
fn scaled_float(mantissa_bits: u32, catalog: &CatalogType) -> OperandType {
    let declared = catalog.declaration.as_deref().and_then(|declaration| {
        let (_, rest) = declaration.split_once('(')?;
        let (inside, _) = rest.split_once(')')?;
        let (precision, scale) = inside.split_once(',')?;
        Some((
            precision.trim().parse::<u32>().ok()?,
            scale.trim().parse::<u32>().ok()?,
        ))
    });
    match declared {
        Some((precision, scale)) => OperandType::ScaledFloat {
            mantissa_bits,
            precision,
            scale,
        },
        None => OperandType::Float { mantissa_bits },
    }
}

fn sqlserver(name: &str, catalog: &CatalogType) -> OperandType {
    match name {
        "bit" => OperandType::Boolean,
        "tinyint" => unsigned(8),
        "smallint" => integer(I16),
        "int" => integer(I32),
        "bigint" => integer(I64),
        "real" => OperandType::Float { mantissa_bits: 24 },
        "float" => OperandType::Float {
            mantissa_bits: if catalog.precision.is_some_and(|precision| precision <= 24) {
                24
            } else {
                53
            },
        },
        "decimal" | "numeric" => decimal(catalog.precision, catalog.scale, 0),
        "money" => OperandType::Decimal {
            precision: 19,
            scale: 4,
        },
        "smallmoney" => OperandType::Decimal {
            precision: 10,
            scale: 4,
        },
        "char" | "nchar" => text_of(catalog.length, true, catalog),
        "varchar" | "nvarchar" => text_of(catalog.length, false, catalog),
        "binary" => OperandType::Binary {
            length: catalog.length,
            padded: true,
        },
        "varbinary" => OperandType::Binary {
            length: catalog.length,
            padded: false,
        },
        "date" => OperandType::Date,
        "datetime2" => OperandType::Timestamp {
            fraction_digits: fraction(catalog.scale.and_then(|s| u64::try_from(s).ok()), 7),
        },
        "datetimeoffset" => OperandType::TimestampTz {
            fraction_digits: fraction(catalog.scale.and_then(|s| u64::try_from(s).ok()), 7),
        },
        "uniqueidentifier" => OperandType::Uuid,
        // `datetime` (1/300 di secondo) e `smalldatetime` non hanno un
        // parametro che li raggiunga esatti: un `datetime2` li confronta
        // convertendo la colonna (`limite dichiarato` in `parameter_fidelity`).
        _ => OperandType::Unknown,
    }
}

fn oracle(name: &str, catalog: &CatalogType) -> OperandType {
    let base = name.split('(').next().unwrap_or(name).trim();
    match base {
        "number" => match (catalog.precision, catalog.scale) {
            (None, None) => OperandType::UnboundedDecimal { digits: 38 },
            (None, Some(0)) => OperandType::Decimal {
                precision: 38,
                scale: 0,
            },
            (precision, scale) => decimal(precision, scale, 38),
        },
        "binary_float" => OperandType::Float { mantissa_bits: 24 },
        "binary_double" => OperandType::Float { mantissa_bits: 53 },
        "varchar2" | "nvarchar2" => text_of(catalog.length, false, catalog),
        "char" | "nchar" => text_of(catalog.length, true, catalog),
        "raw" => OperandType::Binary {
            length: catalog.length,
            padded: false,
        },
        // `DATE` di Oracle porta i secondi.
        "date" => OperandType::Timestamp { fraction_digits: 0 },
        "timestamp" if name.contains("local time zone") => OperandType::Unknown,
        "timestamp" if name.contains("time zone") => OperandType::TimestampTz {
            fraction_digits: fraction(catalog.scale.and_then(|s| u64::try_from(s).ok()), 6),
        },
        "timestamp" => OperandType::Timestamp {
            fraction_digits: fraction(catalog.scale.and_then(|s| u64::try_from(s).ok()), 6),
        },
        "json" => OperandType::Json,
        "boolean" => OperandType::Boolean,
        _ => OperandType::Unknown,
    }
}

fn db2(name: &str, catalog: &CatalogType) -> OperandType {
    match name {
        "boolean" => OperandType::Boolean,
        "smallint" => integer(I16),
        "integer" | "int" => integer(I32),
        "bigint" => integer(I64),
        "real" => OperandType::Float { mantissa_bits: 24 },
        "double" => OperandType::Float { mantissa_bits: 53 },
        "decimal" | "numeric" => decimal(catalog.precision, catalog.scale, 0),
        "decfloat" => OperandType::UnboundedDecimal {
            digits: u32::try_from(catalog.precision.unwrap_or(34)).unwrap_or(34),
        },
        "character" | "char" => text_of(catalog.length, true, catalog),
        "varchar" => text_of(catalog.length, false, catalog),
        "binary" => OperandType::Binary {
            length: catalog.length,
            padded: true,
        },
        "varbinary" => OperandType::Binary {
            length: catalog.length,
            padded: false,
        },
        "date" => OperandType::Date,
        "timestamp" => OperandType::Timestamp {
            fraction_digits: fraction(catalog.scale.and_then(|s| u64::try_from(s).ok()), 6),
        },
        _ => OperandType::Unknown,
    }
}

fn refusal(provider: ProviderKind, message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Prepare,
        Some(provider),
        message,
    )
}

/// Un decimale normalizzato: segno, cifre intere senza zeri iniziali, cifre
/// frazionarie senza zeri finali. Zero e positivo e senza cifre.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactDecimal {
    pub negative: bool,
    pub integer: String,
    pub fraction: String,
}

impl ExactDecimal {
    fn normalized(negative: bool, integer: &str, fraction: &str) -> Self {
        let integer = integer.trim_start_matches('0').to_owned();
        let fraction = fraction.trim_end_matches('0').to_owned();
        let zero = integer.is_empty() && fraction.is_empty();
        Self {
            negative: negative && !zero,
            integer,
            fraction,
        }
    }

    /// Il testo posizionale (`-12.5`, `0`, `0.001`).
    #[must_use]
    pub fn text(&self) -> String {
        let mut text = String::new();
        if self.negative {
            text.push('-');
        }
        text.push_str(if self.integer.is_empty() {
            "0"
        } else {
            &self.integer
        });
        if !self.fraction.is_empty() {
            text.push('.');
            text.push_str(&self.fraction);
        }
        text
    }

    fn significant_digits(&self) -> usize {
        if self.integer.is_empty() {
            self.fraction.trim_start_matches('0').len()
        } else {
            self.integer.len() + self.fraction.len()
        }
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

/// Il valore decimale esatto di un double finito: ogni double e una
/// frazione diadica, quindi ha un'espansione decimale finita.
#[must_use]
pub fn exact_decimal_of_f64(value: f64) -> Option<ExactDecimal> {
    if !value.is_finite() {
        return None;
    }
    let bits = value.to_bits();
    let negative = bits >> 63 == 1;
    let exponent_bits = i32::try_from((bits >> 52) & 0x7ff).ok()?;
    let fraction_bits = bits & ((1_u64 << 52) - 1);
    let (mantissa, exponent) = if exponent_bits == 0 {
        (fraction_bits, -1074)
    } else {
        (fraction_bits | (1_u64 << 52), exponent_bits - 1075)
    };
    if mantissa == 0 {
        return Some(ExactDecimal::normalized(false, "", ""));
    }
    // mantissa * 2^exponent in base 10^9.
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
    let scale = if exponent >= 0 {
        for _ in 0..exponent {
            multiply(&mut limbs, 2);
        }
        0_usize
    } else {
        // m / 2^k = m * 5^k / 10^k
        for _ in 0..(-exponent) {
            multiply(&mut limbs, 5);
        }
        usize::try_from(-exponent).ok()?
    };
    let mut digits = String::new();
    for (index, limb) in limbs.iter().rev().enumerate() {
        if index == 0 {
            digits.push_str(&limb.to_string());
        } else {
            let _ = write!(digits, "{limb:09}");
        }
    }
    let digits = digits.trim_start_matches('0').to_owned();
    let (integer, fraction) = if digits.len() > scale {
        let split = digits.len() - scale;
        (digits[..split].to_owned(), digits[split..].to_owned())
    } else {
        (
            String::new(),
            format!("{}{digits}", "0".repeat(scale - digits.len())),
        )
    };
    Some(ExactDecimal::normalized(negative, &integer, &fraction))
}

/// Un decimale in notazione posizionale, normalizzato.
fn parse_decimal(provider: ProviderKind, value: &str) -> Result<ExactDecimal> {
    let literal = crate::parameter_fidelity::DecimalLiteral::parse(provider, value)?;
    Ok(ExactDecimal::normalized(
        literal.negative,
        &literal.integer,
        &literal.fraction,
    ))
}

fn integer_digits(value: i128) -> u32 {
    value
        .unsigned_abs()
        .checked_ilog10()
        .map_or(0, |log| log + 1)
}

fn fits_decimal(value: &ExactDecimal, precision: u32, scale: u32) -> bool {
    let fraction = u32::try_from(value.fraction.len()).unwrap_or(u32::MAX);
    let integer = u32::try_from(value.integer.len()).unwrap_or(u32::MAX);
    fraction <= scale && integer <= precision.saturating_sub(scale)
}

/// Un valore esatto in un decimale a scala negativa: nessuna frazione, gli
/// ultimi `rounding` cifre intere nulle, al piu `precision` cifre prima.
fn fits_rounded(integer: &str, fraction: &str, precision: u32, rounding: u32) -> bool {
    let Ok(rounding) = usize::try_from(rounding) else {
        return false;
    };
    let Ok(precision) = usize::try_from(precision) else {
        return false;
    };
    let integer = integer.trim_start_matches('0');
    if !fraction.trim_end_matches('0').is_empty() {
        return false;
    }
    if integer.is_empty() {
        return true;
    }
    integer.len() <= precision.saturating_add(rounding)
        && integer.len() > rounding
        && integer
            .bytes()
            .rev()
            .take(rounding)
            .all(|digit| digit == b'0')
}

fn float_of(value: &ExactDecimal, mantissa_bits: u32) -> Option<f64> {
    let text = value.text();
    let parsed = if mantissa_bits <= 24 {
        f64::from(text.parse::<f32>().ok()?)
    } else {
        text.parse::<f64>().ok()?
    };
    (exact_decimal_of_f64(parsed).as_ref() == Some(value)).then_some(parsed)
}

/// Le parti di un testo temporale: data, ora, cifre frazionarie
/// significative (senza zeri finali).
fn temporal_parts(value: &str) -> (&str, &str, String) {
    let (date, rest) = value.split_once(['T', 't', ' ']).unwrap_or((value, ""));
    let (time, fraction) = rest.split_once('.').unwrap_or((rest, ""));
    let digits = fraction
        .bytes()
        .take_while(u8::is_ascii_digit)
        .map(char::from)
        .collect::<String>();
    (date, time, digits.trim_end_matches('0').to_owned())
}

/// La forma di un valore testuale tipato, verificata prima di qualunque
/// riscrittura: un `Uuid` o una data che diventano testo non perdono il
/// controllo che il binder del loro tipo avrebbe fatto.
fn validate_form(provider: ProviderKind, value: &ParameterValue) -> Result<()> {
    let malformed = |message| Err(refusal(provider, message));
    match value {
        ParameterValue::Uuid(text) => crate::parameter_fidelity::canonical_uuid(provider, text),
        ParameterValue::Date(text) => {
            if chrono_free_date(text) {
                Ok(())
            } else {
                malformed("parametro date non valido")
            }
        }
        ParameterValue::Timestamp(text) | ParameterValue::TimestampTz(text) => {
            crate::parameter_fidelity::reject_sub_microsecond(provider, text)
        }
        _ => Ok(()),
    }
}

/// `AAAA-MM-GG` di un giorno che esiste, senza dipendere da `chrono` (il
/// core non lo usa).
fn chrono_free_date(text: &str) -> bool {
    let parts = text.split('-').collect::<Vec<_>>();
    let [year, month, day] = parts.as_slice() else {
        return false;
    };
    let digits =
        |part: &str, width: usize| part.len() == width && part.bytes().all(|b| b.is_ascii_digit());
    if !digits(year, 4) || !digits(month, 2) || !digits(day, 2) {
        return false;
    }
    let (Ok(year), Ok(month), Ok(day)) = (
        year.parse::<u32>(),
        month.parse::<u32>(),
        day.parse::<u32>(),
    ) else {
        return false;
    };
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    (1..=days).contains(&day)
}

/// Ammette `value` contro un operando di tipo `operand` e lo rende nel tipo
/// dell'operando.
///
/// # Errors
///
/// `DataMapping` in fase `Prepare` se il valore non e rappresentabile esatto
/// nel tipo dell'operando. Il messaggio non riporta il valore.
#[allow(clippy::too_many_lines)]
pub fn admit(
    provider: ProviderKind,
    value: &ParameterValue,
    operand: &OperandType,
) -> Result<ParameterValue> {
    use OperandType as O;
    use ParameterValue as V;
    let inexact = || {
        refusal(
            provider,
            "parametro non rappresentabile esatto nel tipo dell'operando",
        )
    };
    validate_form(provider, value)?;
    match (value, operand) {
        (V::Null { .. }, _)
        | (V::Bool(_), O::Boolean)
        | (V::Uuid(_), O::Uuid)
        | (V::Json(_), O::Json)
        | (V::Enum { .. }, O::Enumeration)
        | (V::Date(_), O::Date) => Ok(value.clone()),
        (_, O::Unknown) => Err(refusal(
            provider,
            "tipo dell'operando non giudicabile esatto: ammette soltanto NULL",
        )),
        (V::Bool(flag), O::Integer { min, max }) if *min <= 0 && *max >= 1 => {
            Ok(V::I64(i64::from(*flag)))
        }
        (V::Bool(flag), O::Decimal { precision, scale }) if *precision > *scale => {
            Ok(V::I64(i64::from(*flag)))
        }
        (V::I32(_) | V::I64(_), _) => {
            let number = match value {
                V::I32(number) => i128::from(*number),
                V::I64(number) => i128::from(*number),
                _ => return Err(inexact()),
            };
            match operand {
                O::Integer { min, max } if (*min..=*max).contains(&number) => Ok(value.clone()),
                O::Float { mantissa_bits } => {
                    let limit = 1_i128 << mantissa_bits;
                    if number.abs() <= limit {
                        #[allow(clippy::cast_precision_loss)] // |n| <= 2^53: esatto
                        Ok(V::F64(number as f64))
                    } else {
                        Err(inexact())
                    }
                }
                O::Decimal { precision, scale }
                    if integer_digits(number) <= precision - scale.min(precision) =>
                {
                    Ok(value.clone())
                }
                O::UnboundedDecimal { digits } if integer_digits(number) <= *digits => {
                    Ok(value.clone())
                }
                O::ScaledFloat {
                    mantissa_bits,
                    precision,
                    scale,
                } if number.abs() <= 1_i128 << mantissa_bits
                    && integer_digits(number) <= precision.saturating_sub(*scale)
                    && integer_digits(number).saturating_add(*scale) <= SCALED_FLOAT_DIGITS =>
                {
                    #[allow(clippy::cast_precision_loss)] // |n| <= 2^53: esatto
                    Ok(V::F64(number as f64))
                }
                O::RoundedDecimal {
                    precision,
                    rounding,
                } if fits_rounded(
                    &number.unsigned_abs().to_string(),
                    "",
                    *precision,
                    *rounding,
                ) =>
                {
                    Ok(value.clone())
                }
                _ => Err(inexact()),
            }
        }
        (V::F64(number), _) if !number.is_finite() => match operand {
            O::Float { .. } => Ok(value.clone()),
            _ => Err(inexact()),
        },
        (
            V::F64(number),
            O::ScaledFloat {
                mantissa_bits,
                precision,
                scale,
            },
        ) => {
            if *mantissa_bits < 53 && crate::parameter_fidelity::exact_f32(*number).is_none() {
                return Err(inexact());
            }
            let exact = exact_decimal_of_f64(*number).ok_or_else(inexact)?;
            if scaled_float_exact(&exact, *precision, *scale) {
                Ok(value.clone())
            } else {
                Err(refusal(
                    provider,
                    "floating point oltre la scala di FLOAT(M, D): il server lo arrotonderebbe",
                ))
            }
        }
        (V::F64(number), O::Float { mantissa_bits }) => {
            if *mantissa_bits >= 53 || crate::parameter_fidelity::exact_f32(*number).is_some() {
                Ok(value.clone())
            } else {
                Err(inexact())
            }
        }
        (V::F64(number), _) => {
            let exact = exact_decimal_of_f64(*number).ok_or_else(inexact)?;
            match operand {
                O::Integer { min, max } => exact
                    .as_i128()
                    .filter(|integer| (*min..=*max).contains(integer))
                    .and_then(|integer| i64::try_from(integer).ok())
                    .map(V::I64)
                    .ok_or_else(inexact),
                O::Decimal { precision, scale } if fits_decimal(&exact, *precision, *scale) => {
                    Ok(V::Decimal(exact.text()))
                }
                O::RoundedDecimal {
                    precision,
                    rounding,
                } if fits_rounded(&exact.integer, &exact.fraction, *precision, *rounding) => {
                    Ok(V::Decimal(exact.text()))
                }
                O::UnboundedDecimal { digits }
                    if u32::try_from(exact.significant_digits()).unwrap_or(u32::MAX) <= *digits =>
                {
                    Ok(V::Decimal(exact.text()))
                }
                _ => Err(inexact()),
            }
        }
        (V::Decimal(text), _) => {
            let exact = parse_decimal(provider, text)?;
            match operand {
                O::Decimal { precision, scale } if fits_decimal(&exact, *precision, *scale) => {
                    Ok(V::Decimal(exact.text()))
                }
                O::RoundedDecimal {
                    precision,
                    rounding,
                } if fits_rounded(&exact.integer, &exact.fraction, *precision, *rounding) => {
                    Ok(V::Decimal(exact.text()))
                }
                O::UnboundedDecimal { digits }
                    if u32::try_from(exact.significant_digits()).unwrap_or(u32::MAX) <= *digits =>
                {
                    Ok(V::Decimal(exact.text()))
                }
                O::Integer { min, max } => exact
                    .as_i128()
                    .filter(|integer| (*min..=*max).contains(integer))
                    .and_then(|integer| i64::try_from(integer).ok())
                    .map(V::I64)
                    .ok_or_else(inexact),
                O::Float { mantissa_bits } => float_of(&exact, *mantissa_bits)
                    .map(V::F64)
                    .ok_or_else(inexact),
                O::ScaledFloat {
                    mantissa_bits,
                    precision,
                    scale,
                } if scaled_float_exact(&exact, *precision, *scale) => {
                    float_of(&exact, *mantissa_bits)
                        .map(V::F64)
                        .ok_or_else(inexact)
                }
                O::Text { .. } => admit_text(provider, text, operand),
                _ => Err(inexact()),
            }
        }
        (V::Date(date), O::Timestamp { .. }) => Ok(V::Timestamp(format!("{date}T00:00:00"))),
        (V::Timestamp(text), O::Timestamp { fraction_digits }) => {
            let (_, _, fraction) = temporal_parts(text);
            if u32::try_from(fraction.len()).unwrap_or(u32::MAX) <= *fraction_digits {
                Ok(value.clone())
            } else {
                Err(refusal(
                    provider,
                    "timestamp oltre la precisione dell'operando: il server la arrotonderebbe",
                ))
            }
        }
        (V::Timestamp(text), O::Date) => {
            let (date, time, fraction) = temporal_parts(text);
            let midnight = time.trim_end_matches(['0', ':']).is_empty() && fraction.is_empty();
            if midnight {
                Ok(V::Date(date.to_owned()))
            } else {
                Err(refusal(
                    provider,
                    "timestamp con un'ora diversa da mezzanotte contro una data",
                ))
            }
        }
        (V::TimestampTz(text), O::TimestampTz { fraction_digits }) => {
            let (_, _, fraction) = temporal_parts(text);
            if u32::try_from(fraction.len()).unwrap_or(u32::MAX) <= *fraction_digits {
                Ok(value.clone())
            } else {
                Err(refusal(
                    provider,
                    "timestamptz oltre la precisione dell'operando: il server la arrotonderebbe",
                ))
            }
        }
        // Un testo piu corto di un `CHAR(n)` si completa con spazi e si
        // confronta con le regole di padding della collation: semantica SQL
        // dichiarata (limite «collation» in `parameter_fidelity`), non una
        // conversione del valore.
        //
        // I valori testuali (data, timestamp, decimale, UUID come scritti) in
        // una colonna di testo sono il proprio testo. Interi, floating point,
        // booleani e byte no: la loro forma testuale non e unica.
        (
            V::String(text)
            | V::Uuid(text)
            | V::Enum { label: text, .. }
            | V::Date(text)
            | V::Timestamp(text)
            | V::TimestampTz(text),
            O::Text { .. },
        ) => admit_text(provider, text, operand),
        (V::String(text), O::Enumeration) => Ok(V::Enum {
            type_name: String::new(),
            label: text.clone(),
        }),
        (V::Json(document), O::Text { padded: false, .. }) => {
            admit_text(provider, &document.to_string(), operand)
        }
        (V::Bytes(bytes), O::Binary { length, padded }) => {
            if length
                .is_some_and(|length| u64::try_from(bytes.len()).map_or(true, |len| len > length))
            {
                return Err(refusal(
                    provider,
                    "byte oltre la lunghezza della colonna: il server li troncherebbe o li rifiuterebbe",
                ));
            }
            if *padded
                && length.is_none_or(|length| u64::try_from(bytes.len()).ok() != Some(length))
            {
                return Err(refusal(
                    provider,
                    "byte piu corti di una colonna binaria fissa: il server li completa con zeri",
                ));
            }
            Ok(value.clone())
        }
        _ => Err(inexact()),
    }
}

/// Un testo contro una colonna testuale: ogni carattere nel charset della
/// colonna e la lunghezza, nella sua unita, entro quella dichiarata.
/// Altrimenti il server sostituisce il carattere (`'日'` verso `'?'` in un
/// `varchar` CP1252 di SQL Server) o tronca il testo (`'abc   '` in un
/// `varchar(3)` di PostgreSQL e Db2 diventava `'abc'`): si rifiuta. Un testo
/// piu corto di un `CHAR(n)` resta ammesso: il riempimento con spazi e la
/// semantica di confronto dichiarata (limite «collation»).
fn admit_text(provider: ProviderKind, text: &str, operand: &OperandType) -> Result<ParameterValue> {
    let OperandType::Text {
        length,
        unit,
        charset,
        ..
    } = *operand
    else {
        return Err(refusal(
            provider,
            "parametro non rappresentabile esatto nel tipo dell'operando",
        ));
    };
    if !text.chars().all(|character| charset.holds(character)) {
        return Err(refusal(
            provider,
            "carattere fuori dal charset della colonna: il server lo sostituirebbe",
        ));
    }
    if length.is_some_and(|length| {
        u64::try_from(text_length(text, unit, charset)).map_or(true, |count| count > length)
    }) {
        return Err(refusal(
            provider,
            "testo oltre la lunghezza della colonna: il server lo troncherebbe o lo rifiuterebbe",
        ));
    }
    Ok(ParameterValue::String(text.to_owned()))
}

fn collect_filter<'a>(expression: &'a FilterExpression, uses: &mut Vec<(&'a str, &'a str)>) {
    match expression {
        FilterExpression::And { args } | FilterExpression::Or { args } => {
            for argument in args {
                collect_filter(argument, uses);
            }
        }
        FilterExpression::Eq { field, parameter }
        | FilterExpression::Ne { field, parameter }
        | FilterExpression::Lt { field, parameter }
        | FilterExpression::Lte { field, parameter }
        | FilterExpression::Gt { field, parameter }
        | FilterExpression::Gte { field, parameter } => uses.push((field, parameter)),
        FilterExpression::In { field, parameters } => {
            uses.extend(
                parameters
                    .iter()
                    .map(|parameter| (field.as_str(), parameter.as_str())),
            );
        }
        FilterExpression::Between {
            field,
            lower_parameter,
            upper_parameter,
        } => {
            uses.push((field, lower_parameter));
            uses.push((field, upper_parameter));
        }
        // Un pattern `LIKE` non e un valore della colonna, e un parametro
        // spatial ha la propria regola (`spatial_probe`).
        FilterExpression::Like { .. }
        | FilterExpression::Spatial { .. }
        | FilterExpression::IsNull { .. }
        | FilterExpression::IsNotNull { .. } => {}
    }
}

/// I parametri del filtro di una lettura, ammessi contro il tipo del campo
/// che ciascuno incontra ([`admit`]) e riscritti nel suo tipo.
///
/// # Errors
///
/// Quelli di [`admit`]; `DataMapping` se lo stesso parametro incontra due
/// campi che lo vorrebbero in forme diverse, o un campo che la descrizione
/// non tipizza ([`OperandType::Unknown`]).
pub fn admit_filter(
    provider: ProviderKind,
    filter: Option<&FilterExpression>,
    parameters: &ParameterBag,
    operand: impl Fn(&str) -> Option<OperandType>,
) -> Result<ParameterBag> {
    let Some(filter) = filter else {
        return Ok(parameters.clone());
    };
    let mut uses = Vec::new();
    collect_filter(filter, &mut uses);
    let mut admitted: BTreeMap<String, ParameterValue> = parameters
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    let mut decided: BTreeMap<&str, ParameterValue> = BTreeMap::new();
    for (field, name) in uses {
        let Some(value) = parameters.get(name) else {
            continue;
        };
        let operand = operand(field).unwrap_or(OperandType::Unknown);
        let coerced = admit(provider, value, &operand)?;
        if let Some(previous) = decided.get(name) {
            if previous != &coerced {
                return Err(refusal(
                    provider,
                    "parametro usato su campi di tipo diverso: nessuna forma e esatta per entrambi",
                ));
            }
        }
        decided.insert(name, coerced.clone());
        admitted.insert(name.to_owned(), coerced);
    }
    Ok(ParameterBag::new(admitted))
}

/// I tipi delle colonne della relazione su cui opera uno statement portable,
/// come li legge `TransactionScope::column_types`.
pub type RelationTypes = BTreeMap<String, CatalogType>;

fn admit_expression(
    provider: ProviderKind,
    expression: &mut Expression,
    column: &str,
    types: &RelationTypes,
) -> Result<()> {
    // Una colonna non e un valore; un `SpatialValue` ha la regola delle
    // sonde (`spatial_probe`), che ne giudica SRID e frame.
    if let Expression::Literal(value) = expression {
        let operand = types.get(column).map_or(OperandType::Unknown, |catalog| {
            operand_type(provider, catalog)
        });
        *value = admit(provider, value, &operand)?;
    }
    Ok(())
}

fn admit_predicate(
    provider: ProviderKind,
    predicate: &mut Predicate,
    types: &RelationTypes,
) -> Result<()> {
    match predicate {
        Predicate::Eq { column, value }
        | Predicate::Ne { column, value }
        | Predicate::Lt { column, value }
        | Predicate::Lte { column, value }
        | Predicate::Gt { column, value }
        | Predicate::Gte { column, value } => admit_expression(provider, value, column, types),
        Predicate::In { column, values } => values
            .iter_mut()
            .try_for_each(|value| admit_expression(provider, value, column, types)),
        Predicate::Between { column, low, high } => {
            admit_expression(provider, low, column, types)?;
            admit_expression(provider, high, column, types)
        }
        Predicate::And { predicates } | Predicate::Or { predicates } => predicates
            .iter_mut()
            .try_for_each(|predicate| admit_predicate(provider, predicate, types)),
        Predicate::Not { predicate } => admit_predicate(provider, predicate, types),
        Predicate::Like { .. }
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::Spatial { .. } => Ok(()),
    }
}

fn admit_rows(
    provider: ProviderKind,
    columns: &[String],
    rows: &mut [Vec<Expression>],
    types: &RelationTypes,
) -> Result<()> {
    for row in rows {
        for (column, value) in columns.iter().zip(row.iter_mut()) {
            admit_expression(provider, value, column, types)?;
        }
    }
    Ok(())
}

fn admit_assignments(
    provider: ProviderKind,
    assignments: &mut [(String, Expression)],
    types: &RelationTypes,
) -> Result<()> {
    assignments
        .iter_mut()
        .try_for_each(|(column, value)| admit_expression(provider, value, column, types))
}

/// La relazione (schema, nome) su cui opera uno statement portable.
#[must_use]
pub fn statement_relation(statement: &PortableStatement) -> (Option<String>, String) {
    let table = match statement {
        PortableStatement::Select(select) => &select.table,
        PortableStatement::Insert(insert) => &insert.table,
        PortableStatement::Update(update) => &update.table,
        PortableStatement::Delete(delete) => &delete.table,
        PortableStatement::Upsert(upsert) => &upsert.table,
    };
    (table.schema.clone(), table.name.clone())
}

/// Lo statement con ogni letterale ammesso contro il tipo della colonna che
/// incontra e riscritto nel suo tipo ([`admit`]).
///
/// # Errors
///
/// Quelli di [`admit`]: una colonna che il catalogo non tipizza rifiuta ogni
/// letterale che non sia `NULL`.
pub fn admit_portable(
    provider: ProviderKind,
    statement: &PortableStatement,
    types: &RelationTypes,
) -> Result<PortableStatement> {
    let mut admitted = statement.clone();
    match &mut admitted {
        PortableStatement::Select(select) => {
            if let Some(filter) = select.filter.as_mut() {
                admit_predicate(provider, filter, types)?;
            }
        }
        PortableStatement::Insert(insert) => {
            admit_rows(provider, &insert.columns, &mut insert.values, types)?;
        }
        PortableStatement::Update(update) => {
            admit_assignments(provider, &mut update.assignments, types)?;
            if let Some(filter) = update.filter.as_mut() {
                admit_predicate(provider, filter, types)?;
            }
        }
        PortableStatement::Delete(delete) => {
            if let Some(filter) = delete.filter.as_mut() {
                admit_predicate(provider, filter, types)?;
            }
        }
        PortableStatement::Upsert(upsert) => {
            admit_rows(provider, &upsert.columns, &mut upsert.values, types)?;
            admit_assignments(provider, &mut upsert.update_on_conflict, types)?;
        }
    }
    Ok(admitted)
}

/// Il tipo del `CAST` con cui il renderer relazionale scrive un parametro
/// tipato (`plenora_database_sql::Renderer::render_parameter_type`): e
/// l'operando che il parametro incontra.
#[must_use]
pub const fn cast_operand(
    provider: ProviderKind,
    parameter_type: crate::relational::QueryParameterType,
) -> OperandType {
    use crate::relational::QueryParameterType as T;
    let unbounded_text = UNBOUNDED_TEXT;
    match (parameter_type, provider) {
        (T::Boolean | T::Integer | T::BigInteger, ProviderKind::Mysql | ProviderKind::Mariadb) => {
            integer(I64)
        }
        (T::Boolean, _) => OperandType::Boolean,
        (T::Integer, ProviderKind::Oracle) => OperandType::Decimal {
            precision: 10,
            scale: 0,
        },
        (T::Integer, _) => integer(I32),
        (T::BigInteger, ProviderKind::Oracle) => OperandType::Decimal {
            precision: 19,
            scale: 0,
        },
        (T::BigInteger, _) => integer(I64),
        (T::Float, _) => OperandType::Float { mantissa_bits: 53 },
        // `VARCHAR2(4000)` del renderer conta i byte (`BYTE` di default).
        (T::String, ProviderKind::Oracle) => OperandType::Text {
            length: Some(4000),
            padded: false,
            unit: LengthUnit::Bytes,
            charset: Charset::Unicode,
        },
        (T::Json, ProviderKind::Postgres | ProviderKind::Mysql) => OperandType::Json,
        (T::String | T::Json, _) => unbounded_text,
        (T::Binary, _) => OperandType::Binary {
            length: None,
            padded: false,
        },
        (T::Date, _) => OperandType::Date,
        (T::Timestamp, ProviderKind::Sqlserver) => OperandType::Timestamp { fraction_digits: 7 },
        (T::Timestamp, _) => OperandType::Timestamp { fraction_digits: 6 },
        // `MySQL` scrive un `TimestampTz` come `DATETIME(6)`: il fuso si
        // perde, quindi un valore con offset non vi entra esatto.
        (T::TimestampTz, ProviderKind::Mysql | ProviderKind::Mariadb) => {
            OperandType::Timestamp { fraction_digits: 6 }
        }
        (T::TimestampTz, ProviderKind::Sqlserver) => {
            OperandType::TimestampTz { fraction_digits: 7 }
        }
        (T::TimestampTz, _) => OperandType::TimestampTz { fraction_digits: 6 },
        (T::Decimal, _) => OperandType::Decimal {
            precision: 38,
            scale: 10,
        },
        (T::Uuid, ProviderKind::Postgres | ProviderKind::Sqlserver) => OperandType::Uuid,
        (T::Uuid, _) => OperandType::Text {
            length: Some(36),
            padded: true,
            unit: LengthUnit::Characters,
            charset: Charset::Ascii,
        },
    }
}

/// I parametri tipati di una `QueryOperation`, ammessi contro il loro `CAST`.
///
/// Anche quelli dentro un `Cast`: il tipo e quello che il renderer scrive
/// ([`cast_operand`]), e un `CAST(? AS DECIMAL(38, 10))` arrotondava in
/// silenzio un decimale con undici cifre frazionarie.
///
/// Limite dichiarato: un `Parameter` senza tipo incontra un operando che la
/// query non dichiara, come lo SQL del chiamante (vedi il modulo).
///
/// # Errors
///
/// Quelli di [`admit`].
pub fn admit_typed_parameters(
    provider: ProviderKind,
    operation: &crate::relational::QueryOperation,
    parameters: &ParameterBag,
) -> Result<ParameterBag> {
    use crate::relational::{QueryExpression, QueryWalkControl, QueryWalkNode};
    let mut uses: Vec<(String, crate::relational::QueryParameterType)> = Vec::new();
    crate::relational::walk_query(operation, |node| {
        if let QueryWalkNode::Expression(expression) = node {
            match expression {
                QueryExpression::TypedParameter {
                    name,
                    parameter_type,
                } => uses.push((name.clone(), *parameter_type)),
                QueryExpression::Cast {
                    expression,
                    target_type,
                } => {
                    if let QueryExpression::Parameter { name } = expression.as_ref() {
                        uses.push((name.clone(), *target_type));
                    }
                }
                _ => {}
            }
        }
        QueryWalkControl::Continue
    });
    let mut admitted: BTreeMap<String, ParameterValue> = parameters
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    let mut decided: BTreeMap<String, ParameterValue> = BTreeMap::new();
    for (name, parameter_type) in uses {
        let Some(value) = parameters.get(&name) else {
            continue;
        };
        let coerced = admit(provider, value, &cast_operand(provider, parameter_type))?;
        if decided
            .get(&name)
            .is_some_and(|previous| previous != &coerced)
        {
            return Err(refusal(
                provider,
                "parametro usato con tipi diversi: nessuna forma e esatta per entrambi",
            ));
        }
        decided.insert(name.clone(), coerced.clone());
        admitted.insert(name, coerced);
    }
    Ok(ParameterBag::new(admitted))
}

#[cfg(test)]
#[path = "operand_admission_tests.rs"]
mod tests;
