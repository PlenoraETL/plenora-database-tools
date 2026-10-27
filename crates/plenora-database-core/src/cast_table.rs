//! La tabella dei `CAST` che la libreria rende, per dialetto (regola C).
//!
//! # Regola C: un `CAST` reso si ammette soltanto se e esatto
//!
//! La libreria rende un `CAST` in due casi: un parametro tipizzato in una
//! posizione confrontata o calcolata (`TypedParameter`, regola T) e un `Cast`
//! esplicito del piano. Il tipo reso ([`rendered_target`]) lo sceglie la
//! libreria, con parametri che il chiamante non ha dato (`DECIMAL(38, 10)`,
//! `VARCHAR2(4000)`, `DATETIME(6)`): un `CAST` si ammette soltanto se
//! l'esattezza si prova, altrimenti si rifiuta prima dell'esecuzione,
//! `invalid_plan` in fase `prepare` ([`CAST_NOT_EXACT`]):
//!
//! - **parametro**: il valore legato sta esatto nel tipo reso
//!   ([`value_fits`]): un decimale entro precisione e scala, un intero entro
//!   l'intervallo, un testo entro i byte, un timestamp entro le cifre
//!   frazionarie, un `timestamp_tz` soltanto dove il tipo reso tiene il fuso;
//! - **colonna**: il tipo del catalogo ([`CatalogType`], classificato da
//!   [`operand_type`]) sta nel tipo reso senza perdita ([`widens`]):
//!   `numeric(30, 12)` non sta in `DECIMAL(38, 10)`, un `VARCHAR(40000)` non
//!   sta in `VARCHAR(32672)`, un `float` non sta in nessun decimale;
//! - **un altro `CAST`**: il tipo reso del `CAST` interno come colonna;
//! - ogni altra espressione (aritmetica, funzioni, `CASE`): non si prova, e
//!   si rifiuta;
//! - **a testo su `MySQL` e `MariaDB`** (`string` reso `CHAR`, `uuid` reso
//!   `CHAR(36)`): si rifiuta per ogni operando, colonna o parametro,
//!   [`CAST_TEXT_COLLATION`]: il `CAST` da al valore la collation della
//!   connessione, e il confronto cambia collation ([`cast_changes_collation`]).
//!   Su `PostgreSQL` e SQL Server il `CAST` a testo tiene la collation della
//!   colonna (rimisurato dai test live della sedicesima lettura).
//!
//! # La tabella, misurata
//!
//! Il tipo reso e la sua semantica, per dialetto. Le righe che decidono un
//! rifiuto le rimisurano i test live della quindicesima lettura del 25/10
//! (`live_rilettura_quindici_*` di `MySQL`/`MariaDB`, `PostgreSQL`, Oracle,
//! Db2; `live_i_nomi_li_lega_il_server_con_la_collation` di SQL Server):
//!
//! | tipo | `PostgreSQL` | `MySQL` | `MariaDB` | SQL Server | Oracle | Db2 |
//! | --- | --- | --- | --- | --- | --- | --- |
//! | `boolean` | `BOOLEAN` | `SIGNED` (intero) | `SIGNED` | `BIT` | `TO_BOOLEAN` | `BOOLEAN` |
//! | `integer` | `INTEGER` | `SIGNED` | `SIGNED` | `INTEGER` | `NUMBER(10)` | `INTEGER` |
//! | `big_integer` | `BIGINT` | `SIGNED` | `SIGNED` | `BIGINT` | `NUMBER(19)` | `BIGINT` |
//! | `float` | `DOUBLE PRECISION` | `DOUBLE` | `DOUBLE` | `FLOAT` | `BINARY_DOUBLE` | `DOUBLE` |
//! | `decimal` | `DECIMAL(38, 10)` | `DECIMAL(38, 10)` | `DECIMAL(38, 10)` | `DECIMAL(38, 10)` | `NUMBER(38, 10)` | `DECIMAL(31, 10)` |
//! | `string` | `TEXT` | `CHAR`, rifiutato (collation) | `CHAR`, rifiutato (collation) | `NVARCHAR(MAX)` | `VARCHAR2(4000)` byte | `VARCHAR(32672)` byte |
//! | `binary` | `BYTEA` | `BINARY` | `BINARY` | `VARBINARY(MAX)` | `RAW(2000)` | `VARCHAR(32672) FOR BIT DATA` |
//! | `date` | `DATE` | `DATE` | `DATE` | `DATE` | `DATE` (con l'ora) | `DATE` |
//! | `timestamp` | `TIMESTAMP` (6) | `DATETIME(6)` | `DATETIME(6)` | `DATETIME2` (7) | `TIMESTAMP` (6) | `TIMESTAMP` (6) |
//! | `timestamp_tz` | `TIMESTAMPTZ` | `DATETIME(6)`, l'offset convertito nel fuso della sessione | `DATETIME(6)`, **l'offset scartato** | `DATETIMEOFFSET` | `TO_TIMESTAMP_TZ` | rifiutato (il tipo non esiste) |
//! | `uuid` | `UUID` | `CHAR(36)`, rifiutato (collation) | `CHAR(36)`, rifiutato (collation) | `UNIQUEIDENTIFIER` | `VARCHAR2(36)` | `CHAR(36)` |
//! | `json` | `JSONB` | `JSON` | `JSON` | `NVARCHAR(MAX)` | `CLOB` | `CLOB` (1M byte) |
//!
//! Misurato: `CAST(? AS DECIMAL(38, 10))` con `0.12345678901` rende
//! `0.1234567890` senza errore su `PostgreSQL`, `MySQL`, `MariaDB`, SQL
//! Server e Oracle (con un `F64` anche su `PostgreSQL` e Oracle), e ritrova una
//! colonna `0.123456789`; Db2 non ha `DECIMAL(38, 10)` (precisione massima 31:
//! la query fallisce), e la libreria rende `DECIMAL(31, 10)`, che arrotonda
//! allo stesso modo; `CAST(? AS DATETIME(6))` con
//! `2024-01-01T10:00:00+02:00` ritrova le righe delle `08:00` (UTC) su `MySQL`
//! e delle `10:00` su `MariaDB`; Db2 tronca un `CLOB` di 40000 caratteri a
//! `VARCHAR(32672)` senza errore, e non ha `TIMESTAMP WITH TIME ZONE` (la
//! query fallisce).
//!
//! # Il modello dei tipi
//!
//! [`CatalogType`], [`OperandType`] e [`operand_type`] sono quelli della regola
//! di ammissione dei parametri (PR #135, `operand_admission`), copiati uguali:
//! una sola classificazione dei tipi del catalogo per le due regole.
//!
//! # Limiti dichiarati
//!
//! - **Regola.** La regola C giudica il `CAST` di una colonna soltanto sul
//!   catalogo, e il `CAST` di un parametro soltanto con i valori.
//! - **Ambito.** La compilazione senza esecuzione e il SQL del chiamante (come
//!   la regola F); i nomi dichiarati dallo SDK (`RelationColumns::Declared`).
//! - **Hazard.** Nessuno nell'esecuzione: ogni percorso che esegue un piano lo
//!   giudica sul catalogo con i valori.
//! - **Rientro.** Nessuno: senza catalogo e senza valori non c'e niente da
//!   provare.
//!
//! - **Regola.** Un `CAST` fra famiglie (un numero o una data a testo, un
//!   testo a numero o a binario) non si prova esatto, in ogni posizione,
//!   anche nella projection.
//! - **Ambito.** Ogni dialetto: la forma testuale di un numero o di una data
//!   la sceglie il server (formato, sessione).
//! - **Hazard.** Nessun passaggio silenzioso; il costo e il rifiuto di un
//!   `CAST` di presentazione.
//! - **Rientro.** Quando la tabella dice, per dialetto, le conversioni fra
//!   famiglie che il server rende esatte.
//!
//! - **Regola.** La lunghezza di un testo del catalogo e in caratteri; contro
//!   un tipo reso in byte (Oracle, Db2) vale quattro byte per carattere.
//! - **Ambito.** Il `CAST` a `string` o `uuid` di una colonna testo su Oracle e
//!   Db2.
//! - **Hazard.** Nessun passaggio silenzioso: un `VARCHAR2(4000 BYTE)` si
//!   rifiuta anche se starebbe. Il costo e un rifiuto in piu.
//! - **Rientro.** Quando il catalogo dice la lunghezza in byte accanto a
//!   quella in caratteri.
//!
//! - **Regola.** Un `CAST` a testo su `MySQL` e `MariaDB` si rifiuta in ogni
//!   posizione, anche dove la collation non decide niente (la projection di
//!   un blocco senza `DISTINCT`).
//! - **Ambito.** `string` e `uuid` di un `Cast` esplicito del piano; il
//!   parametro tipizzato `string` o `uuid` si rende senza `CAST` e resta.
//! - **Hazard.** Nessun passaggio silenzioso; il costo e il rifiuto di un
//!   `CAST` di presentazione.
//! - **Rientro.** Un `CAST` reso con la collation della colonna
//!   (`CONVERT(... USING cs) COLLATE c`), misurato per ogni collation.
//!
//! - **Regola.** Un `F64` legato a un `CAST` decimale o intero, un decimale
//!   legato a un `CAST` `float` si rifiutano.
//! - **Ambito.** Ogni dialetto, finche il valore si lega nel proprio tipo e
//!   il server lo converte.
//! - **Hazard.** Nessun passaggio silenzioso; il costo e il rifiuto di valori
//!   che il server convertirebbe esatti (`0.5`).
//! - **Rientro.** La riscrittura del valore nel tipo reso
//!   (`operand_admission::admit` della PR #135), che li rende esatti prima
//!   del binding.

use crate::plan::ProviderKind;
use crate::provider::ParameterValue;
use crate::relational::QueryParameterType;
use crate::{DatabaseError, ErrorCategory, ErrorPhase};

/// Motivo stabile del rifiuto di un `CAST` reso dalla libreria che non e
/// esatto (regola C).
pub const CAST_NOT_EXACT: &str = "CAST non esatto: il tipo che la libreria rende (DECIMAL(38, 10) o DECIMAL(31, 10) su Db2, VARCHAR2(4000), VARCHAR(32672), RAW(2000), DATETIME(6), un timestamp_tz su MariaDB o Db2) non tiene esatto il valore legato o il tipo della colonna, o l'operando non si prova; si confronti la colonna con un parametro del suo tipo";

/// Motivo stabile del rifiuto di un `CAST` a testo su `MySQL` e `MariaDB`
/// (regola C, [`cast_changes_collation`]).
pub const CAST_TEXT_COLLATION: &str = "CAST a testo rifiutato su MySQL e MariaDB: CAST(... AS CHAR) da al valore la collation della connessione, e il confronto cambia collation senza errore; si confronti la colonna con un parametro del suo tipo, senza CAST";

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
    Text {
        length: Option<u64>,
        padded: bool,
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
        (None, _) => OperandType::UnboundedDecimal {
            digits: unbounded_digits,
        },
    }
}

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
        "text" | "name" => OperandType::Text {
            length: None,
            padded: false,
        },
        "varchar" | "character varying" => OperandType::Text {
            length: catalog.length,
            padded: false,
        },
        "bpchar" | "character" => OperandType::Text {
            length: catalog.length,
            padded: true,
        },
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
    let text = |padded| OperandType::Text {
        length: catalog.length,
        padded,
    };
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
        ("float", _) => OperandType::Float { mantissa_bits: 24 },
        ("double" | "real", _) => OperandType::Float { mantissa_bits: 53 },
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
        "char" | "nchar" => OperandType::Text {
            length: catalog.length,
            padded: true,
        },
        "varchar" | "nvarchar" => OperandType::Text {
            length: catalog.length,
            padded: false,
        },
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
        "varchar2" | "nvarchar2" => OperandType::Text {
            length: catalog.length,
            padded: false,
        },
        "char" | "nchar" => OperandType::Text {
            length: catalog.length,
            padded: true,
        },
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
        "character" | "char" => OperandType::Text {
            length: catalog.length,
            padded: true,
        },
        "varchar" => OperandType::Text {
            length: catalog.length,
            padded: false,
        },
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

/// Il rifiuto di un `CAST` che non si prova esatto.
#[must_use]
pub fn cast_refusal(provider: ProviderKind) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::InvalidPlan,
        ErrorPhase::Prepare,
        Some(provider),
        CAST_NOT_EXACT,
    )
}

/// Un `CAST` a testo su `MySQL` e `MariaDB` cambia la collation del confronto.
///
/// `CAST(... AS CHAR)`, un `uuid` reso `CHAR(36)`: il valore prende la
/// collation della connessione
/// (`collation_connection`) con coercibilita implicita, e il confronto con una
/// colonna puo cambiare collation senza errore (l'errore 1267 non e
/// garantito): una colonna `latin1_swedish_ci` con `'ü'`, `'u'` e `'y'`, una
/// `utf8mb4_bin` con `'A'` e `'a'`, su `MariaDB` una `utf8mb3_general_ci`
/// con `'ß'` rendono righe diverse (sedicesima lettura del 25/10). Non si
/// prova che il `CAST` lasci la collation: si rifiuta, per una colonna e per
/// un parametro, in ogni posizione.
#[must_use]
pub const fn cast_changes_collation(
    provider: ProviderKind,
    parameter_type: QueryParameterType,
) -> bool {
    matches!(provider, ProviderKind::Mysql | ProviderKind::Mariadb)
        && matches!(
            rendered_target(provider, parameter_type),
            OperandType::Text { .. }
        )
}

/// Il rifiuto di un `CAST` a testo su `MySQL` e `MariaDB`
/// ([`cast_changes_collation`]).
#[must_use]
pub fn text_cast_refusal(provider: ProviderKind) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::InvalidPlan,
        ErrorPhase::Prepare,
        Some(provider),
        CAST_TEXT_COLLATION,
    )
}

/// Il tipo che il renderer scrive per un `CAST(... AS tipo)` o un parametro
/// tipizzato (`typed_parameter::CastDialect::type_name`).
///
/// E un [`OperandType`]: la tabella del modulo. Le lunghezze dei testi resi in
/// byte (Oracle, Db2) sono byte.
///
/// Un `timestamp_tz` su `MariaDB` e un `DATETIME(6)` che scarta l'offset: un
/// valore con fuso non vi sta (misurato). Su Db2 `TIMESTAMP WITH TIME ZONE`
/// non esiste: niente vi sta.
#[must_use]
pub const fn rendered_target(
    provider: ProviderKind,
    parameter_type: QueryParameterType,
) -> OperandType {
    use ProviderKind as P;
    use QueryParameterType as T;
    let mysql = matches!(provider, P::Mysql | P::Mariadb);
    match parameter_type {
        T::Boolean | T::Integer | T::BigInteger if mysql => integer(I64),
        T::Boolean => OperandType::Boolean,
        T::Integer if matches!(provider, P::Oracle) => OperandType::Decimal {
            precision: 10,
            scale: 0,
        },
        T::Integer => integer(I32),
        T::BigInteger if matches!(provider, P::Oracle) => OperandType::Decimal {
            precision: 19,
            scale: 0,
        },
        T::BigInteger => integer(I64),
        T::Float => OperandType::Float { mantissa_bits: 53 },
        T::Decimal if matches!(provider, P::Db2) => OperandType::Decimal {
            precision: 31,
            scale: 10,
        },
        T::Decimal => OperandType::Decimal {
            precision: 38,
            scale: 10,
        },
        T::String => OperandType::Text {
            length: text_limit(provider),
            padded: false,
        },
        T::Binary => OperandType::Binary {
            length: binary_limit(provider),
            padded: false,
        },
        // `DATE` di Oracle porta l'ora.
        T::Date if matches!(provider, P::Oracle) => OperandType::Timestamp { fraction_digits: 0 },
        T::Date => OperandType::Date,
        T::Timestamp if matches!(provider, P::Sqlserver) => {
            OperandType::Timestamp { fraction_digits: 7 }
        }
        T::Timestamp => OperandType::Timestamp { fraction_digits: 6 },
        T::TimestampTz => match provider {
            P::Sqlserver => OperandType::TimestampTz { fraction_digits: 7 },
            P::Mariadb => OperandType::Timestamp { fraction_digits: 6 },
            P::Db2 => OperandType::Unknown,
            _ => OperandType::TimestampTz { fraction_digits: 6 },
        },
        T::Uuid if matches!(provider, P::Postgres | P::Sqlserver) => OperandType::Uuid,
        T::Uuid => OperandType::Text {
            length: Some(36),
            padded: true,
        },
        T::Json if matches!(provider, P::Postgres | P::Mysql | P::Mariadb) => OperandType::Json,
        // `CLOB` senza lunghezza su Db2 e `CLOB(1M)`.
        T::Json if matches!(provider, P::Db2) => OperandType::Text {
            length: Some(1_048_576),
            padded: false,
        },
        T::Json => OperandType::Text {
            length: None,
            padded: false,
        },
    }
}

/// I byte di un testo reso in byte: `VARCHAR2(4000)` su Oracle,
/// `VARCHAR(32672)` su Db2; illimitato altrove.
const fn text_limit(provider: ProviderKind) -> Option<u64> {
    match provider {
        ProviderKind::Oracle => Some(4000),
        ProviderKind::Db2 => Some(32672),
        _ => None,
    }
}

/// I byte di un binario reso: `RAW(2000)` su Oracle,
/// `VARCHAR(32672) FOR BIT DATA` su Db2; illimitato altrove.
const fn binary_limit(provider: ProviderKind) -> Option<u64> {
    match provider {
        ProviderKind::Oracle => Some(2000),
        ProviderKind::Db2 => Some(32672),
        _ => None,
    }
}

/// Le lunghezze di un testo reso si contano in byte su Oracle e Db2.
const fn byte_lengths(provider: ProviderKind) -> bool {
    matches!(provider, ProviderKind::Oracle | ProviderKind::Db2)
}

/// Cio che serve sapere di un valore legato per giudicarne il `CAST`, senza
/// tenerne i byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueFact {
    Null,
    Bool,
    /// Un intero (`I32`, `I64`).
    Integer(i128),
    /// Un `F64`: soltanto un tipo `float` lo tiene senza conversione.
    Float,
    /// Un `Decimal` in notazione posizionale; `None` per ogni altra forma.
    Decimal(Option<DecimalDigits>),
    /// Un testo: caratteri, byte, se e un uuid canonico.
    Text {
        chars: u64,
        bytes: u64,
        uuid: bool,
    },
    Bytes(u64),
    Date,
    /// Cifre frazionarie significative.
    Timestamp(u32),
    TimestampTz(u32),
    Uuid,
    Json,
    Enum,
    Wkb,
}

/// Le cifre di un decimale normalizzato: intere senza zeri iniziali,
/// frazionarie senza zeri finali.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalDigits {
    pub integer: u32,
    pub fraction: u32,
}

impl ValueFact {
    /// Il fatto di un valore.
    #[must_use]
    pub fn of(value: &ParameterValue) -> Self {
        let length = |text: &str| u64::try_from(text.len()).unwrap_or(u64::MAX);
        match value {
            ParameterValue::Null { .. } => Self::Null,
            ParameterValue::Bool(_) => Self::Bool,
            ParameterValue::I32(number) => Self::Integer(i128::from(*number)),
            ParameterValue::I64(number) => Self::Integer(i128::from(*number)),
            ParameterValue::F64(_) => Self::Float,
            ParameterValue::Decimal(text) => Self::Decimal(decimal_digits(text)),
            ParameterValue::String(text) => Self::Text {
                chars: u64::try_from(text.chars().count()).unwrap_or(u64::MAX),
                bytes: length(text),
                uuid: canonical_uuid(text),
            },
            ParameterValue::Bytes(bytes) => {
                Self::Bytes(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            }
            ParameterValue::Date(_) => Self::Date,
            ParameterValue::Timestamp(text) => Self::Timestamp(fraction_digits(text)),
            ParameterValue::TimestampTz(text) => Self::TimestampTz(fraction_digits(text)),
            ParameterValue::Uuid(_) => Self::Uuid,
            ParameterValue::Json(_) => Self::Json,
            ParameterValue::Enum { .. } => Self::Enum,
            ParameterValue::Wkb { .. } => Self::Wkb,
        }
    }
}

/// Le cifre di un decimale in notazione posizionale (segno facoltativo,
/// cifre, al piu un punto): ogni altra forma (esponente, spazi) e `None`.
#[must_use]
pub fn decimal_digits(text: &str) -> Option<DecimalDigits> {
    let digits = text.strip_prefix(['-', '+']).unwrap_or(text);
    let (integer, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    let all_digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if (integer.is_empty() && fraction.is_empty()) || !all_digits(integer) || !all_digits(fraction)
    {
        return None;
    }
    let count = |part: &str| u32::try_from(part.len()).unwrap_or(u32::MAX);
    Some(DecimalDigits {
        integer: count(integer.trim_start_matches('0')),
        fraction: count(fraction.trim_end_matches('0')),
    })
}

/// Le cifre frazionarie significative di un testo temporale.
fn fraction_digits(text: &str) -> u32 {
    let (_, rest) = text.split_once(['T', 't', ' ']).unwrap_or((text, ""));
    let (_, fraction) = rest.split_once('.').unwrap_or((rest, ""));
    let digits = fraction
        .bytes()
        .take_while(u8::is_ascii_digit)
        .map(char::from)
        .collect::<String>();
    u32::try_from(digits.trim_end_matches('0').len()).unwrap_or(u32::MAX)
}

/// Un uuid nella forma canonica `8-4-4-4-12` esadecimale.
fn canonical_uuid(text: &str) -> bool {
    let bytes = text.as_bytes();
    bytes.len() == 36
        && bytes.iter().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => *byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
}

/// Un decimale `(precisione, scala)` sta in un altro senza perdita: non piu
/// cifre frazionarie, non piu cifre intere.
const fn decimal_widens(source: (u32, u32), target: (u32, u32)) -> bool {
    let (precision, scale) = source;
    let (target_precision, target_scale) = target;
    let integer = precision.saturating_sub(scale);
    let target_integer = target_precision.saturating_sub(target_scale);
    scale <= target_scale && integer <= target_integer
}

const fn decimal_fits(digits: DecimalDigits, precision: u32, scale: u32) -> bool {
    digits.fraction <= scale && digits.integer <= precision.saturating_sub(scale)
}

fn integer_digits(value: i128) -> u32 {
    value
        .unsigned_abs()
        .checked_ilog10()
        .map_or(0, |log| log + 1)
}

/// Il valore legato sta esatto nel tipo reso `target`, senza che il server
/// lo converta in modo diverso da come e scritto (la tabella del modulo).
///
/// Il sottoinsieme che si prova senza riscrivere il valore: un `F64` sta
/// soltanto in un `float`, un decimale non sta in un `float` (limite
/// dichiarato); un testo sta in un testo entro la lunghezza, in un uuid se e
/// canonico; un timestamp entro le cifre frazionarie, un `timestamp_tz`
/// soltanto in un tipo con fuso.
#[must_use]
pub fn value_fits(provider: ProviderKind, fact: &ValueFact, target: &OperandType) -> bool {
    use OperandType as O;
    use ValueFact as V;
    let within = |length: Option<u64>, chars: u64, bytes: u64| {
        length.is_none_or(|length| {
            if byte_lengths(provider) {
                bytes <= length
            } else {
                chars <= length
            }
        })
    };
    match (fact, target) {
        (V::Null, _)
        | (V::Bool, O::Boolean)
        | (V::Float, O::Float { mantissa_bits: 53 })
        | (V::Uuid, O::Uuid)
        | (V::Json, O::Json)
        | (V::Enum, O::Enumeration)
        | (V::Date, O::Date | O::Timestamp { .. })
        | (V::Enum | V::Json, O::Text { length: None, .. }) => true,
        (V::Bool, O::Integer { min, max }) => *min <= 0 && *max >= 1,
        (V::Integer(number), O::Integer { min, max }) => (*min..=*max).contains(number),
        (V::Integer(number), O::Decimal { precision, scale }) => {
            integer_digits(*number) <= precision.saturating_sub(*scale)
        }
        (V::Integer(number), O::Float { mantissa_bits }) => {
            number.unsigned_abs() <= 1_u128 << mantissa_bits
        }
        (V::Decimal(Some(digits)), O::Decimal { precision, scale }) => {
            decimal_fits(*digits, *precision, *scale)
        }
        (V::Decimal(Some(digits)), O::Integer { min, max }) => {
            // Un decimale intero entro l'intervallo: al piu 18 cifre stanno
            // in ogni intervallo a 64 bit, e l'intervallo piu stretto che si
            // rende e quello a 32 bit, 9 cifre.
            digits.fraction == 0
                && digits.integer
                    <= if *min >= i128::from(i32::MIN) && *max <= i128::from(i32::MAX) {
                        9
                    } else {
                        18
                    }
        }
        (V::Text { chars, bytes, .. }, O::Text { length, .. }) => within(*length, *chars, *bytes),
        (V::Text { uuid, .. }, O::Uuid) => *uuid,
        (V::Uuid, O::Text { length, .. }) => within(*length, 36, 36),
        (V::Bytes(bytes), O::Binary { length, .. }) => length.is_none_or(|length| *bytes <= length),
        (V::Timestamp(fraction), O::Timestamp { fraction_digits })
        | (V::TimestampTz(fraction), O::TimestampTz { fraction_digits }) => {
            fraction <= fraction_digits
        }
        _ => false,
    }
}

/// Il tipo di una colonna (`source`, con il suo nome nel catalogo) sta senza
/// perdita nel tipo reso `target`: ogni valore della colonna resta lo stesso
/// dopo il `CAST` (la tabella del modulo).
///
/// Un testo a lunghezza fissa di `PostgreSQL` perde gli spazi finali in
/// `TEXT`; un `json` di `PostgreSQL` si normalizza in `JSONB`; il fuso di
/// Oracle passa da `TO_TIMESTAMP_TZ` attraverso il testo della sessione: si
/// rifiutano.
#[must_use]
pub fn widens(
    provider: ProviderKind,
    source: &OperandType,
    source_name: &str,
    target: &OperandType,
) -> bool {
    use OperandType as O;
    let range = |min: i128, max: i128| (min, max);
    let integer_range = |source: &OperandType| match source {
        O::Integer { min, max } => Some(range(*min, *max)),
        O::Decimal {
            precision,
            scale: 0,
        } if *precision <= 38 => {
            let bound = 10_i128.pow(*precision) - 1;
            Some(range(-bound, bound))
        }
        O::Boolean => Some(range(0, 1)),
        _ => None,
    };
    match (source, target) {
        (O::Boolean, O::Boolean)
        | (O::Uuid, O::Uuid)
        | (O::Date, O::Date | O::Timestamp { .. })
        | (O::Enumeration, O::Text { length: None, .. }) => true,
        (O::Json, O::Json) => {
            !(provider == ProviderKind::Postgres && source_name.eq_ignore_ascii_case("json"))
        }
        (
            O::Float { mantissa_bits },
            O::Float {
                mantissa_bits: target,
            },
        ) => mantissa_bits <= target,
        (_, O::Integer { min, max }) => {
            integer_range(source).is_some_and(|(low, high)| low >= *min && high <= *max)
        }
        (O::Integer { .. } | O::Boolean, O::Decimal { precision, scale }) => integer_range(source)
            .is_some_and(|(low, high)| {
                integer_digits(low.abs().max(high.abs())) <= precision.saturating_sub(*scale)
            }),
        (O::Integer { .. }, O::Float { mantissa_bits }) => {
            integer_range(source).is_some_and(|(low, high)| {
                low.unsigned_abs().max(high.unsigned_abs()) <= 1_u128 << mantissa_bits
            })
        }
        (
            O::Decimal { precision, scale },
            O::Decimal {
                precision: target_precision,
                scale: target_scale,
            },
        ) => decimal_widens((*precision, *scale), (*target_precision, *target_scale)),
        (
            O::RoundedDecimal {
                precision,
                rounding,
            },
            O::Decimal {
                precision: target_precision,
                scale: target_scale,
            },
        ) => precision.saturating_add(*rounding) <= target_precision.saturating_sub(*target_scale),
        (O::Text { length, padded }, O::Text { length: target, .. }) => {
            if *padded && provider == ProviderKind::Postgres {
                return false;
            }
            match (length, target) {
                (_, None) => true,
                (None, Some(_)) => false,
                (Some(length), Some(target)) => {
                    let bytes = if byte_lengths(provider) {
                        length.saturating_mul(4)
                    } else {
                        *length
                    };
                    bytes <= *target
                }
            }
        }
        (O::Binary { length, .. }, O::Binary { length: target, .. }) => match (length, target) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(length), Some(target)) => length <= target,
        },
        (
            O::Timestamp { fraction_digits },
            O::Timestamp {
                fraction_digits: target,
            },
        ) => fraction_digits <= target,
        (
            O::TimestampTz { fraction_digits },
            O::TimestampTz {
                fraction_digits: target,
            },
        ) => provider != ProviderKind::Oracle && fraction_digits <= target,
        _ => false,
    }
}

#[cfg(test)]
#[path = "cast_table_tests.rs"]
mod tests;
