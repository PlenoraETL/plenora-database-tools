//! La regola S: un valore che il server riscrive al salvataggio.
//!
//! # La regola
//!
//! Un valore non nullo che il server, **al salvataggio**, sostituisce con un
//! valore diverso e una perdita, anche quando lo statement riesce:
//!
//! - con `Strict` la scrittura si rifiuta prima del COMMIT (`data_mapping`,
//!   [`REWRITTEN_VALUE`]);
//! - con le altre politiche la perdita si dichiara nel `LossReport` del
//!   preflight, per colonna, con il motivo preciso della riscrittura
//!   ([`StoreRewrite::reason`]), e il valore passa;
//! - un valore riscritto **non e mai una chiave**: le colonne di
//!   `operation.keys` (le mode per chiavi e la chiave primaria di Create) lo
//!   rifiutano con ogni politica ([`REWRITTEN_KEY`]). La regola K prova
//!   l'unicita sui valori dell'ingresso; se il server salva un altro valore,
//!   la prova non vale piu per cio che c'e nel target, e gli statement che
//!   confrontano la chiave non trovano la riga che hanno scritto.
//!
//! «Diverso» si legge sul valore Arrow che la lettura ne restituirebbe:
//! testo e binari byte per byte, `NULL` diverso da ogni valore, i float bit
//! per bit (il segno di zero compreso). La regola riguarda il testo verso i
//! tipi di testo, i binari verso i tipi binari e i numeri verso i tipi
//! numerici; i documenti (`json`, `jsonb`, `xml`) hanno le proprie regole (J)
//! e la loro forma canonica e la semantica dichiarata del tipo.
//!
//! # Le riscritture misurate (25/10, sesta rilettura indipendente)
//!
//! - [`StoreRewrite::EmptyToNull`]: Oracle salva come `NULL` il testo vuoto e
//!   il binario vuoto legati a qualunque colonna (un bind di lunghezza zero e
//!   `NULL`). Append `Strict` di `[(1,""),(2,"x")]` dava `committed`,
//!   `inserted 2`, `LossReport` vuoto e la riga 1 con `V IS NULL`; due
//!   Upsert `Strict` sulla chiave `V = ''` di un `UNIQUE (V)` confermavano 1
//!   ciascuno e lasciavano due righe con `V` nullo (`MERGE ... ON (T.V =
//!   S.V)` non trova mai `NULL`). Db2 fa lo stesso con la semantica
//!   `VARCHAR2` (`varchar2_compat`).
//! - [`StoreRewrite::PadText`]: `CHAR(n)` completa con spazi un testo piu
//!   corto (Oracle, PostgreSQL, SQL Server, Db2). Su Oracle il confronto con
//!   un bind `VARCHAR2` e senza riempimento: `WHERE C = :1` con `'a'` non
//!   trova la riga salvata come `'a  '`.
//! - [`StoreRewrite::PadBinary`]: `binary(n)` completa con `0x00` un binario
//!   piu corto (SQL Server).
//! - [`StoreRewrite::StripTrailing`]: una colonna SQL Server creata con
//!   `ANSI_PADDING OFF` toglie gli spazi finali di un testo e gli `0x00`
//!   finali di un binario.
//! - [`StoreRewrite::ExcessSpaces`]: `CHAR(n)` e `VARCHAR(n)` di Db2
//!   troncano gli spazi finali oltre la lunghezza (`'abc   '` verso
//!   `VARCHAR(3)` diventava `'abc'`).
//! - [`StoreRewrite::Underflow`]: il `FLOAT` di Oracle (un `NUMBER`) salva
//!   come `0` un valore non nullo sotto `1e-130` (`1e-200` diventava `0`).
//! - [`StoreRewrite::NegativeZero`]: nessun tipo numerico Oracle conserva
//!   `-0.0` fedelmente; la riscrittura si dichiara per ogni ingresso float.
//!   Lo stesso vale per `DOUBLE` e `REAL` di Db2 e `DOUBLE` e `FLOAT` di `MariaDB`
//!   (misurato sul percorso dei bind e con uno statement preparato).
//!
//! Il provider dichiara, per colonna, le riscritture del tipo del target
//! ([`ColumnRewrites`]); [`RewriteGuard`] le verifica valore per valore e
//! [`RewriteGuard::losses`] le dichiara. La regola vale per ogni percorso che
//! scrive o confronta chiavi: Append, Replace, Create con la scrittura,
//! Upsert, Update, `DeleteByKeys`, ogni profilo d'inserimento, il dry run di
//! PostgreSQL, e quindi SDK, facade e CLI, che passano dagli stessi metodi del
//! provider.
//!
//! # Che cosa non e una riscrittura (misurato lo stesso giorno)
//!
//! - Il carattere NUL nel testo: Oracle, Db2, SQL Server, `MySQL` lo
//!   conservano; PostgreSQL lo rifiuta con un errore.
//! - `-0.0`: lo conservano `float8` di PostgreSQL, `float` di SQL Server
//!   (misurato con un letterale), `DOUBLE` di `MySQL`. **Non** Oracle: fino al
//!   round 8 qui si leggeva che `BINARY_DOUBLE` lo conserva, ed era falso
//!   (PR #136, misurato): legato dal driver si salva in una forma non
//!   canonica che il server non confronta uguale a `0` e normalizza a `+0`,
//!   e ogni altro tipo numerico lo salva `+0`. Il provider Oracle lo rifiuta
//!   al bind (`plenora-db-oracle`, `parameter.rs`); la sonda della regola A,
//!   che rilegge con il decodificatore della libreria, non vede la forma non
//!   canonica, e la prova e il rifiuto. Il piano Oracle aggiunge quindi
//!   `NegativeZero` anche quando il fixture della sonda indica soltanto il
//!   rifiuto: con `Strict` e per le chiavi interviene la regola S.
//! - Le liste verso le collezioni Oracle, `CHAR`, `BINARY`, `ENUM` e `SET` di
//!   `MySQL` e `MariaDB`: non sono tipi scrivibili (il preflight li rifiuta).
//! - `EMPTY_STRING_IS_NULL` e `PAD_CHAR_TO_FULL_LENGTH` di `MariaDB` e
//!   `MySQL`: la sessione della scrittura fissa `sql_mode` e lo rilegge.
//! - Il testo oltre la lunghezza: la regola P (PostgreSQL, SQL Server,
//!   `MySQL`); Oracle lo rifiuta con `ORA-12899`; Db2 lo tronca e qui e
//!   [`StoreRewrite::ExcessSpaces`].
//! - Due chiavi che il server confronta uguali senza riscriverle (gli spazi
//!   e gli `0x00` finali): la condizione 2 della regola K
//!   (`crate::write_keys`, «Gli spazi finali delle chiavi»).
//!
//! # Limite dichiarato: la verifica per eccesso
//!
//! - **Regola**: un valore che il tipo del target potrebbe riscrivere si
//!   tratta come riscritto.
//! - **Ambito**: [`StoreRewrite::PadText`] conta i caratteri anche dove la
//!   colonna conta i byte (`CHAR(3 BYTE)` su Oracle, `char(n)` di SQL Server
//!   fuori da UTF-8); [`StoreRewrite::Underflow`] usa una soglia per eccesso.
//! - **Hazard**: piu stretto: si rifiuta anche un valore che riempirebbe la
//!   colonna in byte con meno caratteri, o un float appena sopra il minimo
//!   del tipo.
//! - **Rientro**: la lunghezza nell'unita della colonna e nel suo charset.
//!
//! # Limite dichiarato: dove si leggono le riscritture
//!
//! - **Regola**: le riscritture vengono dal tipo del target letto dal
//!   catalogo.
//! - **Ambito**: PostgreSQL e la semantica `VARCHAR2` di Db2 si leggono nella
//!   transazione della scrittura; i tipi di Oracle, Db2 e SQL Server
//!   (`is_ansi_padded` compreso) nel preflight, su un'altra sessione prima
//!   della transazione, come gli altri fatti del catalogo di quei provider.
//! - **Hazard**: un DDL concorrente fra il preflight e la scrittura (una
//!   colonna ricreata come `CHAR`) non si vede; con `Strict` le riscritture
//!   di Oracle, Db2 e `MySQL` si rifiutano comunque per valore, ma soltanto
//!   per i tipi letti. Db2 senza il permesso di leggere `SYSIBMADM.DBCFG`
//!   (concesso a `PUBLIC` per difetto, misurato con un utente che ha soltanto
//!   `CONNECT`) rifiuta ogni scrittura: piu stretto.
//! - **Rientro**: i tipi riletti nella transazione dopo il lock, con la
//!   regola R.

use crate::loss::{LossCategory, LossSeverity, MappingLoss, MappingPolicy};
use crate::plan::{ProviderKind, WriteOperation};
use crate::provider::{BatchStream, ProviderFuture};
use crate::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, SchemaRef, TimeUnit};

/// Il motivo pubblico del rifiuto di un valore riscritto con `Strict`.
pub const REWRITTEN_VALUE: &str = "il server riscriverebbe al salvataggio il valore in uno \
     diverso (regola S): con Strict e una perdita";

/// Il motivo pubblico del rifiuto di una chiave riscritta, con ogni politica.
pub const REWRITTEN_KEY: &str = "il server riscriverebbe al salvataggio il valore di una \
     chiave in uno diverso (regola S): la regola K non lo prova, con nessuna politica";

/// Una riscrittura che il tipo del target applica al salvataggio.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StoreRewrite {
    /// Il testo vuoto e il binario vuoto diventano `NULL`.
    EmptyToNull,
    /// Un testo con meno di `chars` caratteri si completa con spazi.
    PadText { chars: u64 },
    /// Un binario con meno di `bytes` byte si completa con `0x00`.
    PadBinary { bytes: u64 },
    /// Gli spazi finali di un testo, o gli `0x00` finali di un binario, si
    /// tolgono.
    StripTrailing,
    /// Gli spazi finali oltre `bytes` byte si troncano: si tratta come
    /// riscritto ogni testo piu lungo che finisce con uno spazio.
    ExcessSpaces { bytes: u64 },
    /// Un float non nullo con modulo sotto `min_abs` diventa `0`.
    Underflow { min_abs: f64 },
    /// `-0.0` diventa `0.0`.
    NegativeZero,
    /// Un istante con piu di `digits` cifre frazionarie dei secondi si
    /// arrotonda o si tronca (Oracle `TIMESTAMP(p)`, Db2 `TIMESTAMP(p)`).
    FractionalSeconds { digits: u8 },
    /// Un testo con un carattere fuori da ASCII si tratta come riscritto: il
    /// charset della colonna non lo prova (verifica per eccesso).
    NonAscii,
    /// Un documento JSON con un numero fuori dalla forma intera breve
    /// (`-?(0|[1-9][0-9]{0,14})`, senza `-0`) si tratta come riscritto: il
    /// tipo JSON del target ne riscrive la forma (`1.0` in `1`, `1e2` in
    /// `100`, `-0` in `0`, le cifre oltre la precisione). Verifica per
    /// eccesso: anche `1.5`, che `jsonb` conserva, si tratta come riscritto.
    JsonNumbers,
    /// Ogni valore non nullo si tratta come riscritto: la misura della regola
    /// A (`crate::write_admission`) ha trovato riscritture che nessuna classe
    /// per valore spiega.
    Every,
}

impl StoreRewrite {
    /// Il motivo della perdita nel `LossReport`.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::EmptyToNull => {
                "regola S: il server salva come NULL il testo vuoto e il binario vuoto"
            }
            Self::PadText { .. } => {
                "regola S: il tipo a lunghezza fissa completa con spazi un testo piu corto"
            }
            Self::PadBinary { .. } => {
                "regola S: il tipo a lunghezza fissa completa con 0x00 un binario piu corto"
            }
            Self::StripTrailing => {
                "regola S: la colonna toglie gli spazi finali del testo e gli 0x00 finali del binario"
            }
            Self::ExcessSpaces { .. } => {
                "regola S: la colonna tronca gli spazi finali oltre la propria lunghezza"
            }
            Self::Underflow { .. } => {
                "regola S: il tipo numerico salva come 0 un float sotto il proprio minimo"
            }
            Self::NegativeZero => "regola S: il tipo numerico salva -0.0 come 0",
            Self::FractionalSeconds { .. } => {
                "regola S: il tipo temporale arrotonda o tronca le cifre frazionarie oltre la propria precisione"
            }
            Self::NonAscii => {
                "regola S: il charset della colonna non conserva ogni carattere fuori da ASCII"
            }
            Self::JsonNumbers => {
                "regola S: il tipo JSON del target riscrive la forma dei numeri fuori dagli interi brevi"
            }
            Self::Every => {
                "regola S: la misura della coppia di tipi (regola A) ha trovato valori riscritti al salvataggio"
            }
        }
    }

    const fn category(self) -> LossCategory {
        match self {
            Self::EmptyToNull => LossCategory::Nullability,
            Self::PadText { .. }
            | Self::PadBinary { .. }
            | Self::StripTrailing
            | Self::ExcessSpaces { .. }
            | Self::Every => LossCategory::NativeType,
            Self::Underflow { .. }
            | Self::NegativeZero
            | Self::FractionalSeconds { .. }
            | Self::JsonNumbers => LossCategory::Precision,
            Self::NonAscii => LossCategory::Encoding,
        }
    }

    /// Se la riscrittura si applica a un tipo Arrow.
    #[must_use]
    pub const fn admits(self, data_type: &DataType) -> bool {
        let text = matches!(data_type, DataType::Utf8 | DataType::LargeUtf8);
        let binary = matches!(
            data_type,
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_)
        );
        let float = matches!(data_type, DataType::Float32 | DataType::Float64);
        let instant = matches!(data_type, DataType::Timestamp(_, _));
        match self {
            Self::EmptyToNull | Self::StripTrailing => text || binary,
            Self::PadText { .. }
            | Self::ExcessSpaces { .. }
            | Self::NonAscii
            | Self::JsonNumbers => text,
            Self::PadBinary { .. } => binary,
            Self::Underflow { .. } | Self::NegativeZero => float,
            Self::FractionalSeconds { .. } => instant,
            Self::Every => true,
        }
    }

    /// Se il valore non nullo di `array` alla riga `row` sarebbe riscritto.
    #[must_use]
    pub fn rewrites(self, array: &dyn Array, row: usize) -> bool {
        if self == Self::Every {
            return !array.is_null(row);
        }
        match (self, value(array, row)) {
            (Self::EmptyToNull, Value::Text(text)) => text.is_empty(),
            (Self::EmptyToNull, Value::Bytes(bytes)) => bytes.is_empty(),
            (Self::PadText { chars }, Value::Text(text)) => {
                u64::try_from(text.chars().count()).unwrap_or(u64::MAX) < chars
            }
            (Self::PadBinary { bytes: length }, Value::Bytes(bytes)) => {
                u64::try_from(bytes.len()).unwrap_or(u64::MAX) < length
            }
            (Self::StripTrailing, Value::Text(text)) => text.ends_with(' '),
            (Self::ExcessSpaces { bytes }, Value::Text(text)) => {
                text.ends_with(' ') && u64::try_from(text.len()).unwrap_or(u64::MAX) > bytes
            }
            (Self::StripTrailing, Value::Bytes(bytes)) => bytes.last() == Some(&0),
            (Self::Underflow { min_abs }, Value::Float(number)) => {
                number != 0.0 && number.abs() < min_abs
            }
            (Self::NegativeZero, Value::Float(number)) => {
                number == 0.0 && number.is_sign_negative()
            }
            (Self::FractionalSeconds { digits }, Value::Instant { subsecond_ns }) => {
                // Le cifre conservate sono `digits` su nove: il resto dei
                // nanosecondi oltre quella unita e cio che si perde.
                let unit = 10_i64.pow(9_u32.saturating_sub(u32::from(digits.min(9))));
                subsecond_ns.rem_euclid(unit) != 0
            }
            (Self::NonAscii, Value::Text(text)) => !text.is_ascii(),
            (Self::JsonNumbers, Value::Text(text)) => has_rewritten_json_number(text),
            _ => false,
        }
    }
}

/// Se il testo contiene, fuori dalle stringhe JSON, un numero che non ha la
/// forma `-?(0|[1-9][0-9]{0,14})` o che e `-0`. Il testo non si valida: un
/// documento non valido lo rifiuta il server, e qui conta soltanto che nessun
/// numero passi in una forma che il tipo riscrive.
fn has_rewritten_json_number(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => {
                index += 1;
                while index < bytes.len() && bytes[index] != b'"' {
                    index += if bytes[index] == b'\\' { 2 } else { 1 };
                }
                index += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = index;
                while index < bytes.len()
                    && matches!(bytes[index], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                {
                    index += 1;
                }
                if !short_integer(&bytes[start..index]) {
                    return true;
                }
            }
            _ => index += 1,
        }
    }
    false
}

fn short_integer(token: &[u8]) -> bool {
    let digits = token.strip_prefix(b"-").unwrap_or(token);
    if digits.is_empty() || digits.len() > 15 || !digits.iter().all(u8::is_ascii_digit) {
        return false;
    }
    if digits == b"0" {
        return token.len() == 1;
    }
    digits[0] != b'0'
}

enum Value<'a> {
    Text(&'a str),
    Bytes(&'a [u8]),
    Float(f64),
    /// I nanosecondi dell'istante dentro il proprio secondo.
    Instant {
        subsecond_ns: i64,
    },
    Other,
}

fn value(array: &dyn Array, row: usize) -> Value<'_> {
    match array.data_type() {
        DataType::Utf8 => Value::Text(array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => Value::Text(array.as_string::<i64>().value(row)),
        DataType::Binary => Value::Bytes(array.as_binary::<i32>().value(row)),
        DataType::LargeBinary => Value::Bytes(array.as_binary::<i64>().value(row)),
        DataType::FixedSizeBinary(_) => Value::Bytes(array.as_fixed_size_binary().value(row)),
        DataType::Float32 => {
            Value::Float(f64::from(array.as_primitive::<Float32Type>().value(row)))
        }
        DataType::Float64 => Value::Float(array.as_primitive::<Float64Type>().value(row)),
        DataType::Timestamp(unit, _) => {
            let (raw, per_second) = match unit {
                TimeUnit::Second => (array.as_primitive::<TimestampSecondType>().value(row), 1),
                TimeUnit::Millisecond => (
                    array.as_primitive::<TimestampMillisecondType>().value(row),
                    1_000,
                ),
                TimeUnit::Microsecond => (
                    array.as_primitive::<TimestampMicrosecondType>().value(row),
                    1_000_000,
                ),
                TimeUnit::Nanosecond => (
                    array.as_primitive::<TimestampNanosecondType>().value(row),
                    1_000_000_000,
                ),
            };
            Value::Instant {
                subsecond_ns: raw.rem_euclid(per_second) * (1_000_000_000 / per_second),
            }
        }
        _ => Value::Other,
    }
}

/// Le riscritture del tipo del target per una colonna dell'ingresso.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRewrites {
    /// L'indice della colonna nello schema Arrow dell'ingresso.
    pub field: usize,
    /// Il tipo nativo del target, per il `LossReport`.
    pub target_type: String,
    pub rewrites: Vec<StoreRewrite>,
}

/// La regola S su un ingresso: le riscritture per colonna, le chiavi e la
/// politica.
#[derive(Debug, Clone)]
pub struct RewriteGuard {
    provider: ProviderKind,
    columns: Vec<(ColumnRewrites, bool)>,
    strict: bool,
}

impl RewriteGuard {
    /// La regola per `operation` sull'ingresso `schema`: le colonne di
    /// `operation.keys` sono chiavi, con ogni mode.
    ///
    /// # Errors
    ///
    /// `internal` se il provider dichiara una riscrittura su un tipo Arrow a
    /// cui non si applica, o un indice fuori dallo schema: la regola non si
    /// verificherebbe, e si rifiuta invece di passare in silenzio.
    pub fn new(
        provider: ProviderKind,
        schema: &SchemaRef,
        operation: &WriteOperation,
        columns: Vec<ColumnRewrites>,
    ) -> Result<Self> {
        let mut guarded = Vec::with_capacity(columns.len());
        for column in columns {
            if column.rewrites.is_empty() {
                continue;
            }
            let field = schema.fields().get(column.field).ok_or_else(|| {
                internal(
                    provider,
                    "regola S: colonna fuori dallo schema dell'ingresso",
                )
            })?;
            if !column
                .rewrites
                .iter()
                .all(|rewrite| rewrite.admits(field.data_type()))
            {
                return Err(internal(
                    provider,
                    "regola S: riscrittura dichiarata su un tipo Arrow a cui non si applica",
                ));
            }
            let key = operation.keys.iter().any(|key| key == field.name());
            guarded.push((column, key));
        }
        Ok(Self {
            provider,
            columns: guarded,
            strict: operation.mapping_policy == MappingPolicy::Strict,
        })
    }

    /// Nessuna colonna con una riscrittura.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Verifica ogni valore non nullo di `batch`.
    ///
    /// # Errors
    ///
    /// `data_mapping` con [`REWRITTEN_KEY`] per un valore riscritto in una
    /// chiave, con ogni politica; con [`REWRITTEN_VALUE`] per un valore
    /// riscritto in un'altra colonna con `Strict`. Il messaggio non porta il
    /// valore.
    pub fn check(&self, batch: &RecordBatch) -> Result<()> {
        for (column, key) in &self.columns {
            if !*key && !self.strict {
                continue;
            }
            let array = batch.column(column.field);
            for row in 0..batch.num_rows() {
                if array.is_null(row) {
                    continue;
                }
                if column
                    .rewrites
                    .iter()
                    .any(|rewrite| rewrite.rewrites(array.as_ref(), row))
                {
                    return Err(rewritten(self.provider, *key));
                }
            }
        }
        Ok(())
    }

    /// Le perdite da dichiarare nel `LossReport` con una politica non
    /// stretta: una per riscrittura di ogni colonna che non e chiave (in una
    /// chiave il valore riscritto si rifiuta). Con `Strict`, nessuna: il
    /// valore riscritto si rifiuta.
    #[must_use]
    pub fn losses(&self) -> Vec<MappingLoss> {
        if self.strict {
            return Vec::new();
        }
        self.columns
            .iter()
            .filter(|(_, key)| !*key)
            .flat_map(|(column, _)| {
                column.rewrites.iter().map(|rewrite| MappingLoss {
                    field_id: u32::try_from(column.field).unwrap_or(u32::MAX),
                    category: rewrite.category(),
                    severity: LossSeverity::DataLoss,
                    reason: rewrite.reason().to_owned(),
                    source_type: None,
                    target_type: Some(column.target_type.clone()),
                })
            })
            .collect()
    }
}

fn rewritten(provider: ProviderKind, key: bool) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Write,
        Some(provider),
        format!(
            "scrittura rifiutata: {}",
            if key { REWRITTEN_KEY } else { REWRITTEN_VALUE }
        ),
    )
}

fn internal(provider: ProviderKind, message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Internal,
        ErrorPhase::Prepare,
        Some(provider),
        message,
    )
}

/// Lo stream dell'ingresso con la regola S: ogni batch passa da
/// [`RewriteGuard::check`] prima di arrivare alla scrittura.
pub struct RewriteStream {
    inner: Box<dyn BatchStream>,
    guard: RewriteGuard,
}

impl BatchStream for RewriteStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            let batch = self.inner.next_batch(cancellation).await?;
            if let Some(batch) = &batch {
                self.guard.check(batch)?;
            }
            Ok(batch)
        })
    }

    fn declared_input_rows(&self) -> Option<u64> {
        self.inner.declared_input_rows()
    }

    fn row_diagnostics_policy(&self) -> crate::row_diagnostics::RowDiagnosticsPolicy {
        self.inner.row_diagnostics_policy()
    }
}

/// L'ingresso con la regola S, o `input` com'e se nessuna colonna ha una
/// riscrittura.
#[must_use]
pub fn guard_input(input: Box<dyn BatchStream>, guard: RewriteGuard) -> Box<dyn BatchStream> {
    if guard.is_empty() {
        input
    } else {
        Box::new(RewriteStream {
            inner: input,
            guard,
        })
    }
}

#[cfg(test)]
#[path = "store_rewrite_tests.rs"]
mod tests;
