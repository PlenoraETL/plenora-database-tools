//! La regola K: una chiave identifica al piu una riga, nel target e
//! nell'ingresso.
//!
//! Le mode per chiavi (`Update`, `DeleteByKeys`, `Upsert`) pubblicano un
//! conteggio per riga dell'ingresso, e il conteggio dice il vero soltanto se
//! ogni chiave dell'ingresso identifica **al piu una** riga del target e
//! **una sola** riga dell'ingresso.
//!
//! Due condizioni, entrambe prima del COMMIT:
//!
//! 1. **Unicita nel target.** Le chiavi coincidono (come insieme) con le
//!    colonne di un indice o vincolo unico del target che non e parziale (ne
//!    filtrato), non e su espressioni, e immediato e valido. Senza, una chiave
//!    tocca piu righe e un'altra nessuna, e i due errori si compensano: con
//!    `(1,a),(1,b),(2,c)` e un Update di `(1,z),(99,q)` il conteggio era
//!    `confirmed 2 skipped 0` con la chiave 99 assente (misurato il 25/10 su
//!    PostgreSQL 16 e `MySQL` 9.7; SQL Server lo rifiutava gia). Ogni provider
//!    legge il proprio catalogo e rifiuta con [`keys_without_unique_index`],
//!    categoria `schema` come SQL Server dal principio.
//! 2. **Unicita nell'ingresso.** Due righe dell'ingresso con la stessa chiave
//!    non hanno una semantica comune: `PostgreSQL` e SQL Server applicavano
//!    le righe una dopo l'altra (`confirmed 2`, vince l'ultima), `MySQL`
//!    passa da uno staging e un `UPDATE ... JOIN` che ne applica una sola
//!    (`confirmed 1 skipped 1`, vince la prima). La regola e una sola: una
//!    chiave ripetuta nell'ingresso rifiuta la scrittura (`data_mapping`,
//!    [`DUPLICATE_INPUT_KEYS`]), in ogni provider, con [`UniqueInputKeys`].
//!
//! # Che cosa e una chiave ripetuta
//!
//! Lo stesso valore Arrow in ogni colonna chiave, confrontato per valore: le
//! stringhe e i binari byte per byte, gli interi, le date, i tempi e i
//! decimali per valore (lo schema fissa unita e scala della colonna), i
//! float con `-0.0` uguale a `0.0` e ogni `NaN` uguale a ogni altro, come li
//! confronta `PostgreSQL`. Una chiave con un `NULL` non si registra: `= NULL`
//! non trova righe, e due chiavi `NULL` non toccano la stessa riga.
//!
//! # Limite dichiarato: chiavi uguali per il target, diverse come valori
//!
//! - **Regola**: la condizione 2 confronta i valori Arrow.
//! - **Ambito**: ogni provider; `MySQL`/`MariaDB` per Update confronta anche
//!   le coppie dello staging con le righe trovate, sotto la collation della
//!   colonna del target, e rifiuta con [`DUPLICATE_INPUT_KEYS`].
//! - **Hazard**: due valori diversi che il target confronta uguali (`'a'` e
//!   `'A'` sotto una collation che ignora le maiuscole, due testi di
//!   `datetimeoffset` con offset diversi per lo stesso istante) non sono
//!   ripetuti per il core. PostgreSQL, SQL Server, Oracle e Db2 applicano le
//!   righe in ordine (vince l'ultima, ciascuna contata); `MySQL`/`MariaDB` in
//!   Upsert le applicano in ordine e in `DeleteByKeys` contano una
//!   cancellata e una `skipped`.
//! - **Rientro**: il confronto sotto la collation della colonna in ogni
//!   provider e per ogni mode per chiavi (per `MySQL` con lo staging di
//!   Update).
//!
//! La memoria delle chiavi viste si prenota dal budget dell'operazione
//! (`MemoryBytes`) prima di crescere e si restituisce alla fine dello stream.

use crate::plan::{ProviderKind, WriteMode};
use crate::provider::{BatchStream, ProviderFuture};
use crate::resource::{ResourceBudget, ResourceKind, ResourceLease};
use crate::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use std::collections::HashSet;

/// Il motivo pubblico della condizione 1.
pub const KEYS_WITHOUT_UNIQUE_INDEX: &str =
    "le chiavi della mode non coincidono con un indice unico, non parziale e immediato del target";

/// Il motivo pubblico della condizione 2.
pub const DUPLICATE_INPUT_KEYS: &str =
    "chiave ripetuta nell'ingresso di una mode per chiavi: nessuna semantica comune fra i provider";

/// Il motivo pubblico di una chiave di un tipo che non si confronta qui.
pub const UNCOMPARABLE_KEY_TYPE: &str =
    "tipo Arrow di una chiave non confrontabile per l'unicita dell'ingresso";

/// Il costo per chiave vista oltre ai suoi byte: la voce dell'insieme e il
/// vettore. Una stima per eccesso, non una misura dell'allocatore.
const ENTRY_OVERHEAD_BYTES: u64 = 64;

/// Le mode a cui la regola K si applica.
#[must_use]
pub const fn keyed_mode(mode: WriteMode) -> bool {
    matches!(
        mode,
        WriteMode::Update | WriteMode::DeleteByKeys | WriteMode::Upsert
    )
}

/// La condizione 3: Update non aggiorna una chiave.
///
/// Gli statement di Update vanno riga per riga (o, in `MySQL`, da uno
/// staging): una riga che sposta la chiave 1 sulla 2 fa trovare alla riga
/// della chiave 2 la riga appena spostata, e il conteggio dice `confirmed`
/// per una chiave che nel target non c'era. SQL Server lo rifiutava gia;
/// dal 25/10 il core lo rifiuta per ogni provider (`invalid_plan`).
///
/// # Errors
///
/// `invalid_plan` se una chiave e anche colonna aggiornata.
pub fn refuse_key_in_update_columns(operation: &crate::plan::WriteOperation) -> Result<()> {
    if operation.mode == WriteMode::Update
        && operation
            .keys
            .iter()
            .any(|key| operation.update_columns.contains(key))
    {
        return Err(DatabaseError::invalid_plan(KEY_IN_UPDATE_COLUMNS));
    }
    Ok(())
}

/// Il motivo pubblico della condizione 3.
pub const KEY_IN_UPDATE_COLUMNS: &str =
    "una chiave di Update non puo essere anche una colonna aggiornata";

/// Il rifiuto della condizione 1, nella fase in cui il provider lo scopre.
#[must_use]
pub fn keys_without_unique_index(provider: ProviderKind, phase: ErrorPhase) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Schema,
        phase,
        Some(provider),
        format!("scrittura rifiutata: {KEYS_WITHOUT_UNIQUE_INDEX}"),
    )
}

fn duplicate_keys() -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Write,
        None,
        format!("scrittura rifiutata: {DUPLICATE_INPUT_KEYS}"),
    )
}

fn uncomparable() -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Unsupported,
        ErrorPhase::Validate,
        None,
        UNCOMPARABLE_KEY_TYPE,
    )
}

/// Le chiavi viste in un ingresso, per la condizione 2.
#[derive(Debug)]
pub struct UniqueInputKeys {
    columns: Vec<usize>,
    seen: HashSet<Vec<u8>>,
    leases: Vec<ResourceLease>,
}

impl UniqueInputKeys {
    /// Le colonne chiave di `schema`, nell'ordine di `keys`.
    ///
    /// # Errors
    ///
    /// `invalid_plan` per una chiave assente dallo schema; `unsupported` per
    /// una chiave di un tipo che [`Self::admit`] non confronta.
    pub fn new(schema: &SchemaRef, keys: &[String]) -> Result<Self> {
        let columns = keys
            .iter()
            .map(|key| {
                schema
                    .index_of(key)
                    .map_err(|_| DatabaseError::invalid_plan("chiave assente nello schema Arrow"))
            })
            .collect::<Result<Vec<_>>>()?;
        for index in &columns {
            if !comparable(schema.field(*index).data_type()) {
                return Err(uncomparable());
            }
        }
        Ok(Self {
            columns,
            seen: HashSet::new(),
            leases: Vec::new(),
        })
    }

    /// Registra le chiavi di `batch`; rifiuta alla prima gia vista.
    ///
    /// # Errors
    ///
    /// `data_mapping` con [`DUPLICATE_INPUT_KEYS`] per una chiave ripetuta,
    /// anche dentro lo stesso batch; `resource_limit` se il budget non
    /// basta per ricordarle; `unsupported` per un batch con un tipo diverso
    /// da quello dello schema.
    pub fn admit(&mut self, batch: &RecordBatch, budget: &ResourceBudget) -> Result<()> {
        let mut batch_keys = HashSet::with_capacity(batch.num_rows());
        let mut bytes = 0_u64;
        'rows: for row in 0..batch.num_rows() {
            let mut key = Vec::new();
            for index in &self.columns {
                let array = batch.column(*index);
                if array.is_null(row) {
                    continue 'rows;
                }
                encode(array.as_ref(), row, &mut key)?;
            }
            bytes = bytes
                .checked_add(u64::try_from(key.len()).unwrap_or(u64::MAX))
                .and_then(|bytes| bytes.checked_add(ENTRY_OVERHEAD_BYTES))
                .ok_or_else(|| {
                    DatabaseError::resource_limit("overflow nella memoria delle chiavi")
                })?;
            if self.seen.contains(&key) || !batch_keys.insert(key) {
                return Err(duplicate_keys());
            }
        }
        if bytes > 0 {
            self.leases
                .push(budget.try_lease(ResourceKind::MemoryBytes, bytes)?);
        }
        self.seen.extend(batch_keys);
        Ok(())
    }
}

/// I tipi che [`encode`] confronta per valore.
const fn comparable(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
            | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
    )
}

fn push_len(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// `-0.0` e `0.0` sono la stessa chiave, e ogni `NaN` e la stessa chiave.
fn float_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

/// Aggiunge a `out` il valore non nullo di `array` alla riga `row`, con la
/// lunghezza davanti ai valori variabili: due chiavi diverse non producono
/// gli stessi byte.
fn encode(array: &dyn Array, row: usize, out: &mut Vec<u8>) -> Result<()> {
    macro_rules! fixed {
        ($type:ty) => {
            out.extend_from_slice(&array.as_primitive::<$type>().value(row).to_le_bytes())
        };
    }
    match array.data_type() {
        DataType::Boolean => out.push(u8::from(array.as_boolean().value(row))),
        DataType::Int8 => fixed!(Int8Type),
        DataType::Int16 => fixed!(Int16Type),
        DataType::Int32 => fixed!(Int32Type),
        DataType::Int64 => fixed!(Int64Type),
        DataType::UInt8 => fixed!(UInt8Type),
        DataType::UInt16 => fixed!(UInt16Type),
        DataType::UInt32 => fixed!(UInt32Type),
        DataType::UInt64 => fixed!(UInt64Type),
        DataType::Float32 => out.extend_from_slice(
            &float_bits(f64::from(array.as_primitive::<Float32Type>().value(row))).to_le_bytes(),
        ),
        DataType::Float64 => out.extend_from_slice(
            &float_bits(array.as_primitive::<Float64Type>().value(row)).to_le_bytes(),
        ),
        DataType::Utf8 => push_len(out, array.as_string::<i32>().value(row).as_bytes()),
        DataType::LargeUtf8 => push_len(out, array.as_string::<i64>().value(row).as_bytes()),
        DataType::Binary => push_len(out, array.as_binary::<i32>().value(row)),
        DataType::LargeBinary => push_len(out, array.as_binary::<i64>().value(row)),
        DataType::FixedSizeBinary(_) => push_len(out, array.as_fixed_size_binary().value(row)),
        DataType::Date32 => fixed!(Date32Type),
        DataType::Date64 => fixed!(Date64Type),
        DataType::Time32(TimeUnit::Second) => fixed!(Time32SecondType),
        DataType::Time32(TimeUnit::Millisecond) => fixed!(Time32MillisecondType),
        DataType::Time64(TimeUnit::Microsecond) => fixed!(Time64MicrosecondType),
        DataType::Time64(TimeUnit::Nanosecond) => fixed!(Time64NanosecondType),
        DataType::Timestamp(TimeUnit::Second, _) => fixed!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => fixed!(TimestampMillisecondType),
        DataType::Timestamp(TimeUnit::Microsecond, _) => fixed!(TimestampMicrosecondType),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => fixed!(TimestampNanosecondType),
        DataType::Decimal128(_, _) => fixed!(Decimal128Type),
        _ => return Err(uncomparable()),
    }
    Ok(())
}

/// Lo stream dell'ingresso con la condizione 2: ogni batch passa da
/// [`UniqueInputKeys::admit`] prima di arrivare alla scrittura.
pub struct UniqueKeyStream {
    inner: Box<dyn BatchStream>,
    keys: UniqueInputKeys,
    budget: ResourceBudget,
}

impl BatchStream for UniqueKeyStream {
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
                self.keys.admit(batch, &self.budget)?;
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

/// L'ingresso di una scrittura con la condizione 2 della regola K.
///
/// Per le mode per chiavi ([`keyed_mode`]) lo stream che rifiuta una chiave
/// ripetuta, per le altre `input` com'e. Ogni provider lo chiama
/// all'ingresso di `write` e del dry run, prima di leggere un batch.
///
/// # Errors
///
/// Gli errori di [`UniqueInputKeys::new`].
pub fn guard_input_keys(
    mode: WriteMode,
    keys: &[String],
    input: Box<dyn BatchStream>,
    budget: &ResourceBudget,
) -> Result<Box<dyn BatchStream>> {
    if !keyed_mode(mode) || keys.is_empty() {
        return Ok(input);
    }
    let keys = UniqueInputKeys::new(&input.schema(), keys)?;
    Ok(Box::new(UniqueKeyStream {
        inner: input,
        keys,
        budget: budget.clone(),
    }))
}

#[cfg(test)]
#[path = "write_keys_tests.rs"]
mod tests;
