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
//! stringhe byte per byte senza gli spazi finali, i binari byte per byte senza
//! gli `0x00` finali, gli interi, le date, i tempi e i
//! decimali per valore (lo schema fissa unita e scala della colonna), i
//! float con `-0.0` uguale a `0.0` e ogni `NaN` uguale a ogni altro, come li
//! confronta `PostgreSQL`.
//!
//! Un `NULL` in una colonna chiave segue l'indice che prova le chiavi
//! ([`NullKeys`]), cioe il prodotto: due chiavi sono la stessa chiave se
//! l'indice le tratterebbe come la stessa voce. Fino al 25/10 (quinta
//! rilettura indipendente) una chiave con un `NULL` non si registrava mai
//! («due chiavi `NULL` non toccano la stessa riga»), e con `UNIQUE NULLS NOT
//! DISTINCT (k)` e la riga `(NULL,'vecchio')` l'Upsert di
//! `[(NULL,'uno'),(NULL,'due')]` su PostgreSQL dava `confirmed 2` con una
//! sola riga `'due'`: `ON CONFLICT` arbitra con l'indice, per cui i due `NULL`
//! sono la stessa voce.
//!
//! - PostgreSQL: [`NullKeys::Distinct`] con un indice `NULLS DISTINCT` (il
//!   predefinito), [`NullKeys::Equal`] se un indice unico che prova le chiavi
//!   e `NULLS NOT DISTINCT` (`pg_index.indnullsnotdistinct`, dalla 15), letto
//!   nella transazione della scrittura e nel dry run con la DSN;
//! - SQL Server: [`NullKeys::Equal`]: un indice unico non filtrato (l'unico
//!   che prova le chiavi) ammette un solo `NULL` per combinazione;
//! - `MySQL` e `MariaDB`: [`NullKeys::Distinct`]: un indice unico ammette piu
//!   righe con un `NULL`;
//! - Oracle: [`NullKeys::EqualUnlessAllNull`]: un indice B-tree non registra
//!   le chiavi tutte `NULL`, e confronta le altre con i `NULL` uguali;
//! - Db2: [`NullKeys::Equal`]: un indice unico ammette un solo `NULL` per
//!   combinazione; con `EXCLUDE NULL KEYS` le chiavi tutte `NULL` sarebbero
//!   distinte, e la regola piu stretta le rifiuta comunque.
//!
//! La regola vale per ogni mode per chiavi: anche dove lo statement confronta
//! con `=` e una chiave `NULL` non trova righe (Update, `DeleteByKeys`), due
//! chiavi che l'indice tratta come una sola si rifiutano.
//!
//! # Limite dichiarato: i `NULL` delle chiavi
//!
//! - **Regola**: due chiavi che l'indice che le prova tratta come la stessa
//!   voce sono ripetute, `NULL` compresi.
//! - **Ambito**: ogni provider, nel modo dell'elenco qui sopra.
//! - **Hazard**: comportamento piu stretto: Update e `DeleteByKeys` con due
//!   chiavi `NULL` (SQL Server, Db2, PostgreSQL `NULLS NOT DISTINCT`), che
//!   prima davano due `skipped`, si rifiutano; Db2 rifiuta anche due chiavi
//!   tutte `NULL` sotto `EXCLUDE NULL KEYS`. Il dry run PostgreSQL senza la
//!   DSN non legge l'indice e confronta come `NULLS DISTINCT`; la scrittura
//!   legge l'indice una volta, prima del primo batch, e non lo rilegge dopo
//!   il lock (lo stesso hazard del DDL concorrente della regola K2).
//! - **Rientro**: la semantica dei `NULL` dello statement, mode per mode, al
//!   posto di quella dell'indice; per Db2 la lettura di `EXCLUDE NULL KEYS`.
//!
//! # Limite dichiarato: chiavi uguali per il target, diverse come valori
//!
//! - **Regola**: la condizione 2 confronta i valori Arrow.
//! - **Ambito**: ogni provider; `MySQL`/`MariaDB` confrontano anche sotto la
//!   collation della colonna del target, nella sessione della scrittura e
//!   prima di ogni statement sul target, e rifiutano con
//!   [`DUPLICATE_INPUT_KEYS`] (`rolled_back`): Update con le coppie dello
//!   staging contro le righe trovate, Upsert e `DeleteByKeys` con una
//!   tabella `TEMPORARY` delle chiavi con lo stesso tipo, charset e
//!   collation e un indice unico (`plenora_db_mysql`
//!   `KeyCollationGuard`), e rifiutano nel preflight (`unsupported`) una
//!   chiave sotto collation non binaria con una colonna `TEXT` o `BLOB`,
//!   che un indice unico confronterebbe soltanto per prefisso. Db2 non ha
//!   collation per colonna: sotto una collation del database (`collate_info`)
//!   diversa da `IDENTITY` e `IDENTITY_16BIT` rifiuta nel preflight ogni
//!   mode per chiavi con una chiave di testo (`unsupported`,
//!   `plenora_db_db2` `refuse_text_keys_under_collation`); sotto `IDENTITY`
//!   l'indice confronta i byte, con gli spazi finali della sezione qui sotto.
//!   PostgreSQL confronta per byte sotto ogni collation deterministica (dopo
//!   l'ordinamento, l'uguaglianza e quella dei byte: la condizione 2 e esatta
//!   o piu stretta) e rifiuta (`unsupported`, `plenora_db_postgres`
//!   `refuse_nondeterministic_keys`) ogni mode per chiavi con una chiave sotto
//!   una collation non deterministica (`pg_collation.collisdeterministic`
//!   falso, ICU), della colonna o di un indice unico che la comprende: nel
//!   preflight, nella transazione della scrittura prima di ogni statement sul
//!   target e nel dry run con la DSN. Fino al 25/10 l'Upsert di `'a'` e `'A'`
//!   su una chiave ICU `und-u-ks-level2` confermava due righe e ne lasciava
//!   una. SQL Server confronta sotto la collation e il tipo della colonna
//!   del target in ogni mode per chiavi: nella transazione della scrittura,
//!   dopo il lock e prima degli statement di ogni batch sul target, le chiavi
//!   passano da una tabella temporanea `#plenora_keys` con le colonne chiave
//!   dello stesso tipo e della stessa collation e un indice unico
//!   (`plenora_db_sqlserver`, `write::keys`); una violazione e
//!   [`DUPLICATE_INPUT_KEYS`] (`rolled_back`). Fino al 25/10 l'Upsert di
//!   `'a'` e `'A'` su una chiave `nvarchar` sotto
//!   `SQL_Latin1_General_CP1_CI_AS` confermava due righe e ne lasciava una.
//! - **Hazard**: due valori diversi che il target confronta uguali (`'a'` e
//!   `'A'` sotto una collation che ignora le maiuscole, due testi di
//!   `datetimeoffset` con offset diversi per lo stesso istante) non sono
//!   ripetuti per il core. Oracle applica le righe in ordine (vince
//!   l'ultima, ciascuna contata); SQL Server le rifiuta (fino al 25/10 le
//!   applicava in ordine, ciascuna contata); PostgreSQL lo farebbe sotto una
//!   collation non deterministica, ma rifiuta prima la chiave (piu stretto:
//!   anche chiavi distinte sotto la collation si rifiutano); Db2 lo farebbe, ma
//!   le rifiuta prima (piu stretto: chiavi di testo distinte anche sotto la
//!   collation si rifiutano); `MySQL`/`MariaDB` le rifiutano (fino al 25/10
//!   in Upsert le applicavano in ordine, `confirmed 2` con una riga, e in
//!   `DeleteByKeys` contavano una cancellata e una `skipped`); piu stretto
//!   per le chiavi `TEXT` e `BLOB` sotto collation non binaria.
//! - **Rientro**: il confronto sotto la collation della colonna in ogni
//!   provider e per ogni mode per chiavi (fatto per `MySQL`/`MariaDB`,
//!   salvo le chiavi `TEXT` e `BLOB`, e per SQL Server; per PostgreSQL con una tabella
//!   temporanea e un indice unico sotto la collation della chiave, al posto
//!   del rifiuto).
//!
//! # Gli spazi finali delle chiavi
//!
//! Due chiavi di testo che differiscono soltanto per gli spazi finali
//! (U+0020) sono la stessa chiave per la condizione 2. SQL Server e Db2
//! confrontano il testo completando con spazi il piu corto, `MariaDB` e
//! `MySQL` fanno lo stesso con le collation `PAD SPACE`, PostgreSQL con
//! `char(n)`: `'a'` e `'a '` sono la stessa voce dell'indice. Allo stesso
//! modo SQL Server confronta i binari senza gli `0x00` finali (`0x01 =
//! 0x0100`, misurato il 25/10 su SQL Server 2022). La condizione 2
//! confrontava byte per byte e non vedeva la chiave ripetuta (sesta rilettura
//! indipendente, la stessa classe della regola S, `crate::store_rewrite`).
//!
//! ## Limite dichiarato: gli spazi finali
//!
//! - **Regola**: gli spazi finali non distinguono due chiavi di testo, gli
//!   `0x00` finali due chiavi binarie.
//! - **Ambito**: ogni provider e ogni mode per chiavi.
//! - **Hazard**: piu stretto dove gli spazi finali contano (`text` e
//!   `varchar` di PostgreSQL, `VARCHAR2` di Oracle, le collation `NO PAD` di
//!   `MySQL`) e gli `0x00` finali (`bytea`, `RAW`, `varbinary` di `MySQL`):
//!   due chiavi `'a'` e `'a '`, o `0x01` e `0x0100`, si rifiutano anche se il
//!   target le distingue.
//! - **Rientro**: la semantica degli spazi del confronto della colonna,
//!   provider per provider.
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

/// Il motivo pubblico della condizione 4: Upsert vuole un solo indice
/// unico sulle chiavi.
///
/// L'arbitro dell'upsert non e soltanto l'indice che prova le chiavi:
/// PostgreSQL (`ON CONFLICT (k)`) arbitra con ogni indice unico sulle stesse
/// colonne, qualunque collation abbia, e `MySQL`/`MariaDB` (`ON DUPLICATE
/// KEY UPDATE`) con ogni indice unico della tabella. Con un secondo indice la
/// riga in ingresso puo aggiornarne un'altra (quarta rilettura del 25/10:
/// `id text COLLATE "C" PRIMARY KEY` e `UNIQUE (id COLLATE ci)`, l'upsert di
/// `('a', 'nuovo')` aggiornava la riga `A`). La regola e chiusa e stretta:
/// un solo indice unico del target coinvolge le colonne chiave, ed e quello
/// che le prova (per `MySQL` e `MariaDB`, dove l'arbitro e ogni indice unico
/// della tabella: soltanto indici unici interi sulle colonne chiave, che
/// arbitrano la stessa riga; nessuno su altre colonne, funzionale o su un
/// prefisso).
pub const UPSERT_WITH_OTHER_UNIQUE_INDEX: &str =
    "Upsert vuole un solo indice unico che coinvolga le chiavi, quello che le prova: un altro \
     indice unico potrebbe arbitrare il conflitto e aggiornare un'altra riga";

/// Il rifiuto della condizione 4, nella fase in cui il provider lo scopre.
#[must_use]
pub fn upsert_with_other_unique_index(provider: ProviderKind, phase: ErrorPhase) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Schema,
        phase,
        Some(provider),
        format!("scrittura rifiutata: {UPSERT_WITH_OTHER_UNIQUE_INDEX}"),
    )
}

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

/// Come l'indice che prova le chiavi tratta un `NULL` in una colonna chiave.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullKeys {
    /// Ogni chiave con un `NULL` e distinta da ogni altra: non si registra.
    Distinct,
    /// `NULL` uguale a `NULL` in ogni colonna chiave.
    Equal,
    /// Una chiave tutta `NULL` e distinta da ogni altra; le altre si
    /// confrontano con `NULL` uguale a `NULL`.
    EqualUnlessAllNull,
}

/// Le chiavi viste in un ingresso, per la condizione 2.
#[derive(Debug)]
pub struct UniqueInputKeys {
    columns: Vec<usize>,
    nulls: NullKeys,
    seen: HashSet<Vec<u8>>,
    leases: Vec<ResourceLease>,
}

impl UniqueInputKeys {
    /// Le colonne chiave di `schema`, nell'ordine di `keys`, con i `NULL`
    /// confrontati come `nulls`.
    ///
    /// # Errors
    ///
    /// `invalid_plan` per una chiave assente dallo schema; `unsupported` per
    /// una chiave di un tipo che [`Self::admit`] non confronta.
    pub fn new(schema: &SchemaRef, keys: &[String], nulls: NullKeys) -> Result<Self> {
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
            nulls,
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
            let mut nulls = 0_usize;
            for index in &self.columns {
                let array = batch.column(*index);
                // Un'etichetta davanti a ogni colonna: un `NULL` e un valore
                // non producono gli stessi byte.
                if array.is_null(row) {
                    if self.nulls == NullKeys::Distinct {
                        continue 'rows;
                    }
                    nulls += 1;
                    key.push(0);
                } else {
                    key.push(1);
                    encode(array.as_ref(), row, &mut key)?;
                }
            }
            if self.nulls == NullKeys::EqualUnlessAllNull && nulls == self.columns.len() {
                continue 'rows;
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

/// Un binario senza gli `0x00` finali.
fn trim_zeros(bytes: &[u8]) -> &[u8] {
    let end = bytes
        .iter()
        .rposition(|byte| *byte != 0)
        .map_or(0, |last| last + 1);
    &bytes[..end]
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
        // Gli spazi finali non distinguono due chiavi (vedi «Gli spazi finali
        // delle chiavi» nella documentazione del modulo).
        DataType::Utf8 => push_len(
            out,
            array
                .as_string::<i32>()
                .value(row)
                .trim_end_matches(' ')
                .as_bytes(),
        ),
        DataType::LargeUtf8 => push_len(
            out,
            array
                .as_string::<i64>()
                .value(row)
                .trim_end_matches(' ')
                .as_bytes(),
        ),
        // Gli `0x00` finali non distinguono due chiavi binarie (SQL Server
        // confronta `0x01 = 0x0100` come vero).
        DataType::Binary => push_len(out, trim_zeros(array.as_binary::<i32>().value(row))),
        DataType::LargeBinary => push_len(out, trim_zeros(array.as_binary::<i64>().value(row))),
        DataType::FixedSizeBinary(_) => {
            push_len(out, trim_zeros(array.as_fixed_size_binary().value(row)));
        }
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

/// Le verifiche di [`UniqueInputKeys::new`] sulle chiavi di una mode per
/// chiavi, per il provider che costruisce lo stream della condizione 2 piu
/// avanti, quando ha letto l'indice che le prova.
///
/// # Errors
///
/// Gli errori di [`UniqueInputKeys::new`].
pub fn validate_input_keys(mode: WriteMode, keys: &[String], schema: &SchemaRef) -> Result<()> {
    if keyed_mode(mode) && !keys.is_empty() {
        UniqueInputKeys::new(schema, keys, NullKeys::Equal)?;
    }
    Ok(())
}

/// L'ingresso di una scrittura con la condizione 2 della regola K.
///
/// Per le mode per chiavi ([`keyed_mode`]) lo stream che rifiuta una chiave
/// ripetuta, con i `NULL` confrontati come l'indice che prova le chiavi
/// (`nulls`); per le altre `input` com'e. Ogni provider lo chiama
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
    nulls: NullKeys,
) -> Result<Box<dyn BatchStream>> {
    if !keyed_mode(mode) || keys.is_empty() {
        return Ok(input);
    }
    let keys = UniqueInputKeys::new(&input.schema(), keys, nulls)?;
    Ok(Box::new(UniqueKeyStream {
        inner: input,
        keys,
        budget: budget.clone(),
    }))
}

#[cfg(test)]
#[path = "write_keys_tests.rs"]
mod tests;
