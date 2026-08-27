//! Primitive runtime condivise dagli adapter.
//!
//! Questo modulo contiene soltanto invarianti provider-neutral: propagazione
//! della deadline del budget, ownership del relativo task e lease del
//! contratto. Classificazione degli errori, cancellazione remota e lifecycle
//! della sessione restano responsabilita del singolo adapter.

use plenora_database_core::arrow::array::{Array, BinaryArray};
use plenora_database_core::ewkb::{inspect_ewkb_detailed, EwkbInspection};
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLease};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};

/// Token figlio limitato dalla deadline del budget.
///
/// Il task viene abortito al `Drop`, oppure trasferito allo stream con
/// [`Self::take_deadline_task`]. In questo modo nessuna operazione lascia un
/// timer orfano e tutti gli adapter applicano la stessa semantica temporale.
pub struct DeadlineGuard {
    token: CancellationToken,
    deadline_task: Option<tokio::task::JoinHandle<()>>,
}

impl DeadlineGuard {
    /// Costruisce il token dopo aver verificato che il budget sia ancora
    /// attivo.
    ///
    /// # Errors
    ///
    /// `ResourceLimit` se la deadline del budget e gia trascorsa.
    pub fn new(parent: &CancellationToken, budget: &ResourceBudget) -> Result<Self> {
        budget.ensure_active()?;
        let token = parent.child_token_with_deadline(Some(budget.deadline()));
        let deadline_token = token.clone();
        let deadline = tokio::time::Instant::from_std(budget.deadline());
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| runtime_error("runtime async non disponibile per la deadline"))?;
        let deadline_task = handle.spawn(async move {
            tokio::time::sleep_until(deadline).await;
            deadline_token.cancel_due_to_deadline();
        });
        Ok(Self {
            token,
            deadline_task: Some(deadline_task),
        })
    }

    #[must_use]
    pub const fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Trasferisce allo stream l'ownership del timer.
    ///
    /// # Errors
    ///
    /// `Internal` se il task era gia stato trasferito.
    pub fn take_deadline_task(&mut self) -> Result<tokio::task::JoinHandle<()>> {
        self.deadline_task
            .take()
            .ok_or_else(|| runtime_error("task deadline gia trasferito"))
    }
}

fn runtime_error(message: &'static str) -> DatabaseError {
    DatabaseError::new(ErrorCategory::Internal, ErrorPhase::Prepare, None, message)
}

impl Drop for DeadlineGuard {
    fn drop(&mut self) {
        if let Some(task) = self.deadline_task.take() {
            task.abort();
        }
    }
}

/// Lease che accompagnano per intero un'operazione tabellare.
pub struct ContractLeases {
    operation: ResourceLease,
    columns: ResourceLease,
}

impl ContractLeases {
    /// Riserva una operazione concorrente e il numero dichiarato di colonne.
    ///
    /// # Errors
    ///
    /// `ResourceLimit` per un numero non rappresentabile, nullo o oltre il
    /// budget residuo.
    pub fn acquire(budget: &ResourceBudget, columns: usize) -> Result<Self> {
        let columns = u64::try_from(columns)
            .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?;
        let operation = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
        let columns = budget.try_lease(ResourceKind::Columns, columns)?;
        Ok(Self { operation, columns })
    }

    #[must_use]
    pub fn into_parts(self) -> (ResourceLease, ResourceLease) {
        (self.operation, self.columns)
    }
}

/// Impedisce di sostituire il budget fra prepare ed execute.
///
/// # Errors
///
/// `InvalidPlan` quando i due handle non condividono gli stessi contatori.
pub fn validate_prepared_budget(
    prepared: &ResourceBudget,
    execution: &ResourceBudget,
) -> Result<()> {
    if prepared.is_same_budget(execution) {
        Ok(())
    } else {
        Err(DatabaseError::invalid_plan(
            "il budget di write non coincide con quello usato in prepare_write",
        ))
    }
}

/// Quota atomica per un batch Arrow di lettura.
///
/// I provider decidono se il piano contiene colonne spatial; contabilita e
/// limiti restano identici per tutti i protocolli.
#[derive(Debug)]
pub struct ReadBatchReservation {
    rows_lease: ResourceLease,
    memory_lease: ResourceLease,
    output_lease: ResourceLease,
    geometry_lease: Option<ResourceLease>,
    pub row_limit: usize,
    pub byte_limit: u64,
    pub component_limit: u64,
}

impl ReadBatchReservation {
    /// Prenota il massimo batch consentito dal budget residuo.
    ///
    /// `max_bytes` applica un tetto provider/configurazione prima della
    /// prenotazione. Un batch spatial richiede anche quota geometrica.
    ///
    /// # Errors
    ///
    /// `ResourceLimit` quando una delle quote necessarie e esaurita.
    pub fn acquire(
        budget: &ResourceBudget,
        batch_rows: usize,
        max_bytes: Option<u64>,
        has_spatial: bool,
    ) -> Result<Self> {
        let rows = budget
            .remaining(ResourceKind::Rows)
            .min(u64::try_from(batch_rows).unwrap_or(u64::MAX));
        let mut bytes = budget
            .remaining(ResourceKind::MemoryBytes)
            .min(budget.remaining(ResourceKind::OutputBytes));
        if let Some(max_bytes) = max_bytes {
            bytes = bytes.min(max_bytes);
        }
        if rows == 0 || bytes == 0 {
            return Err(DatabaseError::resource_limit("budget read esaurito"));
        }
        let component_limit = if has_spatial {
            budget.remaining(ResourceKind::GeometryComponents)
        } else {
            0
        };
        if has_spatial && component_limit == 0 {
            return Err(DatabaseError::resource_limit(
                "budget componenti geometriche esaurito",
            ));
        }
        Ok(Self {
            rows_lease: budget.try_lease(ResourceKind::Rows, rows)?,
            memory_lease: budget.try_lease(ResourceKind::MemoryBytes, bytes)?,
            output_lease: budget.try_lease(ResourceKind::OutputBytes, bytes)?,
            geometry_lease: has_spatial
                .then(|| budget.try_lease(ResourceKind::GeometryComponents, component_limit))
                .transpose()?,
            row_limit: usize::try_from(rows).unwrap_or(usize::MAX),
            byte_limit: bytes,
            component_limit,
        })
    }

    /// Consuma la quota realmente pubblicata e restituisce quella inutilizzata.
    ///
    /// # Errors
    ///
    /// `ResourceLimit` per byte fuori prenotazione o componenti senza lease.
    pub fn commit(self, rows: u64, bytes: u64, geometry_components: u64) -> Result<()> {
        if bytes == 0 || bytes > self.byte_limit {
            return Err(DatabaseError::resource_limit(
                "batch Arrow oltre il budget memoria/output",
            ));
        }
        self.rows_lease.commit(rows)?;
        self.memory_lease.commit(bytes)?;
        self.output_lease.commit(bytes)?;
        if geometry_components > 0 {
            self.geometry_lease
                .ok_or_else(|| DatabaseError::resource_limit("budget geometrico assente"))?
                .commit(geometry_components)?;
        }
        Ok(())
    }
}

/// Quota per righe Arrow in ingresso a una write.
pub struct WriteResourceReservation {
    pub rows: u64,
    rows_lease: Option<ResourceLease>,
    output_lease: Option<ResourceLease>,
    memory_lease: Option<ResourceLease>,
    output_bytes: u64,
    geometry_components: u64,
    geometry_lease: Option<ResourceLease>,
}

impl WriteResourceReservation {
    /// Prenota contabilita di input e memoria temporanea del codec.
    ///
    /// # Errors
    ///
    /// `ResourceLimit` quando una quota e insufficiente.
    pub fn acquire(
        budget: &ResourceBudget,
        rows: u64,
        output_bytes: u64,
        memory_bytes: u64,
        geometry_components: u64,
    ) -> Result<Self> {
        budget.ensure_active()?;
        if rows == 0 {
            return Ok(Self {
                rows,
                rows_lease: None,
                output_lease: None,
                memory_lease: None,
                output_bytes: 0,
                geometry_components: 0,
                geometry_lease: None,
            });
        }
        if output_bytes > budget.remaining(ResourceKind::OutputBytes)
            || memory_bytes > budget.remaining(ResourceKind::MemoryBytes)
            || geometry_components > budget.remaining(ResourceKind::GeometryComponents)
        {
            return Err(DatabaseError::resource_limit(
                "batch Arrow oltre il budget write",
            ));
        }
        Ok(Self {
            rows,
            rows_lease: Some(budget.try_lease(ResourceKind::Rows, rows)?),
            output_lease: (output_bytes > 0)
                .then(|| budget.try_lease(ResourceKind::OutputBytes, output_bytes))
                .transpose()?,
            memory_lease: (memory_bytes > 0)
                .then(|| budget.try_lease(ResourceKind::MemoryBytes, memory_bytes))
                .transpose()?,
            output_bytes,
            geometry_components,
            geometry_lease: (geometry_components > 0)
                .then(|| budget.try_lease(ResourceKind::GeometryComponents, geometry_components))
                .transpose()?,
        })
    }

    /// Consuma righe, output e geometrie; la memoria temporanea viene liberata.
    ///
    /// # Errors
    ///
    /// Propaga una violazione dei contatori condivisi.
    pub fn commit(self) -> Result<()> {
        let Some(rows) = self.rows_lease else {
            return Ok(());
        };
        rows.commit(self.rows)?;
        if let Some(output) = self.output_lease {
            output.commit(self.output_bytes)?;
        }
        if let Some(memory) = self.memory_lease {
            memory.release()?;
        }
        if let Some(geometry) = self.geometry_lease {
            geometry.commit(self.geometry_components)?;
        }
        Ok(())
    }
}

/// Verifica celle WKB gia decodificate dai driver e somma i componenti.
///
/// Il caller seleziona le colonne spatial e applica l'eventuale contratto
/// provider-specifico all'ispezione; limiti di cella, profondita, overflow e
/// attribuzione riga/colonna sono condivisi.
///
/// # Errors
///
/// `ResourceLimit`, errore EWKB attribuito dal caller o rifiuto del contratto
/// specifico restituito da `validate`.
pub fn inspect_spatial_arrays<'a, I, M, V>(
    arrays: I,
    component_limit: u64,
    cell_limit: u64,
    nesting_depth: u64,
    mut map_error: M,
    mut validate: V,
) -> Result<u64>
where
    I: IntoIterator<Item = (usize, &'a BinaryArray)>,
    M: FnMut(DatabaseError, usize, usize) -> DatabaseError,
    V: FnMut(&EwkbInspection, usize, usize) -> Result<()>,
{
    let mut components = 0_u64;
    for (column, array) in arrays {
        for row in 0..array.len() {
            if array.is_null(row) {
                continue;
            }
            let value = array.value(row);
            if u64::try_from(value.len()).unwrap_or(u64::MAX) > cell_limit {
                return Err(DatabaseError::resource_limit("WKB oltre il limite cella"));
            }
            let remaining = component_limit.checked_sub(components).ok_or_else(|| {
                DatabaseError::resource_limit("budget componenti geometriche esaurito")
            })?;
            if remaining == 0 {
                return Err(DatabaseError::resource_limit(
                    "budget componenti geometriche esaurito",
                ));
            }
            let inspection = inspect_ewkb_detailed(value, remaining, nesting_depth)
                .map_err(|error| map_error(error, row, column))?;
            validate(&inspection, row, column)?;
            components = components
                .checked_add(inspection.stats.components)
                .ok_or_else(|| DatabaseError::resource_limit("componenti geometriche overflow"))?;
        }
    }
    Ok(components)
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
