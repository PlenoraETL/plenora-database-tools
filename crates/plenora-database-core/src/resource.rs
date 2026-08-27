//! Budget condivisi e lease atomiche per le risorse consumate dalle operazioni.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    MemoryBytes,
    Rows,
    Columns,
    GeometryComponents,
    ConcurrentOperations,
    OutputBytes,
    SpillBytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLimits {
    pub memory_bytes: u64,
    pub rows: u64,
    pub columns: u64,
    pub geometry_components: u64,
    pub nesting_depth: u64,
    pub concurrent_operations: u64,
    pub output_bytes: u64,
    pub duration_ms: u64,
    pub spill_bytes: u64,
    pub cell_bytes: u64,
    pub decompression_ratio: u64,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            memory_bytes: 512 * 1024 * 1024,
            rows: u64::MAX,
            columns: 65_536,
            geometry_components: 16_777_216,
            nesting_depth: 64,
            concurrent_operations: 64,
            output_bytes: u64::MAX,
            duration_ms: 30_000,
            spill_bytes: 4 * 1024 * 1024 * 1024,
            cell_bytes: 64 * 1024 * 1024,
            decompression_ratio: 1_000,
        }
    }
}

impl ResourceLimits {
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` quando un limite è zero.
    pub fn validate(&self) -> crate::Result<()> {
        if self.memory_bytes == 0
            || self.rows == 0
            || self.columns == 0
            || self.geometry_components == 0
            || self.nesting_depth == 0
            || self.concurrent_operations == 0
            || self.output_bytes == 0
            || self.duration_ms == 0
            || self.spill_bytes == 0
            || self.cell_bytes == 0
            || self.decompression_ratio == 0
        {
            return Err(crate::DatabaseError::invalid_plan(
                "i limiti di risorsa devono essere maggiori di zero",
            ));
        }
        if self.cell_bytes > self.memory_bytes {
            return Err(crate::DatabaseError::invalid_plan(
                "cell_bytes supera il budget di memoria",
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Counters {
    memory_bytes: AtomicU64,
    rows: AtomicU64,
    columns: AtomicU64,
    geometry_components: AtomicU64,
    concurrent_operations: AtomicU64,
    output_bytes: AtomicU64,
    spill_bytes: AtomicU64,
}

impl Counters {
    const fn new(limits: &ResourceLimits) -> Self {
        Self {
            memory_bytes: AtomicU64::new(limits.memory_bytes),
            rows: AtomicU64::new(limits.rows),
            columns: AtomicU64::new(limits.columns),
            geometry_components: AtomicU64::new(limits.geometry_components),
            concurrent_operations: AtomicU64::new(limits.concurrent_operations),
            output_bytes: AtomicU64::new(limits.output_bytes),
            spill_bytes: AtomicU64::new(limits.spill_bytes),
        }
    }

    const fn get(&self, kind: ResourceKind) -> &AtomicU64 {
        match kind {
            ResourceKind::MemoryBytes => &self.memory_bytes,
            ResourceKind::Rows => &self.rows,
            ResourceKind::Columns => &self.columns,
            ResourceKind::GeometryComponents => &self.geometry_components,
            ResourceKind::ConcurrentOperations => &self.concurrent_operations,
            ResourceKind::OutputBytes => &self.output_bytes,
            ResourceKind::SpillBytes => &self.spill_bytes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResourceBudget {
    limits: Arc<ResourceLimits>,
    counters: Arc<Counters>,
    deadline: Instant,
}

impl ResourceBudget {
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` per limiti incoerenti.
    pub fn new(limits: ResourceLimits) -> crate::Result<Self> {
        limits.validate()?;
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(limits.duration_ms))
            .ok_or_else(|| crate::DatabaseError::invalid_plan("deadline risorse oltre Instant"))?;
        let counters = Counters::new(&limits);
        Ok(Self {
            limits: Arc::new(limits),
            counters: Arc::new(counters),
            deadline,
        })
    }

    #[must_use]
    pub fn limits(&self) -> &ResourceLimits {
        &self.limits
    }

    #[must_use]
    pub fn remaining(&self, kind: ResourceKind) -> u64 {
        self.counters.get(kind).load(Ordering::Acquire)
    }

    #[must_use]
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    #[must_use]
    pub fn remaining_duration(&self) -> Option<Duration> {
        self.deadline.checked_duration_since(Instant::now())
    }

    /// # Errors
    ///
    /// Restituisce `ResourceLimit` quando la durata del budget è scaduta.
    pub fn ensure_active(&self) -> crate::Result<()> {
        if self.remaining_duration().is_none() {
            Err(crate::DatabaseError::resource_limit(
                "durata del budget di risorse esaurita",
            ))
        } else {
            Ok(())
        }
    }

    /// Restituisce `true` soltanto quando i due handle condividono gli stessi
    /// contatori. È usato per impedire la sostituzione del budget tra prepare
    /// ed execute.
    #[must_use]
    pub fn is_same_budget(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.counters, &other.counters)
    }

    /// Riserva una quota che viene restituita automaticamente al `Drop`.
    ///
    /// # Errors
    ///
    /// Restituisce `ResourceLimit` prima di qualsiasi allocazione se la quota
    /// è zero o supera il residuo.
    pub fn try_lease(&self, kind: ResourceKind, amount: u64) -> crate::Result<ResourceLease> {
        if amount == 0 {
            return Err(crate::DatabaseError::resource_limit(
                "una lease di risorsa deve essere maggiore di zero",
            ));
        }
        let counter = self.counters.get(kind);
        let mut current = counter.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_sub(amount) else {
                return Err(crate::DatabaseError::resource_limit(
                    "budget di risorsa esaurito",
                ));
            };
            match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Ok(ResourceLease {
                        budget: self.clone(),
                        kind,
                        amount,
                        released: false,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn release(&self, kind: ResourceKind, amount: u64) -> crate::Result<()> {
        let counter = self.counters.get(kind);
        let maximum = maximum_for(&self.limits, kind);
        let mut current = counter.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(amount) else {
                return Err(crate::DatabaseError::resource_limit(
                    "overflow durante la restituzione del budget",
                ));
            };
            if next > maximum {
                return Err(crate::DatabaseError::resource_limit(
                    "restituzione superiore alla quota originaria",
                ));
            }
            match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }
}

const fn maximum_for(limits: &ResourceLimits, kind: ResourceKind) -> u64 {
    match kind {
        ResourceKind::MemoryBytes => limits.memory_bytes,
        ResourceKind::Rows => limits.rows,
        ResourceKind::Columns => limits.columns,
        ResourceKind::GeometryComponents => limits.geometry_components,
        ResourceKind::ConcurrentOperations => limits.concurrent_operations,
        ResourceKind::OutputBytes => limits.output_bytes,
        ResourceKind::SpillBytes => limits.spill_bytes,
    }
}

#[derive(Debug)]
pub struct ResourceLease {
    budget: ResourceBudget,
    kind: ResourceKind,
    amount: u64,
    released: bool,
}

impl ResourceLease {
    #[must_use]
    pub const fn amount(&self) -> u64 {
        self.amount
    }

    /// Trasforma una parte della prenotazione in consumo definitivo e
    /// restituisce al budget la quota inutilizzata.
    ///
    /// # Errors
    ///
    /// Restituisce `ResourceLimit` se `used` è zero, supera la prenotazione o
    /// se la contabilita interna non puo essere ripristinata.
    pub fn commit(mut self, used: u64) -> crate::Result<()> {
        if used == 0 || used > self.amount {
            return Err(crate::DatabaseError::resource_limit(
                "consumo non valido per la lease di risorsa",
            ));
        }
        let unused = self.amount - used;
        if unused > 0 {
            self.budget.release(self.kind, unused)?;
        }
        // La parte `used` resta sottratta: è consumo cumulativo, non memoria
        // temporanea da restituire al Drop.
        self.released = true;
        Ok(())
    }

    /// Restituisce esplicitamente la quota e rende osservabile un'eventuale
    /// violazione dell'invariante interna.
    ///
    /// # Errors
    ///
    /// Restituisce `ResourceLimit` se la contabilità eccederebbe il massimo.
    pub fn release(mut self) -> crate::Result<()> {
        self.budget.release(self.kind, self.amount)?;
        self.released = true;
        Ok(())
    }
}

impl Drop for ResourceLease {
    fn drop(&mut self) {
        if !self.released && self.budget.release(self.kind, self.amount).is_ok() {
            self.released = true;
        }
    }
}

#[cfg(test)]
#[path = "resource_tests.rs"]
mod tests;
