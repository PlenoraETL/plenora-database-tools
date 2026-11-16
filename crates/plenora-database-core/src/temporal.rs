//! Aritmetica dei limiti temporali operativi. Il dominio comune e un giorno.
use crate::{DatabaseError, Result};
use std::time::{Duration, Instant};

/// Orizzonte operativo massimo, uguale al limite del retry sul protocollo.
pub const MAX_OPERATIONAL_MS: u64 = 86_400_000;

/// Valida un timeout prima di passarlo a timer, pool o socket.
/// # Errors
/// Restituisce `InvalidPlan` per zero, fuori dominio o deadline non rappresentabile.
pub fn timeout(value: Duration) -> Result<Duration> {
    if value.is_zero() || value > Duration::from_millis(MAX_OPERATIONAL_MS) {
        return Err(DatabaseError::invalid_plan(
            "timeout fuori dominio operativo",
        ));
    }
    deadline(value)?;
    Ok(value)
}

/// Converte millisecondi di timeout senza perdita.
/// # Errors
/// Restituisce `InvalidPlan` fuori dominio.
pub fn timeout_ms(value: u64) -> Result<Duration> {
    timeout(Duration::from_millis(value))
}

/// Converte secondi di keepalive senza perdita.
/// # Errors
/// Restituisce `InvalidPlan` fuori dominio.
pub fn keepalive_seconds(value: u64) -> Result<Duration> {
    timeout(Duration::from_secs(value))
}

/// Il retry ammette anche zero; il cap della policy viene applicato prima.
/// # Errors
/// Restituisce `InvalidPlan` fuori dominio o se il clock non puo rappresentarlo.
pub fn retry_ms(value: u64) -> Result<Duration> {
    if value > MAX_OPERATIONAL_MS {
        return Err(DatabaseError::invalid_plan("retry fuori dominio operativo"));
    }
    let duration = Duration::from_millis(value);
    deadline(duration)?;
    Ok(duration)
}

/// Costruisce una deadline senza overflow, inclusa la scadenza immediata.
/// # Errors
/// Restituisce `InvalidPlan` se il limite o la somma non sono rappresentabili.
pub fn deadline(value: Duration) -> Result<Instant> {
    if value > Duration::from_millis(MAX_OPERATIONAL_MS) {
        return Err(DatabaseError::invalid_plan(
            "deadline fuori dominio operativo",
        ));
    }
    Instant::now()
        .checked_add(value)
        .ok_or_else(|| DatabaseError::invalid_plan("deadline non rappresentabile"))
}

/// Timeout ODBC: secondi interi, assenza distinta da conversione fallita.
/// # Errors
/// Restituisce `InvalidPlan` per precisione, dominio o target non rappresentabili.
pub fn odbc_timeout_ms(value: Option<u64>) -> Result<Option<usize>> {
    value
        .map(|ms| {
            timeout_ms(ms)?;
            if !ms.is_multiple_of(1000) {
                return Err(DatabaseError::invalid_plan(
                    "timeout ODBC richiede secondi interi",
                ));
            }
            usize::try_from(ms / 1000)
                .map_err(|_| DatabaseError::invalid_plan("timeout ODBC non rappresentabile"))
        })
        .transpose()
}

#[cfg(test)]
#[path = "temporal_tests.rs"]
mod tests;
