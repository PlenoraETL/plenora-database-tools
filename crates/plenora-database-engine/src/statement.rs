//! Statement nativo immutabile con valori legati per singola esecuzione.

use plenora_database_core::provider::ParameterValue;
use plenora_database_core::transaction::Statement as LegacyStatement;
use plenora_database_core::{DatabaseError, Result};
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::Arc;

struct NativeStatementInner {
    sql: String,
    parameter_count: usize,
    fingerprint: [u8; 32],
}

/// Template SQL riusabile che non contiene valori applicativi.
#[derive(Clone)]
pub struct NativeStatement {
    inner: Arc<NativeStatementInner>,
}

impl NativeStatement {
    /// Costruisce un template dichiarando il numero di parametri posizionali.
    ///
    /// Il testo resta provider-specifico e continua a passare dalla policy
    /// `native_query` della transazione.
    ///
    /// # Errors
    ///
    /// Rifiuta SQL vuoto o contenente NUL.
    pub fn new(sql: impl Into<String>, parameter_count: usize) -> Result<Self> {
        let sql = sql.into();
        if sql.trim().is_empty() || sql.contains('\0') {
            return Err(DatabaseError::invalid_plan(
                "template SQL nativo vuoto o non rappresentabile",
            ));
        }
        let fingerprint = fingerprint(&sql, parameter_count);
        Ok(Self {
            inner: Arc::new(NativeStatementInner {
                sql,
                parameter_count,
                fingerprint,
            }),
        })
    }

    #[must_use]
    pub fn sql(&self) -> &str {
        &self.inner.sql
    }

    #[must_use]
    pub fn parameter_count(&self) -> usize {
        self.inner.parameter_count
    }

    #[must_use]
    pub fn fingerprint(&self) -> [u8; 32] {
        self.inner.fingerprint
    }

    /// Lega valori a una singola esecuzione senza modificare il template.
    ///
    /// # Errors
    ///
    /// Rifiuta un numero di valori diverso da quello dichiarato.
    pub fn bind(&self, parameters: Vec<ParameterValue>) -> Result<BoundStatement> {
        if parameters.len() != self.inner.parameter_count {
            return Err(DatabaseError::invalid_plan(
                "numero di parametri incompatibile con il template SQL",
            ));
        }
        let legacy = LegacyStatement::new(self.inner.sql.clone()).with_params(parameters);
        Ok(BoundStatement {
            template: self.clone(),
            legacy,
        })
    }
}

impl fmt::Debug for NativeStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeStatement")
            .field("parameter_count", &self.parameter_count())
            .field("fingerprint", &self.fingerprint())
            .finish_non_exhaustive()
    }
}

/// Template e valori di una singola esecuzione, entrambi immutabili.
pub struct BoundStatement {
    template: NativeStatement,
    legacy: LegacyStatement,
}

impl BoundStatement {
    #[must_use]
    pub const fn template(&self) -> &NativeStatement {
        &self.template
    }

    #[must_use]
    pub fn parameters(&self) -> &[ParameterValue] {
        &self.legacy.params
    }

    pub(crate) const fn legacy(&self) -> &LegacyStatement {
        &self.legacy
    }
}

impl fmt::Debug for BoundStatement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundStatement")
            .field("parameter_count", &self.parameters().len())
            .field("fingerprint", &self.template.fingerprint())
            .finish_non_exhaustive()
    }
}

fn fingerprint(sql: &str, parameter_count: usize) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"plenora.native-statement.v1\0");
    digest.update((sql.len() as u128).to_le_bytes());
    digest.update(sql.as_bytes());
    digest.update((parameter_count as u128).to_le_bytes());
    digest.finalize().into()
}

#[cfg(test)]
#[path = "statement_tests.rs"]
mod tests;
