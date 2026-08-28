//! Risultato bufferizzato uniforme sopra le righe canoniche.

use plenora_database_core::provider::ParameterValue;
use plenora_database_core::{
    at_most_one_row, exactly_one_row, exactly_one_value, ColumnDescriptor, DatabaseError,
    ErrorCategory, ErrorPhase, Result, Row,
};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Risultato one-shot con metadata disponibili prima del consumo.
#[derive(Debug, PartialEq)]
pub struct QueryResult {
    columns: Option<Arc<[String]>>,
    descriptors: Option<Arc<[ColumnDescriptor]>>,
    rows: Vec<Row>,
}

impl QueryResult {
    /// Costruisce un risultato e verifica che tutte le righe abbiano lo
    /// stesso schema. Un insieme vuoto non dichiara colonne.
    ///
    /// # Errors
    ///
    /// `DataMapping` se le righe non condividono gli stessi metadata.
    pub fn from_rows(rows: Vec<Row>) -> Result<Self> {
        let columns = rows.first().map(Row::shared_columns);
        Self::with_optional_columns(columns, rows)
    }

    /// Costruisce anche un risultato vuoto con colonne osservate dal driver.
    ///
    /// # Errors
    ///
    /// `DataMapping` se una riga non corrisponde alle colonne dichiarate.
    pub fn with_columns(columns: Arc<[String]>, rows: Vec<Row>) -> Result<Self> {
        Self::with_optional_columns(Some(columns), rows)
    }

    fn with_optional_columns(columns: Option<Arc<[String]>>, rows: Vec<Row>) -> Result<Self> {
        if let Some(expected) = &columns {
            if rows.iter().any(|row| row.columns() != expected.as_ref()) {
                return Err(DatabaseError::new(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Read,
                    None,
                    "result set con metadata di colonna incoerenti",
                ));
            }
        }
        let descriptors = columns.as_ref().map(|columns| {
            columns
                .iter()
                .enumerate()
                .map(|(index, name)| ColumnDescriptor::new(index, name.clone()))
                .collect::<Vec<_>>()
                .into()
        });
        Ok(Self {
            columns,
            descriptors,
            rows,
        })
    }

    #[must_use]
    pub fn columns(&self) -> Option<&[String]> {
        self.columns.as_deref()
    }

    #[must_use]
    pub fn column_descriptors(&self) -> Option<&[ColumnDescriptor]> {
        self.descriptors.as_deref()
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.rows.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    #[must_use]
    pub fn all(self) -> Vec<Row> {
        self.rows
    }

    #[must_use]
    pub fn first(self) -> Option<Row> {
        self.rows.into_iter().next()
    }

    /// # Errors
    ///
    /// `NotFound` per zero righe, `Conflict` per piu di una.
    pub fn one(self) -> Result<Row> {
        exactly_one_row(self.rows)
    }

    /// # Errors
    ///
    /// `Conflict` per piu di una riga.
    pub fn one_or_none(self) -> Result<Option<Row>> {
        at_most_one_row(self.rows)
    }

    /// Estrae zero o una cella senza scartare righe o colonne eccedenti.
    ///
    /// # Errors
    ///
    /// `Conflict` per piu righe, `DataMapping` per ampiezza diversa da uno.
    pub fn scalar(self) -> Result<Option<ParameterValue>> {
        at_most_one_row(self.rows)?.map_or(Ok(None), |row| exactly_one_value(row).map(Some))
    }

    /// # Errors
    ///
    /// Richiede esattamente una riga e una colonna.
    pub fn scalar_one(self) -> Result<ParameterValue> {
        exactly_one_value(exactly_one_row(self.rows)?)
    }

    #[must_use]
    pub fn into_mappings(self) -> Vec<BTreeMap<String, ParameterValue>> {
        self.rows
            .into_iter()
            .map(|row| {
                let columns = row.shared_columns();
                columns.iter().cloned().zip(row.into_values()).collect()
            })
            .collect()
    }
}

impl IntoIterator for QueryResult {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

#[cfg(test)]
#[path = "result_tests.rs"]
mod tests;
