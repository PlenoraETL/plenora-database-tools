//! Row provider-neutral.
//!
//! Tuple di valori canonici accompagnata dai nomi delle colonne.
//! Sostituisce il precedente `Vec<ParameterValue>` grezzo esposto dalle
//! facade OLTP e dal cursor stream, così il consumer può accedere ai
//! campi per nome (`row["id"]`) invece che per posizione.

use crate::provider::ParameterValue;
use std::ops::Index;
use std::sync::Arc;

/// Riga tipizzata restituita dalla facade OLTP.
///
/// I nomi delle colonne sono condivisi tramite `Arc<[String]>` fra tutte
/// le righe di uno stesso batch/stream (evita allocazioni per riga).
///
/// **Non implementa `Serialize`/`Deserialize`**: la condivisione via `Arc`
/// non è supportata nativamente da serde senza la feature `rc`. I consumer
/// che vogliono serializzare devono passare per `columns()` + `values()`.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    columns: Arc<[String]>,
    values: Vec<ParameterValue>,
}

impl Row {
    /// Costruisce una `Row`. Il chiamante è responsabile di garantire che
    /// `columns.len() == values.len()`; violazioni sono un bug del driver.
    #[must_use]
    pub fn new(columns: Arc<[String]>, values: Vec<ParameterValue>) -> Self {
        debug_assert_eq!(
            columns.len(),
            values.len(),
            "Row: mismatch tra nomi colonna e valori"
        );
        Self { columns, values }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.values.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    #[must_use]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    #[must_use]
    pub fn values(&self) -> &[ParameterValue] {
        &self.values
    }

    /// Ritorna il valore per nome colonna. Case-sensitive.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&ParameterValue> {
        self.columns
            .iter()
            .position(|c| c == name)
            .and_then(|i| self.values.get(i))
    }

    /// Ritorna il valore per posizione (0-based).
    #[must_use]
    pub fn get_index(&self, index: usize) -> Option<&ParameterValue> {
        self.values.get(index)
    }

    /// Consuma la riga e restituisce solo i valori. Utile ai consumer che
    /// non hanno bisogno dei nomi (es. facade scalar).
    #[must_use]
    pub fn into_values(self) -> Vec<ParameterValue> {
        self.values
    }
}

impl Index<&str> for Row {
    type Output = ParameterValue;

    fn index(&self, name: &str) -> &ParameterValue {
        self.get(name).unwrap_or_else(|| {
            panic!(
                "colonna `{name}` non presente in Row (colonne: {:?})",
                self.columns
            )
        })
    }
}

impl Index<usize> for Row {
    type Output = ParameterValue;

    fn index(&self, index: usize) -> &ParameterValue {
        &self.values[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_row() -> Row {
        Row::new(
            Arc::from(vec!["id".to_owned(), "name".to_owned()]),
            vec![
                ParameterValue::I32(42),
                ParameterValue::String("plenora".to_owned()),
            ],
        )
    }

    #[test]
    fn row_len_matches_values() {
        assert_eq!(sample_row().len(), 2);
        assert!(!sample_row().is_empty());
    }

    #[test]
    fn get_by_name_returns_the_value() {
        let row = sample_row();
        assert!(matches!(row.get("id"), Some(ParameterValue::I32(42))));
        assert!(matches!(
            row.get("name"),
            Some(ParameterValue::String(s)) if s == "plenora"
        ));
        assert!(row.get("missing").is_none());
    }

    #[test]
    fn get_by_index_returns_the_value() {
        let row = sample_row();
        assert!(matches!(row.get_index(0), Some(ParameterValue::I32(42))));
        assert!(matches!(row.get_index(1), Some(ParameterValue::String(_))));
        assert!(row.get_index(2).is_none());
    }

    #[test]
    fn index_by_name_shortcut_works() {
        let row = sample_row();
        assert!(matches!(&row["id"], ParameterValue::I32(42)));
    }

    #[test]
    fn index_by_position_shortcut_works() {
        let row = sample_row();
        assert!(matches!(&row[0], ParameterValue::I32(42)));
    }

    #[test]
    #[should_panic(expected = "colonna `absent`")]
    fn index_by_name_panics_when_missing() {
        let row = sample_row();
        let _ = &row["absent"];
    }

    #[test]
    fn into_values_consumes_and_returns_values() {
        let row = sample_row();
        let values = row.into_values();
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn shared_columns_avoid_per_row_allocations() {
        let columns: Arc<[String]> = Arc::from(vec!["a".to_owned(), "b".to_owned()]);
        let row1 = Row::new(
            Arc::clone(&columns),
            vec![ParameterValue::I32(1), ParameterValue::I32(2)],
        );
        let row2 = Row::new(
            Arc::clone(&columns),
            vec![ParameterValue::I32(3), ParameterValue::I32(4)],
        );
        assert!(Arc::ptr_eq(&row1.columns, &row2.columns));
    }

    #[test]
    fn columns_and_values_are_accessible_for_manual_serialization() {
        let row = sample_row();
        let json = serde_json::json!({
            "columns": row.columns(),
            "values": row.values(),
        });
        let text = serde_json::to_string(&json).expect("serialize");
        assert!(text.contains("id"));
        assert!(text.contains("plenora"));
    }
}
