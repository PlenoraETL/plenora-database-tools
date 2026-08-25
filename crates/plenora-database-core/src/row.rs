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
    /// Costruisce una `Row` verificando che nomi e valori si corrispondano.
    ///
    /// Prima la parità era un `debug_assert_eq!`, cioè un controllo che
    /// spariva in release: una riga malformata prodotta da un driver veniva
    /// accettata in produzione e falliva più tardi, altrove — su un accesso
    /// per nome che non trovava il valore, o su un indice fuori dai valori.
    /// Il posto giusto per accorgersene è qui, dove si sa ancora quale driver
    /// l'ha costruita.
    ///
    /// Sostituisce `Row::new`, che era infallibile e non poteva quindi
    /// segnalare niente. Non e stata affiancata: un costruttore che accetta
    /// una riga malformata resta un modo per costruirne una, e i sette
    /// chiamanti stanno tutti in questo workspace. Il crate e `publish =
    /// false` e nessun altro repository lo referenzia per path, quindi non
    /// esistono chiamanti esterni da rompere; e la major di cui parla la
    /// regola 2 di AGENTS.md e quella del contratto in `contracts/v2/`, che
    /// questa firma non tocca.
    ///
    /// # Errors
    ///
    /// `DataMapping` se `columns.len() != values.len()`. Il messaggio riporta
    /// i due conteggi e nessun nome: i nomi di colonna sono identificatori
    /// dello schema remoto, e un errore pubblico non li trasporta.
    pub fn try_new(columns: Arc<[String]>, values: Vec<ParameterValue>) -> crate::Result<Self> {
        if columns.len() != values.len() {
            return Err(crate::DatabaseError {
                category: crate::ErrorCategory::DataMapping,
                phase: crate::ErrorPhase::Read,
                remote_effect: crate::RemoteEffect::None,
                retry: crate::RetryDisposition::Never,
                provider: None,
                execution_id: None,
                message: format!(
                    "riga malformata: {} nomi di colonna e {} valori",
                    columns.len(),
                    values.len()
                ),
                diagnostics: None,
            });
        }
        Ok(Self { columns, values })
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

/// Accesso per nome, comodo ma panicante.
///
/// Il messaggio non elenca piu le colonne presenti: `Row` trasporta i nomi
/// dello schema remoto, e un panico finisce nei log come qualsiasi altro
/// output: quell'elenco era un inventario dello schema su un percorso che
/// nessuno redige. Chi deve sapere quali colonne ci sono ha [`Row::columns`];
/// chi non vuole panicare ha [`Row::get`].
impl Index<&str> for Row {
    type Output = ParameterValue;

    fn index(&self, name: &str) -> &ParameterValue {
        self.get(name).unwrap_or_else(|| {
            panic!(
                "accesso a una colonna non presente in una Row di {} colonne \
                 (usare Row::get per un accesso fallibile)",
                self.columns.len()
            )
        })
    }
}

/// Accesso posizionale, comodo ma panicante. Vedi [`Row::get_index`] per la
/// variante fallibile.
impl Index<usize> for Row {
    type Output = ParameterValue;

    fn index(&self, index: usize) -> &ParameterValue {
        self.values.get(index).unwrap_or_else(|| {
            panic!(
                "indice {index} fuori da una Row di {} valori \
                 (usare Row::get_index per un accesso fallibile)",
                self.values.len()
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_row() -> Row {
        Row::try_new(
            Arc::from(vec!["id".to_owned(), "name".to_owned()]),
            vec![
                ParameterValue::I32(42),
                ParameterValue::String("plenora".to_owned()),
            ],
        )
        .expect("fixture coerente")
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

    /// Il panico dice che la colonna manca, non quali ci sono.
    ///
    /// Il messaggio elencava `self.columns`: un panico raggiungibile da un
    /// nome sbagliato pubblicava l'intero schema della riga nei log.
    #[test]
    #[should_panic(expected = "colonna non presente in una Row di 2 colonne")]
    fn index_by_name_panics_when_missing() {
        let row = sample_row();
        let _ = &row["absent"];
    }

    #[test]
    fn index_by_position_panics_out_of_range() {
        let row = sample_row();
        let panicked = std::panic::catch_unwind(move || {
            let _ = &row[7];
        });
        assert!(panicked.is_err());
    }

    /// La parita fra nomi e valori non e piu un `debug_assert`: vale anche in
    /// release, che e dove i driver girano.
    #[test]
    fn a_row_whose_names_and_values_disagree_is_rejected() {
        let error = Row::try_new(
            Arc::from(vec!["id".to_owned(), "name".to_owned()]),
            vec![ParameterValue::I32(42)],
        )
        .expect_err("2 nomi e 1 valore");
        assert_eq!(error.category, crate::ErrorCategory::DataMapping);
        // Conteggi si, nomi no.
        assert!(error.message.contains('2'), "{}", error.message);
        assert!(!error.message.contains("name"), "{}", error.message);
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
        let row1 = Row::try_new(
            Arc::clone(&columns),
            vec![ParameterValue::I32(1), ParameterValue::I32(2)],
        )
        .expect("fixture coerente");
        let row2 = Row::try_new(
            Arc::clone(&columns),
            vec![ParameterValue::I32(3), ParameterValue::I32(4)],
        )
        .expect("fixture coerente");
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
