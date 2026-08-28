//! Row provider-neutral.
//!
//! Tuple di valori canonici accompagnata dai nomi delle colonne, così il
//! consumer può accedere ai campi per nome (`row["id"]`) o per posizione.

use crate::provider::ParameterValue;
use std::ops::Index;
use std::sync::Arc;

/// Identita stabile di una colonna dentro uno schema risultato.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDescriptor {
    index: usize,
    name: String,
}

impl ColumnDescriptor {
    #[must_use]
    pub const fn new(index: usize, name: String) -> Self {
        Self { index, name }
    }

    #[must_use]
    pub const fn index(&self) -> usize {
        self.index
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

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
    /// La parità è verificata anche in release: una riga malformata deve
    /// fallire nel punto in cui il driver la costruisce, non durante un accesso
    /// successivo per nome o posizione.
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

    /// Clona soltanto l'handle ai nomi condivisi, non le stringhe.
    #[must_use]
    pub fn shared_columns(&self) -> Arc<[String]> {
        Arc::clone(&self.columns)
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

    /// Accede per descrittore verificando posizione e identita dello schema.
    #[must_use]
    pub fn get_descriptor(&self, descriptor: &ColumnDescriptor) -> Option<&ParameterValue> {
        (self.columns.get(descriptor.index())? == descriptor.name())
            .then(|| &self.values[descriptor.index()])
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
/// Il messaggio non elenca le colonne presenti, perche sono nomi dello schema
/// remoto e un panic puo finire nei log. Chi deve ispezionarle usa
/// [`Row::columns`]; chi vuole un accesso fallibile usa [`Row::get`].
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
#[path = "row_tests.rs"]
mod tests;
