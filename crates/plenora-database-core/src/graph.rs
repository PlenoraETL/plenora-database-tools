//! Contratto graph portabile per Apache AGE.
//!
//! AGE resta un'estensione del provider PostgreSQL: questo modulo non aggiunge
//! un nuovo [`ProviderKind`](crate::plan::ProviderKind), ma definisce una
//! superficie separata dal contratto relazionale v2. Tutte le capability sono
//! fail-closed e vengono aperte solo dalla combinazione qualificata.

use crate::{DatabaseError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub const QUALIFIED_AGE_VERSION: &str = "1.7.0";
pub const QUALIFIED_POSTGRES_MAJOR: u16 = 18;
pub const MAX_CYPHER_BYTES: usize = 1_048_576;
pub const MAX_GRAPH_COLUMNS: usize = 64;
pub const MAX_GRAPH_PARAMETERS: usize = 256;
pub const MAX_GRAPH_PARAMETER_BYTES: usize = 1_048_576;
pub const DEFAULT_GRAPH_MAX_ROWS: usize = 10_000;
pub const MAX_GRAPH_ROWS: usize = 1_000_000;

/// Capability AGE pubblicate separatamente dal documento provider v2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)] // wire contract: promesse indipendenti e fail-closed
pub struct AgeCapabilities {
    pub schema_version: u32,
    pub postgres_major: Option<u16>,
    pub extension_version: Option<String>,
    pub query: bool,
    pub parameters: bool,
    pub writes: bool,
    pub transactions: bool,
    pub vertex: bool,
    pub edge: bool,
    pub path: bool,
}

impl Default for AgeCapabilities {
    fn default() -> Self {
        Self {
            schema_version: 1,
            postgres_major: None,
            extension_version: None,
            query: false,
            parameters: false,
            writes: false,
            transactions: false,
            vertex: false,
            edge: false,
            path: false,
        }
    }
}

impl AgeCapabilities {
    /// Costruisce il documento dal probe. Le promesse si aprono soltanto per
    /// AGE 1.7.0 su PostgreSQL 18 e quando tutti gli oggetti runtime richiesti
    /// sono presenti.
    #[must_use]
    pub fn from_probe(
        postgres_major: u16,
        extension_version: Option<String>,
        runtime_objects_present: bool,
    ) -> Self {
        let qualified = postgres_major == QUALIFIED_POSTGRES_MAJOR
            && extension_version.as_deref() == Some(QUALIFIED_AGE_VERSION)
            && runtime_objects_present;
        Self {
            schema_version: 1,
            postgres_major: Some(postgres_major),
            extension_version,
            query: qualified,
            parameters: qualified,
            writes: qualified,
            transactions: qualified,
            vertex: qualified,
            edge: qualified,
            path: qualified,
        }
    }

    #[must_use]
    pub const fn qualified(&self) -> bool {
        self.query
            && self.parameters
            && self.writes
            && self.transactions
            && self.vertex
            && self.edge
            && self.path
    }
}

/// Capability amministrative AGE, pubblicate in un documento additivo
/// separato per non modificare il contratto `AgeCapabilities` v1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgeAdminCapabilities {
    pub schema_version: u32,
    pub postgres_major: Option<u16>,
    pub extension_version: Option<String>,
    pub list_graphs: bool,
    pub create_graph: bool,
    pub drop_graph: bool,
}

impl Default for AgeAdminCapabilities {
    fn default() -> Self {
        Self {
            schema_version: 1,
            postgres_major: None,
            extension_version: None,
            list_graphs: false,
            create_graph: false,
            drop_graph: false,
        }
    }
}

impl AgeAdminCapabilities {
    #[must_use]
    pub fn from_age(capabilities: &AgeCapabilities) -> Self {
        let qualified = capabilities.qualified();
        Self {
            schema_version: 1,
            postgres_major: capabilities.postgres_major,
            extension_version: capabilities.extension_version.clone(),
            list_graphs: qualified,
            create_graph: qualified,
            drop_graph: qualified,
        }
    }

    #[must_use]
    pub const fn qualified(&self) -> bool {
        self.list_graphs && self.create_graph && self.drop_graph
    }
}

/// Richiesta Cypher. Il nome del grafo e le colonne sono identificatori, il
/// testo Cypher e opaco e i valori restano in una mappa bindata separata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphStatement {
    pub graph: String,
    pub cypher: String,
    pub columns: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, Value>,
    #[serde(default = "default_graph_max_rows")]
    pub max_rows: usize,
}

impl GraphStatement {
    #[must_use]
    pub fn new(graph: impl Into<String>, cypher: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            graph: graph.into(),
            cypher: cypher.into(),
            columns,
            params: BTreeMap::new(),
            max_rows: DEFAULT_GRAPH_MAX_ROWS,
        }
    }

    #[must_use]
    pub fn with_params(mut self, params: BTreeMap<String, Value>) -> Self {
        self.params = params;
        self
    }

    #[must_use]
    pub const fn with_max_rows(mut self, max_rows: usize) -> Self {
        self.max_rows = max_rows;
        self
    }

    /// Valida esclusivamente forma e limiti, senza riportare payload nei
    /// messaggi pubblici.
    ///
    /// # Errors
    ///
    /// Ritorna `InvalidPlan` per identificatori, cardinalita o payload oltre
    /// i limiti del contratto.
    pub fn validate(&self) -> Result<()> {
        validate_identifier(&self.graph, "nome del grafo")?;
        if self.cypher.trim().is_empty() {
            return Err(DatabaseError::invalid_plan(
                "la query Cypher non puo essere vuota",
            ));
        }
        if self.cypher.len() > MAX_CYPHER_BYTES {
            return Err(DatabaseError::invalid_plan(
                "la query Cypher supera il limite di 1 MiB",
            ));
        }
        if self.columns.is_empty() || self.columns.len() > MAX_GRAPH_COLUMNS {
            return Err(DatabaseError::invalid_plan(
                "le colonne Cypher devono essere tra 1 e 64",
            ));
        }
        let mut seen = BTreeSet::new();
        for column in &self.columns {
            validate_identifier(column, "nome colonna Cypher")?;
            if !seen.insert(column) {
                return Err(DatabaseError::invalid_plan(
                    "i nomi delle colonne Cypher devono essere univoci",
                ));
            }
        }
        if self.params.len() > MAX_GRAPH_PARAMETERS {
            return Err(DatabaseError::invalid_plan(
                "la mappa parametri Cypher supera 256 elementi",
            ));
        }
        if self.max_rows == 0 || self.max_rows > MAX_GRAPH_ROWS {
            return Err(DatabaseError::invalid_plan(
                "il limite righe Cypher deve essere tra 1 e 1000000",
            ));
        }
        for name in self.params.keys() {
            validate_identifier(name, "nome parametro Cypher")?;
        }
        let parameter_bytes = serde_json::to_vec(&self.params).map_err(|_| {
            DatabaseError::invalid_plan("la mappa parametri Cypher non e serializzabile")
        })?;
        if parameter_bytes.len() > MAX_GRAPH_PARAMETER_BYTES {
            return Err(DatabaseError::invalid_plan(
                "la mappa parametri Cypher supera il limite di 1 MiB",
            ));
        }
        Ok(())
    }
}

const fn default_graph_max_rows() -> usize {
    DEFAULT_GRAPH_MAX_ROWS
}

/// Valida un nome graph prima che venga passato alle funzioni amministrative
/// AGE. La funzione e condivisa con `GraphStatement` per mantenere un solo
/// confine contro identificatori arbitrari.
///
/// # Errors
///
/// Restituisce `InvalidPlan` se il nome non e un identificatore ASCII semplice
/// lungo da 1 a 63 byte.
pub fn validate_graph_name(value: &str) -> Result<()> {
    validate_identifier(value, "nome del grafo")
}

fn validate_identifier(value: &str, role: &str) -> Result<()> {
    if value.is_empty() || value.len() > 63 {
        return Err(DatabaseError::invalid_plan(format!(
            "{role} deve contenere tra 1 e 63 byte"
        )));
    }
    let mut chars = value.chars();
    let valid_first = chars
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_');
    if !valid_first || !chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(DatabaseError::invalid_plan(format!(
            "{role} deve essere un identificatore ASCII semplice"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphVertex {
    pub id: i64,
    pub label: String,
    pub properties: BTreeMap<String, GraphValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphEdge {
    pub id: i64,
    pub label: String,
    pub start_id: i64,
    pub end_id: i64,
    pub properties: BTreeMap<String, GraphValue>,
}

/// Valore `agtype` preservato senza appiattire vertex, edge e path in JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum GraphValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    Numeric(String),
    String(String),
    List(Vec<Self>),
    Map(BTreeMap<String, Self>),
    Vertex(GraphVertex),
    Edge(GraphEdge),
    Path(Vec<Self>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphRow {
    pub values: BTreeMap<String, GraphValue>,
}

#[cfg(test)]
#[path = "graph_tests.rs"]
mod tests;
