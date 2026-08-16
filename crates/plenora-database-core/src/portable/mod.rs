//! `PortableStatement` — AST portable per l'application plane OLTP.
//!
//! Il consumer PFM costruisce lo statement tramite l'AST (`select`, `insert`,
//! `update`, `delete`, `upsert`) e chiama [`compile_portable`] con il
//! `ProviderKind` corrente per ottenere uno `Statement` (SQL + parametri
//! positional) pronto da eseguire tramite `TransactionScope::execute` /
//! `query`.
//!
//! Il vantaggio è duplice:
//!
//! 1. **Zero SQL vendor-specific nel dominio PFM**: nessun `RETURNING`,
//!    `OUTPUT`, `ON CONFLICT`, `ON DUPLICATE KEY UPDATE` scritto a mano.
//! 2. **Governance uniforme**: la validazione (identificatori, keyword,
//!    bind safety) vive in un solo posto.
//!
//! Scope Fase 1: primitive minime CRUD + RETURNING. `JOIN`, `CTE`, window
//! functions, aggregation sono estensioni additive future.

use crate::provider::ParameterValue;
use crate::spatial_predicate::{SpatialPredicate, SpatialReference};
use serde::{Deserialize, Serialize};

// ============================================================================
//  AST
// ============================================================================

/// Riferimento a una tabella, opzionalmente qualificato dallo schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub name: String,
}

impl TableRef {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            schema: None,
            name: name.into(),
        }
    }

    #[must_use]
    pub fn qualified(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: Some(schema.into()),
            name: name.into(),
        }
    }
}

/// Espressione atomica: valore letterale (verrà bindato) o riferimento a
/// colonna del target.
#[allow(clippy::derive_partial_eq_without_eq)] // ParameterValue::F64 contiene f64
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Expression {
    Literal(ParameterValue),
    Column(String),
}

impl Expression {
    #[must_use]
    pub const fn literal(value: ParameterValue) -> Self {
        Self::Literal(value)
    }

    #[must_use]
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(name.into())
    }
}

/// Ordinamento singolo (una colonna, una direzione, opzionale null-order).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderBy {
    pub column: String,
    pub direction: Direction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nulls: Option<Nulls>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Nulls {
    First,
    Last,
}

/// Predicato del WHERE clause. Tutti gli operatori sono bind-safe: il
/// consumer non può iniettare SQL, solo valori.
#[allow(clippy::derive_partial_eq_without_eq)] // Expression contiene f64
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Predicate {
    Eq {
        column: String,
        value: Expression,
    },
    Ne {
        column: String,
        value: Expression,
    },
    Lt {
        column: String,
        value: Expression,
    },
    Lte {
        column: String,
        value: Expression,
    },
    Gt {
        column: String,
        value: Expression,
    },
    Gte {
        column: String,
        value: Expression,
    },
    In {
        column: String,
        values: Vec<Expression>,
    },
    Between {
        column: String,
        low: Expression,
        high: Expression,
    },
    Like {
        column: String,
        pattern: Expression,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    And {
        predicates: Vec<Self>,
    },
    Or {
        predicates: Vec<Self>,
    },
    Not {
        predicate: Box<Self>,
    },
    /// Predicato spaziale su una colonna geometry. La geometria di
    /// riferimento viene bindata come `bytea` (EWKB) e rehydratata
    /// server-side (`ST_GeomFromEWKB($n)::geometry` su `PostGIS`).
    Spatial {
        column: String,
        predicate: SpatialPredicate,
        reference: SpatialReference,
    },
}

/// Costruttore fluente per un predicato spaziale.
#[must_use]
pub fn spatial(
    column: impl Into<String>,
    predicate: SpatialPredicate,
    reference: SpatialReference,
) -> Predicate {
    Predicate::Spatial {
        column: column.into(),
        predicate,
        reference,
    }
}

/// Projection: tutte le colonne o lista esplicita.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Projection {
    All,
    Columns(Vec<String>),
}

#[allow(clippy::derive_partial_eq_without_eq)] // filter contiene Expression con f64
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectStatement {
    pub table: TableRef,
    pub projection: Projection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Predicate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<OrderBy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InsertStatement {
    pub table: TableRef,
    pub columns: Vec<String>,
    /// Vec di righe; ogni riga è Vec di espressioni allineato a `columns`.
    pub values: Vec<Vec<Expression>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateStatement {
    pub table: TableRef,
    /// SET column = expression. Ordine preservato.
    pub assignments: Vec<(String, Expression)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Predicate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteStatement {
    pub table: TableRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Predicate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
}

/// UPSERT (INSERT + on-conflict update). `conflict_target` è la chiave che
/// definisce il conflict (tipicamente PK o unique key).
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpsertStatement {
    pub table: TableRef,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expression>>,
    pub conflict_target: Vec<String>,
    /// Assignments applicati in caso di conflict; vuoto = ON CONFLICT DO NOTHING.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub update_on_conflict: Vec<(String, Expression)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
}

/// Nodo top-level dell'AST portable.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PortableStatement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Upsert(UpsertStatement),
}

// ============================================================================
//  Builder API
// ============================================================================

/// Costruisce un `SelectStatement` con `Projection::All`.
#[must_use]
pub fn select_all(table: impl Into<String>) -> SelectStatement {
    SelectStatement {
        table: TableRef::new(table),
        projection: Projection::All,
        filter: None,
        order_by: Vec::new(),
        limit: None,
    }
}

/// Costruisce un `SelectStatement` con projection esplicita.
#[must_use]
pub fn select(table: impl Into<String>, columns: Vec<&str>) -> SelectStatement {
    SelectStatement {
        table: TableRef::new(table),
        projection: Projection::Columns(columns.into_iter().map(String::from).collect()),
        filter: None,
        order_by: Vec::new(),
        limit: None,
    }
}

impl SelectStatement {
    #[must_use]
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.table.schema = Some(schema.into());
        self
    }

    #[must_use]
    pub fn where_(mut self, predicate: Predicate) -> Self {
        self.filter = Some(predicate);
        self
    }

    #[must_use]
    pub fn order_by(mut self, column: impl Into<String>, direction: Direction) -> Self {
        self.order_by.push(OrderBy {
            column: column.into(),
            direction,
            nulls: None,
        });
        self
    }

    #[must_use]
    pub const fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    #[must_use]
    pub const fn into_statement(self) -> PortableStatement {
        PortableStatement::Select(self)
    }
}

/// Predicato di uguaglianza tra colonna e valore letterale.
#[must_use]
pub fn eq(column: impl Into<String>, value: ParameterValue) -> Predicate {
    Predicate::Eq {
        column: column.into(),
        value: Expression::literal(value),
    }
}

#[must_use]
pub const fn and(predicates: Vec<Predicate>) -> Predicate {
    Predicate::And { predicates }
}

#[must_use]
pub const fn or(predicates: Vec<Predicate>) -> Predicate {
    Predicate::Or { predicates }
}

mod compiler;
pub use compiler::compile_portable;

#[cfg(test)]
mod tests;
