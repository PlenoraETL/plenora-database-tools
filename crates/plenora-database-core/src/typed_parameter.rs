//! Il tipo dichiarato di un parametro e il valore legato (regola T).
//!
//! # Regola T: un parametro tipizzato accetta soltanto i valori del suo tipo
//!
//! Un parametro con un tipo dichiarato (`TypedParameter`, `bind(nome, tipo)`
//! dello SDK) ammette un valore legato soltanto se il genere del valore
//! ([`ParameterValue`]) e uno di quelli del tipo dichiarato, che si
//! convertono nel tipo senza perdita:
//!
//! | tipo dichiarato | valori ammessi |
//! | --- | --- |
//! | `boolean` | `Bool` |
//! | `integer`, `big_integer` | `I32`, `I64` |
//! | `float` | `F64`, `I32` |
//! | `decimal` | `Decimal`, `I32`, `I64` |
//! | `string` | `String`, `Enum` |
//! | `binary` | `Bytes`; anche `Wkb` come argomento di un nodo spatial o di un valore spatial |
//! | `date`, `timestamp`, `timestamp_tz` | `Date`, `Timestamp`, `TimestampTz`, ciascuno il suo |
//! | `uuid`, `json` | `Uuid`, `Json` |
//!
//! Un `Null` e ammesso da ogni tipo. Ogni altro valore si rifiuta prima
//! dell'esecuzione, `invalid_plan` in fase `prepare`
//! ([`TYPED_PARAMETER_MISMATCH`]), in ogni posizione del piano: confronti,
//! projection, scritture. Fino al 25/10 (quattordicesima lettura) la regola F
//! prendeva la famiglia di un parametro tipizzato dal tipo dichiarato e
//! ignorava il valore: `UPDATE ... WHERE name = ?` con un `string` dichiarato
//! e il valore `I64` 1 aggiornava tre righe su `MySQL` (`'1'`, `'01'`,
//! `'1.0'`), perche il lowering delle mutazioni non rendeva il `CAST` che il
//! renderer delle query rende.
//!
//! # Una sola forma per ogni renderer
//!
//! Un parametro tipizzato in una posizione **confrontata o calcolata** si
//! rende con [`CastDialect::render`], la stessa funzione per il renderer
//! delle query (ogni dialetto) e per i predicati del lowering delle mutazioni
//! (`relational_mutation`): `CAST(? AS tipo)`, `TO_BOOLEAN` e
//! `TO_TIMESTAMP_TZ` su Oracle, il segnaposto nudo per un `string` o un
//! `uuid` su `MySQL` e `MariaDB` (il `CAST` gli darebbe la collation della
//! connessione, e il confronto con una colonna di un'altra collation fallisce
//! con 1267). In una posizione **scritta** (i valori di `INSERT`, le
//! assegnazioni di `UPDATE`, le righe di un upsert) si rende il solo
//! segnaposto: il tipo lo decide la colonna di destinazione, e il valore e
//! gia del tipo dichiarato (su Oracle un `timestamp_tz` scritto resta in
//! `TO_TIMESTAMP_TZ`, la conversione che il driver thin richiede). Come
//! argomento di un nodo spatial (la geometria che il renderer costruisce) il
//! parametro e l'input del costruttore.
//!
//! Il `CAST` non deve perdere il valore (regola C, [`crate::cast_table`]):
//! nelle posizioni rese con il `CAST` ogni valore legato deve stare esatto nel
//! tipo reso ([`crate::cast_table::rendered_target`],
//! [`crate::cast_table::value_fits`]), altrimenti si rifiuta
//! ([`TYPED_PARAMETER_MISMATCH`]): un `decimal` oltre `DECIMAL(38, 10)` (oltre
//! `DECIMAL(31, 10)` su Db2, che non ha precisione oltre 31), un `string`
//! oltre 4000 byte su Oracle (`VARCHAR2(4000)`) o 32672 su Db2
//! (`VARCHAR(32672)`), un `binary` oltre 2000 byte su Oracle (`RAW(2000)`) o
//! 32672 su Db2, un `timestamp` oltre le cifre frazionarie del tipo reso, un
//! `timestamp_tz` su `MariaDB` (`DATETIME(6)` scarta l'offset, misurato il
//! 25/10: `2024-01-01 10:00:00+02:00` diventa `10:00`) e su Db2 (che non ha
//! `TIMESTAMP WITH TIME ZONE`). Fino alla quindicesima lettura del 25/10 il
//! `timestamp_tz` di `MariaDB` si rendeva senza giudizio e ritrovava la riga
//! sbagliata, e il `decimal` di Db2 si rendeva `DECIMAL(38, 10)`, che il
//! server rifiuta.
//!
//! # Limiti dichiarati
//!
//! - **Regola.** La regola T giudica il valore soltanto dove il percorso lo
//!   conosce.
//! - **Ambito.** La compilazione senza esecuzione (`compile_relational_query`
//!   dello SDK, senza valori); lo SQL del chiamante, che non ha un piano.
//! - **Hazard.** Nessuno nell'esecuzione: ogni percorso che esegue un piano
//!   con i valori lo giudica (`facade::query_judged`, `execute_judged`, il
//!   path query di `PostgreSQL`, `MySQL`/`MariaDB`, SQL Server).
//! - **Rientro.** Nessuno: senza valori non c'e niente da giudicare.
//!
//! - **Regola.** Le frazioni di secondo di un `timestamp` oltre la precisione
//!   del tipo reso (`DATETIME(6)` su `MySQL`) non sono di questa regola.
//! - **Ambito.** I tipi temporali tipizzati.
//! - **Hazard.** Un arrotondamento del server oltre il microsecondo.
//! - **Rientro.** La regola di fedelta dei parametri (PR #135), che rifiuta le
//!   frazioni oltre il microsecondo sul testo del valore.

use crate::cast_table::ValueFact;
use crate::plan::ProviderKind;
use crate::provider::ParameterValue;
use crate::relational::{
    walk_query, walk_query_expression, MutationOperation, QueryExpression, QueryParameterType,
    QueryWalkControl, QueryWalkNode,
};
use crate::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::{BTreeMap, BTreeSet};

/// Motivo stabile del rifiuto di un valore che non e del tipo dichiarato del
/// suo parametro (regola T).
pub const TYPED_PARAMETER_MISMATCH: &str = "valore di un parametro tipizzato fuori dal suo tipo: un bind dichiarato accetta soltanto i valori del proprio tipo (un intero anche per decimal e, se a 32 bit, per float), entro i limiti del CAST che lo rende (decimal fino a DECIMAL(38, 10), DECIMAL(31, 10) su Db2, string fino a 4000 byte su Oracle e 32672 su Db2, binary fino a 2000 byte su Oracle e 32672 su Db2, timestamp entro le cifre frazionarie del tipo reso, nessun timestamp_tz su MariaDB e Db2); il server non converte mai da se";

/// Il dialetto di cui [`CastDialect::render`] rende un parametro tipizzato:
/// i dialetti del renderer delle query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastDialect {
    Postgres,
    Mysql,
    SqlServer,
    Oracle,
    Db2,
    Sqlite,
    Duckdb,
}

impl CastDialect {
    /// Il dialetto di un provider.
    #[must_use]
    pub const fn of(provider: ProviderKind) -> Self {
        match provider {
            ProviderKind::Mysql | ProviderKind::Mariadb => Self::Mysql,
            ProviderKind::Sqlserver => Self::SqlServer,
            ProviderKind::Oracle => Self::Oracle,
            ProviderKind::Db2 => Self::Db2,
            _ => Self::Postgres,
        }
    }

    /// Il tipo SQL di un tipo dichiarato, per `CAST(... AS tipo)`: la tabella
    /// unica del renderer delle query e del lowering delle mutazioni.
    #[must_use]
    pub const fn type_name(self, parameter_type: QueryParameterType) -> &'static str {
        match parameter_type {
            QueryParameterType::Boolean => match self {
                Self::Mysql => "SIGNED",
                Self::SqlServer => "BIT",
                _ => "BOOLEAN",
            },
            QueryParameterType::Integer => match self {
                Self::Mysql => "SIGNED",
                Self::Oracle => "NUMBER(10)",
                _ => "INTEGER",
            },
            QueryParameterType::BigInteger => match self {
                Self::Mysql => "SIGNED",
                Self::Oracle => "NUMBER(19)",
                _ => "BIGINT",
            },
            QueryParameterType::Float => match self {
                Self::Postgres => "DOUBLE PRECISION",
                Self::SqlServer => "FLOAT",
                Self::Sqlite => "REAL",
                Self::Oracle => "BINARY_DOUBLE",
                _ => "DOUBLE",
            },
            QueryParameterType::String => match self {
                Self::Postgres | Self::Sqlite => "TEXT",
                Self::Mysql => "CHAR",
                Self::SqlServer => "NVARCHAR(MAX)",
                Self::Duckdb => "VARCHAR",
                Self::Oracle => "VARCHAR2(4000)",
                Self::Db2 => "VARCHAR(32672)",
            },
            QueryParameterType::Binary => match self {
                Self::Postgres => "BYTEA",
                Self::Mysql => "BINARY",
                Self::SqlServer => "VARBINARY(MAX)",
                Self::Sqlite | Self::Duckdb => "BLOB",
                Self::Oracle => "RAW(2000)",
                Self::Db2 => "VARCHAR(32672) FOR BIT DATA",
            },
            QueryParameterType::Date => "DATE",
            QueryParameterType::Timestamp => match self {
                Self::Mysql => "DATETIME(6)",
                Self::SqlServer => "DATETIME2",
                Self::Sqlite => "TEXT",
                _ => "TIMESTAMP",
            },
            QueryParameterType::TimestampTz => match self {
                Self::Postgres | Self::Sqlite | Self::Duckdb => "TIMESTAMPTZ",
                Self::SqlServer => "DATETIMEOFFSET",
                Self::Oracle | Self::Db2 => "TIMESTAMP WITH TIME ZONE",
                Self::Mysql => "DATETIME(6)",
            },
            // Db2 non ha precisione oltre 31 (la query fallisce, misurato il 25/10).
            QueryParameterType::Decimal => match self {
                Self::Oracle => "NUMBER(38, 10)",
                Self::Db2 => "DECIMAL(31, 10)",
                _ => "DECIMAL(38, 10)",
            },
            QueryParameterType::Uuid => match self {
                Self::Postgres => "UUID",
                Self::SqlServer => "UNIQUEIDENTIFIER",
                Self::Oracle => "VARCHAR2(36)",
                _ => "CHAR(36)",
            },
            QueryParameterType::Json => match self {
                Self::Postgres => "JSONB",
                Self::Mysql | Self::Sqlite | Self::Duckdb => "JSON",
                Self::SqlServer => "NVARCHAR(MAX)",
                Self::Oracle | Self::Db2 => "CLOB",
            },
        }
    }

    /// Il parametro tipizzato reso attorno al suo segnaposto: la sola forma
    /// di ogni renderer per una posizione confrontata o calcolata.
    #[must_use]
    pub fn render(self, placeholder: &str, parameter_type: QueryParameterType) -> String {
        // Su `MySQL` e `MariaDB` il `CAST` di un testo gli da la collation
        // della connessione con coercibilita implicita, come la colonna: con
        // due collation diverse il confronto fallisce (1267, misurato il
        // 25/10 su 9.7.2). Il valore e gia un testo (regola T): il segnaposto
        // nudo confronta con la collation della colonna, senza conversioni.
        if self == Self::Mysql
            && matches!(
                parameter_type,
                QueryParameterType::String | QueryParameterType::Uuid
            )
        {
            return placeholder.to_owned();
        }
        if self == Self::Oracle {
            match parameter_type {
                QueryParameterType::Boolean => return format!("TO_BOOLEAN({placeholder})"),
                QueryParameterType::TimestampTz => {
                    return format!(
                        "TO_TIMESTAMP_TZ({placeholder}, '{}')",
                        crate::provider::ORACLE_TIMESTAMP_TZ_FORMAT_MODEL
                    )
                }
                _ => {}
            }
        }
        format!("CAST({placeholder} AS {})", self.type_name(parameter_type))
    }
}

/// Il genere di un valore legato, un bit per variante.
mod kind {
    pub const BOOL: u16 = 1;
    pub const I32: u16 = 1 << 1;
    pub const I64: u16 = 1 << 2;
    pub const F64: u16 = 1 << 3;
    pub const DECIMAL: u16 = 1 << 4;
    pub const STRING: u16 = 1 << 5;
    pub const ENUM: u16 = 1 << 6;
    pub const BYTES: u16 = 1 << 7;
    pub const DATE: u16 = 1 << 8;
    pub const TIMESTAMP: u16 = 1 << 9;
    pub const TIMESTAMP_TZ: u16 = 1 << 10;
    pub const UUID: u16 = 1 << 11;
    pub const JSON: u16 = 1 << 12;
    pub const WKB: u16 = 1 << 13;
}

/// Il genere `Uuid` di un valore legato.
pub const UUID_KIND: u16 = kind::UUID;
/// Il genere `Json` di un valore legato.
pub const JSON_KIND: u16 = kind::JSON;

/// La famiglia di ogni genere di valore che non dipende dal provider
/// (`Uuid` e `Json` ne dipendono: `spatial_operand::ParameterFamilies`).
pub const KIND_FAMILIES: [(u16, crate::spatial_operand::OperandFamily); 12] = {
    use crate::spatial_operand::OperandFamily as F;
    [
        (kind::BOOL, F::Numeric),
        (kind::I32, F::Numeric),
        (kind::I64, F::Numeric),
        (kind::F64, F::Numeric),
        (kind::DECIMAL, F::Numeric),
        (kind::STRING, F::Text),
        (kind::ENUM, F::Text),
        (kind::BYTES, F::Binary),
        (kind::DATE, F::Temporal),
        (kind::TIMESTAMP, F::Temporal),
        (kind::TIMESTAMP_TZ, F::Temporal),
        (kind::WKB, F::Geometry),
    ]
};

/// I generi che un tipo dichiarato ammette (la tabella del modulo).
const fn admitted(parameter_type: QueryParameterType) -> u16 {
    match parameter_type {
        QueryParameterType::Boolean => kind::BOOL,
        QueryParameterType::Integer | QueryParameterType::BigInteger => kind::I32 | kind::I64,
        QueryParameterType::Float => kind::F64 | kind::I32,
        QueryParameterType::Decimal => kind::DECIMAL | kind::I32 | kind::I64,
        QueryParameterType::String => kind::STRING | kind::ENUM,
        QueryParameterType::Binary => kind::BYTES,
        QueryParameterType::Date => kind::DATE,
        QueryParameterType::Timestamp => kind::TIMESTAMP,
        QueryParameterType::TimestampTz => kind::TIMESTAMP_TZ,
        QueryParameterType::Uuid => kind::UUID,
        QueryParameterType::Json => kind::JSON,
    }
}

/// Cio che serve sapere dei valori legati a un nome: i generi e, per la
/// regola C, i fatti di ogni valore ([`ValueFact`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BoundValue {
    kinds: u16,
    facts: Vec<ValueFact>,
}

impl BoundValue {
    /// Un valore in piu legato allo stesso nome.
    pub fn add(&mut self, value: &ParameterValue) {
        let bit = match value {
            ParameterValue::Bool(_) => kind::BOOL,
            ParameterValue::I32(_) => kind::I32,
            ParameterValue::I64(_) => kind::I64,
            ParameterValue::F64(_) => kind::F64,
            ParameterValue::Decimal(_) => kind::DECIMAL,
            ParameterValue::String(_) => kind::STRING,
            ParameterValue::Enum { .. } => kind::ENUM,
            ParameterValue::Bytes(_) => kind::BYTES,
            ParameterValue::Date(_) => kind::DATE,
            ParameterValue::Timestamp(_) => kind::TIMESTAMP,
            ParameterValue::TimestampTz(_) => kind::TIMESTAMP_TZ,
            ParameterValue::Uuid(_) => kind::UUID,
            ParameterValue::Json(_) => kind::JSON,
            ParameterValue::Wkb { .. } => kind::WKB,
            ParameterValue::Null { .. } => 0,
        };
        self.kinds |= bit;
        let fact = ValueFact::of(value);
        if !self.facts.contains(&fact) {
            self.facts.push(fact);
        }
    }

    /// Ogni valore legato sta esatto nel tipo reso (regola C).
    #[must_use]
    pub fn fits(&self, provider: ProviderKind, target: &crate::cast_table::OperandType) -> bool {
        self.facts
            .iter()
            .all(|fact| crate::cast_table::value_fits(provider, fact, target))
    }

    /// I generi dei valori legati.
    #[must_use]
    pub const fn kinds(&self) -> u16 {
        self.kinds
    }

    fn merge(&mut self, other: &Self) {
        self.kinds |= other.kinds;
        for fact in &other.facts {
            if !self.facts.contains(fact) {
                self.facts.push(fact.clone());
            }
        }
    }
}

/// La posizione di un parametro tipizzato: come lo rende il renderer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot {
    /// Reso con il `CAST` ([`CastDialect::render`]): ogni posizione di una
    /// query, il predicato di una mutazione.
    Cast,
    /// L'argomento di un nodo spatial o di un valore spatial: l'input del
    /// costruttore della geometria.
    Spatial,
    /// Un valore scritto: il solo segnaposto.
    Written,
}

fn mismatch(provider: ProviderKind) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::InvalidPlan,
        ErrorPhase::Prepare,
        Some(provider),
        TYPED_PARAMETER_MISMATCH,
    )
}

/// La regola T su un parametro tipizzato.
fn judge(
    provider: ProviderKind,
    parameter_type: QueryParameterType,
    slot: Slot,
    bound: &BoundValue,
) -> Result<()> {
    let mut allowed = admitted(parameter_type);
    if slot == Slot::Spatial && parameter_type == QueryParameterType::Binary {
        allowed |= kind::WKB;
    }
    if bound.kinds & !allowed != 0 {
        return Err(mismatch(provider));
    }
    // Regola C: il valore sta esatto nel tipo reso. Su `MySQL` e `MariaDB`
    // un `string` e un `uuid` si rendono senza `CAST` ([`CastDialect::render`]).
    let bare = matches!(provider, ProviderKind::Mysql | ProviderKind::Mariadb)
        && matches!(
            parameter_type,
            QueryParameterType::String | QueryParameterType::Uuid
        );
    if slot == Slot::Cast
        && !bare
        && !bound.fits(
            provider,
            &crate::cast_table::rendered_target(provider, parameter_type),
        )
    {
        return Err(mismatch(provider));
    }
    Ok(())
}

/// Nessun valore legato.
static NO_VALUES: BoundValue = BoundValue {
    kinds: 0,
    facts: Vec::new(),
};

/// I valori legati ai parametri di un piano, per nome.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BoundValues(BTreeMap<String, BoundValue>);

impl BoundValues {
    /// Un valore legato al nome, in piu di quelli gia visti.
    pub fn add(&mut self, name: &str, value: &ParameterValue) {
        self.0.entry(name.to_owned()).or_default().add(value);
    }

    /// I valori di `other` in piu.
    pub fn merge(&mut self, other: &Self) {
        for (name, value) in &other.0 {
            self.0.entry(name.clone()).or_default().merge(value);
        }
    }

    /// I valori legati al nome; nessuno se il nome non e legato.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&BoundValue> {
        self.0.get(name)
    }

    /// La regola T su ogni parametro tipizzato di una query.
    ///
    /// # Errors
    ///
    /// `invalid_plan` ([`TYPED_PARAMETER_MISMATCH`]) per il primo valore
    /// fuori dal tipo del suo parametro.
    pub fn judge_query(
        &self,
        provider: ProviderKind,
        operation: &crate::relational::QueryOperation,
    ) -> Result<()> {
        let mut outcome = Ok(());
        let mut spatial = BTreeSet::new();
        walk_query(operation, |node| {
            self.visit(provider, node, &mut spatial, Slot::Cast, &mut outcome)
        });
        outcome
    }

    /// La regola T su ogni parametro tipizzato di una mutazione: il
    /// predicato e la chiave di conflitto resi con il `CAST`, i valori scritti
    /// con il solo segnaposto.
    ///
    /// # Errors
    ///
    /// Quelli di [`Self::judge_query`].
    pub fn judge_mutation(
        &self,
        provider: ProviderKind,
        mutation: &MutationOperation,
    ) -> Result<()> {
        let (filter, rows, assignments) = match mutation {
            MutationOperation::Insert(insert) => (None, insert.rows.as_slice(), &[][..]),
            MutationOperation::Update(update) => (
                update.filter.as_ref(),
                &[][..],
                update.assignments.as_slice(),
            ),
            MutationOperation::Delete(delete) => (delete.filter.as_ref(), &[][..], &[][..]),
            MutationOperation::Upsert(upsert) => (
                None,
                upsert.rows.as_slice(),
                upsert.update_on_conflict.as_slice(),
            ),
        };
        let written = rows
            .iter()
            .flatten()
            .chain(assignments.iter().map(|assignment| &assignment.value));
        let mut outcome = Ok(());
        let mut spatial = BTreeSet::new();
        for expression in written {
            walk_query_expression(expression, |node| {
                self.visit(provider, node, &mut spatial, Slot::Written, &mut outcome)
            });
        }
        if let Some(filter) = filter {
            walk_query_expression(filter, |node| {
                self.visit(provider, node, &mut spatial, Slot::Cast, &mut outcome)
            });
        }
        outcome
    }

    fn visit(
        &self,
        provider: ProviderKind,
        node: QueryWalkNode<'_>,
        spatial: &mut BTreeSet<usize>,
        slot: Slot,
        outcome: &mut Result<()>,
    ) -> QueryWalkControl {
        let QueryWalkNode::Expression(expression) = node else {
            return QueryWalkControl::Continue;
        };
        let address = |expression: &QueryExpression| std::ptr::from_ref(expression) as usize;
        match expression {
            QueryExpression::Spatial { arguments, .. }
            | QueryExpression::SpatialWindow { arguments, .. } => {
                spatial.extend(arguments.iter().map(address));
            }
            QueryExpression::SpatialOperator { left, right, .. } => {
                spatial.extend([address(left), address(right)]);
            }
            QueryExpression::SpatialValue { expression, .. } => {
                spatial.insert(address(expression));
            }
            QueryExpression::TypedParameter {
                name,
                parameter_type,
            } => {
                let slot = if spatial.contains(&address(expression)) {
                    Slot::Spatial
                } else {
                    slot
                };
                if let Err(error) = judge(
                    provider,
                    *parameter_type,
                    slot,
                    self.get(name).unwrap_or(&NO_VALUES),
                ) {
                    *outcome = Err(error);
                    return QueryWalkControl::Break;
                }
            }
            _ => {}
        }
        QueryWalkControl::Continue
    }
}

#[cfg(test)]
#[path = "typed_parameter_tests.rs"]
mod tests;
