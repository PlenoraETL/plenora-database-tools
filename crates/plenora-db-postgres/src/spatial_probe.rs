//! Le sonde spatial PostgreSQL contro lo SRID del termine con cui si
//! confrontano, prima dell'esecuzione.
//!
//! Fino al 25/10 il codec legava i byte della sonda e scartava lo SRID di
//! `ParameterValue::Wkb`, affidandosi all'errore «mixed SRID» di `PostGIS`.
//! Quell'errore non arriva in tre casi, tutti misurati: una colonna
//! `geography` (il WKB senza SRID diventa 4326 per cast implicito), gli
//! operatori di bounding box `&&`, `~`, `@` (non confrontano gli SRID), e un
//! `ST_*` indicizzato la cui sonda, in un altro sistema, non tocca nessun
//! candidato dell'indice (zero righe, nessun errore). Ora ogni sonda porta uno
//! SRID esplicito ([`plenora_database_core::spatial_probe::probe_srid`]),
//! confrontato con quello della colonna letto dal catalogo (typmod), e il
//! valore legato e l'EWKB con quello SRID: il server non deve indovinarlo.
//!
//! Il termine di confronto si risolve in modo conservativo: una colonna di
//! una relazione nominata senza ambiguita, un'altra sonda, un `SpatialValue`,
//! o una funzione che conserva il frame del suo unico argomento geometrico.
//! Cio che non si risolve cosi (una sorgente derivata, una CTE, un riferimento
//! correlato, una funzione che cambia frame) non si confronta con una sonda:
//! si rifiuta con `crs` invece di lasciar decidere al server.
//!
//! # Limite dichiarato
//!
//! - **Regola.** Il confronto degli SRID prima dell'esecuzione vale per ogni
//!   termine confrontato con una sonda o con un `SpatialValue`. Un confronto
//!   fra due colonne, senza sonde, resta del server.
//! - **Ambito.** Path `query` di `PostgreSQL`: operatori di bounding box e
//!   funzioni spatial con due argomenti geometrici fra colonne, anche di
//!   tabelle diverse in un join.
//! - **Hazard.** `PostGIS` non confronta gli SRID in `&&`, `~`, `@`: due
//!   colonne in sistemi diversi danno righe senza errore, e un `ST_*`
//!   indicizzato fra colonne in sistemi diversi puo rendere zero righe.
//! - **Rientro.** Leggere dal catalogo lo SRID di entrambe le colonne e
//!   confrontarlo come per una sonda: costa una lettura del catalogo per
//!   colonna su query che oggi non ne fanno, e si decide con una misura.

use crate::types::ColumnSpec;
use plenora_database_core::plan::{FilterExpression, ProviderKind};
use plenora_database_core::provider::{ParameterBag, ParameterValue};
use plenora_database_core::relational::{
    walk_query, walk_query_expression, ColumnRef, QueryExpression, QueryOperation,
    QueryWalkControl, QueryWalkNode, SpatialFunction,
};
use plenora_database_core::spatial_catalog::CrsRule;
use plenora_database_core::spatial_probe::{
    check_spatial_value, compare_frames, stamped_probe, PROBE_PEER_WITHOUT_SRID,
    PROBE_SRID_MISMATCH,
};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::{BTreeMap, BTreeSet};

const PROVIDER: ProviderKind = ProviderKind::Postgres;

fn crs(message: &str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Crs,
        ErrorPhase::Prepare,
        Some(PROVIDER),
        message,
    )
}

/// Un parametro usato come sonda e anche altrove: legarlo con lo SRID
/// incorporato cambierebbe il valore nell'altro uso.
const PROBE_SHARED: &str =
    "parametro usato come sonda spatial e come altro valore: servono due parametri";

/// I parametri gia giudicati: sonde con il loro SRID, e la borsa da legare
/// con l'EWKB di ogni sonda.
struct Probes {
    srids: BTreeMap<String, u32>,
    bound: BTreeMap<String, ParameterValue>,
}

impl Probes {
    fn new(parameters: &ParameterBag) -> Self {
        Self {
            srids: BTreeMap::new(),
            bound: parameters
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        }
    }

    /// Lo SRID della sonda `name`, giudicata e marcata una volta sola.
    fn srid(
        &mut self,
        name: &str,
        parameters: &ParameterBag,
        shared: &BTreeSet<String>,
        max_depth: u64,
    ) -> Result<Option<u32>> {
        if let Some(srid) = self.srids.get(name) {
            return Ok(Some(*srid));
        }
        // Un parametro assente resta del codec, che lo rifiuta come ogni bind
        // mancante.
        let Some(value) = parameters.get(name) else {
            return Ok(None);
        };
        let Some((srid, bytes)) = stamped_probe(PROVIDER, value, max_depth)? else {
            return Ok(None);
        };
        let original = match value {
            ParameterValue::Bytes(original)
            | ParameterValue::Wkb {
                bytes: original, ..
            } => original.as_slice(),
            _ => &[],
        };
        if original != bytes.as_slice() {
            if shared.contains(name) {
                return Err(DatabaseError::invalid_plan(PROBE_SHARED));
            }
            self.bound
                .insert(name.to_owned(), ParameterValue::Bytes(bytes));
        }
        self.srids.insert(name.to_owned(), srid);
        Ok(Some(srid))
    }

    fn into_bag(self) -> ParameterBag {
        ParameterBag::new(self.bound)
    }
}

/// Il filtro di una lettura: ogni sonda contro lo SRID della colonna, dal
/// catalogo gia letto (`ColumnSpec::spatial_srid`, typmod).
///
/// Restituisce la borsa da legare, con l'EWKB di ogni sonda.
///
/// # Errors
///
/// `crs` per una sonda senza SRID, con uno SRID diverso da quello della
/// colonna, o contro una colonna senza SRID dichiarato.
pub fn verify_read_filter(
    filter: Option<&FilterExpression>,
    columns: &[ColumnSpec],
    parameters: &ParameterBag,
    max_depth: u64,
) -> Result<ParameterBag> {
    let mut probes = Probes::new(parameters);
    let Some(filter) = filter else {
        return Ok(probes.into_bag());
    };
    let mut spatial = Vec::new();
    let mut other = BTreeSet::new();
    collect_filter(filter, &mut spatial, &mut other);
    for (field, name) in spatial {
        let Some(srid) = probes.srid(name, parameters, &other, max_depth)? else {
            continue;
        };
        let column = columns
            .iter()
            .find(|column| column.name == field)
            .and_then(|column| column.spatial_srid);
        compare_frames(PROVIDER, srid, column)?;
    }
    Ok(probes.into_bag())
}

fn collect_filter<'a>(
    filter: &'a FilterExpression,
    spatial: &mut Vec<(&'a str, &'a str)>,
    other: &mut BTreeSet<String>,
) {
    match filter {
        FilterExpression::And { args } | FilterExpression::Or { args } => {
            for argument in args {
                collect_filter(argument, spatial, other);
            }
        }
        FilterExpression::Spatial {
            function,
            field,
            geometry_parameter,
            distance_parameter,
        } => {
            // `is_empty` e `is_valid` non legano la geometria: il parametro,
            // se c'e, non arriva al server.
            if !matches!(
                function,
                SpatialFunction::IsEmpty | SpatialFunction::IsValid
            ) {
                if let Some(name) = geometry_parameter {
                    spatial.push((field, name));
                }
            }
            other.extend(distance_parameter.iter().cloned());
        }
        FilterExpression::Eq { parameter, .. }
        | FilterExpression::Ne { parameter, .. }
        | FilterExpression::Lt { parameter, .. }
        | FilterExpression::Lte { parameter, .. }
        | FilterExpression::Gt { parameter, .. }
        | FilterExpression::Gte { parameter, .. }
        | FilterExpression::Like { parameter, .. } => {
            other.insert(parameter.clone());
        }
        FilterExpression::In { parameters, .. } => other.extend(parameters.iter().cloned()),
        FilterExpression::Between {
            lower_parameter,
            upper_parameter,
            ..
        } => {
            other.insert(lower_parameter.clone());
            other.insert(upper_parameter.clone());
        }
        FilterExpression::IsNull { .. } | FilterExpression::IsNotNull { .. } => {}
    }
}

/// Una colonna di cui leggere lo SRID dal catalogo, e quello che deve avere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnFrame {
    pub schema: Option<String>,
    pub object: String,
    pub column: String,
    pub srid: u32,
}

/// Le sonde di una query giudicate senza server: la borsa da legare e le
/// colonne da confrontare con il catalogo prima dell'esecuzione.
#[derive(Debug)]
pub struct QueryProbes {
    pub parameters: ParameterBag,
    pub columns: Vec<ColumnFrame>,
}

/// Il frame di un termine geometrico.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Frame {
    /// Una colonna di una relazione di base, da leggere nel catalogo.
    Column {
        schema: Option<String>,
        object: String,
        column: String,
    },
    /// Uno SRID noto senza server: una sonda o un `SpatialValue`.
    Known(u32),
    /// Un termine senza sonde il cui frame non si risolve qui.
    Unresolved,
}

/// Le relazioni di un'operazione con cui si risolve una colonna.
struct Scope<'a> {
    operation: &'a QueryOperation,
    common_tables: &'a BTreeSet<String>,
}

impl Scope<'_> {
    fn column(&self, column: &ColumnRef) -> Frame {
        let operation = self.operation;
        let sources = operation
            .source
            .iter()
            .chain(
                operation
                    .joins
                    .iter()
                    .filter_map(|join| join.source.as_ref()),
            )
            .collect::<Vec<_>>();
        let derived = operation
            .derived_source
            .iter()
            .chain(
                operation
                    .joins
                    .iter()
                    .filter_map(|join| join.derived_source.as_ref()),
            )
            .collect::<Vec<_>>();
        let named = match &column.relation {
            Some(relation) => {
                if derived.iter().any(|source| source.alias == *relation) {
                    return Frame::Unresolved;
                }
                sources
                    .iter()
                    .filter(|source| {
                        source.alias.as_deref().map_or_else(
                            || source.object.object == *relation,
                            |alias| alias == relation,
                        )
                    })
                    .collect::<Vec<_>>()
            }
            None if derived.is_empty() => sources.iter().collect(),
            None => return Frame::Unresolved,
        };
        let [source] = named.as_slice() else {
            return Frame::Unresolved;
        };
        // Un nome senza schema puo essere una CTE della query: non e la
        // tabella del catalogo con lo stesso nome.
        if source.object.schema.is_none() && self.common_tables.contains(&source.object.object) {
            return Frame::Unresolved;
        }
        Frame::Column {
            schema: source.object.schema.clone(),
            object: source.object.object.clone(),
            column: column.field.clone(),
        }
    }
}

/// Le posizioni geometriche di un nodo spatial: gli argomenti che il
/// renderer avvolge in `ST_GeomFromEWKB` se sono parametri.
fn geometry_arguments(expression: &QueryExpression) -> Option<Vec<&QueryExpression>> {
    match expression {
        QueryExpression::Spatial {
            function,
            arguments,
        }
        | QueryExpression::SpatialWindow {
            function,
            arguments,
            ..
        } => Some(
            arguments
                .iter()
                .enumerate()
                .filter(|(index, _)| function.takes_geometry_at(*index))
                .map(|(_, argument)| argument)
                .collect(),
        ),
        QueryExpression::SpatialOperator { left, right, .. } => {
            Some(vec![left.as_ref(), right.as_ref()])
        }
        _ => None,
    }
}

const fn parameter_name(expression: &QueryExpression) -> Option<&String> {
    match expression {
        QueryExpression::Parameter { name } | QueryExpression::TypedParameter { name, .. } => {
            Some(name)
        }
        _ => None,
    }
}

/// Le sonde di una query, giudicate senza server.
///
/// # Errors
///
/// `crs` per una sonda senza SRID, per due SRID noti diversi nello stesso
/// confronto, per un termine non risolvibile confrontato con una sonda, o per
/// un `SpatialValue` che lega un valore con un altro SRID.
pub fn plan_query_probes(
    operation: &QueryOperation,
    parameters: &ParameterBag,
    max_depth: u64,
) -> Result<QueryProbes> {
    let mut operations = Vec::new();
    let mut common_tables = BTreeSet::new();
    let mut geometry_uses = BTreeMap::<&str, usize>::new();
    let mut all_uses = BTreeMap::<&str, usize>::new();
    walk_query(operation, |node| {
        match node {
            QueryWalkNode::Operation(operation) => {
                operations.push(operation);
                common_tables.extend(
                    operation
                        .common_table_expressions
                        .iter()
                        .map(|cte| cte.name.clone()),
                );
            }
            QueryWalkNode::Expression(expression) => {
                if let Some(name) = parameter_name(expression) {
                    *all_uses.entry(name).or_default() += 1;
                }
                for argument in geometry_arguments(expression).unwrap_or_default() {
                    if let Some(name) = parameter_name(argument) {
                        *geometry_uses.entry(name).or_default() += 1;
                    }
                }
            }
            QueryWalkNode::Source(_) => {}
        }
        QueryWalkControl::Continue
    });
    let shared = geometry_uses
        .iter()
        .filter(|(name, uses)| all_uses.get(*name).copied().unwrap_or(0) > **uses)
        .map(|(name, _)| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    let mut probes = Probes::new(parameters);
    let mut columns = Vec::new();
    for operation in operations {
        let scope = Scope {
            operation,
            common_tables: &common_tables,
        };
        let expressions = operation
            .clause_expressions()
            .chain(operation.joins.iter().filter_map(|join| join.on.as_ref()));
        for root in expressions {
            let mut failure = None;
            walk_query_expression(root, |node| {
                let QueryWalkNode::Expression(expression) = node else {
                    // Le query annidate hanno il proprio scope: le visita il
                    // ciclo esterno.
                    return QueryWalkControl::Skip;
                };
                let outcome = judge_node(
                    expression,
                    &scope,
                    &mut probes,
                    parameters,
                    &shared,
                    max_depth,
                    &mut columns,
                );
                match outcome {
                    Ok(()) => QueryWalkControl::Continue,
                    Err(error) => {
                        failure = Some(error);
                        QueryWalkControl::Break
                    }
                }
            });
            if let Some(error) = failure {
                return Err(error);
            }
        }
    }
    columns.sort_by(|left, right| {
        (&left.schema, &left.object, &left.column, left.srid).cmp(&(
            &right.schema,
            &right.object,
            &right.column,
            right.srid,
        ))
    });
    columns.dedup();
    Ok(QueryProbes {
        parameters: probes.into_bag(),
        columns,
    })
}

#[allow(clippy::too_many_arguments)]
fn judge_node(
    expression: &QueryExpression,
    scope: &Scope<'_>,
    probes: &mut Probes,
    parameters: &ParameterBag,
    shared: &BTreeSet<String>,
    max_depth: u64,
    columns: &mut Vec<ColumnFrame>,
) -> Result<()> {
    if let QueryExpression::SpatialValue {
        expression, srid, ..
    } = expression
    {
        if let Some(value) = parameter_name(expression).and_then(|name| parameters.get(name)) {
            check_spatial_value(PROVIDER, value, *srid, max_depth)?;
        }
        return Ok(());
    }
    let Some(arguments) = geometry_arguments(expression) else {
        return Ok(());
    };
    let mut frames = Vec::with_capacity(arguments.len());
    let mut with_probe = false;
    for argument in arguments {
        let (frame, from_probe) = frame_of(argument, scope, probes, parameters, shared, max_depth)?;
        with_probe |= from_probe;
        frames.push(frame);
    }
    // Un confronto senza sonde (colonna contro colonna) resta com'era: lo
    // decide il server, e il limite e dichiarato.
    if !with_probe || frames.len() < 2 {
        return Ok(());
    }
    let known = frames
        .iter()
        .filter_map(|frame| match frame {
            Frame::Known(srid) => Some(*srid),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let [srid] = known.into_iter().collect::<Vec<_>>()[..] else {
        return Err(crs(PROBE_SRID_MISMATCH));
    };
    for frame in frames {
        match frame {
            Frame::Known(_) => {}
            Frame::Unresolved => return Err(crs(PROBE_PEER_WITHOUT_SRID)),
            Frame::Column {
                schema,
                object,
                column,
            } => columns.push(ColumnFrame {
                schema,
                object,
                column,
                srid,
            }),
        }
    }
    Ok(())
}

/// Il frame di un termine geometrico, e se viene da una sonda o da un valore
/// con SRID dichiarato.
fn frame_of(
    expression: &QueryExpression,
    scope: &Scope<'_>,
    probes: &mut Probes,
    parameters: &ParameterBag,
    shared: &BTreeSet<String>,
    max_depth: u64,
) -> Result<(Frame, bool)> {
    let mut current = expression;
    loop {
        match current {
            QueryExpression::Column { column } => return Ok((scope.column(column), false)),
            QueryExpression::Parameter { name } | QueryExpression::TypedParameter { name, .. } => {
                return Ok(probes
                    .srid(name, parameters, shared, max_depth)?
                    .map_or((Frame::Unresolved, false), |srid| {
                        (Frame::Known(srid), true)
                    }));
            }
            QueryExpression::SpatialValue { srid, .. } => return Ok((Frame::Known(*srid), true)),
            QueryExpression::Spatial {
                function,
                arguments,
            } if function.returns_geometry() && function.crs_rule() == Some(CrsRule::Preserves) => {
                let geometric = arguments
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| function.takes_geometry_at(*index))
                    .map(|(_, argument)| argument)
                    .collect::<Vec<_>>();
                let [argument] = geometric.as_slice() else {
                    return Ok((Frame::Unresolved, false));
                };
                current = argument;
            }
            _ => return Ok((Frame::Unresolved, false)),
        }
    }
}

/// Le colonne confrontate con una sonda contro il catalogo: lo SRID del
/// typmod (`geometry` o `geography`), con la relazione risolta come la
/// risolve la query.
///
/// # Errors
///
/// `crs` se la colonna non e una colonna spatial con SRID dichiarato o ne ha
/// un altro; gli errori del catalogo in fase `prepare`.
pub async fn verify_query_columns(
    client: &tokio_postgres::Client,
    columns: &[ColumnFrame],
) -> Result<()> {
    for frame in columns {
        let row = client
            .query_opt(
                "SELECT postgis_typmod_srid(a.atttypmod) \
                 FROM pg_catalog.pg_attribute a \
                 WHERE a.attrelid = to_regclass(CASE WHEN $1::text IS NULL \
                         THEN quote_ident($2::text) \
                         ELSE quote_ident($1::text) || '.' || quote_ident($2::text) END) \
                   AND a.attname = $3::text AND a.attnum > 0 AND NOT a.attisdropped \
                   AND a.atttypid IN ('geometry'::regtype, 'geography'::regtype)",
                &[&frame.schema, &frame.object, &frame.column],
            )
            .await
            .map_err(|error| crate::error::classify_error(ErrorPhase::Prepare, &error))?;
        let declared = row
            .and_then(|row| row.get::<_, Option<i32>>(0))
            .and_then(|srid| u32::try_from(srid).ok());
        compare_frames(PROVIDER, frame.srid, declared)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "spatial_probe_tests.rs"]
mod tests;
