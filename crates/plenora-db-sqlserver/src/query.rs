//! Validazione, rendering e descrizione delle `QueryOperation` SQL Server.
//!
//! Le funzioni spatial sono pubblicate per semantica; il preflight legge dal
//! catalogo il tipo della colonna e impedisce di renderizzare membri T-SQL sul
//! ricevitore sbagliato.

use crate::error::driver_error;
use crate::parameter::{bind_parameters, parameter_declarations};
use crate::types::{SqlServerColumnSpec, SqlServerReadPlan};
use crate::SqlServerSession;
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::plan::{ObjectRef, ProviderKind};
use plenora_database_core::provider::{ParameterBag, ParameterValue};
use plenora_database_core::relational::{
    walk_query, walk_query_expression, ColumnRef, QueryExpression, QueryOperation, QueryProjection,
    QuerySource, QueryWalkControl, QueryWalkNode, SpatialFunction,
};
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
};
use plenora_database_sql::{
    Dialect, DialectCapabilities, RenderedSql, Renderer, SqlServerSpatialParameter,
    SqlServerSpatialShape,
};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use tiberius::{FromSql, Query, Row};

const DESCRIBE_QUERY_SQL: &str = r"
SELECT
    column_ordinal,
    name,
    is_nullable,
    system_type_name,
    collation_name,
    user_type_name,
    error_number,
    error_message
FROM sys.dm_exec_describe_first_result_set(@P1, @P2, 0)
WHERE is_hidden = 0
   OR error_number IS NOT NULL
ORDER BY column_ordinal;
";

/// Se il provider offre questa funzione su **qualche** semantica.
///
/// I controlli di politica usano l'unione delle liste; il preflight restringe
/// poi l'operazione alla semantica letta dal catalogo.
fn sql_server_offers(function: SpatialFunction) -> bool {
    VERIFIED_SPATIAL_FUNCTIONS.contains(&function)
        || GEOMETRY_ONLY_SPATIAL_FUNCTIONS.contains(&function)
}

/// Le funzioni che SQL Server offre su `geometry` e **non** su `geography`.
///
/// `SpatialCapabilities::functions` resta l'intersezione fra le semantiche,
/// mentre `functions_by_semantics` pubblica anche questa estensione.
///
/// # Chi impedisce di chiamarle sul tipo sbagliato
///
/// Il preflight, che legge la semantica della colonna dal catalogo — la stessa
/// lettura che serve alle due coordinate.
pub const GEOMETRY_ONLY_SPATIAL_FUNCTIONS: &[SpatialFunction] = &[
    SpatialFunction::Centroid,
    SpatialFunction::Envelope,
    SpatialFunction::Boundary,
    SpatialFunction::PointOnSurface,
    SpatialFunction::IsSimple,
    SpatialFunction::Touches,
    SpatialFunction::Crosses,
];

/// Le funzioni spatial qualificate su SQL Server.
///
/// # Cosa la delimita
///
/// Il censimento live attraversa l'intero catalogo portabile su entrambe le
/// semantiche e richiede una ragione esplicita per ogni esclusione.
///
/// # Perche l'intersezione
///
/// `ProviderCapabilities::functions` e l'intersezione fra le semantiche
/// dichiarate. Offrire l'unione prometterebbe funzioni non disponibili su una
/// delle due famiglie di colonne.
///
/// Il gate lo pretende: `live_every_verified_spatial_function_is_crossed`
/// attraversa ogni funzione su **entrambe** le semantiche, e una che regga solo
/// su una fa fallire il riferimento. Le arieta invece si perdonano fra loro,
/// perche una funzione che non vale su ogni forma geometrica non e assente dal
/// prodotto.
///
/// # Come si scrive cio che c'e qui
///
/// Nome e forma di ogni membro T-SQL stanno in
/// `plenora_database_sql::sql_server_spatial_method`, e in nessun altro posto.
/// `what_the_provider_offers_and_what_the_renderer_can_write_are_the_same_list`
/// tiene questa lista e quella tabella allineate.
pub const VERIFIED_SPATIAL_FUNCTIONS: &[SpatialFunction] = &[
    SpatialFunction::GeometryType,
    SpatialFunction::Srid,
    SpatialFunction::Dimensions,
    SpatialFunction::NPoints,
    SpatialFunction::IsEmpty,
    SpatialFunction::IsValid,
    SpatialFunction::IsClosed,
    SpatialFunction::Intersects,
    SpatialFunction::Contains,
    SpatialFunction::Within,
    SpatialFunction::Disjoint,
    SpatialFunction::Equals,
    SpatialFunction::Distance,
    SpatialFunction::Area,
    SpatialFunction::Length,
    SpatialFunction::StartPoint,
    SpatialFunction::EndPoint,
    SpatialFunction::PointN,
    SpatialFunction::Buffer,
    SpatialFunction::Intersection,
    SpatialFunction::Difference,
    SpatialFunction::SymDifference,
    SpatialFunction::Union,
    SpatialFunction::ConvexHull,
    // Presenti e verificate su entrambe le semantiche.
    SpatialFunction::Z,
    SpatialFunction::M,
    SpatialFunction::Overlaps,
    SpatialFunction::Simplify,
    SpatialFunction::MakeValid,
    // Il membro T-SQL cambia fra le semantiche (`STX`/`STY` contro
    // `Long`/`Lat`). Il preflight legge la semantica dal catalogo e la porta al renderer insieme alla
    // semantica dei parametri. Una colonna di cui il piano non dice la
    // semantica resta rifiutata, ed e la sola risposta onesta — indovinare
    // renderebbe SQL valido su meta delle tabelle.
    SpatialFunction::X,
    SpatialFunction::Y,
];

const MAX_SPATIAL_OUTPUTS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpatialOutputContract {
    pub projection_index: usize,
    pub semantics: SpatialSemantics,
    pub srid: Option<u32>,
    pub dimensions: Dimensions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpatialValidation {
    pub outputs: Vec<SpatialOutputContract>,
    pub source_tokens: Vec<SpatialSourceToken>,
    /// La semantica di ogni colonna spatial che il piano nomina, letta dal
    /// catalogo. Il renderer ne ha bisogno per i membri che i due tipi CLR
    /// chiamano in modo diverso.
    pub column_semantics: BTreeMap<(Option<String>, String), SpatialSemantics>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpatialSourceToken {
    pub object: ObjectRef,
    pub token: crate::SqlServerSchemaToken,
}

/// La resa, con la semantica delle colonne che il preflight ha letto.
///
/// # Perche non basta la forma senza mappa
///
/// Perche due funzioni del contratto — `X` e `Y` — si scrivono con membri
/// diversi sui due tipi CLR, e senza sapere quale sia la colonna il renderer
/// non puo scrivere ne l'uno ne l'altro. Le rifiuta, ed e giusto: la mappa
/// vuota e la condizione di chi non ha ancora chiesto al catalogo.
///
/// # Errors
///
/// Come `render_query`, e in piu `Unsupported` per una funzione il cui membro
/// dipende dalla semantica su una colonna che la mappa non nomina.
pub fn render_query(
    operation: &QueryOperation,
    parameters: &ParameterBag,
    budget: &ResourceBudget,
    columns: &BTreeMap<(Option<String>, String), SpatialSemantics>,
) -> Result<RenderedSql> {
    validate_bound_spatial_arguments(operation, parameters)?;
    sql_server_renderer()
        .with_sql_server_spatial_parameters(spatial_parameter_profiles(parameters, budget)?)
        .with_sql_server_spatial_columns(columns.clone())
        .render_query(operation)
}

fn spatial_parameter_profiles(
    parameters: &ParameterBag,
    budget: &ResourceBudget,
) -> Result<BTreeMap<String, SqlServerSpatialParameter>> {
    let max_components = budget.limits().geometry_components;
    let max_depth = budget.limits().nesting_depth;
    let mut profiles = BTreeMap::new();
    for (name, value) in parameters.iter() {
        let ParameterValue::Wkb {
            bytes,
            srid,
            dimensions,
            semantics,
        } = value
        else {
            continue;
        };
        if u64::try_from(bytes.len()).map_or(true, |length| length > budget.limits().cell_bytes) {
            return Err(DatabaseError::resource_limit(
                "parametro WKB SQL Server oltre il limite cella",
            ));
        }
        let inspection =
            plenora_database_core::ewkb::inspect_ewkb_detailed(bytes, max_components, max_depth)?;
        if inspection.root.srid.is_some() {
            return Err(query_error(
                ErrorCategory::DataMapping,
                "parametro SQL Server richiede WKB senza SRID embedded",
            ));
        }
        let expected = match dimensions {
            plenora_database_core::geometry::Dimensions::Xy => "xy",
            plenora_database_core::geometry::Dimensions::Xyz => "xyz",
            plenora_database_core::geometry::Dimensions::Xym => "xym",
            plenora_database_core::geometry::Dimensions::Xyzm => "xyzm",
            plenora_database_core::geometry::Dimensions::Unknown => {
                return Err(query_error(
                    ErrorCategory::DataMapping,
                    "parametro WKB SQL Server richiede dimensioni risolte",
                ));
            }
        };
        if inspection.root.dimensions_label() != expected {
            return Err(query_error(
                ErrorCategory::DataMapping,
                "dimensioni parametro WKB diverse dal contratto",
            ));
        }
        let srid = srid.ok_or_else(|| {
            query_error(
                ErrorCategory::DataMapping,
                "parametro WKB SQL Server richiede SRID risolto",
            )
        })?;
        profiles.insert(
            name.clone(),
            SqlServerSpatialParameter {
                semantics: *semantics,
                srid,
            },
        );
    }
    Ok(profiles)
}

/// Descrive l'output senza eseguire il piano utente.
///
/// `sys.dm_exec_describe_first_result_set` applica le regole di binding e type
/// inference del server anche a CTE, join, aggregati, window e set operation.
/// I metadati TDS reali vengono ricontrollati dal worker al momento
/// dell'esecuzione.
pub async fn describe_query(
    session: &mut SqlServerSession,
    rendered: RenderedSql,
    parameters: &ParameterBag,
    cancellation: &CancellationToken,
) -> Result<SqlServerReadPlan> {
    let bind_names = rendered
        .binds
        .iter()
        .map(|bind| bind.name.clone())
        .collect::<Vec<_>>();
    let declarations = parameter_declarations(&bind_names, parameters)?;
    let mut query = Query::new(DESCRIBE_QUERY_SQL);
    query.bind(rendered.sql.clone());
    query.bind(declarations);
    let mut results = session
        .execute_query(query, ErrorPhase::Prepare, cancellation)
        .await?;
    if results.len() != 1 {
        return Err(query_error(
            ErrorCategory::Protocol,
            "descrizione QueryOperation SQL Server con numero result set inatteso",
        ));
    }
    let rows = results.pop().ok_or_else(|| {
        query_error(
            ErrorCategory::Protocol,
            "descrizione QueryOperation SQL Server senza result set",
        )
    })?;
    let columns = result_columns(&rows)?;
    SqlServerReadPlan::from_query_result(rendered.sql, bind_names, columns)
}

async fn describe_native_spatial_types(
    session: &mut SqlServerSession,
    sql: String,
    bind_names: &[String],
    parameters: &ParameterBag,
    cancellation: &CancellationToken,
) -> Result<Vec<Option<SpatialSemantics>>> {
    let declarations = parameter_declarations(bind_names, parameters)?;
    let mut query = Query::new(DESCRIBE_QUERY_SQL);
    query.bind(sql);
    query.bind(declarations);
    let mut results = session
        .execute_query(query, ErrorPhase::Prepare, cancellation)
        .await?;
    if results.len() != 1 {
        return Err(query_error(
            ErrorCategory::Protocol,
            "descrizione spatial nativa con numero result set inatteso",
        ));
    }
    let rows = results.pop().ok_or_else(|| {
        query_error(
            ErrorCategory::Protocol,
            "descrizione spatial nativa senza result set",
        )
    })?;
    if rows.is_empty() {
        return Err(query_error(
            ErrorCategory::Schema,
            "descrizione spatial nativa senza colonne",
        ));
    }
    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            if let Some(number) = optional::<i32>(row, 6, "error_number")? {
                return Err(query_error(
                    ErrorCategory::Schema,
                    format!("descrizione spatial nativa fallita (codice {number})"),
                ));
            }
            let ordinal = required::<i32>(row, 0, "column_ordinal")?;
            let expected = i32::try_from(index + 1).map_err(|_| {
                query_error(
                    ErrorCategory::ResourceLimit,
                    "numero colonne spatial native non rappresentabile",
                )
            })?;
            if ordinal != expected {
                return Err(query_error(
                    ErrorCategory::Protocol,
                    "ordinali descrizione spatial nativa non contigui",
                ));
            }
            let user_type = optional::<&str>(row, 5, "user_type_name")?;
            let system_type = optional::<&str>(row, 3, "system_type_name")?;
            Ok(spatial_semantics_from_type_name(user_type)
                .or_else(|| spatial_semantics_from_type_name(system_type)))
        })
        .collect()
}

fn spatial_semantics_from_type_name(value: Option<&str>) -> Option<SpatialSemantics> {
    let terminal = value?
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .trim_matches(|character| character == '[' || character == ']')
        .to_ascii_lowercase();
    match terminal.as_str() {
        "geometry" => Some(SpatialSemantics::Geometry),
        "geography" => Some(SpatialSemantics::Geography),
        _ => None,
    }
}

#[allow(clippy::too_many_lines)]
pub async fn validate_spatial_inputs(
    session: &mut SqlServerSession,
    operation: &QueryOperation,
    parameters: &ParameterBag,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<SpatialValidation> {
    if !operation_has_spatial(operation) {
        return Ok(SpatialValidation {
            outputs: Vec::new(),
            column_semantics: BTreeMap::new(),
            source_tokens: Vec::new(),
        });
    }
    let mut uses = Vec::new();
    let mut column_semantics = BTreeMap::new();
    collect_operation_spatial_uses(operation, &mut uses)?;
    if operation
        .joins
        .iter()
        .any(|join| join.source.is_none() && join.derived_source.is_none())
    {
        return Err(query_error(
            ErrorCategory::Unsupported,
            "AST spatial SQL Server contiene una relazione join non risolta",
        ));
    }
    if !operation.common_table_expressions.is_empty()
        || operation.derived_source.is_some()
        || operation
            .joins
            .iter()
            .any(|join| join.derived_source.is_some())
        || !operation.set_operations.is_empty()
        || operation_contains_spatial_subquery(operation)
    {
        return validate_nested_spatial_inputs(
            session,
            operation,
            parameters,
            budget,
            cancellation,
        )
        .await;
    }
    if uses.is_empty() {
        return Err(query_error(
            ErrorCategory::Unsupported,
            "AST spatial SQL Server non risolto sulla query principale",
        ));
    }
    let primary_source = operation.source.as_ref().ok_or_else(|| {
        query_error(
            ErrorCategory::InvalidPlan,
            "AST spatial SQL Server senza source fisica",
        )
    })?;
    let mut query_sources = vec![primary_source];
    query_sources.extend(
        operation
            .joins
            .iter()
            .filter_map(|join| join.source.as_ref()),
    );
    let mut sources = Vec::with_capacity(query_sources.len());
    let mut source_relations = BTreeSet::new();
    for source in query_sources {
        let relation = source
            .alias
            .as_deref()
            .unwrap_or(&source.object.object)
            .to_owned();
        if !source_relations.insert(relation.clone()) {
            return Err(query_error(
                ErrorCategory::InvalidPlan,
                "alias sorgente SQL Server duplicato nella query spatial",
            ));
        }
        let schema = source.object.schema.as_deref().unwrap_or("dbo");
        let description =
            crate::catalog::describe_object(session, schema, &source.object.object, cancellation)
                .await?;
        sources.push(SpatialPhysicalSource {
            relation,
            source: source.clone(),
            description,
        });
    }
    let mut checked_uses = BTreeSet::new();
    for usage in uses {
        let use_identity = (
            usage.column.relation.clone(),
            usage.column.field.clone(),
            usage.argument.clone(),
        );
        if !checked_uses.insert(use_identity) {
            continue;
        }
        let receiver_source = resolve_spatial_source(&usage.column, &sources)?;
        let column = receiver_source
            .description
            .columns
            .iter()
            .find(|column| column.name == usage.column.field)
            .ok_or_else(|| {
                query_error(
                    ErrorCategory::Schema,
                    "colonna spatial SQL Server assente dal catalogo",
                )
            })?;
        let observed_semantics = match column.native_type.as_str() {
            "geometry" => plenora_database_core::geometry::SpatialSemantics::Geometry,
            "geography" => plenora_database_core::geometry::SpatialSemantics::Geography,
            _ => {
                return Err(query_error(
                    ErrorCategory::DataMapping,
                    "funzione spatial applicata a colonna SQL Server non spatial",
                ));
            }
        };
        // Sette funzioni esistono su `geometry` e non su `geography`, e la
        // capability le pubblica soltanto nella voce di `geometry`. Qui si
        // impedisce di chiamarle sull'altro tipo, ed e il solo posto che
        // puo: il renderer il tipo della colonna non lo conosce, e il server
        // risponderebbe «Could not find method» — vero e inutile, perche non
        // direbbe che la funzione c'e e che la colonna e quella sbagliata.
        if observed_semantics == plenora_database_core::geometry::SpatialSemantics::Geography
            && GEOMETRY_ONLY_SPATIAL_FUNCTIONS.contains(&usage.function)
        {
            return Err(query_error(
                ErrorCategory::Unsupported,
                "funzione spatial offerta solo su geometry, chiamata su una colonna geography",
            ));
        }
        // La stessa lettura che serve a validare serve a rendere: qui la
        // semantica c'e gia, e senza questa riga il renderer avrebbe dovuto
        // chiederla una seconda volta al catalogo.
        column_semantics.insert(
            (usage.column.relation.clone(), usage.column.field.clone()),
            observed_semantics,
        );
        match &usage.argument {
            SpatialArgument::None => {}
            SpatialArgument::PointIndex(name) => {
                if !matches!(parameters.get(name), Some(ParameterValue::I32(value)) if *value >= 1)
                {
                    return Err(query_error(
                        ErrorCategory::InvalidPlan,
                        "STPointN SQL Server richiede un indice int bindato maggiore o uguale a 1",
                    ));
                }
            }
            SpatialArgument::Distance(name) => {
                if !matches!(parameters.get(name), Some(ParameterValue::F64(value)) if value.is_finite())
                {
                    return Err(query_error(
                        ErrorCategory::InvalidPlan,
                        "argomento numerico di lunghezza SQL Server: richiede un float finito bindato",
                    ));
                }
            }
            SpatialArgument::Geometry(name) => {
                let ParameterValue::Wkb {
                    srid: Some(expected_srid),
                    semantics,
                    ..
                } = parameters.get(name).ok_or_else(|| {
                    query_error(
                        ErrorCategory::InvalidPlan,
                        "parametro spatial SQL Server mancante",
                    )
                })?
                else {
                    return Err(query_error(
                        ErrorCategory::DataMapping,
                        "operando spatial SQL Server richiede un parametro WKB risolto",
                    ));
                };
                if *semantics != observed_semantics {
                    return Err(query_error(
                        ErrorCategory::DataMapping,
                        "semantica parametro spatial diversa dalla colonna SQL Server",
                    ));
                }
                if observe_spatial_srid(session, receiver_source, &usage.column.field, cancellation)
                    .await?
                    .is_some_and(|observed| u32::try_from(observed).ok() != Some(*expected_srid))
                {
                    return Err(query_error(
                        ErrorCategory::DataMapping,
                        "SRID parametro spatial diverso dalla colonna SQL Server",
                    ));
                }
            }
            SpatialArgument::GeometryColumn(other) => {
                let other_source =
                    resolve_spatial_source_parts(other.relation.as_deref(), &sources)?;
                let other_column = other_source
                    .description
                    .columns
                    .iter()
                    .find(|column| column.name == other.field)
                    .ok_or_else(|| {
                        query_error(
                            ErrorCategory::Schema,
                            "operando colonna spatial SQL Server assente dal catalogo",
                        )
                    })?;
                let other_semantics = match other_column.native_type.as_str() {
                    "geometry" => SpatialSemantics::Geometry,
                    "geography" => SpatialSemantics::Geography,
                    _ => {
                        return Err(query_error(
                            ErrorCategory::DataMapping,
                            "operando colonna SQL Server non spatial",
                        ));
                    }
                };
                if observed_semantics != other_semantics {
                    return Err(query_error(
                        ErrorCategory::DataMapping,
                        "semantica diversa fra colonne spatial SQL Server",
                    ));
                }
                let receiver_srid = observe_spatial_srid(
                    session,
                    receiver_source,
                    &usage.column.field,
                    cancellation,
                )
                .await?;
                let other_srid =
                    observe_spatial_srid(session, other_source, &other.field, cancellation).await?;
                if matches!((receiver_srid, other_srid), (Some(left), Some(right)) if left != right)
                {
                    return Err(query_error(
                        ErrorCategory::DataMapping,
                        "SRID diverso fra colonne spatial SQL Server",
                    ));
                }
            }
        }
    }
    let outputs =
        profile_spatial_outputs(session, operation, parameters, budget, cancellation).await?;
    Ok(SpatialValidation {
        outputs,
        column_semantics,
        source_tokens: sources
            .into_iter()
            .map(|source| SpatialSourceToken {
                object: source.source.object,
                token: source.description.token,
            })
            .collect(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObservedSpatialColumn {
    semantics: SpatialSemantics,
    srid: Option<u32>,
}

#[allow(clippy::too_many_lines)]
async fn validate_nested_spatial_inputs(
    session: &mut SqlServerSession,
    operation: &QueryOperation,
    parameters: &ParameterBag,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<SpatialValidation> {
    let mut objects = Vec::new();
    collect_physical_spatial_sources(operation, &BTreeSet::new(), &mut objects)?;
    let mut seen = BTreeSet::new();
    let mut source_tokens = Vec::new();
    for object in objects {
        let identity = (
            object.catalog.clone(),
            object.schema.clone(),
            object.object.clone(),
        );
        if !seen.insert(identity) {
            continue;
        }
        let schema = object.schema.as_deref().unwrap_or("dbo");
        let description =
            crate::catalog::describe_object(session, schema, &object.object, cancellation).await?;
        source_tokens.push(SpatialSourceToken {
            object,
            token: description.token,
        });
    }

    validate_nested_spatial_operation(session, operation, parameters, budget, cancellation).await?;
    let outputs =
        profile_spatial_outputs(session, operation, parameters, budget, cancellation).await?;
    Ok(SpatialValidation {
        outputs,
        column_semantics: BTreeMap::new(),
        source_tokens,
    })
}

#[allow(clippy::too_many_lines)]
fn validate_nested_spatial_operation<'a>(
    session: &'a mut SqlServerSession,
    operation: &'a QueryOperation,
    parameters: &'a ParameterBag,
    budget: &'a ResourceBudget,
    cancellation: &'a CancellationToken,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        if operation
            .derived_source
            .as_ref()
            .is_some_and(|derived| !derived.query.common_table_expressions.is_empty())
            || operation.joins.iter().any(|join| {
                join.derived_source
                    .as_ref()
                    .is_some_and(|derived| !derived.query.common_table_expressions.is_empty())
            })
        {
            return Err(query_error(
                ErrorCategory::Unsupported,
                "SQL Server 2022 non ammette CTE dichiarate dentro derived source",
            ));
        }
        let mut uses = Vec::new();
        collect_operation_spatial_uses(operation, &mut uses)?;
        let local_relations = local_relation_names(operation);
        for usage in &uses {
            validate_local_spatial_relation(usage.column.relation.as_deref(), &local_relations)?;
            if let SpatialArgument::GeometryColumn(other) = &usage.argument {
                validate_local_spatial_relation(other.relation.as_deref(), &local_relations)?;
            }
        }
        let mut observed = BTreeMap::new();
        for usage in &uses {
            let receiver = SpatialColumnRef {
                relation: usage.column.relation.clone(),
                field: usage.column.field.clone(),
            };
            if !observed.contains_key(&receiver) {
                let profile = observe_scoped_spatial_column(
                    session,
                    operation,
                    &receiver,
                    parameters,
                    budget,
                    cancellation,
                )
                .await?;
                observed.insert(receiver.clone(), profile);
            }
            if let SpatialArgument::GeometryColumn(other) = &usage.argument {
                if !observed.contains_key(other) {
                    let profile = observe_scoped_spatial_column(
                        session,
                        operation,
                        other,
                        parameters,
                        budget,
                        cancellation,
                    )
                    .await?;
                    observed.insert(other.clone(), profile);
                }
            }
        }
        for usage in uses {
            let receiver = SpatialColumnRef {
                relation: usage.column.relation,
                field: usage.column.field,
            };
            let receiver_profile = observed.get(&receiver).ok_or_else(|| {
                query_error(
                    ErrorCategory::Protocol,
                    "profilo ricevitore spatial SQL Server non disponibile",
                )
            })?;
            match usage.argument {
                SpatialArgument::None => {}
                SpatialArgument::PointIndex(name) => {
                    if !matches!(
                        parameters.get(&name),
                        Some(ParameterValue::I32(value)) if *value >= 1
                    ) {
                        return Err(query_error(
                            ErrorCategory::InvalidPlan,
                            "STPointN SQL Server richiede un indice int bindato maggiore o uguale a 1",
                        ));
                    }
                }
                SpatialArgument::Distance(name) => {
                    if !matches!(
                        parameters.get(&name),
                        Some(ParameterValue::F64(value)) if value.is_finite()
                    ) {
                        return Err(query_error(
                            ErrorCategory::InvalidPlan,
                            "argomento numerico di lunghezza SQL Server: richiede un float finito bindato",
                        ));
                    }
                }
                SpatialArgument::Geometry(name) => {
                    let ParameterValue::Wkb {
                        srid: Some(expected_srid),
                        semantics,
                        ..
                    } = parameters.get(&name).ok_or_else(|| {
                        query_error(
                            ErrorCategory::InvalidPlan,
                            "parametro spatial SQL Server mancante",
                        )
                    })?
                    else {
                        return Err(query_error(
                            ErrorCategory::DataMapping,
                            "operando spatial SQL Server richiede un parametro WKB risolto",
                        ));
                    };
                    if *semantics != receiver_profile.semantics {
                        return Err(query_error(
                            ErrorCategory::DataMapping,
                            "semantica parametro spatial diversa dallo scope SQL Server",
                        ));
                    }
                    if receiver_profile
                        .srid
                        .is_some_and(|observed_srid| observed_srid != *expected_srid)
                    {
                        return Err(query_error(
                            ErrorCategory::DataMapping,
                            "SRID parametro spatial diverso dallo scope SQL Server",
                        ));
                    }
                }
                SpatialArgument::GeometryColumn(other) => {
                    let other_profile = observed.get(&other).ok_or_else(|| {
                        query_error(
                            ErrorCategory::Protocol,
                            "profilo secondo operando spatial SQL Server non disponibile",
                        )
                    })?;
                    if receiver_profile.semantics != other_profile.semantics {
                        return Err(query_error(
                            ErrorCategory::DataMapping,
                            "semantica diversa fra colonne spatial SQL Server",
                        ));
                    }
                    if matches!(
                        (receiver_profile.srid, other_profile.srid),
                        (Some(left), Some(right)) if left != right
                    ) {
                        return Err(query_error(
                            ErrorCategory::DataMapping,
                            "SRID diverso fra colonne spatial SQL Server",
                        ));
                    }
                }
            }
        }

        for cte in &operation.common_table_expressions {
            validate_nested_spatial_operation(
                session,
                &cte.query,
                parameters,
                budget,
                cancellation,
            )
            .await?;
        }
        if let Some(derived) = &operation.derived_source {
            validate_nested_spatial_operation(
                session,
                &derived.query,
                parameters,
                budget,
                cancellation,
            )
            .await?;
        }
        for join in &operation.joins {
            if let Some(derived) = &join.derived_source {
                validate_nested_spatial_operation(
                    session,
                    &derived.query,
                    parameters,
                    budget,
                    cancellation,
                )
                .await?;
            }
            if let Some(on) = &join.on {
                validate_spatial_subqueries(session, on, parameters, budget, cancellation).await?;
            }
        }
        for set in &operation.set_operations {
            validate_nested_spatial_operation(
                session,
                &set.query,
                parameters,
                budget,
                cancellation,
            )
            .await?;
        }
        for expression in operation.clause_expressions() {
            validate_spatial_subqueries(session, expression, parameters, budget, cancellation)
                .await?;
        }
        Ok(())
    })
}

async fn observe_scoped_spatial_column(
    session: &mut SqlServerSession,
    operation: &QueryOperation,
    column: &SpatialColumnRef,
    parameters: &ParameterBag,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<ObservedSpatialColumn> {
    let column_expression = QueryExpression::Column {
        column: ColumnRef {
            relation: column.relation.clone(),
            field: column.field.clone(),
        },
    };
    let native_probe = scoped_probe_operation(
        operation,
        QueryProjection {
            expression: column_expression.clone(),
            alias: Some("_plenora_spatial_input".to_owned()),
        },
        false,
    );
    let sql_renderer = sql_server_renderer()
        .with_sql_server_spatial_parameters(spatial_parameter_profiles(parameters, budget)?);
    let native_rendered = sql_renderer.render_query_native_spatial(&native_probe)?;
    let bind_names = native_rendered
        .binds
        .iter()
        .map(|bind| bind.name.clone())
        .collect::<Vec<_>>();
    let native_parameters = parameters_for_binds(parameters, &bind_names)?;
    let semantics = describe_native_spatial_types(
        session,
        native_rendered.sql,
        &bind_names,
        &native_parameters,
        cancellation,
    )
    .await?
    .first()
    .copied()
    .flatten()
    .ok_or_else(|| {
        query_error(
            ErrorCategory::DataMapping,
            "colonna scope SQL Server non descritta come geometry/geography",
        )
    })?;

    let srid_probe = scoped_probe_operation(
        operation,
        QueryProjection {
            expression: QueryExpression::Spatial {
                function: SpatialFunction::Srid,
                arguments: vec![column_expression],
            },
            alias: Some("_plenora_spatial_srid".to_owned()),
        },
        true,
    );
    let srid_rendered = sql_renderer.render_query_native_spatial(&srid_probe)?;
    let bind_names = srid_rendered
        .binds
        .iter()
        .map(|bind| bind.name.clone())
        .collect::<Vec<_>>();
    let srid_parameters = parameters_for_binds(parameters, &bind_names)?;
    let mut query = Query::new(srid_rendered.sql);
    bind_parameters(&mut query, &bind_names, &srid_parameters)?;
    let mut results = session
        .execute_query(query, ErrorPhase::Prepare, cancellation)
        .await?;
    if results.len() != 1 {
        return Err(query_error(
            ErrorCategory::Protocol,
            "profilo SRID scope SQL Server con numero result set inatteso",
        ));
    }
    let rows = results.pop().ok_or_else(|| {
        query_error(
            ErrorCategory::Protocol,
            "profilo SRID scope SQL Server senza result set",
        )
    })?;
    let mut srids = BTreeSet::new();
    for row in &rows {
        if let Some(srid) = optional::<i32>(row, 0, "SRID scope spatial")? {
            let srid = u32::try_from(srid).map_err(|_| {
                query_error(
                    ErrorCategory::DataMapping,
                    "SRID scope spatial SQL Server negativo",
                )
            })?;
            srids.insert(srid);
        }
    }
    if srids.len() > 1 {
        return Err(query_error(
            ErrorCategory::DataMapping,
            "colonna scope spatial SQL Server con SRID misti",
        ));
    }
    Ok(ObservedSpatialColumn {
        semantics,
        srid: srids.into_iter().next(),
    })
}

fn parameters_for_binds(parameters: &ParameterBag, bind_names: &[String]) -> Result<ParameterBag> {
    let mut values = BTreeMap::new();
    for name in bind_names {
        let value = parameters.get(name).ok_or_else(|| {
            query_error(
                ErrorCategory::InvalidPlan,
                "parametro preflight spatial SQL Server mancante",
            )
        })?;
        values.insert(name.clone(), value.clone());
    }
    Ok(ParameterBag::new(values))
}

fn scoped_probe_operation(
    operation: &QueryOperation,
    projection: QueryProjection,
    distinct: bool,
) -> QueryOperation {
    let mut probe = operation.clone();
    probe.projection = vec![projection];
    // Il predicato può essere proprio l'uso spatial da validare. Profilare
    // dopo il filtro permetterebbe a un mismatch SRID di produrre zero righe
    // e quindi di nascondere il contratto della sorgente.
    probe.filter = None;
    probe.group_by.clear();
    probe.having = None;
    probe.order_by.clear();
    probe.distinct = distinct;
    probe.distinct_on.clear();
    probe.set_operations.clear();
    probe.row_limit = distinct.then_some(2);
    probe.row_offset = None;
    probe.locking = None;
    probe
}

fn local_relation_names(operation: &QueryOperation) -> BTreeSet<String> {
    let mut relations = BTreeSet::new();
    if let Some(source) = &operation.source {
        relations.insert(
            source
                .alias
                .clone()
                .unwrap_or_else(|| source.object.object.clone()),
        );
    }
    if let Some(derived) = &operation.derived_source {
        relations.insert(derived.alias.clone());
    }
    for join in &operation.joins {
        if let Some(source) = &join.source {
            relations.insert(
                source
                    .alias
                    .clone()
                    .unwrap_or_else(|| source.object.object.clone()),
            );
        }
        if let Some(derived) = &join.derived_source {
            relations.insert(derived.alias.clone());
        }
    }
    relations
}

fn validate_local_spatial_relation(
    relation: Option<&str>,
    local_relations: &BTreeSet<String>,
) -> Result<()> {
    if relation.is_some_and(|relation| !local_relations.contains(relation)) {
        return Err(query_error(
            ErrorCategory::Unsupported,
            "subquery spatial correlata SQL Server non ancora qualificata",
        ));
    }
    Ok(())
}

async fn validate_spatial_subqueries(
    session: &mut SqlServerSession,
    expression: &QueryExpression,
    parameters: &ParameterBag,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<()> {
    let mut nested = Vec::new();
    walk_query_expression(expression, |node| match node {
        QueryWalkNode::Operation(operation) => {
            nested.push(operation);
            QueryWalkControl::Skip
        }
        QueryWalkNode::Expression(_) | QueryWalkNode::Source(_) => QueryWalkControl::Continue,
    });
    for operation in nested {
        validate_nested_spatial_operation(session, operation, parameters, budget, cancellation)
            .await?;
    }
    Ok(())
}

fn collect_physical_spatial_sources(
    operation: &QueryOperation,
    inherited_ctes: &BTreeSet<String>,
    objects: &mut Vec<ObjectRef>,
) -> Result<()> {
    let mut visible_ctes = inherited_ctes.clone();
    visible_ctes.extend(
        operation
            .common_table_expressions
            .iter()
            .map(|cte| cte.name.clone()),
    );
    for cte in &operation.common_table_expressions {
        collect_physical_spatial_sources(&cte.query, &visible_ctes, objects)?;
    }
    if let Some(source) = &operation.source {
        collect_physical_source(source, &visible_ctes, objects);
    }
    if let Some(derived) = &operation.derived_source {
        collect_physical_spatial_sources(&derived.query, &visible_ctes, objects)?;
    }
    for join in &operation.joins {
        if let Some(source) = &join.source {
            collect_physical_source(source, &visible_ctes, objects);
        }
        if let Some(derived) = &join.derived_source {
            collect_physical_spatial_sources(&derived.query, &visible_ctes, objects)?;
        }
        if let Some(on) = &join.on {
            collect_expression_physical_sources(on, &visible_ctes, objects)?;
        }
    }
    for expression in operation.clause_expressions() {
        collect_expression_physical_sources(expression, &visible_ctes, objects)?;
    }
    for set in &operation.set_operations {
        collect_physical_spatial_sources(&set.query, &visible_ctes, objects)?;
    }
    Ok(())
}

fn collect_physical_source(
    source: &QuerySource,
    visible_ctes: &BTreeSet<String>,
    objects: &mut Vec<ObjectRef>,
) {
    let is_cte = source.object.catalog.is_none()
        && source.object.schema.is_none()
        && visible_ctes.contains(&source.object.object);
    if !is_cte {
        objects.push(source.object.clone());
    }
}

fn collect_expression_physical_sources(
    expression: &QueryExpression,
    visible_ctes: &BTreeSet<String>,
    objects: &mut Vec<ObjectRef>,
) -> Result<()> {
    let mut nested = Vec::new();
    walk_query_expression(expression, |node| match node {
        QueryWalkNode::Operation(operation) => {
            nested.push(operation);
            QueryWalkControl::Skip
        }
        QueryWalkNode::Expression(_) | QueryWalkNode::Source(_) => QueryWalkControl::Continue,
    });
    for operation in nested {
        collect_physical_spatial_sources(operation, visible_ctes, objects)?;
    }
    Ok(())
}

struct SpatialPhysicalSource {
    relation: String,
    source: QuerySource,
    description: crate::catalog::SqlServerObjectDescription,
}

fn resolve_spatial_source<'a>(
    column: &ColumnRef,
    sources: &'a [SpatialPhysicalSource],
) -> Result<&'a SpatialPhysicalSource> {
    resolve_spatial_source_parts(column.relation.as_deref(), sources)
}

fn resolve_spatial_source_parts<'a>(
    relation: Option<&str>,
    sources: &'a [SpatialPhysicalSource],
) -> Result<&'a SpatialPhysicalSource> {
    if let Some(relation) = relation {
        return sources
            .iter()
            .find(|source| source.relation == relation)
            .ok_or_else(|| {
                query_error(
                    ErrorCategory::Schema,
                    "relazione colonna spatial non risolta sulle source SQL Server",
                )
            });
    }
    if sources.len() == 1 {
        return Ok(&sources[0]);
    }
    Err(query_error(
        ErrorCategory::Schema,
        "colonna spatial non qualificata in query SQL Server con più source",
    ))
}

async fn observe_spatial_srid(
    session: &mut SqlServerSession,
    source: &SpatialPhysicalSource,
    column: &str,
    cancellation: &CancellationToken,
) -> Result<Option<i32>> {
    let renderer = sql_server_renderer();
    let schema = source.source.object.schema.as_deref().unwrap_or("dbo");
    let quoted_schema =
        renderer.quote_identifier(&plenora_database_sql::Identifier::new(schema)?)?;
    let quoted_object = renderer.quote_identifier(&plenora_database_sql::Identifier::new(
        source.source.object.object.clone(),
    )?)?;
    let quoted_column =
        renderer.quote_identifier(&plenora_database_sql::Identifier::new(column)?)?;
    let sql = format!(
        "SELECT COUNT_BIG(DISTINCT {quoted_column}.STSrid), \
         MIN({quoted_column}.STSrid) FROM {quoted_schema}.{quoted_object} \
         WHERE {quoted_column} IS NOT NULL;"
    );
    let mut results = session
        .execute_query(Query::new(sql), ErrorPhase::Prepare, cancellation)
        .await?;
    let rows = results.pop().ok_or_else(|| {
        query_error(
            ErrorCategory::Protocol,
            "preflight SRID AST spatial senza result set",
        )
    })?;
    if !results.is_empty() || rows.len() != 1 {
        return Err(query_error(
            ErrorCategory::Protocol,
            "preflight SRID AST spatial con cardinalita inattesa",
        ));
    }
    let distinct: i64 = required(&rows[0], 0, "numero SRID spatial")?;
    if distinct > 1 {
        return Err(query_error(
            ErrorCategory::DataMapping,
            "colonna AST spatial SQL Server con SRID misti",
        ));
    }
    optional(&rows[0], 1, "SRID spatial")
}

#[derive(Debug, Clone, Copy)]
struct SpatialOutputCandidate {
    projection_index: usize,
}

#[allow(clippy::too_many_lines)]
async fn profile_spatial_outputs(
    session: &mut SqlServerSession,
    operation: &QueryOperation,
    parameters: &ParameterBag,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<Vec<SpatialOutputContract>> {
    let mut candidates = Vec::new();
    for (projection_index, projection) in operation.projection.iter().enumerate() {
        let QueryExpression::Spatial {
            function,
            arguments,
        } = &projection.expression
        else {
            continue;
        };
        if !function.returns_geometry() {
            continue;
        }
        // `returns_geometry` identifica gia la forma; qui resta da verificare
        // soltanto che la tabella autorevole del provider offra la funzione.
        if !sql_server_offers(*function) {
            return Err(query_error(
                ErrorCategory::Unsupported,
                "output spatial SQL Server fuori dal sottoinsieme verificato",
            ));
        }
        let QueryExpression::Column { .. } = &arguments[0] else {
            return Err(query_error(
                ErrorCategory::Unsupported,
                "output spatial SQL Server richiede una colonna come ricevitore",
            ));
        };
        candidates.push(SpatialOutputCandidate { projection_index });
    }
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    if candidates.len() > MAX_SPATIAL_OUTPUTS {
        return Err(DatabaseError::resource_limit(
            "troppi output spatial SQL Server da profilare",
        ));
    }

    let mut profile_operation = operation.clone();
    for (index, projection) in profile_operation.projection.iter_mut().enumerate() {
        projection.alias = Some(format!("_plenora_spatial_profile_{index}"));
    }
    if profile_operation.row_limit.is_none() && profile_operation.row_offset.is_none() {
        profile_operation.order_by.clear();
    }
    let rendered = sql_server_renderer()
        .with_sql_server_spatial_parameters(spatial_parameter_profiles(parameters, budget)?)
        .render_query_native_spatial_parts(&profile_operation)?;
    let bind_names = rendered
        .binds
        .iter()
        .map(|bind| bind.name.clone())
        .collect::<Vec<_>>();
    let native_types = describe_native_spatial_types(
        session,
        format!("{}{}", rendered.with_clause, rendered.body),
        &bind_names,
        parameters,
        cancellation,
    )
    .await?;
    let identifier_renderer = sql_server_renderer();
    let derived_alias = identifier_renderer.quote_identifier(
        &plenora_database_sql::Identifier::new("_plenora_spatial_result")?,
    )?;
    let mut contracts = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let semantics = native_types
            .get(candidate.projection_index)
            .copied()
            .flatten()
            .ok_or_else(|| {
                query_error(
                    ErrorCategory::DataMapping,
                    "output spatial SQL Server non descritto come geometry/geography",
                )
            })?;
        let field =
            identifier_renderer.quote_identifier(&plenora_database_sql::Identifier::new(
                format!("_plenora_spatial_profile_{}", candidate.projection_index),
            )?)?;
        let value = format!("{derived_alias}.{field}");
        let sql = format!(
            "{}SELECT \
             COUNT_BIG(DISTINCT CASE WHEN {value} IS NULL THEN NULL ELSE {value}.STSrid END), \
             MIN(CASE WHEN {value} IS NULL THEN NULL ELSE {value}.STSrid END), \
             COALESCE(SUM(CONVERT(bigint, CASE WHEN {value}.STGeometryType() = N'FullGlobe' \
             THEN 1 ELSE 0 END)), 0), \
             COUNT_BIG(DISTINCT CASE WHEN {value} IS NULL THEN NULL ELSE \
             CONVERT(int, {value}.HasZ) * 2 + CONVERT(int, {value}.HasM) END), \
             MIN(CASE WHEN {value} IS NULL THEN NULL ELSE \
             CONVERT(int, {value}.HasZ) * 2 + CONVERT(int, {value}.HasM) END) \
             FROM ({}) AS {derived_alias};",
            rendered.with_clause, rendered.body
        );
        let mut query = Query::new(sql);
        bind_parameters(&mut query, &bind_names, parameters)?;
        let mut results = session
            .execute_query(query, ErrorPhase::Prepare, cancellation)
            .await?;
        if results.len() != 1 || results.first().is_none_or(|rows| rows.len() != 1) {
            return Err(query_error(
                ErrorCategory::Protocol,
                "profilo output spatial SQL Server con cardinalita inattesa",
            ));
        }
        let row = results
            .pop()
            .and_then(|mut rows| rows.pop())
            .ok_or_else(|| {
                query_error(
                    ErrorCategory::Protocol,
                    "profilo output spatial SQL Server senza riga",
                )
            })?;
        let srid_count: i64 = required(&row, 0, "numero SRID output spatial")?;
        let srid: Option<i32> = optional(&row, 1, "SRID output spatial")?;
        let full_globe: i64 = required(&row, 2, "FullGlobe output spatial")?;
        let dimension_count: i64 = required(&row, 3, "numero profili dimensionali output spatial")?;
        let dimension_code: Option<i32> = optional(&row, 4, "profilo dimensionale output spatial")?;
        if srid_count > 1 {
            return Err(query_error(
                ErrorCategory::DataMapping,
                "output spatial SQL Server con SRID misti",
            ));
        }
        if full_globe > 0 {
            return Err(query_error(
                ErrorCategory::Unsupported,
                "output FullGlobe SQL Server non rappresentabile nel profilo WKB",
            ));
        }
        let srid = srid
            .map(|value| {
                u32::try_from(value).map_err(|_| {
                    query_error(
                        ErrorCategory::DataMapping,
                        "SRID output spatial SQL Server negativo",
                    )
                })
            })
            .transpose()?;
        let dimensions =
            crate::types::spatial_dimensions_from_profile(dimension_count, dimension_code)?;
        contracts.push(SpatialOutputContract {
            projection_index: candidate.projection_index,
            semantics,
            srid,
            dimensions,
        });
    }
    Ok(contracts)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum SpatialArgument {
    None,
    Geometry(String),
    GeometryColumn(SpatialColumnRef),
    PointIndex(String),
    Distance(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SpatialColumnRef {
    relation: Option<String>,
    field: String,
}

fn validate_bound_spatial_arguments(
    operation: &QueryOperation,
    parameters: &ParameterBag,
) -> Result<()> {
    let mut uses = Vec::new();
    collect_operation_spatial_uses(operation, &mut uses)?;
    for usage in uses {
        match usage.argument {
            SpatialArgument::PointIndex(name)
                if !matches!(
                    parameters.get(&name),
                    Some(ParameterValue::I32(value)) if *value >= 1
                ) =>
            {
                return Err(query_error(
                    ErrorCategory::InvalidPlan,
                    "STPointN SQL Server richiede un indice int bindato maggiore o uguale a 1",
                ));
            }
            SpatialArgument::Distance(name)
                if !matches!(
                    parameters.get(&name),
                    Some(ParameterValue::F64(value)) if value.is_finite()
                ) =>
            {
                return Err(query_error(
                    ErrorCategory::InvalidPlan,
                    "argomento numerico di lunghezza SQL Server: richiede un float finito bindato",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Debug)]
struct SpatialUse {
    column: ColumnRef,
    argument: SpatialArgument,
    /// Quale funzione ha usato quella colonna.
    ///
    /// Serve per applicare le capability per semantica: alcune funzioni sono
    /// qualificate su `geometry` ma non su `geography`.
    function: SpatialFunction,
}

fn collect_operation_spatial_uses(
    operation: &QueryOperation,
    uses: &mut Vec<SpatialUse>,
) -> Result<()> {
    for expression in operation.clause_expressions() {
        collect_expression_spatial_uses(expression, uses)?;
    }
    for join in &operation.joins {
        if let Some(on) = &join.on {
            collect_expression_spatial_uses(on, uses)?;
        }
    }
    Ok(())
}

fn collect_expression_spatial_uses(
    expression: &QueryExpression,
    uses: &mut Vec<SpatialUse>,
) -> Result<()> {
    let mut failure = None;
    walk_query_expression(expression, |node| match node {
        QueryWalkNode::Expression(QueryExpression::Spatial {
            function,
            arguments,
        }) => match collect_spatial_use(*function, arguments, uses) {
            Ok(()) => QueryWalkControl::Skip,
            Err(error) => {
                failure = Some(error);
                QueryWalkControl::Break
            }
        },
        QueryWalkNode::Operation(_) => QueryWalkControl::Skip,
        QueryWalkNode::Expression(_) | QueryWalkNode::Source(_) => QueryWalkControl::Continue,
    });
    failure.map_or(Ok(()), Err)
}

#[allow(clippy::too_many_lines)]
fn collect_spatial_use(
    function: SpatialFunction,
    arguments: &[QueryExpression],
    uses: &mut Vec<SpatialUse>,
) -> Result<()> {
    let QueryExpression::Column { column } = arguments.first().ok_or_else(|| {
        query_error(
            ErrorCategory::InvalidPlan,
            "AST spatial SQL Server senza ricevitore",
        )
    })?
    else {
        return Err(query_error(
            ErrorCategory::Unsupported,
            "AST spatial SQL Server richiede una colonna come ricevitore",
        ));
    };
    // La politica viene verificata prima della forma: la firma del metodo e
    // un fatto del prodotto, distinto dall'arita ammessa dal contratto.
    if !sql_server_offers(function) {
        return Err(query_error(
            ErrorCategory::Unsupported,
            "funzione spatial fuori dal sottoinsieme SQL Server verificato",
        ));
    }
    let argument = if sql_server_unary_spatial_function(function) {
        if arguments.len() != 1 {
            return Err(query_error(
                ErrorCategory::Unsupported,
                "overload unary spatial SQL Server fuori dal sottoinsieme verificato",
            ));
        }
        SpatialArgument::None
    } else if sql_server_binary_spatial_function(function) {
        if arguments.len() != 2 {
            return Err(query_error(
                ErrorCategory::Unsupported,
                "overload binary spatial SQL Server fuori dal sottoinsieme verificato",
            ));
        }
        match &arguments[1] {
            QueryExpression::Parameter { name } => SpatialArgument::Geometry(name.clone()),
            QueryExpression::Column { column } => {
                SpatialArgument::GeometryColumn(SpatialColumnRef {
                    relation: column.relation.clone(),
                    field: column.field.clone(),
                })
            }
            _ => {
                return Err(query_error(
                    ErrorCategory::Unsupported,
                    "AST spatial SQL Server richiede WKB bindato o una colonna",
                ));
            }
        }
    } else if sql_server_numeric_spatial_function(function) {
        if arguments.len() != 2 {
            return Err(query_error(
                ErrorCategory::Unsupported,
                "overload numerico spatial SQL Server fuori dal sottoinsieme verificato",
            ));
        }
        let QueryExpression::Parameter { name } = &arguments[1] else {
            return Err(query_error(
                ErrorCategory::Unsupported,
                "AST spatial SQL Server richiede argomento numerico bindato",
            ));
        };
        if function == SpatialFunction::PointN {
            SpatialArgument::PointIndex(name.clone())
        } else {
            SpatialArgument::Distance(name.clone())
        }
    } else {
        return Err(query_error(
            ErrorCategory::Unsupported,
            "funzione spatial fuori dal sottoinsieme SQL Server verificato",
        ));
    };
    uses.push(SpatialUse {
        column: column.clone(),
        argument,
        function,
    });
    Ok(())
}

/// La forma T-SQL di una funzione, chiesta a chi la scrive.
///
/// La firma appartiene al prodotto e non puo essere dedotta dall'arieta del
/// contratto: per esempio `ST_IsValid` ammette due argomenti nel contratto, ma
/// `STIsValid()` di T-SQL non ne accetta nessuno.
fn sql_server_spatial_shape(function: SpatialFunction) -> Option<SqlServerSpatialShape> {
    // La **forma** non dipende dalla semantica: `STX` e `Long` si scrivono
    // entrambe senza parentesi, e cosi ogni altra coppia. Il nome si, ed e per
    // quello che la tabella la chiede — qui non serve, e
    // `the_tsql_shape_does_not_depend_on_the_semantics` non lascia che quella
    // affermazione invecchi.
    plenora_database_sql::sql_server_spatial_method(function, Some(SpatialSemantics::Geometry))
        .map(|(_, shape)| shape)
}

fn sql_server_unary_spatial_function(function: SpatialFunction) -> bool {
    matches!(
        sql_server_spatial_shape(function),
        Some(
            SqlServerSpatialShape::Property
                | SqlServerSpatialShape::Unary
                | SqlServerSpatialShape::UnaryPredicate
        )
    )
}

fn sql_server_binary_spatial_function(function: SpatialFunction) -> bool {
    matches!(
        sql_server_spatial_shape(function),
        Some(SqlServerSpatialShape::BinaryValue | SqlServerSpatialShape::BinaryPredicate)
    )
}

fn sql_server_numeric_spatial_function(function: SpatialFunction) -> bool {
    matches!(
        sql_server_spatial_shape(function),
        Some(SqlServerSpatialShape::Numeric)
    )
}

fn operation_has_spatial(operation: &QueryOperation) -> bool {
    !walk_query(operation, |node| match node {
        QueryWalkNode::Expression(
            QueryExpression::Spatial { .. }
            | QueryExpression::SpatialOperator { .. }
            | QueryExpression::SpatialWindow { .. },
        ) => QueryWalkControl::Break,
        QueryWalkNode::Operation(_) | QueryWalkNode::Expression(_) | QueryWalkNode::Source(_) => {
            QueryWalkControl::Continue
        }
    })
}

fn operation_contains_spatial_subquery(operation: &QueryOperation) -> bool {
    operation
        .clause_expressions()
        .any(expression_contains_spatial_subquery)
        || operation.joins.iter().any(|join| {
            join.on
                .as_ref()
                .is_some_and(expression_contains_spatial_subquery)
        })
}

fn expression_contains_spatial_subquery(expression: &QueryExpression) -> bool {
    !walk_query_expression(expression, |node| match node {
        QueryWalkNode::Operation(operation) if operation_has_spatial(operation) => {
            QueryWalkControl::Break
        }
        QueryWalkNode::Operation(_) => QueryWalkControl::Skip,
        QueryWalkNode::Expression(_) | QueryWalkNode::Source(_) => QueryWalkControl::Continue,
    })
}

/// Verifica le sorgenti a ogni profondità dopo che il renderer ha già
/// applicato i limiti strutturali comuni.
pub fn validate_query_sources(operation: &QueryOperation, database: &str) -> Result<()> {
    let mut failure = None;
    walk_query(operation, |node| {
        let QueryWalkNode::Source(source) = node else {
            return QueryWalkControl::Continue;
        };
        if let Err(error) = validate_source(&source.object, database) {
            failure = Some(error);
            QueryWalkControl::Break
        } else {
            QueryWalkControl::Continue
        }
    });
    failure.map_or(Ok(()), Err)
}

fn validate_source(source: &ObjectRef, database: &str) -> Result<()> {
    if source
        .catalog
        .as_deref()
        .is_some_and(|catalog| catalog != database)
    {
        return Err(DatabaseError::unsupported(
            ProviderKind::Sqlserver,
            ErrorPhase::Prepare,
            "accesso cross-database SQL Server non supportato dal provider",
        ));
    }
    Ok(())
}

fn result_columns(rows: &[Row]) -> Result<Vec<SqlServerColumnSpec>> {
    if rows.is_empty() {
        return Err(query_error(
            ErrorCategory::Schema,
            "QueryOperation SQL Server non produce colonne",
        ));
    }
    let mut names = BTreeSet::new();
    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            if let Some(number) = optional::<i32>(row, 6, "error_number")? {
                return Err(query_error(
                    ErrorCategory::Schema,
                    format!("descrizione QueryOperation SQL Server fallita (codice {number})"),
                ));
            }
            let ordinal = required::<i32>(row, 0, "column_ordinal")?;
            let expected = i32::try_from(index + 1).map_err(|_| {
                query_error(
                    ErrorCategory::ResourceLimit,
                    "numero colonne QueryOperation non rappresentabile",
                )
            })?;
            if ordinal != expected {
                return Err(query_error(
                    ErrorCategory::Protocol,
                    "ordinali output QueryOperation SQL Server non contigui",
                ));
            }
            let name = optional::<&str>(row, 1, "name")?
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    query_error(
                        ErrorCategory::Schema,
                        "projection calcolata SQL Server senza alias",
                    )
                })?
                .to_owned();
            if !names.insert(name.clone()) {
                return Err(query_error(
                    ErrorCategory::Schema,
                    "nomi colonna output QueryOperation SQL Server duplicati",
                ));
            }
            let nullable = required::<bool>(row, 2, "is_nullable")?;
            let user_type = optional::<&str>(row, 5, "user_type_name")?;
            if user_type.is_some_and(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "geometry" | "geography"
                )
            }) {
                return Err(query_error(
                    ErrorCategory::Unsupported,
                    "output spatial QueryOperation SQL Server non ancora tipizzato",
                ));
            }
            let declaration = optional::<&str>(row, 3, "system_type_name")?
                .ok_or_else(|| {
                    query_error(
                        ErrorCategory::Unsupported,
                        "tipo output QueryOperation SQL Server non descrivibile",
                    )
                })?
                .to_owned();
            let collation = optional::<&str>(row, 4, "collation_name")?.map(ToOwned::to_owned);
            SqlServerColumnSpec::from_query_metadata(name, declaration, nullable, collation)
        })
        .collect()
}

fn required<'a, T>(row: &'a Row, index: usize, field: &'static str) -> Result<T>
where
    T: FromSql<'a>,
{
    optional(row, index, field)?.ok_or_else(|| {
        query_error(
            ErrorCategory::DataMapping,
            format!("campo descrizione QueryOperation obbligatorio assente: {field}"),
        )
    })
}

fn optional<'a, T>(row: &'a Row, index: usize, field: &'static str) -> Result<Option<T>>
where
    T: FromSql<'a>,
{
    row.try_get(index).map_err(|error| {
        let mut public = driver_error(&error, ErrorPhase::Prepare, RemoteEffect::None);
        public.message =
            format!("tipo descrizione QueryOperation SQL Server incompatibile: {field}");
        public
    })
}

const fn sql_server_renderer() -> Renderer {
    Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
}

fn query_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        category,
        ErrorPhase::Prepare,
        Some(ProviderKind::Sqlserver),
        message,
    )
}

#[cfg(test)]
#[path = "query_tests.rs"]
mod tests;
