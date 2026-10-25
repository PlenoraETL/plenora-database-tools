use crate::control::select_with_cancellation;
use crate::error::classify_error;
use crate::parameter_codec::{
    bind_parameters, typed_filter_parameter_types, typed_query_parameter_types,
};
use crate::pool::PooledClient;
use crate::query_plan::{mark_query_spatial_columns, plan_read, render_query};
use crate::read_stream::{
    cancel_and_invalidate, cancelled_read_error, PostgresBatchStream, PostgresRows,
    ReadStreamSource,
};
use crate::types::ColumnSpec;
use crate::{contract_schema, PostgresProvider};
use plenora_database_core::plan::ReadOperation;
use plenora_database_core::provider::{BatchStream, ParameterBag, SecretString};
use plenora_database_core::relational::QueryOperation;
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorPhase, Result};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::RowStream;

async fn cancel_inflight(
    provider: &PostgresProvider,
    client: &mut PooledClient,
    cancellation: &CancellationToken,
) -> DatabaseError {
    cancel_and_invalidate(
        client,
        provider.tls_mode,
        provider.tls_config.connector(),
        provider.network_options.connect_timeout_ms,
    )
    .await;
    cancelled_read_error(cancellation)
}

fn validate_stream_configuration(provider: &PostgresProvider) -> Result<()> {
    if provider.batch_rows == 0
        || provider.max_batch_bytes == 0
        || provider.target_batch_bytes == Some(0)
    {
        return Err(DatabaseError::invalid_plan(
            "batch_rows e budget byte PostgreSQL devono essere maggiori di zero",
        ));
    }
    Ok(())
}

async fn connect_cancel_safe(
    provider: &PostgresProvider,
    secret: &SecretString,
    cancellation: &CancellationToken,
) -> Result<PooledClient> {
    select_with_cancellation(provider.connect_session(secret), cancellation)
        .await
        .unwrap_or_else(|| {
            provider.metrics.cancellation();
            Err(cancelled_read_error(cancellation))
        })
}

async fn start_read_query(
    provider: &PostgresProvider,
    client: &mut PooledClient,
    sql: &str,
    parameter_refs: Vec<&(dyn ToSql + Sync)>,
    parameter_types: Option<Vec<Type>>,
    cancellation: &CancellationToken,
) -> Result<RowStream> {
    let parameter_count = parameter_refs.len();
    let (result, error_phase) = if let Some(parameter_types) = parameter_types {
        let query = client
            .client()?
            .query_typed_raw(sql, parameter_refs.into_iter().zip(parameter_types));
        (
            select_with_cancellation(query, cancellation)
                .await
                .map(|result| {
                    result.inspect(|_| {
                        provider.metrics.read_typed_fast_path();
                        if parameter_count > 0 {
                            provider.metrics.read_parameterized_typed_fast_path();
                        }
                    })
                }),
            ErrorPhase::Prepare,
        )
    } else {
        provider.metrics.read_prepared_fallback();
        let statement = client
            .client()?
            .prepare(sql)
            .await
            .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
        (
            select_with_cancellation(
                client.client()?.query_raw(&statement, parameter_refs),
                cancellation,
            )
            .await,
            ErrorPhase::Read,
        )
    };
    if let Some(result) = result {
        return result.map_err(|error| classify_error(error_phase, &error));
    }
    Err(cancel_inflight(provider, client, cancellation).await)
}

#[allow(clippy::significant_drop_tightening)]
pub async fn read_stream(
    provider: &PostgresProvider,
    secret: &SecretString,
    operation: &ReadOperation,
    parameters: &ParameterBag,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<Box<dyn BatchStream>> {
    if cancellation.is_cancelled() {
        provider.metrics.cancellation();
        return Err(cancelled_read_error(cancellation));
    }
    validate_stream_configuration(provider)?;
    let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
    let mut client = connect_cancel_safe(provider, secret, cancellation).await?;
    // La regola T: il catalog e quello della connessione
    // (`target_schema::check_catalog`), e la sorgente senza schema si
    // risolve una volta, con il percorso della sessione; colonne, statement e cache nominano lo stesso
    // oggetto.
    let resolved = ReadOperation {
        source: crate::target_schema::resolve(
            client.client()?,
            &operation.source,
            crate::target_schema::Purpose::Existing,
            ErrorPhase::Prepare,
        )
        .await?,
        ..operation.clone()
    };
    let operation = &resolved;
    let (available, _schema_token) = provider.cached_columns(&client, &operation.source).await?;
    let plan = plan_read(operation, &available)?;
    // Ogni sonda spatial porta uno SRID esplicito, quello della colonna
    // (typmod), e si lega come EWKB con quello SRID.
    let parameters = &crate::spatial_probe::verify_read_filter(
        operation.filter.as_ref(),
        &available,
        parameters,
        budget.limits().nesting_depth,
    )?;
    let columns = u64::try_from(plan.columns.len())
        .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?;
    let columns_lease = budget.try_lease(ResourceKind::Columns, columns)?;
    let owned_parameters = bind_parameters(parameters, &plan.bind_names)?;
    let parameter_refs = owned_parameters
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();
    let parameter_types = if provider.parameterized_read_fast_path || plan.bind_names.is_empty() {
        typed_filter_parameter_types(
            operation.filter.as_ref(),
            &plan.bind_names,
            parameters,
            &available,
        )
    } else {
        None
    };
    let rows = start_read_query(
        provider,
        &mut client,
        &plan.sql,
        parameter_refs,
        parameter_types,
        cancellation,
    )
    .await?;
    let cancel_token = client.client()?.cancel_token();
    let schema = contract_schema(
        plan.columns
            .iter()
            .map(ColumnSpec::arrow_field)
            .collect::<Vec<_>>(),
    )?;
    Ok(Box::new(PostgresBatchStream::new(
        provider,
        ReadStreamSource::new(client, cancel_token, Box::pin(rows), plan.columns, schema),
        budget,
        operation_lease,
        columns_lease,
    )))
}

#[allow(clippy::significant_drop_tightening)]
#[allow(clippy::too_many_lines)]
pub async fn query_stream(
    provider: &PostgresProvider,
    secret: &SecretString,
    operation: &QueryOperation,
    parameters: &ParameterBag,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<Box<dyn BatchStream>> {
    if cancellation.is_cancelled() {
        provider.metrics.cancellation();
        return Err(cancelled_read_error(cancellation));
    }
    validate_stream_configuration(provider)?;
    let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
    let rendered = render_query(operation)?;
    // Le sonde spatial si giudicano prima della connessione; le colonne con
    // cui si confrontano, sul catalogo prima della query.
    let probes = crate::spatial_probe::plan_query_probes(
        operation,
        parameters,
        budget.limits().nesting_depth,
    )?;
    let parameters = &probes.parameters;
    let mut client = connect_cancel_safe(provider, secret, cancellation).await?;
    crate::spatial_probe::verify_query_columns(client.client()?, &probes.columns).await?;
    let bind_names = rendered
        .binds
        .iter()
        .map(|bind| bind.name.clone())
        .collect::<Vec<_>>();
    let owned_parameters = bind_parameters(parameters, &bind_names)?;
    let parameter_refs = owned_parameters
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();
    let typed_parameter_types = provider
        .parameterized_read_fast_path
        .then(|| typed_query_parameter_types(&bind_names, parameters))
        .flatten();
    // Risolvi l'identita prima di avviare lo stream: una query al catalogo
    // sulla stessa connessione con righe non consumate puo bloccare il driver.
    let statement = client
        .client()?
        .prepare_typed(
            &rendered.sql,
            typed_parameter_types.as_deref().unwrap_or(&[]),
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
    let mut columns = statement
        .columns()
        .iter()
        .map(ColumnSpec::from_statement_column)
        .collect::<Result<Vec<_>>>()?;
    mark_query_spatial_columns(operation, &mut columns);
    // GEO-009: lo SRID di una geometria calcolata che eredita quello della
    // colonna d'ingresso, dal catalogo; la lettura lo verifica valore per
    // valore. Senza, il campo resta `missing`.
    let inherited_geometries = crate::query_plan::inherited_geometries(operation);
    // La regola Q: i nomi di PostGIS con lo schema dell'estensione.
    let postgis = if inherited_geometries.is_empty() {
        String::new()
    } else {
        crate::postgis_schema::qualifier(client.client()?, ErrorPhase::Prepare).await?
    };
    for inherited in inherited_geometries {
        // La relazione si risolve come la risolve la query: con lo schema
        // dichiarato, o lungo `search_path` senza (`to_regclass`). Lo SRID e
        // quello del typmod della colonna (0 senza typmod), `geometry` o
        // `geography` dell'estensione PostGIS.
        let row = client
            .client()?
            .query_opt(
                &crate::postgis_schema::qualify(
                    "SELECT postgis_typmod_srid(a.atttypmod) \
                     FROM pg_catalog.pg_attribute a \
                     JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
                     JOIN pg_catalog.pg_extension e \
                       ON e.extname = 'postgis' AND e.extnamespace = t.typnamespace \
                     WHERE a.attrelid = pg_catalog.to_regclass(\
                             CASE WHEN $1::pg_catalog.text IS NULL \
                             THEN pg_catalog.quote_ident($2::pg_catalog.text) \
                             ELSE pg_catalog.quote_ident($1::pg_catalog.text) || '.' \
                                  || pg_catalog.quote_ident($2::pg_catalog.text) END) \
                       AND a.attname = $3::pg_catalog.text AND a.attnum > 0 \
                       AND NOT a.attisdropped AND t.typname IN ('geometry', 'geography')",
                    &postgis,
                ),
                &[&inherited.schema, &inherited.object, &inherited.column],
            )
            .await
            .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
        let srid = row
            .and_then(|row| row.get::<_, Option<i32>>(0))
            .and_then(|srid| u32::try_from(srid).ok())
            .filter(|srid| *srid > 0);
        if let Some(column) = columns.get_mut(inherited.position) {
            column.spatial_srid = srid;
        }
    }
    for column in &mut columns {
        if let Some((relation, number)) = column.source {
            let row = client
                .client()?
                .query_one(
                    "SELECT pg_catalog.col_description($1::pg_catalog.oid, $2::pg_catalog.int4)",
                    &[&relation, &i32::from(number)],
                )
                .await
                .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
            let comment: Option<String> = row.get(0);
            column.registered_field_id = crate::field_identity::registered_id(comment.as_deref())?;
        }
    }
    let columns = without_shared_origins(columns);
    let mut typed_rows = None;
    if let Some(parameter_types) = typed_parameter_types {
        let typed_parameters = parameter_refs.iter().copied().zip(parameter_types);
        if let Some(result) = select_with_cancellation(
            client
                .client()?
                .query_typed_raw(&rendered.sql, typed_parameters),
            cancellation,
        )
        .await
        {
            if let Ok(rows) = result {
                provider.metrics.query_typed_fast_path();
                typed_rows = Some(rows);
            }
        } else {
            return Err(cancel_inflight(provider, &mut client, cancellation).await);
        }
    }
    let rows: PostgresRows = if let Some(rows) = typed_rows {
        Box::pin(rows)
    } else {
        provider.metrics.query_prepared_fallback();
        let rows = if let Some(result) = select_with_cancellation(
            client.client()?.query_raw(&statement, parameter_refs),
            cancellation,
        )
        .await
        {
            result.map_err(|error| classify_error(ErrorPhase::Read, &error))?
        } else {
            return Err(cancel_inflight(provider, &mut client, cancellation).await);
        };
        Box::pin(rows)
    };
    let schema = contract_schema(
        columns
            .iter()
            .map(ColumnSpec::arrow_field)
            .collect::<Vec<_>>(),
    )?;
    let column_count = u64::try_from(columns.len())
        .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?;
    let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
    let cancel_token = client.client()?.cancel_token();
    Ok(Box::new(PostgresBatchStream::new(
        provider,
        ReadStreamSource::new(client, cancel_token, rows, columns, schema),
        budget,
        operation_lease,
        columns_lease,
    )))
}

/// Risolve le proiezioni ripetute solo dopo aver letto e validato i commenti.
/// Per origini non registrate resta il fallback al nome per tutte le copie.
/// Per origini registrate la prima conserva l'id, le successive partono
/// dall'hash del nome di uscita e aggiungono 1 finche trovano un id libero.
/// Si riservano prima tutti gli id degli altri campi (in forma canonica):
/// nessuna collisione puo rinumerare un'identita persistita, anche successiva.
/// L'assegnazione delle copie segue l'ordine della proiezione ed e deterministica
/// nello stesso schema; non promette stabilita fra proiezioni diverse.
/// Espressioni e origini non registrate restano dipendenti dal nome; le viste
/// possono riportare origini diverse fra lettura di catalogo e query.
fn without_shared_origins(mut columns: Vec<ColumnSpec>) -> Vec<ColumnSpec> {
    use plenora_database_core::protocol::{canonical_field_id, name_field_id, FIELD_ID};
    let mut counts = std::collections::HashMap::new();
    for source in columns.iter().filter_map(|column| column.source) {
        *counts.entry(source).or_insert(0_usize) += 1;
    }
    let mut seen = std::collections::HashSet::new();
    let mut duplicates = Vec::new();
    for (index, column) in columns.iter_mut().enumerate() {
        if let Some(source) = column.source {
            if column.registered_field_id.is_some() {
                if !seen.insert(source) {
                    duplicates.push(index);
                }
            } else if counts.get(&source).copied().unwrap_or(0) > 1 {
                column.source = None;
            }
        }
    }
    if duplicates.is_empty() {
        return columns;
    }
    let mut reserved = columns
        .iter()
        .map(|column| {
            column
                .arrow_field()
                .metadata()
                .get(FIELD_ID)
                .and_then(|id| canonical_field_id(id))
                .unwrap_or_else(|| name_field_id(&column.name).to_string())
        })
        .collect::<std::collections::HashSet<_>>();
    for index in duplicates {
        let column = &mut columns[index];
        let mut candidate = u64::from(name_field_id(&column.name));
        while !reserved.insert(candidate.to_string()) {
            candidate += 1;
        }
        column.registered_field_id = Some(candidate.to_string());
        column.source = None;
    }
    columns
}

#[cfg(test)]
#[path = "query_execution_tests.rs"]
mod tests;
