use crate::control::select_with_cancellation;
use crate::error::{classify_error, public_error};
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
use futures_util::StreamExt;
use plenora_database_core::plan::ReadOperation;
use plenora_database_core::provider::{BatchStream, ParameterBag, SecretString};
use plenora_database_core::query::QueryOperation;
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
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
        .map_or_else(
            || {
                provider.metrics.cancellation();
                Err(cancelled_read_error(cancellation))
            },
            std::convert::identity,
        )
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
        let query = client.client()?.query_typed_raw(
            sql,
            parameter_refs.into_iter().zip(parameter_types.into_iter()),
        );
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
    if let Some(catalog) = &operation.source.catalog {
        let current_database: String = client
            .client()?
            .query_one("SELECT current_database()", &[])
            .await
            .map_err(|error| classify_error(ErrorPhase::Probe, &error))?
            .get(0);
        if catalog != &current_database {
            return Err(public_error(
                ErrorCategory::NotFound,
                ErrorPhase::Prepare,
                false,
                "catalog PostgreSQL diverso dalla connessione corrente",
            ));
        }
    }
    let (available, _schema_token) = provider.cached_columns(&client, &operation.source).await?;
    let plan = plan_read(operation, &available)?;
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
    );
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
    let mut client = connect_cancel_safe(provider, secret, cancellation).await?;
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
    let mut typed_rows = None;
    if let Some(parameter_types) = typed_parameter_types {
        let typed_parameters = parameter_refs
            .iter()
            .copied()
            .zip(parameter_types.into_iter());
        if let Some(result) = select_with_cancellation(
            client
                .client()?
                .query_typed_raw(&rendered.sql, typed_parameters),
            cancellation,
        )
        .await
        {
            if let Ok(rows) = result {
                typed_rows = Some(rows);
            }
        } else {
            return Err(cancel_inflight(provider, &mut client, cancellation).await);
        }
    }
    let (rows, columns): (PostgresRows, Vec<ColumnSpec>) = if let Some(raw_rows) = typed_rows {
        let mut raw_rows = Box::pin(raw_rows);
        let first = if let Some(result) =
            select_with_cancellation(raw_rows.as_mut().next(), cancellation).await
        {
            result.transpose().map_err(|error| {
                client.invalidate();
                classify_error(ErrorPhase::Read, &error)
            })?
        } else {
            return Err(cancel_inflight(provider, &mut client, cancellation).await);
        };
        provider.metrics.query_typed_fast_path();
        if let Some(first) = first {
            let mut columns = first
                .columns()
                .iter()
                .map(ColumnSpec::from_statement_column)
                .collect::<Result<Vec<_>>>()?;
            mark_query_spatial_columns(operation, &mut columns);
            let rows = futures_util::stream::once(async move { Ok(first) }).chain(raw_rows);
            (Box::pin(rows), columns)
        } else {
            let statement = client
                .client()?
                .prepare(&rendered.sql)
                .await
                .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
            let mut columns = statement
                .columns()
                .iter()
                .map(ColumnSpec::from_statement_column)
                .collect::<Result<Vec<_>>>()?;
            mark_query_spatial_columns(operation, &mut columns);
            (Box::pin(futures_util::stream::empty()), columns)
        }
    } else {
        provider.metrics.query_prepared_fallback();
        let statement = client
            .client()?
            .prepare(&rendered.sql)
            .await
            .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
        let mut columns = statement
            .columns()
            .iter()
            .map(ColumnSpec::from_statement_column)
            .collect::<Result<Vec<_>>>()?;
        mark_query_spatial_columns(operation, &mut columns);
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
        (Box::pin(rows), columns)
    };
    let schema = contract_schema(
        columns
            .iter()
            .map(ColumnSpec::arrow_field)
            .collect::<Vec<_>>(),
    );
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
