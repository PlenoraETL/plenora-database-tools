//! Create permissivo: input completo in memoria e verifica su oggetto temporaneo.
use crate::{loss_execution::Evaluation, write::MysqlWritePlan, MysqlSession};
use mysql_async::Value;
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::plan::WriteOperation;
use plenora_database_core::provider::{BatchStream, ProviderFuture};
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::{CancellationToken, ErrorPhase, Result};
use std::collections::VecDeque;

#[derive(Default, PartialEq)]
pub struct Catalog {
    columns: Vec<Vec<Value>>,
    definition: String,
}
pub struct Buffer {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
    pub catalog: Catalog,
}
impl BatchStream for Buffer {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(plenora_database_core::DatabaseError::new(
                    plenora_database_core::ErrorCategory::Cancelled,
                    ErrorPhase::Write,
                    None,
                    "replay Create annullato",
                ));
            }
            Ok(self.batches.pop_front())
        })
    }
}
fn invalid() -> plenora_database_core::DatabaseError {
    crate::write::mapping_error("catalogo Create diverso dai fatti congelati")
}
pub async fn catalog(
    session: &mut MysqlSession,
    quoted: &str,
    token: &CancellationToken,
) -> Result<Catalog> {
    use plenora_database_core::identifier::{quote_identifier, IdentifierDialect};
    let columns = session
        .query_rows(
            &format!("SHOW FULL COLUMNS FROM {quoted}"),
            ErrorPhase::Prepare,
            token,
        )
        .await?
        .into_iter()
        .map(|row| {
            let mut values = row.unwrap();
            if values.len() != 9 {
                return Err(invalid());
            }
            // Privileges non e un fatto di mapping ed e diverso sulle TEMPORARY.
            values.remove(7);
            Ok(values)
        })
        .collect::<Result<Vec<_>>>()?;
    let rows = session
        .query_rows(
            &format!("SHOW CREATE TABLE {quoted}"),
            ErrorPhase::Prepare,
            token,
        )
        .await?;
    let row = rows.first().ok_or_else(invalid)?;
    let name = row
        .get_opt::<String, _>(0)
        .and_then(std::result::Result::ok)
        .ok_or_else(invalid)?;
    let definition = row
        .get_opt::<String, _>(1)
        .and_then(std::result::Result::ok)
        .ok_or_else(invalid)?;
    let definition = definition
        .strip_prefix("CREATE TEMPORARY TABLE ")
        .or_else(|| definition.strip_prefix("CREATE TABLE "))
        .and_then(|rest| {
            rest.strip_prefix(&quote_identifier(IdentifierDialect::Mysql, &name).ok()?)
        })
        .ok_or_else(invalid)?
        .to_owned();
    Ok(Catalog {
        columns,
        definition,
    })
}
pub fn quoted(operation: &WriteOperation, database: &str) -> Result<String> {
    use plenora_database_core::identifier::{quote_identifier, IdentifierDialect};
    Ok(format!(
        "{}.{}",
        quote_identifier(
            IdentifierDialect::Mysql,
            operation.target.schema.as_deref().unwrap_or(database)
        )?,
        quote_identifier(IdentifierDialect::Mysql, &operation.target.object)?
    ))
}
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn prepare(
    session: &mut MysqlSession,
    mut input: Box<dyn BatchStream>,
    operation: &WriteOperation,
    plan: &MysqlWritePlan,
    evaluation: &mut Evaluation,
    database: &str,
    budget: &ResourceBudget,
    token: &CancellationToken,
) -> Result<Buffer> {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    crate::write::verify_write_session(session, token).await?;
    let schema = input.schema();
    let mut temporary = operation.clone();
    temporary.target.object = format!(
        "_plenora_create_{}",
        SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let name = quoted(&temporary, database)?;
    let ddl =
        crate::write::build_create_table_sql(&schema, &temporary, database, session.profile())?
            .replacen("CREATE TABLE", "CREATE TEMPORARY TABLE", 1);
    session
        .exec_control(&ddl, ErrorPhase::Prepare, token)
        .await?;
    let snapshot = catalog(session, &name, token).await?;
    let expected = plan.create_facts(session.kind())?;
    let mut facts = Vec::new();
    for row in &snapshot.columns {
        let text = |index: usize| -> Result<String> {
            match row.get(index) {
                Some(Value::Bytes(bytes)) => {
                    String::from_utf8(bytes.clone()).map_err(|_| invalid())
                }
                _ => Err(invalid()),
            }
        };
        let column_type = text(1)?.to_ascii_lowercase();
        let character_set = match row.get(2) {
            Some(Value::NULL) => None,
            _ => Some(text(2)?.split('_').next().ok_or_else(invalid)?.to_owned()),
        };
        facts.push(crate::write_admission::ColumnFacts {
            name: text(0)?,
            data_type: column_type
                .split(['(', ' '])
                .next()
                .ok_or_else(invalid)?
                .to_owned(),
            column_type,
            character_set,
            json_valid: false,
        });
    }
    for fact in &expected {
        if !facts.contains(fact) {
            return Err(invalid());
        }
    }
    let server = crate::write_admission::server_facts(session, token).await?;
    let rewrites = crate::write_admission::admit(
        session.kind(),
        &operation.target.object,
        &schema,
        &plan.scalar_columns(),
        &facts,
        &server,
    )?;
    if rewrites != plan.rewrites() {
        return Err(invalid());
    }
    let mut batches = VecDeque::new();
    let mut total = 0u64;
    while let Some(batch) = plenora_database_core::arrow_validation::validated_batch(
        input.next_batch(token),
        ErrorPhase::Write,
    )
    .await?
    {
        budget.ensure_active()?;
        crate::write::validate_batch_schema(&batch, &schema)?;
        if batch.num_rows() == 0 {
            continue;
        }
        evaluation.reserve_rows(budget, batch.num_rows())?;
        evaluation.begin(&batch)?;
        plan.validate_spatial_batch(&batch, budget)?;
        total = total.checked_add(batch.num_rows() as u64).ok_or_else(|| {
            plenora_database_core::DatabaseError::resource_limit(
                "conteggio righe Create oltre budget",
            )
        })?;
        for start in (0..batch.num_rows()).step_by(plan.rows_per_statement()) {
            let count = plan.rows_per_statement().min(batch.num_rows() - start);
            let parameters = evaluation.bind(plan, &batch, start, count)?;
            // Il vincolo completo della TEMPORARY controlla anche le chiavi sotto collation.
            let inserted = session
                .exec_write(
                    &plan.render_insert_into_staging(&name, count)?,
                    parameters,
                    ErrorPhase::Write,
                    token,
                )
                .await?;
            if inserted != count as u64 {
                return Err(invalid());
            }
        }
        evaluation.freeze_batch();
        batches.push_back(batch);
    }
    if input
        .declared_input_rows()
        .is_some_and(|rows| rows != total)
    {
        return Err(invalid());
    }
    evaluation.context.prepare_commit(true)?;
    evaluation.replay();
    // Solo l'oggetto TEMPORARY: nessun DROP del target permanente.
    session
        .exec_control(
            &format!("DROP TEMPORARY TABLE {name}"),
            ErrorPhase::Prepare,
            token,
        )
        .await?;
    Ok(Buffer {
        schema,
        batches,
        catalog: snapshot,
    })
}
