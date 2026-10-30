//! Misura dei LOB legacy senza CONVERT al tipo intermedio del writer.
use super::*;
use plenora_database_core::transaction::{Statement, TransactionOptions};
use write_probe::{direct_error, direct_value, DirectObservation};

pub(super) async fn observe(
    hooks: &Hooks,
    native: &str,
    values: &dyn Array,
    row: usize,
) -> plenora_database_core::Result<DirectObservation> {
    let field = Field::new("V0", values.data_type().clone(), true);
    let mut value =
        plenora_database_engine::arrow_write::arrow_parameter_value(values, &field, row, 'T')?;
    if let plenora_database_core::provider::ParameterValue::Null { type_name } = &mut value {
        *type_name = native.to_owned();
    }
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let mut transaction = hooks
        .provider
        .begin_transaction(
            &hooks.secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await?;
    let insert = Statement::new(format!(
        "INSERT INTO dbo.{PROBE_TABLE} (ID, V0) VALUES (0, @P1)"
    ))
    .with_params(vec![value]);
    let (observed, reading) = if let Err(error) = transaction.execute(&insert, &cancellation).await
    {
        (Err(error), false)
    } else {
        let select = Statement::new(format!("SELECT V0 FROM dbo.{PROBE_TABLE} WHERE ID = 0"));
        (transaction.query(&select, &cancellation).await, true)
    };
    transaction
        .rollback(&CancellationToken::new())
        .await
        .expect("rollback confermato dell'oracolo");
    Ok(match observed {
        Err(error) => direct_error(&error, reading),
        Ok(rows) => {
            assert_eq!(rows.len(), 1, "riga dell'oracolo");
            DirectObservation::Read(direct_value(&rows[0].values()[0]))
        }
    })
}
