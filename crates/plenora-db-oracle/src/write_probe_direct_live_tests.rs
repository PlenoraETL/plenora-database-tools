//! DML del driver senza CAST o guardie del writer, sempre con rollback.
use super::*;
use plenora_database_core::arrow::array::Array;
use write_probe::{direct_error, direct_value, DirectObservation};

pub(super) async fn observe(
    hooks: &Hooks,
    values: &dyn Array,
    row: usize,
) -> plenora_database_core::Result<DirectObservation> {
    let field = Field::new("V0", values.data_type().clone(), true);
    let value = if matches!(values.data_type(), DataType::Timestamp(_, Some(_)))
        && !values.is_null(row)
    {
        let array = values
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp");
        let instant = chrono::DateTime::from_timestamp_micros(array.value(row)).expect("istante");
        plenora_database_core::provider::ParameterValue::String(
            instant.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),
        )
    } else {
        plenora_database_engine::arrow_write::arrow_parameter_value(values, &field, row, 'T')?
    };
    let operand = if matches!(values.data_type(), DataType::Timestamp(_, Some(_))) {
        format!(
            "TO_TIMESTAMP_TZ(:1, '{}')",
            plenora_database_core::provider::ORACLE_TIMESTAMP_TZ_FORMAT_MODEL
        )
    } else {
        ":1".to_owned()
    };
    let cancellation = CancellationToken::new();
    let config = hooks.provider.config();
    let pool = hooks.provider.pool_for(&hooks.secret)?;
    let mut lease = pool.checkout(&cancellation).await?;
    let raw = lease.connection()?;
    let mut cache = crate::parameter::LobCache::default();
    let params = if matches!(&value, plenora_database_core::provider::ParameterValue::String(text) if text.len() > 4_000)
    {
        crate::parameter::bind_parameters_with_lobs(
            raw,
            &[value],
            true,
            &mut cache,
            config.operation_timeout(),
            ErrorPhase::Write,
            &cancellation,
        )
        .await?
    } else {
        crate::parameter::bind_parameters(&[value])?
    };
    let written = crate::connection::with_timeout(
        config,
        ErrorPhase::Write,
        &cancellation,
        raw.execute(
            &format!("INSERT INTO {PROBE_TABLE} (ID, V0) VALUES (0, {operand})"),
            &params,
        ),
    )
    .await;
    let (observed, reading) = if let Err(error) = written {
        (Err(error), false)
    } else {
        let rows = async {
            let result = crate::connection::with_timeout(
                config,
                ErrorPhase::Read,
                &cancellation,
                raw.query(&format!("SELECT V0 FROM {PROBE_TABLE} WHERE ID = 0"), &[]),
            )
            .await?;
            crate::decode::rows_from_result(
                raw,
                config.operation_timeout(),
                ErrorPhase::Read,
                &cancellation,
                result,
            )
            .await
        }
        .await;
        (rows, true)
    };
    crate::connection::with_timeout(
        config,
        ErrorPhase::Rollback,
        &CancellationToken::new(),
        raw.rollback(),
    )
    .await
    .expect("rollback confermato dell'oracolo");
    lease.allow_reuse();
    drop(lease);
    Ok(match observed {
        Err(error) => direct_error(&error, reading),
        Ok(rows) => {
            assert_eq!(rows.len(), 1, "riga dell'oracolo");
            DirectObservation::Read(direct_value(&rows[0].values()[0]))
        }
    })
}
