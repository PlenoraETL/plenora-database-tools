//! DML del driver senza CAST o guardie del writer, sempre con rollback.
use super::*;
use crate::write_probe_tests::{
    probe_failure, probe_kind, probe_operand, probe_projection, probe_value,
};
use plenora_database_core::arrow::array::Array;
use write_probe::{direct_error, DirectObservation};

pub(super) async fn observe(
    hooks: &Hooks,
    spec: &ProbeSpec,
    values: &dyn Array,
    row: usize,
) -> plenora_database_core::Result<DirectObservation> {
    eprintln!("sonda Oracle tipo={} indice={row}: avvio", probe_kind(spec));
    let value = probe_value(spec, values, row)?;
    let operand = probe_operand(spec);
    let cancellation = CancellationToken::new();
    let config = hooks.provider.config();
    let pool = hooks.provider.pool_for(&hooks.secret)?;
    let mut lease = pool.checkout(&cancellation).await?;
    let raw = lease.connection()?;
    let params = bind_probe(hooks, raw, spec, value, &cancellation).await?;
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
                raw.query(
                    &format!(
                        "SELECT {} FROM {PROBE_TABLE} WHERE ID = 0",
                        probe_projection(spec)
                    ),
                    &[],
                ),
            )
            .await?;
            // La misura non rifiuta un float non finito che il driver ha riletto.
            if let Some(oracle_rs::Value::Float(value)) =
                result.rows.first().and_then(|row| row.get(0))
            {
                return Ok(vec![plenora_database_core::Row::try_new(
                    vec!["V0".to_owned()].into(),
                    vec![plenora_database_core::provider::ParameterValue::F64(*value)],
                )?]);
            }
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
    if let Err(error) = &observed {
        eprintln!("{}", probe_failure(spec, row, error));
    }
    crate::connection::with_timeout(
        config,
        ErrorPhase::Rollback,
        &CancellationToken::new(),
        raw.rollback(),
    )
    .await
    .inspect_err(|error| {
        eprintln!("rollback fallito: {}", probe_failure(spec, row, error));
    })?;
    lease.allow_reuse();
    drop(lease);
    Ok(match observed {
        Err(error) => direct_error(&error, reading),
        Ok(rows) => {
            assert_eq!(rows.len(), 1, "riga dell'oracolo");
            DirectObservation::Read(crate::write_probe_tests::probe_read_value(
                spec,
                &rows[0].values()[0],
            ))
        }
    })
}

async fn bind_probe(
    hooks: &Hooks,
    raw: &oracle_rs::Connection,
    spec: &ProbeSpec,
    value: plenora_database_core::provider::ParameterValue,
    cancellation: &CancellationToken,
) -> plenora_database_core::Result<Vec<oracle_rs::Value>> {
    let config = hooks.provider.config();
    let mut cache = crate::parameter::LobCache::default();
    let params = if let ("BLOB", plenora_database_core::provider::ParameterValue::Bytes(bytes)) =
        (probe_kind(spec), &value)
    {
        crate::parameter::bind_probe_blob(
            raw,
            bytes,
            &mut cache,
            config.operation_timeout(),
            cancellation,
        )
        .await?
    } else if matches!(&value, plenora_database_core::provider::ParameterValue::String(text) if text.len() > 4_000)
    {
        crate::parameter::bind_parameters_with_lobs(
            raw,
            &[value],
            true,
            &mut cache,
            config.operation_timeout(),
            ErrorPhase::Write,
            cancellation,
        )
        .await?
    } else {
        crate::parameter::bind_probe_parameters(&[value])?
    };
    Ok(params)
}
