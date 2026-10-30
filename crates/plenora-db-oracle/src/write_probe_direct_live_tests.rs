//! DML del driver senza CAST o guardie del writer, sempre con rollback.
use super::*;
use crate::write_probe_tests::{probe_failure, probe_kind, probe_value};
use plenora_database_core::arrow::array::Array;
use write_probe::{direct_error, direct_value, DirectObservation};

pub(super) async fn observe(
    hooks: &Hooks,
    spec: &ProbeSpec,
    values: &dyn Array,
    row: usize,
) -> plenora_database_core::Result<DirectObservation> {
    eprintln!("sonda Oracle tipo={} indice={row}: avvio", probe_kind(spec));
    let value = probe_value(spec, values, row)?;
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
            DirectObservation::Read(direct_value(&rows[0].values()[0]))
        }
    })
}
