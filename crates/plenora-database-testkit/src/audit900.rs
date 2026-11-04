//! Regressioni dell'audit 9.0.0 condivise dai fixture dei sei provider.

use crate::parameter_fidelity::{begin, describe, insert_statement, NativeSql, Target};
use plenora_database_core::facade::execute_portable;
use plenora_database_core::plan::{FilterExpression, ReadOperation};
use plenora_database_core::portable::{
    eq, Expression, PortableStatement, TableRef, UpdateStatement,
};
use plenora_database_core::provider::{ParameterBag, ParameterValue as V};
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::transaction::Statement;
use plenora_database_core::{CancellationToken, ErrorPhase};

/// L1: INSERT e UPDATE passano dal catalogo reale; dopo ogni rifiuto si
/// controlla che il valore positivo sia ancora presente nella transazione.
///
/// # Errors
/// Restituisce fase e categoria se il fixture fallisce o un valore malformato passa.
#[allow(clippy::too_many_lines, reason = "sequenza della prova end-to-end L1")]
pub async fn malformed_temporals(
    target: &Target<'_>,
    text_column: &str,
    date_column: Option<&str>,
) -> Result<(), String> {
    let cancel = CancellationToken::new();
    let mut cases = vec![
        (
            text_column,
            V::Timestamp("not-a-timestamp".into()),
            V::String("control".into()),
        ),
        (
            text_column,
            V::TimestampTz("not-a-timestamp".into()),
            V::String("control".into()),
        ),
    ];
    if let Some(column) = date_column {
        cases.push((
            column,
            V::Timestamp("2024-01-01T00:00:00.000000PAYLOAD".into()),
            V::Timestamp("2024-01-01T00:00:00".into()),
        ));
    }
    for (column, invalid, valid) in cases {
        let mut tx = begin(target).await?;
        let table = TableRef {
            schema: target.table.schema.clone(),
            name: target.table.object.clone(),
        };
        execute_portable(
            tx.as_mut(),
            &insert_statement(target, column, 900, &valid),
            &cancel,
        )
        .await
        .map_err(|e| describe(&e))?;
        let select = Statement::new((target.native)(NativeSql::Select, column))
            .with_params(vec![V::I32(900)]);
        let before = tx.query(&select, &cancel).await.map_err(|e| describe(&e))?;
        for operation in [
            insert_statement(target, column, 901, &invalid),
            PortableStatement::Update(UpdateStatement {
                table: table.clone(),
                assignments: vec![(column.into(), Expression::literal(invalid.clone()))],
                filter: Some(eq(target.id_column, V::I32(900))),
                returning: vec![],
            }),
        ] {
            match execute_portable(tx.as_mut(), &operation, &cancel).await {
                Err(error)
                    if matches!(error.phase, ErrorPhase::Prepare | ErrorPhase::Validate)
                        && error.category == plenora_database_core::ErrorCategory::DataMapping
                        && error.remote_effect == plenora_database_core::RemoteEffect::None => {}
                Err(error) => return Err(describe(&error)),
                Ok(_) => return Err("temporale malformato scritto".into()),
            }
            let after = tx.query(&select, &cancel).await.map_err(|e| describe(&e))?;
            if after != before {
                return Err("UPDATE rifiutato ha cambiato la riga".into());
            }
            let absent = tx
                .query(
                    &Statement::new((target.native)(NativeSql::Select, column))
                        .with_params(vec![V::I32(901)]),
                    &cancel,
                )
                .await
                .map_err(|e| describe(&e))?;
            if !absent.is_empty() {
                return Err("INSERT rifiutato ha lasciato una riga".into());
            }
        }
        println!("L1 INSERT/UPDATE: DataMapping prima dell'invio; righe intatte");
        tx.commit(&cancel).await.map_err(|e| describe(&e))?;
        let query = ReadOperation {
            source: target.table.clone(),
            projection: vec![column.into()],
            filter: Some(FilterExpression::Eq {
                field: column.into(),
                parameter: "p".into(),
            }),
            order_by: vec![],
            row_limit: None,
            row_offset: None,
            declared_crs: vec![],
        };
        let budget = ResourceBudget::new(target.limits.clone()).map_err(|e| describe(&e))?;
        let mut positive = query.clone();
        positive.filter = None;
        let empty = ParameterBag::default();
        let mut stream = target
            .provider
            .read(target.secret, &positive, &empty, &budget, &cancel)
            .await
            .map_err(|e| describe(&e))?;
        let mut rows = 0;
        while let Some(batch) = stream.next_batch(&cancel).await.map_err(|e| describe(&e))? {
            rows += batch.num_rows();
        }
        if rows != 1 {
            return Err("lettura di controllo senza la riga valida".into());
        }
        drop(stream);
        // Il confronto temporale/testo e chiuso dalla regola di famiglia,
        // mentre INSERT/UPDATE sopra raggiungono il cancello di ammissione.
        let expected_category = if column == text_column {
            plenora_database_core::ErrorCategory::InvalidPlan
        } else {
            plenora_database_core::ErrorCategory::DataMapping
        };
        let bag = ParameterBag::new([("p".into(), invalid)].into());
        match target
            .provider
            .read(target.secret, &query, &bag, &budget, &cancel)
            .await
        {
            Err(error)
                if matches!(error.phase, ErrorPhase::Prepare | ErrorPhase::Validate)
                    && error.category == expected_category
                    && (column != text_column
                        || error.message
                            == plenora_database_core::spatial_operand::OPERAND_FAMILY_MISMATCH)
                    && error.remote_effect == plenora_database_core::RemoteEffect::None => {}
            Err(error) => return Err(describe(&error)),
            Ok(_) => return Err("filtro temporale malformato ammesso".into()),
        }
        let mut cleanup = begin(target).await?;
        cleanup
            .execute(
                &Statement::new((target.native)(NativeSql::Delete, column))
                    .with_params(vec![V::I32(900)]),
                &cancel,
            )
            .await
            .map_err(|e| describe(&e))?;
        cleanup.commit(&cancel).await.map_err(|e| describe(&e))?;
    }
    Ok(())
}

/// L-B: il piano con SRID incoerente si rifiuta prima di leggere il catalogo
/// o eseguire SQL (la relazione e lo statement sono deliberatamente assenti).
///
/// # Errors
/// Fallisce se il provider raggiunge I/O invece del rifiuto `Crs`.
pub async fn mismatched_spatial_frame(
    tx: &mut dyn plenora_database_core::transaction::TransactionScope,
) -> Result<(), String> {
    use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
    let operation = serde_json::from_value(serde_json::json!({
        "source":{"object":{"catalog":null,"schema":null,"object":"audit900_absent"},"alias":null},
        "projection":[{"expression":{"kind":"column","column":{"relation":null,"field":"id"}},"alias":null}],
        "filter":{"kind":"spatial","function":"intersects","arguments":[
            {"kind":"column","column":{"relation":null,"field":"g"}},
            {"kind":"spatial_value","expression":{"kind":"typed_parameter","name":"p","parameter_type":"binary"},"srid":4326,"semantics":"geometry"}
        ]}
    })).map_err(|_| "piano L-B invalido".to_owned())?;
    let mut bytes = vec![1, 1, 0, 0, 0];
    bytes.extend(1_f64.to_le_bytes());
    bytes.extend(2_f64.to_le_bytes());
    let statement = Statement::new("AUDIT900 MUST NOT EXECUTE").with_params(vec![V::Wkb {
        bytes,
        srid: Some(3857),
        dimensions: Dimensions::Xy,
        semantics: SpatialSemantics::Geometry,
    }]);
    let outcome = plenora_database_core::facade::query_judged(
        tx,
        &statement,
        Some(&plenora_database_core::spatial_operand::OperandPlan::Query(
            &operation,
        )),
        &["p".into()],
        &CancellationToken::new(),
    )
    .await;
    match outcome {
        Err(error)
            if error.category == plenora_database_core::ErrorCategory::Crs
                && error.phase == ErrorPhase::Prepare =>
        {
            Ok(())
        }
        Err(error) => Err(describe(&error)),
        Ok(_) => Err("SRID incoerente eseguito".to_owned()),
    }
}
