//! La regola K su Db2 sotto una collation non binaria, live
//! (`crate::write::refuse_text_keys_under_collation`): un database creato con
//! `COLLATE USING` una collation che ignora le maiuscole (per esempio
//! `CLDR181_S1`), in `PLENORA_DB2_COLLATION_DATABASE`.
//!
//! Non e nel gate: il modulo e il nome del test non hanno il prefisso dei test
//! live, e `scripts/check_db2_reference.py` non lo esegue. Si esegue a mano,
//! come la prova di `varchar2_compat` (`write_rewrite_ora_tests.rs`).

use super::write_admission_live_tests::{ddl, drop_table, write, Batches, SCHEMA};
use super::write_rewrite_live_tests::{environment, provider};
use crate::Db2Provider;
use plenora_database_core::arrow::array::{ArrayRef, Int32Array, StringArray};
use plenora_database_core::arrow::schema::{DataType, Field};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{ObjectRef, TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::protocol::contract_schema;
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase};
use std::collections::VecDeque;
use std::sync::Arc;

#[tokio::test]
#[ignore = "richiede un database Db2 con una collation non binaria (PLENORA_DB2_COLLATION_DATABASE)"]
async fn db2_collation_k_chiavi_di_testo_sotto_una_collation_non_binaria_si_rifiutano() {
    let (provider, secret) = provider(&environment("PLENORA_DB2_COLLATION_DATABASE", "plncoll"));
    let table = "PLN_K_COLL";
    drop_table(&provider, &secret, table).await;
    ddl(
        &provider,
        &secret,
        &format!("CREATE TABLE {SCHEMA}.{table} (ID INTEGER NOT NULL, V VARCHAR(10) NOT NULL)"),
    )
    .await;
    ddl(
        &provider,
        &secret,
        &format!("CREATE UNIQUE INDEX {SCHEMA}.{table}_K ON {SCHEMA}.{table} (V)"),
    )
    .await;
    // La misura: sotto la collation l'indice unico tratta 'a' e 'A' come la
    // stessa voce.
    ddl(
        &provider,
        &secret,
        &format!("INSERT INTO {SCHEMA}.{table} (ID, V) VALUES (1, 'a')"),
    )
    .await;
    let duplicate = provider
        .execute_ddl(
            &secret,
            &format!("INSERT INTO {SCHEMA}.{table} (ID, V) VALUES (2, 'A')"),
            &CancellationToken::new(),
        )
        .await;
    assert!(duplicate.is_err(), "la collation confronta 'a' = 'A'");
    // Upsert e Update di [('a', 10), ('A', 20)]: la regola K non vede la
    // chiave ripetuta, e si rifiuta nel preflight.
    let keys = || -> ArrayRef { Arc::new(StringArray::from(vec!["a", "A"])) };
    let mut results = Vec::new();
    for mode in [WriteMode::Upsert, WriteMode::Update] {
        let identifiers: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        results.push(write(&provider, &secret, table, mode, &["V"], identifiers, keys()).await);
    }
    results.push(delete_by_text_keys(&provider, &secret, table, keys()).await);
    for result in results {
        let error = result.expect_err("chiave di testo sotto una collation non binaria");
        assert_eq!(error.category, ErrorCategory::Unsupported, "{error:?}");
        assert_eq!(error.phase, ErrorPhase::Prepare, "{error:?}");
        assert_eq!(error.message, crate::write::TEXT_KEYS_UNDER_COLLATION);
    }
    drop_table(&provider, &secret, table).await;
}

async fn delete_by_text_keys(
    provider: &Db2Provider,
    secret: &SecretString,
    table: &str,
    keys: ArrayRef,
) -> Result<WriteOutcome, DatabaseError> {
    let schema = contract_schema(vec![Field::new("V", DataType::Utf8, false)]).expect("schema");
    let batch = RecordBatch::try_new(schema, vec![keys]).expect("batch");
    let operation = WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some(SCHEMA.to_owned()),
            object: table.to_owned(),
        },
        mode: WriteMode::DeleteByKeys,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: vec!["V".to_owned()],
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = provider
        .prepare_write(secret, &operation, batch.schema(), &budget, &cancellation)
        .await?;
    let rows = batch.num_rows() as u64;
    provider
        .write(
            secret,
            prepared,
            Box::new(Batches {
                schema: batch.schema(),
                rows,
                queue: VecDeque::from([batch]),
            }),
            &budget,
            &cancellation,
        )
        .await
}
