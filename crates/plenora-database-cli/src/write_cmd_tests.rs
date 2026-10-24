use super::*;
use plenora_database_core::arrow::array::RecordBatch;
use plenora_database_core::arrow::schema::{DataType, Field, Schema, SchemaRef};
use plenora_database_core::provider::ProviderFuture;
use plenora_database_core::ErrorCategory;
use std::collections::HashMap;
use std::sync::Arc;

struct SchemaOnly(SchemaRef);

impl BatchStream for SchemaOnly {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.0)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async { Ok(None) })
    }
}

fn operation() -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("public".to_owned()),
            object: "target".to_owned(),
        },
        mode: WriteMode::Append,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

/// REJ-001: il dry run giudica lo schema come la scrittura. Fino al 24/10
/// rispondeva `dry_run` anche a uno schema che la scrittura rifiuta, qui un
/// campo `geoarrow.wkb` senza le chiavi obbligatorie del vocabolario.
#[tokio::test]
async fn dry_run_rejects_a_schema_the_write_would_reject() {
    let field = Field::new("geom", DataType::Binary, true).with_metadata(HashMap::from([(
        "ARROW:extension:name".to_owned(),
        "geoarrow.wkb".to_owned(),
    )]));
    let schema = Arc::new(Schema::new_with_metadata(
        vec![field],
        HashMap::from([("plenora.contract.version".to_owned(), "1".to_owned())]),
    ));
    let error = execute_bulk_write(
        "UNUSED_DSN",
        operation(),
        Box::new(SchemaOnly(schema)),
        true,
    )
    .await
    .expect_err("schema non conforme in dry run");
    assert_eq!(error.database_error().category, ErrorCategory::Schema);
}
