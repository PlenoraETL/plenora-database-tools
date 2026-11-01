use super::*;
use plenora_database_core::arrow::array::{ArrayRef, BinaryArray, StringArray};
use plenora_database_core::arrow::schema::{DataType, Field};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{ObjectRef, TransactionProfile, WriteMode};
use plenora_database_core::protocol::contract_schema;
use plenora_database_core::resource::ResourceLimits;
use plenora_database_core::RemoteEffect;
use std::sync::Arc;

struct Input(VecDeque<RecordBatch>, SchemaRef);
impl BatchStream for Input {
    fn schema(&self) -> SchemaRef {
        self.1.clone()
    }
    fn next_batch<'a>(
        &'a mut self,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move { Ok(self.0.pop_front()) })
    }
}

fn operation(mode: WriteMode, key: bool) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("S".into()),
            object: "T".into(),
        },
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: if key { vec!["V".into()] } else { vec![] },
        update_columns: vec![],
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

fn input(binary: bool, empty: bool) -> Box<dyn BatchStream> {
    let schema = contract_schema(vec![Field::new(
        "V",
        if binary {
            DataType::Binary
        } else {
            DataType::Utf8
        },
        true,
    )])
    .unwrap();
    let batches = [false, empty].map(|empty| {
        let array: ArrayRef = if binary {
            Arc::new(BinaryArray::from(vec![
                None,
                Some(if empty { &b""[..] } else { &b"x"[..] }),
            ]))
        } else {
            Arc::new(StringArray::from(vec![
                None,
                Some(if empty { "" } else { "x" }),
            ]))
        };
        RecordBatch::try_new(schema.clone(), vec![array]).unwrap()
    });
    Box::new(Input(VecDeque::from(batches), schema))
}

#[tokio::test]
async fn vuoto_nel_secondo_batch_rifiutato_senza_effetti_in_ogni_mode() {
    for mode in [
        WriteMode::Append,
        WriteMode::Create,
        WriteMode::Replace,
        WriteMode::Update,
        WriteMode::Upsert,
        WriteMode::DeleteByKeys,
    ] {
        for binary in [false, true] {
            for key in [false, true] {
                let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
                let before = budget.remaining(ResourceKind::MemoryBytes);
                let error = reject_empty_input(
                    input(binary, true),
                    &operation(mode, key),
                    &budget,
                    &CancellationToken::new(),
                )
                .await
                .err()
                .expect("rifiuto");
                assert_eq!(error.category, ErrorCategory::DataMapping);
                assert_eq!(error.phase, ErrorPhase::Write);
                assert_eq!(error.remote_effect, RemoteEffect::None);
                assert_eq!(error.message, if key { EMPTY_KEY } else { EMPTY_VALUE });
                assert_eq!(budget.remaining(ResourceKind::MemoryBytes), before);
            }
        }
    }
}

#[tokio::test]
async fn null_e_valori_pieni_restano_distinti_e_il_buffer_rispetta_il_budget() {
    for binary in [false, true] {
        let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
        let before = budget.remaining(ResourceKind::MemoryBytes);
        let cancellation = CancellationToken::new();
        let mut stream = reject_empty_input(
            input(binary, false),
            &operation(WriteMode::Append, false),
            &budget,
            &cancellation,
        )
        .await
        .unwrap();
        assert!(budget.remaining(ResourceKind::MemoryBytes) < before);
        for _ in 0..2 {
            let batch = stream.next_batch(&cancellation).await.unwrap().unwrap();
            assert!(batch.column(0).is_null(0));
            assert!(!batch.column(0).is_null(1));
        }
        assert!(stream.next_batch(&cancellation).await.unwrap().is_none());
        assert_eq!(budget.remaining(ResourceKind::MemoryBytes), before);
        let budget = ResourceBudget::new(ResourceLimits {
            memory_bytes: 1,
            cell_bytes: 1,
            ..ResourceLimits::default()
        })
        .unwrap();
        assert!(reject_empty_input(
            input(binary, false),
            &operation(WriteMode::Append, false),
            &budget,
            &cancellation
        )
        .await
        .is_err());
    }
}
