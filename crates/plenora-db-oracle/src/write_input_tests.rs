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
                    &[],
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
            &[],
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
            &cancellation,
            &[],
        )
        .await
        .is_err());
    }
}

#[tokio::test]
async fn oversized_second_batch_is_refused_before_any_write_effect() {
    let schema = contract_schema(vec![Field::new("V", DataType::Utf8, true)]).unwrap();
    let facts = crate::write_admission::ColumnFacts {
        name: "V".into(),
        data_type: "CHAR".into(),
        data_length: 1,
        char_length: 1,
        char_used: Some("B".into()),
        precision: None,
        scale: None,
        collation: Some("BINARY".into()),
    };
    let targets = [Some(
        crate::write_intermediate::TargetType::from_catalog(&facts).unwrap(),
    )];
    for mode in [
        WriteMode::Append,
        WriteMode::Create,
        WriteMode::Replace,
        WriteMode::Update,
        WriteMode::Upsert,
        WriteMode::DeleteByKeys,
    ] {
        for key in [false, true] {
            let batches = ["x", "\u{e9}"].map(|value| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(StringArray::from(vec![Some(value)]))],
                )
                .unwrap()
            });
            let input = Box::new(Input(VecDeque::from(batches), schema.clone()));
            let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
            let before = budget.remaining(ResourceKind::MemoryBytes);
            let error = reject_empty_input(
                input,
                &operation(mode, key),
                &budget,
                &CancellationToken::new(),
                &targets,
            )
            .await
            .err()
            .expect("length refusal");
            assert_eq!(error.category, ErrorCategory::DataMapping);
            assert_eq!(error.remote_effect, RemoteEffect::None);
            assert_eq!(error.message, "testo oltre la lunghezza del target Oracle");
            assert_eq!(budget.remaining(ResourceKind::MemoryBytes), before);
        }
    }
}

#[tokio::test]
async fn raw_oversized_second_batch_is_refused_before_any_write_effect() {
    use plenora_database_core::arrow::array::LargeBinaryArray;
    for data_type in [DataType::Binary, DataType::LargeBinary] {
        for length in [1, 10] {
            let schema = contract_schema(vec![Field::new("V", data_type.clone(), true)]).unwrap();
            let facts = crate::write_admission::ColumnFacts {
                name: "V".into(),
                data_type: "RAW".into(),
                data_length: length,
                char_length: 0,
                char_used: None,
                precision: None,
                scale: None,
                collation: None,
            };
            let targets = [Some(
                crate::write_intermediate::TargetType::from_catalog(&facts).unwrap(),
            )];
            for mode in [
                WriteMode::Append,
                WriteMode::Create,
                WriteMode::Replace,
                WriteMode::Update,
                WriteMode::Upsert,
                WriteMode::DeleteByKeys,
            ] {
                for key in [false, true] {
                    let batches = [length, length + 1].map(|size| {
                        RecordBatch::try_new(
                            schema.clone(),
                            vec![{
                                let value = vec![0xAB; usize::try_from(size).unwrap()];
                                let values = vec![None, Some(value.as_slice())];
                                match data_type {
                                    DataType::Binary => {
                                        Arc::new(BinaryArray::from(values)) as ArrayRef
                                    }
                                    _ => Arc::new(LargeBinaryArray::from(values)) as ArrayRef,
                                }
                            }],
                        )
                        .unwrap()
                    });
                    let input = Box::new(Input(VecDeque::from(batches), schema.clone()));
                    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
                    let before = budget.remaining(ResourceKind::MemoryBytes);
                    let error = reject_empty_input(
                        input,
                        &operation(mode, key),
                        &budget,
                        &CancellationToken::new(),
                        &targets,
                    )
                    .await
                    .err()
                    .expect("length refusal");
                    assert_eq!(error.category, ErrorCategory::DataMapping);
                    assert_eq!(error.remote_effect, RemoteEffect::None);
                    assert_eq!(
                        error.message,
                        "binario oltre la lunghezza del target Oracle"
                    );
                    assert_eq!(budget.remaining(ResourceKind::MemoryBytes), before);
                }
            }
        }
    }
}
