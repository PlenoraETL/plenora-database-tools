use super::*;
use crate::arrow::{DataType, Field};
use crate::loss::{LossReport, MappingPolicy};
use crate::plan::{ObjectRef, TransactionProfile, WriteMode};
use crate::resource::{ResourceKind, ResourceLimits};

#[test]
fn driver_state_is_opaque_typed_and_single_use() {
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let schema = crate::protocol::contract_schema(vec![Field::new("id", DataType::Int64, false)]);
    let mut prepared = PreparedWrite::new(
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
        },
        schema,
        LossReport {
            schema_version: 2,
            policy: MappingPolicy::Strict,
            losses: Vec::new(),
        },
        budget.clone(),
        budget
            .try_lease(ResourceKind::ConcurrentOperations, 1)
            .expect("operation lease"),
        budget
            .try_lease(ResourceKind::Columns, 1)
            .expect("column lease"),
    )
    .with_driver_state(17_u32);

    assert_eq!(prepared.take_driver_state::<String>(), None);
    assert_eq!(prepared.take_driver_state::<u32>(), Some(17));
    assert_eq!(prepared.take_driver_state::<u32>(), None);
}
