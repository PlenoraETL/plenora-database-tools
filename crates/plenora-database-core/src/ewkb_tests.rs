use super::*;

fn point() -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&0_f64.to_le_bytes());
    bytes.extend_from_slice(&1_f64.to_le_bytes());
    bytes
}

fn collection(child: &[u8]) -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&7_u32.to_le_bytes());
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(child);
    bytes
}

fn point_z_srid() -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&(0xa000_0001_u32).to_le_bytes());
    bytes.extend_from_slice(&4_326_u32.to_le_bytes());
    bytes.extend_from_slice(&0_f64.to_le_bytes());
    bytes.extend_from_slice(&1_f64.to_le_bytes());
    bytes.extend_from_slice(&2_f64.to_le_bytes());
    bytes
}

#[test]
fn counts_components_without_recursive_calls() {
    let bytes = collection(&collection(&point()));
    let stats = inspect_ewkb(&bytes, 10, 3).expect("valid collection");
    assert_eq!(stats.components, 4);
    assert_eq!(stats.max_depth, 3);
}

#[test]
fn rejects_depth_and_component_bombs() {
    let bytes = collection(&collection(&point()));
    assert_eq!(
        inspect_ewkb(&bytes, 10, 2).expect_err("depth").category,
        ErrorCategory::ResourceLimit
    );
    assert_eq!(
        inspect_ewkb(&bytes, 3, 3).expect_err("components").category,
        ErrorCategory::ResourceLimit
    );
}

#[test]
fn rejects_truncation_and_trailing_bytes() {
    let mut bytes = point();
    bytes.pop();
    assert_eq!(
        inspect_ewkb(&bytes, 10, 3).expect_err("truncated").category,
        ErrorCategory::DataMapping
    );
    let mut trailing = point();
    trailing.push(0);
    assert!(inspect_ewkb(&trailing, 10, 3).is_err());
}

#[test]
fn reports_root_contract_metadata_from_the_validated_header() {
    let inspection = inspect_ewkb_detailed(&point_z_srid(), 10, 1).expect("valid point");
    assert_eq!(inspection.stats.components, 2);
    assert_eq!(inspection.root.base_type, 1);
    assert_eq!(inspection.root.dimensions_label(), "xyz");
    assert_eq!(inspection.root.geometry_type_name(), Some("Point"));
    assert_eq!(inspection.root.srid, Some(4_326));
}
