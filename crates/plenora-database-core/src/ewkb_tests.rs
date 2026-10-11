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

fn unmarked_point_z() -> Vec<u8> {
    let mut bytes = point();
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
    assert!(inspection.has_any_embedded_srid);
    assert_eq!(inspection.embedded_srid_count, 1);
}

#[test]
fn counts_embedded_srids_beyond_the_root() {
    let bytes = collection(&point_z_srid());
    let inspection = inspect_ewkb_detailed(&bytes, 10, 2).expect("valid collection");
    assert_eq!(inspection.root.srid, None);
    assert!(inspection.has_any_embedded_srid);
    assert_eq!(inspection.embedded_srid_count, 1);
}

#[test]
fn normalizes_unmarked_xyz_in_nested_geometries() {
    let bytes = collection(&unmarked_point_z());
    assert!(inspect_ewkb_detailed(&bytes, 10, 2).is_err());
    let normalized = normalize_unmarked_xyz(&bytes, 10, 2).expect("normalizza XYZ senza flag");
    let inspection = inspect_ewkb_detailed(&normalized, 10, 2).expect("EWKB XYZ normalizzata");
    assert!(inspection.root.has_z);
    assert!(inspection.has_any_z);
    assert!(!inspection.has_any_m);
}

/// Un WKB ISO diventa EWKB con lo SRID dichiarato, nel suo byte order; un
/// valore che ne porta gia uno e rifiutato.
#[test]
fn iso_wkb_gets_the_declared_srid_in_its_byte_order() {
    let mut little = vec![1_u8];
    little.extend_from_slice(&1_u32.to_le_bytes());
    little.extend_from_slice(&[0_u8; 16]);
    let stamped = with_root_srid(&little, 4326, 8).expect("little endian");
    let inspection = inspect_ewkb_detailed(&stamped, 10, 1).expect("EWKB");
    assert_eq!(inspection.root.srid, Some(4326));
    assert_eq!(stamped.len(), little.len() + 4);

    let mut big = vec![0_u8];
    big.extend_from_slice(&1_u32.to_be_bytes());
    big.extend_from_slice(&[0_u8; 16]);
    let stamped = with_root_srid(&big, 3857, 8).expect("big endian");
    assert_eq!(
        inspect_ewkb_detailed(&stamped, 10, 1)
            .expect("EWKB")
            .root
            .srid,
        Some(3857)
    );

    assert!(
        with_root_srid(&stamped, 4326, 8).is_err(),
        "SRID gia presente"
    );
    assert!(with_root_srid(&[1, 1, 0], 4326, 8).is_err(), "troncato");
}
