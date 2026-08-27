use super::*;

#[test]
fn predicate_requires_distance_only_for_dwithin() {
    assert!(!SpatialPredicate::Intersects.requires_distance());
    assert!(!SpatialPredicate::Contains.requires_distance());
    assert!(!SpatialPredicate::Within.requires_distance());
    assert!(!SpatialPredicate::BoundingBox.requires_distance());
    assert!(SpatialPredicate::DWithin {
        distance_meters: 1.0
    }
    .requires_distance());
}

#[test]
fn predicate_serializes_snake_case() {
    let p = SpatialPredicate::DWithin {
        distance_meters: 500.0,
    };
    let json = serde_json::to_string(&p).unwrap();
    assert!(json.contains("d_within"));
    assert!(json.contains("distance_meters"));
}

// Fixture EWKB per la validazione dei riferimenti spaziali.

/// Costruisce un EWKB Point 2D con SRID prefixed. Little-endian.
/// Formato: byte order + `type_with_srid_flag` + srid + x + y.
fn ewkb_point_xy(srid: u32, x: f64, y: f64) -> Vec<u8> {
    let mut b = Vec::with_capacity(25);
    b.push(0x01); // little-endian
                  // Type = 1 (Point) | 0x20000000 (SRID flag)
    b.extend_from_slice(&0x2000_0001_u32.to_le_bytes());
    b.extend_from_slice(&srid.to_le_bytes());
    b.extend_from_slice(&x.to_le_bytes());
    b.extend_from_slice(&y.to_le_bytes());
    b
}

/// Come sopra ma Point Z (3D) — flag Z = 0x80000000.
fn ewkb_point_xyz(srid: u32, x: f64, y: f64, z: f64) -> Vec<u8> {
    let mut b = Vec::with_capacity(33);
    b.push(0x01);
    b.extend_from_slice(&0xA000_0001_u32.to_le_bytes()); // SRID + Z
    b.extend_from_slice(&srid.to_le_bytes());
    b.extend_from_slice(&x.to_le_bytes());
    b.extend_from_slice(&y.to_le_bytes());
    b.extend_from_slice(&z.to_le_bytes());
    b
}

#[test]
fn new_validated_accepts_matching_srid_and_dimensions() {
    let ewkb = ewkb_point_xy(4326, 9.19, 45.46);
    let r =
        SpatialReference::new_validated(ewkb, 4326, Dimensions::Xy, SpatialSemantics::Geography)
            .unwrap();
    assert_eq!(r.srid, 4326);
}

#[test]
fn new_validated_rejects_srid_mismatch() {
    let ewkb = ewkb_point_xy(4326, 9.19, 45.46);
    // Dichiaro 3857 ma l'EWKB è WGS84 → deve fallire (attacco
    // di aggiramento della policy DWithin+Geometry+geog_srid).
    let err =
        SpatialReference::new_validated(ewkb, 3857, Dimensions::Xy, SpatialSemantics::Geometry)
            .unwrap_err();
    assert_eq!(err.category, crate::ErrorCategory::InvalidPlan);
    assert!(err.message.contains("SRID"));
}

#[test]
fn new_validated_rejects_dimensions_mismatch() {
    // EWKB Point Z (3D) ma dichiaro Xy → fail.
    let ewkb = ewkb_point_xyz(4326, 9.19, 45.46, 100.0);
    let err =
        SpatialReference::new_validated(ewkb, 4326, Dimensions::Xy, SpatialSemantics::Geometry)
            .unwrap_err();
    assert_eq!(err.category, crate::ErrorCategory::InvalidPlan);
    assert!(err.message.contains("dimensioni"));
}

#[test]
fn new_validated_accepts_dimensions_unknown_as_wildcard() {
    // Consumer che non conosce le dims a priori dichiara Unknown.
    let ewkb = ewkb_point_xyz(4326, 9.19, 45.46, 100.0);
    SpatialReference::new_validated(ewkb, 4326, Dimensions::Unknown, SpatialSemantics::Geometry)
        .unwrap();
}

#[test]
fn new_validated_rejects_malformed_ewkb() {
    // inspect_ewkb propaga la propria categoria (DataMapping per
    // bytes malformati) — non normalizzata a InvalidPlan.
    assert!(SpatialReference::new_validated(
        vec![0x00, 0x01, 0x02],
        4326,
        Dimensions::Xy,
        SpatialSemantics::Geometry,
    )
    .is_err());
}
