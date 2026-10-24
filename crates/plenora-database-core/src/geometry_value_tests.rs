use super::*;

fn header(code: u32) -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&code.to_le_bytes());
    bytes
}

fn coordinates(bytes: &mut Vec<u8>, values: &[f64]) {
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
}

fn point(code: u32, srid: Option<u32>, values: &[f64]) -> Vec<u8> {
    let mut bytes = header(code);
    if let Some(srid) = srid {
        bytes.extend_from_slice(&srid.to_le_bytes());
    }
    coordinates(&mut bytes, values);
    bytes
}

fn contract(ewkb: bool, srid: Option<u32>) -> ValueContract<'static> {
    ValueContract {
        ewkb: Some(ewkb),
        srid,
        types: None,
        dimensions: Some("xy"),
    }
}

fn verify(contract: ValueContract<'_>, bytes: &[u8]) -> Result<EwkbInspection> {
    verify_value(contract, bytes, u64::MAX, u64::MAX, ErrorPhase::Write)
}

fn category(contract: ValueContract<'_>, bytes: &[u8]) -> ErrorCategory {
    verify(contract, bytes)
        .expect_err("valore rifiutato")
        .category
}

#[test]
fn iso_point_is_accepted_under_both_encodings() {
    let value = point(1, None, &[10.0, 20.0]);
    assert!(verify(contract(false, None), &value).is_ok());
    assert!(verify(contract(true, Some(4326)), &value).is_ok());
}

/// GEO-009: un valore senza SRID ha lo SRID del campo. Fino al 24/10 la
/// scrittura PostgreSQL lo rifiutava come SRID diverso.
#[test]
fn ewkb_value_without_srid_takes_the_field_srid() {
    let value = point(1, None, &[10.0, 20.0]);
    let inspection = verify(contract(true, Some(4326)), &value).expect("accettato");
    assert_eq!(inspection.root.srid, None);
}

/// GEO-008: sotto `wkb` il flag SRID e malformato, anche uguale al campo.
#[test]
fn srid_flag_under_wkb_is_a_value_defect() {
    let value = point(0x2000_0001, Some(4326), &[10.0, 20.0]);
    assert_eq!(
        category(contract(false, Some(4326)), &value),
        ErrorCategory::DataMapping
    );
}

/// GEO-009: uno SRID diverso da quello del campo, o su un campo che non ne
/// dichiara, contraddice il CRS. Un campo con SRID `0` non e un campo senza
/// SRID: fino al 24/10 PostgreSQL saltava il confronto con `0`.
#[test]
fn ewkb_srid_contradicting_the_field_is_crs() {
    let value = point(0x2000_0001, Some(4326), &[10.0, 20.0]);
    assert_eq!(
        category(contract(true, Some(3857)), &value),
        ErrorCategory::Crs
    );
    assert_eq!(
        category(contract(true, Some(0)), &value),
        ErrorCategory::Crs
    );
    assert_eq!(category(contract(true, None), &value), ErrorCategory::Crs);
    assert!(verify(contract(true, Some(4326)), &value).is_ok());
}

#[test]
fn srid_on_a_member_is_malformed_even_when_equal() {
    let mut value = header(0x2000_0004);
    value.extend_from_slice(&4326_u32.to_le_bytes());
    value.extend_from_slice(&1_u32.to_le_bytes());
    value.extend(point(0x2000_0001, Some(4326), &[1.0, 2.0]));
    assert_eq!(
        category(contract(true, Some(4326)), &value),
        ErrorCategory::DataMapping
    );
}

#[test]
fn iso_code_with_extended_flags_is_malformed() {
    let value = point(0x03e9 | 0x8000_0000, None, &[1.0, 2.0, 3.0]);
    let mut xyz = contract(true, None);
    xyz.dimensions = Some("xyz");
    assert_eq!(category(xyz, &value), ErrorCategory::DataMapping);
    // Le due forme da sole sono ammesse.
    assert!(verify(xyz, &point(1001, None, &[1.0, 2.0, 3.0])).is_ok());
    assert!(verify(xyz, &point(0x8000_0001, None, &[1.0, 2.0, 3.0])).is_ok());
}

#[test]
fn structural_defects_are_malformed() {
    let mut line = header(2);
    line.extend_from_slice(&1_u32.to_le_bytes());
    coordinates(&mut line, &[1.0, 2.0]);

    let mut open_ring = header(3);
    open_ring.extend_from_slice(&1_u32.to_le_bytes());
    open_ring.extend_from_slice(&4_u32.to_le_bytes());
    coordinates(&mut open_ring, &[0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0]);

    let mut triangle = header(17);
    triangle.extend_from_slice(&2_u32.to_le_bytes());
    for _ in 0..2 {
        triangle.extend_from_slice(&4_u32.to_le_bytes());
        coordinates(&mut triangle, &[0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0]);
    }

    let mut wrong_member = header(4);
    wrong_member.extend_from_slice(&1_u32.to_le_bytes());
    wrong_member.extend(line.clone());

    let mut mixed_dimensions = header(7);
    mixed_dimensions.extend_from_slice(&1_u32.to_le_bytes());
    mixed_dimensions.extend(point(1001, None, &[1.0, 2.0, 3.0]));

    let mut trailing = point(1, None, &[1.0, 2.0]);
    trailing.push(0);

    let truncated = &point(1, None, &[1.0, 2.0])[..12];

    let mut deep = Vec::new();
    for _ in 0..33 {
        deep.extend(header(7));
        deep.extend_from_slice(&1_u32.to_le_bytes());
    }
    deep.extend(point(1, None, &[1.0, 2.0]));

    for (name, value) in [
        ("linestring di un punto", line.as_slice()),
        ("anello aperto", open_ring.as_slice()),
        ("triangolo con due anelli", triangle.as_slice()),
        ("membro non ammesso", wrong_member.as_slice()),
        ("dimensioni dei membri", mixed_dimensions.as_slice()),
        ("byte residui", trailing.as_slice()),
        ("troncato", truncated),
        ("33 livelli", deep.as_slice()),
        ("byte order", &[2, 1, 0, 0, 0]),
        ("tipo astratto", point(13, None, &[]).as_slice()),
    ] {
        let error = verify(contract(true, None), value).expect_err(name);
        assert_eq!(error.category, ErrorCategory::DataMapping, "{name}");
        assert_eq!(error.phase, ErrorPhase::Write, "{name}");
    }
}

#[test]
fn thirty_two_levels_are_admitted() {
    let mut nested = Vec::new();
    for _ in 0..32 {
        nested.extend(header(7));
        nested.extend_from_slice(&1_u32.to_le_bytes());
    }
    nested.extend(point(1, None, &[1.0, 2.0]));
    let mut any = contract(true, None);
    any.dimensions = None;
    assert!(verify(any, &nested).is_ok());
}

#[test]
fn declared_types_and_dimensions_bind_the_root() {
    let value = point(1, None, &[1.0, 2.0]);
    let mut polygons = contract(true, None);
    polygons.types = Some("polygon");
    assert_eq!(category(polygons, &value), ErrorCategory::DataMapping);
    polygons.types = Some("point,polygon");
    assert!(verify(polygons, &value).is_ok());
    polygons.types = Some("unknown");
    assert!(verify(polygons, &value).is_ok());
    let mut xyz = contract(true, None);
    xyz.dimensions = Some("xyz");
    assert_eq!(category(xyz, &value), ErrorCategory::DataMapping);
}

/// REJ-002: la buona forma prima dello SRID, lo SRID prima del tipo.
#[test]
fn checks_follow_the_contract_order() {
    let mut polygons = contract(true, Some(3857));
    polygons.types = Some("polygon");
    let srid_and_type = point(0x2000_0001, Some(4326), &[1.0, 2.0]);
    assert_eq!(category(polygons, &srid_and_type), ErrorCategory::Crs);
    let truncated_with_srid = &srid_and_type[..srid_and_type.len() - 1];
    assert_eq!(
        category(polygons, truncated_with_srid),
        ErrorCategory::DataMapping
    );
}

#[test]
fn resource_bounds_stay_resource_limits() {
    let value = point(1, None, &[1.0, 2.0]);
    let error = verify_value(contract(true, None), &value, 1, u64::MAX, ErrorPhase::Read)
        .expect_err("oltre i componenti");
    assert_eq!(error.category, ErrorCategory::ResourceLimit);
    let error = verify_value(contract(true, None), &value, u64::MAX, 0, ErrorPhase::Read)
        .expect_err("oltre la profondita");
    assert_eq!(error.category, ErrorCategory::ResourceLimit);
}

#[test]
fn phase_is_the_callers() {
    let error = verify_value(
        contract(false, None),
        &[2],
        u64::MAX,
        u64::MAX,
        ErrorPhase::Read,
    )
    .expect_err("malformato");
    assert_eq!(error.phase, ErrorPhase::Read);
}

/// Un conteggio di punti che i byte non contengono e un valore troncato,
/// anche quando supera il budget dei componenti: la classe e del valore.
#[test]
fn a_count_beyond_the_bytes_is_truncation_not_a_resource_limit() {
    let mut line = header(2);
    line.extend_from_slice(&u32::MAX.to_le_bytes());
    let error = verify_value(contract(true, None), &line, 16, u64::MAX, ErrorPhase::Read)
        .expect_err("troncato");
    assert_eq!(error.category, ErrorCategory::DataMapping);
}
