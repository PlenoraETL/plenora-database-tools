use super::catalog_type;

#[test]
fn typmods_decode_precision_scale_length_and_fraction() {
    let numeric = catalog_type("numeric", (12 << 16 | 4) + 4);
    assert_eq!((numeric.precision, numeric.scale), (Some(12), Some(4)));
    let unbounded = catalog_type("numeric", -1);
    assert_eq!((unbounded.precision, unbounded.scale), (None, None));
    assert_eq!(catalog_type("varchar", 14).length, Some(10));
    assert_eq!(catalog_type("bpchar", 8).length, Some(4));
    assert_eq!(catalog_type("timestamp", 0).datetime_precision, Some(0));
    assert_eq!(catalog_type("timestamptz", 3).datetime_precision, Some(3));
    assert_eq!(catalog_type("timestamp", -1).datetime_precision, None);
}
