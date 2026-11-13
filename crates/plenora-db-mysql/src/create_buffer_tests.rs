use super::*;

#[test]
fn temporary_extra_absence_is_the_only_normalization() {
    let permanent = vec![Value::Bytes(Vec::new()); 9];
    let mut temporary = permanent.clone();
    temporary[6] = Value::NULL;
    normalize_extra(&mut temporary);
    assert_eq!(temporary, permanent);
    for column in 0..9 {
        let mut changed = permanent.clone();
        changed[column] = Value::Bytes(b"different".to_vec());
        normalize_extra(&mut changed);
        assert_ne!(changed, permanent);
    }
    let mut nullable_default = permanent.clone();
    nullable_default[5] = Value::NULL;
    normalize_extra(&mut nullable_default);
    assert_ne!(nullable_default, permanent);
}
