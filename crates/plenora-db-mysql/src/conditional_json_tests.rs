use super::*;

#[test]
fn conditional_relations_are_conservative_and_preserve_names() {
    assert_eq!(
        relation("UPDATE `Db`.`T` SET hit=? WHERE id=?", true).unwrap(),
        (Some("Db".into()), "T".into())
    );
    assert_eq!(
        relation("SELECT id FROM `T` WHERE id=?", false).unwrap(),
        (None, "T".into())
    );
    for sql in [
        "UPDATE t JOIN j SET hit=1",
        "UPDATE t SET hit=(SELECT 1)",
        "UPDATE t SET hit=1 WHERE id=CAST(? AS JSON)",
        "UPDATE t SET hit=1 /* hidden */",
        "UPDATE t SET hit=1 WHERE id=@v",
    ] {
        assert_eq!(
            relation(sql, true).unwrap_err().category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );
    }
    for sql in [
        "SELECT id FROM t UNION SELECT id FROM j",
        "SELECT id FROM t,j",
        "SELECT id FROM t WHERE EXISTS(SELECT 1)",
    ] {
        assert!(relation(sql, false).is_err());
    }
}
