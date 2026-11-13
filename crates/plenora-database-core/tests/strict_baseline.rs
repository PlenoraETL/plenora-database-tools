//! Severita dei rischi da preflight PG, `rule_p` SQL Server e `StoreRewrite` in
//! 99ec6332 (9.0.3). La matrice deve enumerare anche ogni nuova regola.
use plenora_database_core::execution_loss::RiskEntry;
use plenora_database_core::loss::{LossReport, LossSeverity, MappingPolicy};
use plenora_database_core::loss_registry::RuleId;

#[test]
fn strict_rule_outcomes_match_903_baseline() {
    use LossSeverity::{DataLoss, Information, Warning};
    use RuleId::*;
    let baseline = [
        (MysqlDoubleUnderflow, DataLoss),
        (MysqlFixedDouble, DataLoss),
        (MysqlFixedFloat, DataLoss),
        (MysqlFractionalSeconds, DataLoss),
        (MysqlNegativeZero, DataLoss),
        (PgAddNullable, Information),
        (PgArrowMismatch, DataLoss),
        (PgCrs, DataLoss),
        // 11.0.0: rischio di layout; la decisione strict e per valore nel ledger.
        (PgGeometryRepresentation, Warning),
        (PgJsonb, Warning),
        (PgMissingColumn, DataLoss),
        (PgNullability, DataLoss),
        (PgNumeric, DataLoss),
        (PgPadText, DataLoss),
        (PgSrid, DataLoss),
        (PgTemporal, DataLoss),
        (PgTextLimit, DataLoss),
        (SsAddNullable, Information),
        (SsBinaryLimit, DataLoss),
        (SsEncoding, Warning),
        (SsOffset, Warning),
        (SsTemporal, DataLoss),
        (SsTextLimit, DataLoss),
        (SsXml, Warning),
        (StoreEmptyToNull, DataLoss),
        (StoreEvery, DataLoss),
        (StoreExcessSpaces, DataLoss),
        (StoreFractionalSeconds, DataLoss),
        (StoreJsonNumbers, DataLoss),
        (StoreNegativeZero, DataLoss),
        (StoreNonAscii, DataLoss),
        (StorePadBinary, DataLoss),
        (StorePadText, DataLoss),
        (StoreStripTrailing, DataLoss),
        (StoreUnderflow, DataLoss),
    ];
    assert_eq!(baseline.map(|(id, _)| id).as_slice(), RuleId::ALL);
    for (id, severity) in baseline {
        let risk = RiskEntry::new(0, id, MappingPolicy::Strict, None, None);
        assert_eq!(risk.severity, severity, "{id:?}");
        let report = LossReport {
            schema_version: 2,
            policy: MappingPolicy::Strict,
            losses: vec![risk.legacy()],
        };
        assert_eq!(report.permits_execution(), severity != DataLoss, "{id:?}");
    }
}
