use super::*;

#[test]
fn cli_catalog_contains_only_cli_operations() {
    let document = public_capabilities(PublicSurface::Cli, "plenora-database", None);
    assert_eq!(document.schema_version, 2);
    assert_eq!(document.interfaces[0].contract, "plenora-cli-v2");
    assert!(document
        .operations
        .iter()
        .any(|operation| operation.id == "database.read"));
    assert!(!document
        .operations
        .iter()
        .any(|operation| operation.id == "database.transaction.commit"));
}

#[test]
fn every_operation_uses_immutable_contract_ids() {
    let document = public_capabilities(PublicSurface::Rust, "plenora-database-core", None);
    for operation in document.operations {
        assert!(operation.input.contract.starts_with("plenora-database-"));
        assert!(operation.input.contract.ends_with("-v1"));
        assert!(operation.output.contract.starts_with("plenora-database-"));
        assert!(operation.output.contract.ends_with("-v1"));
    }
}
