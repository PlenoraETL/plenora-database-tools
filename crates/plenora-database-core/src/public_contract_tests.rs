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

#[test]
fn runtime_catalog_lists_the_operations_bound_by_runtime_v1() {
    let document = public_capabilities(PublicSurface::Runtime, RUNTIME_CAPABILITY, None);
    assert_eq!(
        document.interfaces[0].contract,
        "plenora-runtime-binding-v1"
    );
    assert_eq!(document.interfaces[0].version, 1);
    assert_eq!(document.interfaces[0].artifact, "plenora.database-tools");
    let ids = document
        .operations
        .iter()
        .map(|operation| operation.id.as_str())
        .collect::<Vec<_>>();
    // `bindings/runtime-v1.json` del pin: sette operazioni `required` e
    // `database.query` `conditional`; execute e transazioni non hanno la
    // superficie runtime nel catalogo.
    assert_eq!(
        ids,
        [
            "database.test_connection",
            "database.list_catalogs",
            "database.list_schemas",
            "database.list_objects",
            "database.describe_object",
            "database.read",
            "database.write",
            "database.query",
        ]
    );
    assert!(document
        .operations
        .iter()
        .all(|operation| operation.surfaces == [PublicSurface::Runtime]));
}
