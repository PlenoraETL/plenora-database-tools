//! Consumer esterno della superficie Rust dichiarata dal contratto pubblico.

use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::plan::{Operation, ReadOperation, WriteOperation};
use plenora_database_core::provider::{
    BatchStream, ParameterBag, PreparedWrite, Provider, SecretString,
};
use plenora_database_core::relational::QueryOperation;
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::transaction::{Statement, TransactionOptions, TransactionScope};
use plenora_database_core::{
    public_capabilities, rust_surface_bindings, CancellationToken, PublicSurface,
};
use std::collections::BTreeSet;

const DOCUMENTED_EXPORTS: &[&str] = &[
    "plenora_database_core::provider::Provider::test_connection",
    "plenora_database_core::provider::Provider::inspect",
    "plenora_database_core::provider::Provider::read",
    "plenora_database_core::provider::Provider::prepare_write",
    "plenora_database_core::provider::Provider::write",
    "plenora_database_core::provider::Provider::query",
    "plenora_database_core::provider::Provider::begin_transaction",
    "plenora_database_core::transaction::TransactionScope::execute",
    "plenora_database_core::transaction::TransactionScope::commit",
    "plenora_database_core::transaction::TransactionScope::rollback",
    "plenora_database_core::transaction::TransactionScope::savepoint",
    "plenora_database_core::transaction::TransactionScope::release_savepoint",
    "plenora_database_core::transaction::TransactionScope::rollback_to_savepoint",
];

#[test]
fn mapping_covers_exactly_the_advertised_rust_operations() {
    let mapping = rust_surface_bindings();
    assert_eq!(mapping.schema_version, 1);
    assert_eq!(mapping.artifact, "plenora-database-core");
    assert_eq!(mapping.artifact_version, env!("CARGO_PKG_VERSION"));

    let advertised = public_capabilities(PublicSurface::Rust, "plenora-database-core", None)
        .operations
        .into_iter()
        .map(|operation| (operation.id, operation.version))
        .collect::<BTreeSet<_>>();
    let mapped = mapping
        .bindings
        .iter()
        .map(|binding| (binding.operation.clone(), binding.version))
        .collect::<BTreeSet<_>>();
    assert_eq!(mapped, advertised);
    assert!(mapping
        .bindings
        .iter()
        .all(|binding| !binding.entrypoints.is_empty()));

    let exports = mapping
        .bindings
        .iter()
        .flat_map(|binding| binding.entrypoints.iter().map(String::as_str))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        exports,
        DOCUMENTED_EXPORTS.iter().copied().collect::<BTreeSet<_>>()
    );
}

// Queste funzioni non vengono eseguite: la loro compilazione e la prova che
// un crate consumer, senza accesso a moduli privati o feature di test, puo
// invocare ogni export nominato dalla mappa.
#[allow(dead_code, clippy::too_many_arguments)]
fn provider_exports_compile<P: Provider>(
    provider: &P,
    secret: &SecretString,
    cancellation: &CancellationToken,
    inspect: &Operation,
    read: &ReadOperation,
    write: &WriteOperation,
    query: &QueryOperation,
    parameters: &ParameterBag,
    budget: &ResourceBudget,
    schema: SchemaRef,
    prepared: PreparedWrite,
    input: Box<dyn BatchStream>,
    transaction: &TransactionOptions,
) {
    drop(Provider::test_connection(provider, secret, cancellation));
    drop(Provider::inspect(provider, secret, inspect, cancellation));
    drop(Provider::read(
        provider,
        secret,
        read,
        parameters,
        budget,
        cancellation,
    ));
    drop(Provider::prepare_write(
        provider,
        secret,
        write,
        schema,
        budget,
        cancellation,
    ));
    drop(Provider::query(
        provider,
        secret,
        query,
        parameters,
        budget,
        cancellation,
    ));
    drop(Provider::begin_transaction(
        provider,
        secret,
        transaction,
        budget,
        cancellation,
    ));
    drop(Provider::write(
        provider,
        secret,
        prepared,
        input,
        budget,
        cancellation,
    ));
}

#[allow(dead_code)]
fn transaction_exports_compile<T: TransactionScope>(
    transaction: &mut T,
    statement: &Statement,
    cancellation: &CancellationToken,
) {
    drop(TransactionScope::execute(
        transaction,
        statement,
        cancellation,
    ));
    drop(TransactionScope::savepoint(
        transaction,
        "public_contract",
        cancellation,
    ));
    drop(TransactionScope::release_savepoint(
        transaction,
        "public_contract",
        cancellation,
    ));
    drop(TransactionScope::rollback_to_savepoint(
        transaction,
        "public_contract",
        cancellation,
    ));
}

#[allow(dead_code)]
fn commit_export_compiles<T: TransactionScope>(
    transaction: Box<T>,
    cancellation: &CancellationToken,
) {
    drop(TransactionScope::commit(transaction, cancellation));
}

#[allow(dead_code)]
fn rollback_export_compiles<T: TransactionScope>(
    transaction: Box<T>,
    cancellation: &CancellationToken,
) {
    drop(TransactionScope::rollback(transaction, cancellation));
}
