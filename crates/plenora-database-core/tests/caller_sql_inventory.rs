//! Inventario del punto obbligato dello SQL del chiamante.
//!
//! `Provider::begin_transaction` ed `Provider::execute_ddl` avvolgono le
//! operazioni dell'adapter in `caller_sql`. Un adapter che li ridefinisse
//! uscirebbe dalla regola senza che il compilatore se ne accorga: questa
//! prova legge i sorgenti del workspace e fallisce se uno di loro li
//! ridefinisce, o se un `impl Provider` apre transazioni senza passare da
//! `open_transaction`.

use std::path::{Path, PathBuf};

const PUBLIC_ENTRY_POINTS: [&str; 2] = ["fn begin_transaction<'a>(", "fn execute_ddl<'a>("];

fn rust_sources(directory: &Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        if path.is_dir() {
            if name != "target" && !name.to_string_lossy().starts_with('.') {
                rust_sources(&path, found);
            }
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            found.push(path);
        }
    }
}

#[test]
fn no_provider_redefines_the_caller_sql_entry_points() {
    let core = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = core.parent().expect("cartella crates");
    let allowed = core.join("src").join("provider.rs");
    let mut sources = Vec::new();
    rust_sources(workspace, &mut sources);
    assert!(
        sources.len() > 50,
        "l'inventario deve vedere i sorgenti del workspace"
    );
    let mut offenders = Vec::new();
    let mut providers = 0_usize;
    for path in &sources {
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        if text.contains("impl Provider for") || text.contains("provider::Provider for") {
            providers += 1;
        }
        if *path == allowed || path.ends_with("caller_sql_inventory.rs") {
            continue;
        }
        for entry in PUBLIC_ENTRY_POINTS {
            if text.contains(entry) {
                offenders.push(format!("{}: {entry}", path.display()));
            }
        }
    }
    assert!(providers >= 6, "l'inventario deve vedere gli adapter");
    assert!(
        offenders.is_empty(),
        "metodi pubblici con SQL del chiamante ridefiniti fuori dal core: {offenders:?}"
    );
}

/// Ogni nuovo metodo del trait richiede una classificazione esplicita:
/// l'inventario non puo limitarsi ai nomi storici degli ingressi.
#[test]
fn every_public_provider_method_is_classified() {
    let source = include_str!("../src/provider.rs");
    let provider = source
        .split("pub trait Provider:")
        .nth(1)
        .expect("trait Provider");
    let methods: Vec<_> = provider
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("fn ")
                .map(|signature| signature.split(['<', '(']).next().expect("nome"))
        })
        .collect();
    assert_eq!(
        methods,
        [
            "kind",
            "test_connection",
            "probe_capabilities",
            "inspect",
            "read",
            "query",
            "prepare_write",
            "write",
            "begin_transaction",
            "open_transaction",
            "execute_ddl",
            "run_ddl",
        ],
        "classificare ogni nuovo ingresso: SQL del chiamante oppure operazione strutturata"
    );
}

#[test]
fn direct_adapter_entry_points_always_use_the_wrapper() {
    let core = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    rust_sources(core.parent().expect("crates"), &mut sources);
    let mut checked = 0;
    for path in sources {
        if path == core.join("src/provider.rs") || path.ends_with("caller_sql_inventory.rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("sorgente");
        for method in ["open_transaction", "run_ddl"] {
            let marker = format!("    fn {method}<'a>(");
            for implementation in text.split(&marker).skip(1) {
                let body = implementation
                    .split_once(" {")
                    .expect("corpo")
                    .1
                    .trim_start();
                let expected = format!("plenora_database_core::caller_sql::{method}(");
                let local = format!("crate::caller_sql::{method}(");
                assert!(
                    body.starts_with(&expected) || body.starts_with(&local),
                    "{}: {method} deve entrare subito nel wrapper",
                    path.display()
                );
                checked += 1;
            }
        }
    }
    assert!(
        checked >= 12,
        "vedere tutti gli adapter e i loro due ingressi"
    );
}

#[test]
fn every_transaction_sql_method_is_wrapped() {
    let source = include_str!("../src/transaction.rs");
    let source = source
        .split("pub trait TransactionScope:")
        .nth(1)
        .expect("trait");
    let methods: Vec<_> = source
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("fn ")
                .map(|signature| signature.split(['<', '(']).next().expect("nome"))
        })
        .collect();
    assert_eq!(
        methods,
        [
            "provider_kind",
            "begin_caller_sql",
            "execute",
            "query",
            "execute_graph",
            "query_stream",
            "savepoint",
            "rollback_to_savepoint",
            "release_savepoint",
            "execute_conditional_update",
            "operand_catalog",
            "column_types",
            "commit",
            "rollback"
        ]
    );
    // `operand_catalog` legge il catalogo per il giudizio degli operandi
    // geometrici: e SQL del provider, non del chiamante, e il wrapper lo
    // inoltra senza aprire il marcatore.
    let wrapper_source = include_str!("../src/caller_sql.rs");
    let forwarded = wrapper_source
        .split_once("fn operand_catalog<'")
        .expect("operand_catalog inoltrato")
        .1;
    let forwarded = forwarded.split("\n    fn ").next().expect("corpo");
    assert!(
        forwarded.contains("self.inner.operand_catalog(")
            && !forwarded.contains("begin_caller_sql"),
        "operand_catalog"
    );
    // `column_types` legge i tipi del catalogo per l'ammissione dei
    // letterali portable: stessa regola di `operand_catalog`.
    let forwarded = wrapper_source
        .split_once("fn column_types<'")
        .expect("column_types inoltrato")
        .1;
    let forwarded = forwarded
        .split(
            "
    fn ",
        )
        .next()
        .expect("corpo");
    assert!(
        forwarded.contains("self.inner.column_types(") && !forwarded.contains("begin_caller_sql"),
        "column_types"
    );
    let wrapper = include_str!("../src/caller_sql.rs");
    for method in [
        "execute",
        "query",
        "execute_graph",
        "query_stream",
        "execute_conditional_update",
        "next_batch",
    ] {
        let marker = format!("fn {method}<'");
        let body = wrapper.split_once(&marker).expect("metodo protetto").1;
        let body = body.split("\n    fn ").next().expect("corpo");
        assert!(
            body.contains("map_err(caller_sql_failure)") || body.contains("sent.failure(error)"),
            "{method}"
        );
    }
}
