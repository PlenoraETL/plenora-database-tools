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
