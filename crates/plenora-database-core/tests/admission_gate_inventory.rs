//! Inventario del cancello unico dell'ammissione (`admission_gate`).
//!
//! Lo SQL compilato da un AST portable non espone i valori legati, e il
//! compilatore lo garantisce fuori dal core. Questa prova chiude cio che il
//! compilatore non vede: dentro il core lo statement eseguibile si chiede
//! soltanto dal cancello, e fuori dal core nessuno chiama le funzioni della
//! regola (`operand_admission::admit*`) se non attraverso il cancello, cosi
//! che ogni punto d'ingresso sia nell'elenco di `admission_gate`.

use std::path::{Path, PathBuf};

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
fn every_executable_compiled_statement_comes_from_the_admission_gate() {
    let core = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = core.parent().expect("cartella crates");
    let core_sources = core.join("src");
    let gate = core_sources.join("admission_gate.rs");
    let compiler = core_sources.join("portable").join("compiler.rs");
    let mut sources = Vec::new();
    rust_sources(workspace, &mut sources);
    assert!(
        sources.len() > 50,
        "l'inventario deve vedere i sorgenti del workspace"
    );
    let mut offenders = Vec::new();
    for path in &sources {
        if path.ends_with("admission_gate_inventory.rs") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        if text.contains("into_admitted_statement") && *path != gate && *path != compiler {
            offenders.push(format!("{}: into_admitted_statement", path.display()));
        }
        if !path.starts_with(&core_sources) {
            for rule in [
                "operand_admission::admit",
                "operand_admission::{admit",
                "operand_admission::admit_portable",
            ] {
                if text.contains(rule) {
                    offenders.push(format!("{}: {rule}", path.display()));
                }
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "statement eseguibili fuori dal cancello dell'ammissione: {offenders:#?}"
    );
}
