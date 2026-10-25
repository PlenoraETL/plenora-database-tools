//! La regola Q su SQL Server: lo SQL che la libreria emette per catalogo,
//! sessione, guardie e scritture nomina le viste di catalogo con `sys.` (o
//! `INFORMATION_SCHEMA`) e le procedure di sistema con `sys.`, cosi lo schema
//! predefinito dell'utente non le puo sostituire.
//!
//! Il resto della classe non serve qui, misurato su SQL Server 2022 il 25/10:
//! una funzione scalare utente si chiama soltanto con lo schema, quindi un
//! nome senza schema e sempre quello di sistema; un tipo utente con il nome
//! di un tipo di sistema non si crea (`CREATE TYPE dbo.[int]`: errore 219), e
//! `CAST`/`CONVERT`/`DECLARE` con `int`, `nvarchar`, `bit` restano di
//! sistema.
//!
//! La prova legge le sorgenti del crate (non le prove) e rifiuta un
//! `FROM`/`JOIN`/`APPLY` verso un nome senza schema che non sia una CTE del
//! modulo, una variabile tabella o un identificatore del target (quotato o
//! composto dal renderer, anche a fine stringa), e un `EXEC` di una procedura `sp_` senza `sys.`.

use std::path::Path;

/// Le CTE e le pseudo-tabelle che lo SQL del crate nomina senza schema.
const LOCAL_NAMES: &[&str] = &["target", "closure", "inserted", "deleted"];

fn sources(directory: &Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(directory).expect("sorgenti") {
        let path = entry.expect("voce").path();
        if path.is_dir() {
            sources(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            if !name.ends_with("_tests.rs") && name != "live_tests.rs" {
                out.push(path);
            }
        }
    }
}

fn qualified(name: &str) -> bool {
    let first = name.chars().next().unwrap_or(' ');
    name.starts_with("sys.")
        || name.starts_with("INFORMATION_SCHEMA.")
        || matches!(first, '@' | '#' | '{' | '[' | '(' | '"')
        || LOCAL_NAMES.contains(&name)
}

#[test]
fn lo_sql_della_libreria_nomina_il_catalogo_con_lo_schema() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    sources(&root, &mut files);
    assert!(files.len() > 10, "sorgenti del crate non trovate");
    let mut unqualified = Vec::new();
    for file in files {
        let text = std::fs::read_to_string(&file).expect("sorgente");
        for (number, line) in text.lines().enumerate() {
            let code = line.trim_start();
            if code.starts_with("//") {
                continue;
            }
            let words = code.split_whitespace().collect::<Vec<_>>();
            for pair in words.windows(2) {
                let (keyword, name) = (pair[0], pair[1].trim_end_matches([',', ';', ')']));
                let relation = matches!(keyword, "FROM" | "JOIN" | "APPLY") && !qualified(name);
                let procedure = matches!(keyword, "EXEC" | "EXECUTE") && name.starts_with("sp_");
                if relation || procedure {
                    unqualified.push(format!(
                        "{}:{}: {keyword} {name}",
                        file.display(),
                        number + 1
                    ));
                }
            }
        }
    }
    assert!(unqualified.is_empty(), "{}", unqualified.join("\n"));
}

/// La guardia vede davvero un nome senza schema.
#[test]
fn la_guardia_riconosce_un_nome_senza_schema() {
    assert!(!qualified("objects"));
    assert!(!qualified("dbo.objects"));
    assert!(qualified("sys.objects"));
    assert!(qualified("INFORMATION_SCHEMA.COLUMNS"));
    assert!(qualified("{quoted_object}"));
    assert!(qualified("target"));
}
