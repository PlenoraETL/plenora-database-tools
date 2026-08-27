use super::{portable_compile, DIALECTS};
use plenora_database_core::portable::{compile_portable, PortableStatement};

/// Ogni dialetto offerto dal comando viene davvero compilato.
///
/// Un elenco di nomi accettati e una promessa; questa prova lo attraversa
/// nome per nome invece di
/// confrontarla con un secondo elenco scritto qui — due elenchi
/// divergono, un elenco attraversato no.
#[test]
fn every_offered_dialect_compiles_a_plan() {
    let ast: PortableStatement = serde_json::from_str(
        r#"{"type":"select","table":{"name":"t"},
                "projection":{"kind":"columns","value":["a"]},
                "filter":null,"order_by":[],"limit":1}"#,
    )
    .expect("piano di prova");
    assert!(!DIALECTS.is_empty(), "nessun dialetto offerto");
    for (name, kind) in DIALECTS {
        let compiled = compile_portable(*kind, &ast);
        assert!(
            compiled.is_ok(),
            "il comando offre `{name}` ma il compilatore lo rifiuta: {:?}",
            compiled.err().map(|error| error.message),
        );
    }
}

/// Un dialetto sconosciuto viene rifiutato nominando quelli che ci sono.
#[test]
fn an_unknown_dialect_names_the_ones_that_exist() {
    let mut args = ["fantasia".to_owned(), "piano.json".to_owned()].into_iter();
    let error = portable_compile(&mut args).expect_err("dialetto inventato");
    // `CliError` non e `Display`: il messaggio sta dentro la variante
    // fatale, che e la sola che questo percorso puo produrre.
    let crate::CliError::Fatal(fatal) = error else {
        panic!("un dialetto sconosciuto non e un fallimento silenzioso");
    };
    let message = fatal.message;
    for (name, _) in DIALECTS {
        assert!(
            message.contains(name),
            "il rifiuto non nomina `{name}`: {message}"
        );
    }
}
