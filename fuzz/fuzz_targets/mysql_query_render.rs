#![no_main]

use libfuzzer_sys::fuzz_target;
use plenora_database_core::limits::Limits;
use plenora_database_core::query::{validate_query_operation, QueryOperation};
use plenora_db_mysql::render_query;

fuzz_target!(|input: &[u8]| {
    // La coppia JSON porta l'AST portabile e il nome del database configurato,
    // che il gating MySQL confronta con ogni sorgente qualificata.
    let Ok((query, database)) = serde_json::from_slice::<(QueryOperation, String)>(input) else {
        return;
    };

    let Ok(rendered) = render_query(&query, &database) else {
        return;
    };

    // Un rendering accettato implica un AST portabile valido: il provider
    // applica la validazione del core prima di ogni ispezione di dialect.
    validate_query_operation(&query, &Limits::default()).expect("AST accettato ma non valido");

    assert!(!rendered.sql.is_empty());
    assert!(!rendered.sql.contains('\0'));
    assert!(rendered.sql.starts_with("WITH ") || rendered.sql.starts_with("SELECT "));

    for (index, bind) in rendered.binds.iter().enumerate() {
        // Gli ordinali restano densi anche se MySQL usa segnaposto anonimi:
        // il driver posiziona i valori solo su questa sequenza.
        assert_eq!(bind.ordinal, index + 1);
        assert!(!bind.name.is_empty());
        assert!(!bind.name.contains('\0'));
    }

    let again = render_query(&query, &database).expect("rendering ripetibile");
    assert_eq!(again, rendered);
});
