#![no_main]

use libfuzzer_sys::fuzz_target;
use plenora_database_core::field_contract::validate_schema_contract;
use plenora_database_core::plan::ReadOperation;
use plenora_database_core::protocol;
use plenora_db_mysql::{MysqlColumnSpec, MysqlObjectDescription, MysqlReadPlan};

fuzz_target!(|input: &[u8]| {
    // Il documento di catalogo e l'operazione di lettura arrivano come coppia
    // JSON: entrambi provengono da fuori del provider.
    let Ok((description, operation)) =
        serde_json::from_slice::<(MysqlObjectDescription, ReadOperation)>(input)
    else {
        return;
    };

    // La traduzione colonna per colonna non deve divergere dalla compilazione
    // completa del piano.
    let specifications = description
        .columns
        .iter()
        .map(MysqlColumnSpec::from_catalog)
        .collect::<Result<Vec<_>, _>>();

    let Ok(plan) = MysqlReadPlan::compile(&description, &operation) else {
        return;
    };
    let specifications = specifications.expect("piano compilato con colonne non traducibili");

    assert!(!plan.sql.is_empty());
    assert!(!plan.sql.contains('\0'));
    assert!(plan.sql.starts_with("SELECT "));
    assert!(plan.sql.ends_with(';'));
    assert!(!plan.columns.is_empty());
    assert!(!specifications.is_empty());
    assert_eq!(plan.schema.fields().len(), plan.columns.len());
    assert_eq!(plan.schema_token, description.token.0);

    assert_eq!(
        plan.schema
            .metadata()
            .get(protocol::CONTRACT_VERSION_KEY)
            .map(String::as_str),
        Some(protocol::CONTRACT_VERSION)
    );

    // Ogni schema emesso dal provider deve soddisfare il contratto Arrow del
    // core: è il confine che il consumatore a valle assume valido.
    validate_schema_contract(&plan.schema).expect("schema emesso fuori contratto");

    for (index, column) in plan.columns.iter().enumerate() {
        assert!(!column.name.is_empty());
        let field = plan.schema.field(index);
        assert_eq!(field.name(), &column.name);
        assert_eq!(field.data_type(), column.arrow_field().data_type());
        // Nessun valore può essere interpolato nel testo SQL: i bind restano
        // nominati e distinti dalla proiezione.
        assert!(!column.native_type.contains('\0'));
    }

    for name in &plan.bind_names {
        assert!(!name.is_empty());
        assert!(!name.contains('\0'));
    }

    // La compilazione è deterministica sullo stesso ingresso.
    let again = MysqlReadPlan::compile(&description, &operation).expect("compilazione ripetibile");
    assert_eq!(again.sql, plan.sql);
    assert_eq!(again.bind_names, plan.bind_names);
});
