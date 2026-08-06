#![no_main]

use libfuzzer_sys::fuzz_target;
use plenora_database_core::field_contract::validate_schema_contract;
use plenora_database_core::protocol;
use plenora_db_sqlserver::{SqlServerColumnSpec, SqlServerObjectDescription, SqlServerReadPlan};

fuzz_target!(|input: &[u8]| {
    // Il documento di catalogo è già decodificato dal wire TDS ma resta un
    // ingresso non fidato per la compilazione del piano.
    let Ok(description) = serde_json::from_slice::<SqlServerObjectDescription>(input) else {
        return;
    };

    let specifications = description
        .columns
        .iter()
        .map(SqlServerColumnSpec::from_catalog)
        .collect::<Result<Vec<_>, _>>();

    let Ok(plan) = SqlServerReadPlan::compile(&description) else {
        return;
    };
    let specifications = specifications.expect("piano compilato con colonne non traducibili");

    // La compilazione del piano completo proietta esattamente le colonne di
    // catalogo, senza aggiungerne né scartarne.
    assert_eq!(plan.columns.len(), specifications.len());
    assert_eq!(plan.schema.fields().len(), plan.columns.len());
    assert_eq!(
        plan.structural_fingerprint,
        description.token.structural_fingerprint
    );

    // Il piano strutturale non ammette valori: nessun bind e nessun NUL nel
    // testo T-SQL.
    assert!(plan.bind_names.is_empty());
    assert!(!plan.sql.contains('\0'));
    assert!(plan.sql.starts_with("SELECT "));
    assert!(plan.sql.ends_with(';'));

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
        assert_eq!(plan.schema.field(index).name(), &column.name);
        // `from_catalog` normalizza il tipo nativo in minuscolo e non lascia
        // passare frammenti SQL liberi.
        assert_eq!(column.native_type, specifications[index].native_type);
        assert!(!column.native_type.contains('\0'));
    }

    let again = SqlServerReadPlan::compile(&description).expect("compilazione ripetibile");
    assert_eq!(again.sql, plan.sql);
    assert_eq!(again.structural_fingerprint, plan.structural_fingerprint);
});
