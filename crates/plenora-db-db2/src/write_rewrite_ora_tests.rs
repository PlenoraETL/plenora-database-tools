//! La regola S su Db2 con la semantica `VARCHAR2` (`varchar2_compat = ON`),
//! live su un database creato con `DB2_COMPATIBILITY_VECTOR=ORA`.
//!
//! Non e nel fixture del gate: il modulo e il nome del test non contengono il
//! prefisso dei test live, e `scripts/check_db2_reference.py` non lo esegue.
//! Si esegue a mano con `PLENORA_DB2_ORA_DATABASE` (vedi
//! `docs/operativo.md`, «Db2 con la semantica VARCHAR2»).
//!
//! Dal 25/10 (regola A, `crate::write_admission`) `varchar2_compat` e nella
//! chiave d'ammissione dei testi, e il fixture misura soltanto il database
//! del gate (`varchar2_compat = OFF`): sotto `ON` ogni scrittura di testo si
//! rifiuta nel preflight come non provata, prima ancora della regola S.

use super::write_rewrite_live_tests::{
    batch, environment, operation, provider, reset, run, single_text, text, write,
};
use plenora_database_core::plan::WriteMode;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::write_admission::UNPROVEN_PAIR;
use plenora_database_core::{ErrorCategory, ErrorPhase};

#[tokio::test]
#[ignore = "richiede un database Db2 creato con DB2_COMPATIBILITY_VECTOR=ORA (PLENORA_DB2_ORA_DATABASE)"]
async fn db2_ora_a_testo_sotto_varchar2_compat_non_misurato_si_rifiuta() {
    let (provider, secret) = provider(&environment("PLENORA_DB2_ORA_DATABASE", "plnora"));
    let table = "PLN_S_ORA";
    reset(
        &provider,
        &secret,
        table,
        "ID INTEGER NOT NULL PRIMARY KEY, V VARCHAR(10), K VARCHAR(10), CL CLOB(1M)",
    )
    .await;
    let rows = run(
        &provider,
        &secret,
        &[],
        Some("SELECT VARCHAR(VALUE, 16) FROM SYSIBMADM.DBCFG WHERE NAME = 'varchar2_compat'"),
        false,
    )
    .await;
    assert_eq!(
        single_text(&rows)
            .map(|value| value.trim().to_owned())
            .as_deref(),
        Some("ON")
    );
    // La misura sul percorso dei bind: il testo vuoto diventa NULL.
    let measured = run(
        &provider,
        &secret,
        &[(
            &format!("INSERT INTO DB2INST1.{table} (ID, V) VALUES (?, ?)"),
            vec![
                ParameterValue::I32(1),
                ParameterValue::String(String::new()),
            ],
        )],
        Some(&format!(
            "SELECT COUNT(*) FROM DB2INST1.{table} WHERE ID = 1 AND V IS NULL"
        )),
        false,
    )
    .await;
    assert_eq!(measured[0].get_index(0), Some(&ParameterValue::I32(1)));
    // Ogni scrittura di testo, anche il testo pieno, non e misurata sotto
    // `varchar2_compat = ON`: rifiutata nel preflight con ogni mode.
    for (column, mode, keys, value) in [
        ("V", WriteMode::Append, &[][..], ""),
        ("CL", WriteMode::Append, &[][..], ""),
        ("K", WriteMode::Upsert, &["K"][..], ""),
        ("V", WriteMode::Append, &[][..], "x"),
    ] {
        let error = write(
            &provider,
            &secret,
            &operation(table, mode, keys),
            batch(column, vec![1], text(vec![Some(value)])),
        )
        .await
        .expect_err("testo non provato sotto varchar2_compat");
        assert_eq!(error.category, ErrorCategory::Unsupported, "{error:?}");
        assert_eq!(error.phase, ErrorPhase::Prepare, "{error:?}");
        assert_eq!(error.message, UNPROVEN_PAIR, "{error:?}");
    }
    let rows = run(
        &provider,
        &secret,
        &[],
        Some(&format!("SELECT COUNT(*) FROM DB2INST1.{table}")),
        false,
    )
    .await;
    assert_eq!(rows[0].get_index(0), Some(&ParameterValue::I32(0)));
}
