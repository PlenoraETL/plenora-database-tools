//! La regola S su Db2 con la semantica `VARCHAR2` (`varchar2_compat = ON`),
//! live su un database creato con `DB2_COMPATIBILITY_VECTOR=ORA`.
//!
//! Non e nel fixture del gate: il modulo e il nome del test non contengono il
//! prefisso dei test live, e `scripts/check_db2_reference.py` non lo esegue.
//! Si esegue a mano con `PLENORA_DB2_ORA_DATABASE` (vedi
//! `docs/operativo.md`, «Db2 con la semantica VARCHAR2»).

use super::write_rewrite_live_tests::{
    assert_rewritten, batch, environment, operation, provider, reset, run, single_text, text, write,
};
use plenora_database_core::plan::WriteMode;
use plenora_database_core::provider::ParameterValue;

#[tokio::test]
#[ignore = "richiede un database Db2 creato con DB2_COMPATIBILITY_VECTOR=ORA (PLENORA_DB2_ORA_DATABASE)"]
async fn db2_ora_s_testo_vuoto_con_varchar2_compat_non_diventa_null() {
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
    // Il caso della rilettura di Oracle, su Db2: [(1,""),(2,"x")].
    for column in ["V", "CL"] {
        let error = write(
            &provider,
            &secret,
            &operation(table, WriteMode::Append, &[]),
            batch(column, vec![1, 2], text(vec![Some(""), Some("x")])),
        )
        .await
        .expect_err("testo vuoto");
        assert_rewritten(&error, false);
    }
    // Due Upsert sulla chiave vuota.
    for id in [1, 2] {
        let error = write(
            &provider,
            &secret,
            &operation(table, WriteMode::Upsert, &["K"]),
            batch("K", vec![id], text(vec![Some("")])),
        )
        .await
        .expect_err("chiave vuota");
        assert_rewritten(&error, true);
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
    write(
        &provider,
        &secret,
        &operation(table, WriteMode::Append, &[]),
        batch("V", vec![1], text(vec![Some("x")])),
    )
    .await
    .expect("testo pieno");
}
