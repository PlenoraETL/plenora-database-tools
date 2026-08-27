//! Roundtrip delle famiglie di tipo PostgreSQL estese.
//!
//! Integra `golden_type_roundtrip.rs` con:
//!
//!   1. enum type — pattern PFM (status, priority, ...)
//!   2. generated column STORED — computed field read-only
//!   3. range types (int4range, tstzrange) via text cast
//!   4. network family (inet, cidr, macaddr) via text cast
//!   5. full-text family (tsvector, tsquery, xml) via text cast
//!   6. money — via numeric cast e text cast
//!   7. composite type — verifica il workaround via ROW-to-text (il read
//!      diretto binario ritorna Unsupported)
//!
//! I test usano il pattern "cast a text" dove il decoder binario non è
//! implementato: documenta al consumer PFM cosa aspettarsi al bordo SDK.
//!
//! `#[ignore]` per default: richiedono Postgres su `dataflow-postgres`.

#![cfg(test)]
#![allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_lossless,
    clippy::cast_precision_loss,
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::uninlined_format_args,
    clippy::single_match_else,
    clippy::match_same_arms,
    clippy::redundant_closure_for_method_calls,
    clippy::unreadable_literal
)]

use plenora_database_core::provider::{ParameterValue, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, ErrorCategory};
use plenora_db_postgres::PostgresProvider;

const DSN: &str = "host=dataflow-postgres user=dataflow password=dataflow_test_2026 \
                   dbname=dataflow_test";

fn secret() -> SecretString {
    SecretString::new(DSN.to_owned())
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

fn provider() -> PostgresProvider {
    PostgresProvider::insecure_local_with_batch_rows(1_024)
}

fn text_of(v: Option<&ParameterValue>) -> String {
    match v {
        Some(ParameterValue::String(s)) => s.clone(),
        other => panic!("atteso String, trovato {other:?}"),
    }
}

fn i32_of(v: Option<&ParameterValue>) -> i32 {
    match v {
        Some(ParameterValue::I32(x)) => *x,
        other => panic!("atteso I32, trovato {other:?}"),
    }
}

fn i64_of(v: Option<&ParameterValue>) -> i64 {
    match v {
        Some(ParameterValue::I64(x)) => *x,
        other => panic!("atteso I64, trovato {other:?}"),
    }
}

fn bool_of(v: Option<&ParameterValue>) -> bool {
    match v {
        Some(ParameterValue::Bool(b)) => *b,
        other => panic!("atteso Bool, trovato {other:?}"),
    }
}

// ============================================================================
//  X.1 — Enum type: create + insert + read (via text)
// ============================================================================
//
// Pattern PFM: colonne `status`, `priority`, `role`. Il read binario del tipo
// enum ritorna Unsupported nel decoder attuale, quindi consumer canonico è
// via cast a text. Verifica anche che il vincolo enum (constraint) rigetti
// valori fuori dal set.
//
// Il nome enum include il nome test per evitare conflitti in parallel run.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn extras_x1_enum_type_roundtrip_via_text() {
    let provider = provider();
    let cancel = CancellationToken::new();
    let mut tx = provider
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin");

    // CREATE TYPE è transazionale su Postgres: rollback lo droppa.
    tx.execute(
        &Statement::new("CREATE TYPE _pfm_extras_x1_status AS ENUM ('draft', 'active', 'closed')"),
        &cancel,
    )
    .await
    .expect("create type");

    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE _pfm_extras_x1 ( \
             id INT PRIMARY KEY, \
             st _pfm_extras_x1_status NOT NULL) ON COMMIT DROP",
        ),
        &cancel,
    )
    .await
    .expect("create table");

    tx.execute(
        &Statement::new(
            "INSERT INTO _pfm_extras_x1 (id, st) VALUES \
             (1, 'draft'), (2, 'active'), (3, 'closed')",
        ),
        &cancel,
    )
    .await
    .expect("seed");

    // Read via text cast.
    let rows = tx
        .query(
            &Statement::new("SELECT id, st::TEXT FROM _pfm_extras_x1 ORDER BY id"),
            &cancel,
        )
        .await
        .expect("read");
    assert_eq!(rows.len(), 3);
    let vals: Vec<(i32, String)> = rows
        .iter()
        .map(|r| (i32_of(r.get_index(0)), text_of(r.get_index(1))))
        .collect();
    assert_eq!(
        vals,
        vec![
            (1, "draft".to_string()),
            (2, "active".to_string()),
            (3, "closed".to_string()),
        ]
    );

    // Valore fuori dal set → errore atteso, contenuto in savepoint.
    tx.savepoint("bad_enum", &cancel).await.expect("sp");
    let err = tx
        .execute(
            &Statement::new("INSERT INTO _pfm_extras_x1 (id, st) VALUES (99, 'archived')"),
            &cancel,
        )
        .await
        .expect_err("insert valore enum non ammesso deve fallire");
    // Il driver wrappa il messaggio Postgres in uno stabile: non asseriamo
    // il testo — verifichiamo solo che la categoria non sia Internal.
    assert!(
        !matches!(err.category, ErrorCategory::Internal),
        "categoria inattesa {:?}: {}",
        err.category,
        err.message
    );
    tx.rollback_to_savepoint("bad_enum", &cancel)
        .await
        .expect("rb sp");
    tx.release_savepoint("bad_enum", &cancel)
        .await
        .expect("rel sp");

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}

// ============================================================================
//  X.2 — Generated column STORED: computed on write, rifiuto scrittura diretta
// ============================================================================
//
// Il PFM userà campi computed (es. `full_name` = first || ' ' || last,
// `area` = ST_Area(geom)). Verifica:
//   - Insert su base column → generated è calcolato correttamente
//   - INSERT diretto sul generated → errore atteso
//   - UPDATE del generated → errore atteso

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn extras_x2_generated_column_stored_computed_and_write_rejected() {
    let provider = provider();
    let cancel = CancellationToken::new();
    let mut tx = provider
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin");

    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE _pfm_extras_x2 ( \
             id INT PRIMARY KEY, \
             first_name TEXT NOT NULL, \
             last_name TEXT NOT NULL, \
             full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED, \
             qty INT NOT NULL, \
             price INT NOT NULL, \
             total INT GENERATED ALWAYS AS (qty * price) STORED \
             ) ON COMMIT DROP",
        ),
        &cancel,
    )
    .await
    .expect("create");

    tx.execute(
        &Statement::new(
            "INSERT INTO _pfm_extras_x2 (id, first_name, last_name, qty, price) VALUES \
             (1, 'Ada', 'Lovelace', 3, 10), \
             (2, 'Alan', 'Turing', 5, 20)",
        ),
        &cancel,
    )
    .await
    .expect("seed");

    let rows = tx
        .query(
            &Statement::new("SELECT id, full_name, total FROM _pfm_extras_x2 ORDER BY id"),
            &cancel,
        )
        .await
        .expect("read");
    assert_eq!(rows.len(), 2);
    assert_eq!(text_of(rows[0].get_index(1)), "Ada Lovelace");
    assert_eq!(i32_of(rows[0].get_index(2)), 30);
    assert_eq!(text_of(rows[1].get_index(1)), "Alan Turing");
    assert_eq!(i32_of(rows[1].get_index(2)), 100);

    // INSERT diretto sul generated → errore.
    tx.savepoint("insert_gen", &cancel).await.expect("sp");
    let err = tx
        .execute(
            &Statement::new(
                "INSERT INTO _pfm_extras_x2 (id, first_name, last_name, full_name, qty, price) \
                 VALUES (3, 'X', 'Y', 'X Y forced', 1, 1)",
            ),
            &cancel,
        )
        .await
        .expect_err("insert su generated column deve fallire");
    assert!(
        err.message.to_lowercase().contains("generated"),
        "messaggio deve menzionare 'generated': {}",
        err.message
    );
    tx.rollback_to_savepoint("insert_gen", &cancel)
        .await
        .expect("rb");
    tx.release_savepoint("insert_gen", &cancel)
        .await
        .expect("rel");

    // UPDATE del generated → errore.
    tx.savepoint("update_gen", &cancel).await.expect("sp");
    let err = tx
        .execute(
            &Statement::new("UPDATE _pfm_extras_x2 SET full_name = 'override' WHERE id = 1"),
            &cancel,
        )
        .await
        .expect_err("update su generated column deve fallire");
    assert!(
        err.message.to_lowercase().contains("generated"),
        "messaggio deve menzionare 'generated': {}",
        err.message
    );
    tx.rollback_to_savepoint("update_gen", &cancel)
        .await
        .expect("rb");
    tx.release_savepoint("update_gen", &cancel)
        .await
        .expect("rel");

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}

// ============================================================================
//  X.3 — Range types: int4range e tstzrange via text
// ============================================================================
//
// I range types (int4range, tstzrange, numrange, daterange) sono usati in
// domain modeling per intervalli (booking, versioning temporale).  Il read
// diretto binario non è implementato nel decoder attuale: cast a text.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn extras_x3_range_types_int4_and_tstz_via_text() {
    let provider = provider();
    let cancel = CancellationToken::new();
    let mut tx = provider
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin");

    let rows = tx
        .query(
            &Statement::new(
                "SELECT \
                   int4range(1, 10, '[)')::TEXT, \
                   tstzrange('2026-01-01 00:00Z', '2026-06-01 00:00Z', '[)')::TEXT, \
                   int4range(5, 100, '[)') @> 42",
            ),
            &cancel,
        )
        .await
        .expect("query ranges");
    let row = rows.first().expect("almeno una riga");
    let int_range = text_of(row.get_index(0));
    let ts_range = text_of(row.get_index(1));
    let contains = bool_of(row.get_index(2));

    assert_eq!(int_range, "[1,10)", "int4range text inatteso: {int_range}");
    assert!(
        ts_range.contains("2026-01-01") && ts_range.contains("2026-06-01"),
        "tstzrange text inatteso: {ts_range}"
    );
    assert!(contains, "42 dovrebbe essere in [5,100)");

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}

// ============================================================================
//  X.4 — Network family: inet, cidr, macaddr via text
// ============================================================================
//
// PFM audit log include IP address (inet). Il decoder binario dei tipi rete
// non è implementato: cast a text è il pattern canonico.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn extras_x4_network_family_inet_cidr_macaddr_via_text() {
    let provider = provider();
    let cancel = CancellationToken::new();
    let mut tx = provider
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin");

    let rows = tx
        .query(
            &Statement::new(
                "SELECT \
                   ('192.168.1.42'::inet)::TEXT, \
                   ('10.0.0.0/8'::cidr)::TEXT, \
                   ('08:00:2b:01:02:03'::macaddr)::TEXT, \
                   ('2001:db8::1'::inet)::TEXT, \
                   host('192.168.1.42'::inet), \
                   ('192.168.0.5'::inet) << ('192.168.0.0/16'::cidr)",
            ),
            &cancel,
        )
        .await
        .expect("query network");
    let row = rows.first().expect("almeno una riga");
    // inet::text include netmask esplicita quando implicita a /32/128;
    // Postgres emette "192.168.1.42/32" per un IPv4 senza mask.
    assert_eq!(text_of(row.get_index(0)), "192.168.1.42/32");
    assert_eq!(text_of(row.get_index(1)), "10.0.0.0/8");
    assert_eq!(text_of(row.get_index(2)), "08:00:2b:01:02:03");
    assert_eq!(text_of(row.get_index(3)), "2001:db8::1/128");
    // Per estrarre solo l'address senza mask: host(inet).
    assert_eq!(text_of(row.get_index(4)), "192.168.1.42");
    assert!(
        bool_of(row.get_index(5)),
        "192.168.0.5 deve essere contenuto in 192.168.0.0/16"
    );

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}

// ============================================================================
//  X.5 — Full-text family: tsvector, tsquery, xml via text
// ============================================================================
//
// tsvector/tsquery servono al PFM per ricerca full-text sui documenti; xml è
// usato in interop con sistemi legacy. Cast a text per lettura, e verifica
// il matching tsvector @@ tsquery lato server.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn extras_x5_fulltext_family_tsvector_tsquery_xml_via_text() {
    let provider = provider();
    let cancel = CancellationToken::new();
    let mut tx = provider
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin");

    let rows = tx
        .query(
            &Statement::new(
                "SELECT \
                   to_tsvector('english', 'The quick brown fox')::TEXT, \
                   to_tsquery('english', 'quick & fox')::TEXT, \
                   to_tsvector('english', 'The quick brown fox') @@ to_tsquery('english', 'quick & fox'), \
                   xmlelement(name Item, xmlattributes(1 AS id), 'Torre')::TEXT",
            ),
            &cancel,
        )
        .await
        .expect("query full-text");
    let row = rows.first().expect("almeno una riga");
    let tsv = text_of(row.get_index(0));
    let tsq = text_of(row.get_index(1));
    let hit = bool_of(row.get_index(2));
    let xml = text_of(row.get_index(3));

    // tsvector Postgres normalizza in "'brown':3 'fox':4 'quick':2".
    assert!(
        tsv.contains("quick") && tsv.contains("fox"),
        "tsvector text inatteso: {tsv}"
    );
    assert!(
        tsq.contains("quick") && tsq.contains("fox"),
        "tsquery text inatteso: {tsq}"
    );
    assert!(hit, "tsvector @@ tsquery deve matchare");
    // Postgres normalizza il nome elemento a lowercase in output text.
    assert!(
        xml.contains("<item") && xml.contains("Torre"),
        "xml inatteso: {xml}"
    );

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}

// ============================================================================
//  X.6 — Money via numeric cast e text
// ============================================================================
//
// Il tipo `money` dipende da lc_monetary (config sessione). Consigliato
// castarlo a numeric per calcoli e a text per display.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn extras_x6_money_via_numeric_and_text_cast() {
    let provider = provider();
    let cancel = CancellationToken::new();
    let mut tx = provider
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin");

    // lc_monetary influenza il text di money; forziamo C locale per test
    // stabilità (indipendente dall'ambiente).
    tx.execute(&Statement::new("SET LOCAL lc_monetary = 'C'"), &cancel)
        .await
        .expect("set lc_monetary");

    let rows = tx
        .query(
            &Statement::new(
                "SELECT \
                   (1234.56::numeric::money)::numeric::TEXT, \
                   (1234.56::numeric::money)::TEXT",
            ),
            &cancel,
        )
        .await
        .expect("query money");
    let row = rows.first().expect("almeno una riga");
    let as_numeric = text_of(row.get_index(0));
    let as_money_text = text_of(row.get_index(1));

    // La numeric cast rimuove il simbolo di valuta: attesa "1234.56".
    assert_eq!(as_numeric, "1234.56", "money::numeric::text inatteso");
    // Text della money col lc_monetary=C (or default): il formato dipende
    // dal locale del server (es. "$1,234.56"). Verifichiamo solo che
    // contenga le cifre significative e un separatore.
    assert!(
        as_money_text.contains('1')
            && as_money_text.contains("234")
            && as_money_text.contains("56"),
        "money text non contiene le cifre attese: {as_money_text}"
    );

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}

// ============================================================================
//  X.7 — Composite type: workaround ROW-to-text
// ============================================================================
//
// I composite types sono usati in RETURNING di funzioni Postgres. Il decoder
// binario del client attuale non ha supporto composite: verifica che il
// workaround "cast dell'intero ROW a text" funzioni, e che il campo estratto
// via `(comp).field` sia consumabile con i decoder base.

#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn extras_x7_composite_type_via_row_text_and_field_extract() {
    let provider = provider();
    let cancel = CancellationToken::new();
    let mut tx = provider
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin");

    tx.execute(
        &Statement::new("CREATE TYPE _pfm_extras_x7_kv AS (label TEXT, amount BIGINT)"),
        &cancel,
    )
    .await
    .expect("create type");

    // Workaround 1: cast dell'intero composite a text — restituisce
    // "(label,amount)" secondo la sintassi Postgres.
    let rows = tx
        .query(
            &Statement::new("SELECT ROW('ricavo', 1000)::_pfm_extras_x7_kv::TEXT"),
            &cancel,
        )
        .await
        .expect("query composite text");
    let composite_text = text_of(rows.first().and_then(|r| r.get_index(0)));
    assert!(
        composite_text.contains("ricavo") && composite_text.contains("1000"),
        "composite::text inatteso: {composite_text}"
    );

    // Workaround 2: estrai i campi individualmente via (comp).field — tipi
    // primitivi decodificati normalmente.
    let rows = tx
        .query(
            &Statement::new(
                "SELECT (ROW('ricavo', 1000)::_pfm_extras_x7_kv).label, \
                        (ROW('ricavo', 1000)::_pfm_extras_x7_kv).amount",
            ),
            &cancel,
        )
        .await
        .expect("query composite fields");
    let row = rows.first().expect("almeno una riga");
    assert_eq!(text_of(row.get_index(0)), "ricavo");
    assert_eq!(i64_of(row.get_index(1)), 1000);

    Box::new(tx).rollback(&cancel).await.expect("rollback");
}
