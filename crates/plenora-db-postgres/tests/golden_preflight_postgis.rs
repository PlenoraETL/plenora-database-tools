//! Preflight / capability probing: comportamento con e senza estensione PostGIS.
//!
//! Chiude il buco identificato in P0.5 pre-Fase 3. Il consumer (Python SDK del
//! PFM) deve poter interrogare `probe_capabilities` per sapere se il target
//! offre supporto spaziale prima di tentare query. Verifica:
//!
//!   1. Positive path — `dataflow-postgres` (con PostGIS): capability doc
//!      espone `spatial.geometry = true`, `spatial.geography = true`,
//!      `spatial.spatial_index = true`, `extension_versions.postgis`
//!      valorizzato.
//!   2. Negative path (via env var `POSTGRES_URL_BARE`) — Postgres senza
//!      PostGIS: capability doc riporta `spatial.geometry = false` e
//!      `extension_versions` privo di postgis. NON deve panic né hangere.
//!   3. Query spaziale su Postgres senza PostGIS deve fallire in modo
//!      categorizzato (non Internal, messaggio menziona la funzione mancante).
//!
//! I test 2 e 3 sono skippati (con un print esplicativo) se `POSTGRES_URL_BARE`
//! non è settata — l'infrastruttura di test attualmente ha solo un servizio
//! Postgres con PostGIS. Per abilitarli: `docker run -d --rm --name pg-bare
//! -e POSTGRES_PASSWORD=bare -p 5433:5432 postgres:16` e
//! `POSTGRES_URL_BARE="host=localhost port=5433 user=postgres password=bare \
//! dbname=postgres"`.
//!
//! `#[ignore]` per default: richiedono almeno il Postgres principale.

#![cfg(test)]
#![allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::uninlined_format_args,
    clippy::single_match_else,
)]

use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, ErrorCategory};
use plenora_db_postgres::PostgresProvider;

const DSN_MAIN: &str = "host=dataflow-postgres user=dataflow \
                        password=dataflow_test_2026 dbname=dataflow_test";

fn secret_main() -> SecretString {
    SecretString::new(DSN_MAIN.to_owned())
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

fn provider() -> PostgresProvider {
    PostgresProvider::insecure_local_with_batch_rows(1_024)
}

/// Legge il DSN "bare" (Postgres senza PostGIS) dall'env. Ritorna `None` se
/// non configurato — il test viene skippato in quel caso.
fn secret_bare() -> Option<SecretString> {
    std::env::var("POSTGRES_URL_BARE")
        .ok()
        .map(SecretString::new)
}

// ============================================================================
//  PF.1 — Positive: capability probe conferma PostGIS presente
// ============================================================================
//
// Contract test per il consumer Python SDK: se io mi connetto al main DSN,
// il capability document deve dirmi che spatial è disponibile. Se un giorno
// PostGIS viene rimosso dal container per errore, questo test fallisce.

#[ignore = "live: richiede Postgres su dataflow-postgres con PostGIS"]
#[tokio::test]
async fn preflight_pf1_capability_positive_reports_postgis_present() {
    let provider = provider();
    let cancel = CancellationToken::new();
    let caps = provider
        .probe_capabilities(&secret_main(), &cancel)
        .await
        .expect("probe main");

    assert!(
        caps.spatial.geometry,
        "spatial.geometry deve essere true su target con PostGIS"
    );
    assert!(
        caps.spatial.geography,
        "spatial.geography deve essere true su target con PostGIS"
    );
    assert!(
        caps.spatial.spatial_index,
        "spatial.spatial_index deve essere true su target con PostGIS"
    );
    assert!(
        caps.spatial.read_wkb && caps.spatial.write_wkb,
        "read_wkb + write_wkb devono essere true su target con PostGIS"
    );
    assert!(
        !caps.spatial.dimensions.is_empty(),
        "spatial.dimensions deve essere non-vuoto su target con PostGIS"
    );
    assert!(
        !caps.spatial.functions.is_empty(),
        "spatial.functions deve essere non-vuoto su target con PostGIS"
    );

    let postgis_v = caps
        .extension_versions
        .get("postgis")
        .expect("extension_versions.postgis deve essere presente");
    assert!(
        !postgis_v.is_empty(),
        "postgis version string non deve essere vuota"
    );
    // Formato tipico "3.4.3" o "3.5.1 with ..." — verifichiamo solo che parta
    // con un digit di major.
    assert!(
        postgis_v.chars().next().is_some_and(|c| c.is_ascii_digit()),
        "versione PostGIS non inizia con digit: {postgis_v}"
    );
}

// ============================================================================
//  PF.2 — Negative: capability probe su Postgres bare (senza PostGIS)
// ============================================================================
//
// Skippato se `POSTGRES_URL_BARE` non è configurato. Quando disponibile,
// verifica che capability doc riporti spatial disabilitato senza panic.

#[ignore = "live: richiede POSTGRES_URL_BARE (Postgres separato senza PostGIS)"]
#[tokio::test]
async fn preflight_pf2_capability_negative_reports_no_postgis() {
    let Some(secret) = secret_bare() else {
        eprintln!(
            "SKIP pf2: POSTGRES_URL_BARE non impostato — vedi docstring del file per come abilitare"
        );
        return;
    };
    let provider = provider();
    let cancel = CancellationToken::new();
    let caps = provider
        .probe_capabilities(&secret, &cancel)
        .await
        .expect("probe bare");

    assert!(
        !caps.spatial.geometry,
        "spatial.geometry deve essere false su target senza PostGIS"
    );
    assert!(
        !caps.spatial.geography,
        "spatial.geography deve essere false su target senza PostGIS"
    );
    assert!(
        !caps.spatial.spatial_index,
        "spatial.spatial_index deve essere false su target senza PostGIS"
    );
    assert!(
        !caps.spatial.read_wkb && !caps.spatial.write_wkb,
        "read_wkb/write_wkb devono essere false su target senza PostGIS"
    );
    assert!(
        caps.spatial.dimensions.is_empty(),
        "dimensions deve essere vuoto senza PostGIS, trovato {:?}",
        caps.spatial.dimensions
    );
    assert!(
        caps.spatial.functions.is_empty(),
        "functions deve essere vuoto senza PostGIS, trovato {:?}",
        caps.spatial.functions
    );
    assert!(
        !caps.extension_versions.contains_key("postgis"),
        "extension_versions non deve contenere postgis su target bare"
    );
    // Comunque il provider_version del Postgres deve esserci.
    assert!(
        !caps.provider_version.is_empty(),
        "provider_version deve essere popolato anche senza estensioni"
    );
}

// ============================================================================
//  PF.3 — Query spaziale su Postgres bare fallisce con errore categorizzato
// ============================================================================
//
// Verifica il contratto degli errori: se il consumer prova ST_GeomFromText
// su un target senza PostGIS, deve ricevere un errore con categoria pulita
// (Schema / Execution / Unsupported / DataMapping) e messaggio che menzioni
// la funzione mancante — non Internal, non panic.

#[ignore = "live: richiede POSTGRES_URL_BARE"]
#[tokio::test]
async fn preflight_pf3_spatial_query_without_postgis_fails_cleanly() {
    let Some(secret) = secret_bare() else {
        eprintln!("SKIP pf3: POSTGRES_URL_BARE non impostato");
        return;
    };
    let provider = provider();
    let cancel = CancellationToken::new();

    let mut tx = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget(), &cancel)
        .await
        .expect("begin");

    let err = tx
        .query(
            &Statement::new("SELECT ST_GeomFromText('POINT(0 0)')"),
            &cancel,
        )
        .await
        .expect_err("ST_GeomFromText senza PostGIS deve fallire");

    // Categoria: non deve essere Internal (bug del driver) né Timeout.
    assert!(
        !matches!(
            err.category,
            ErrorCategory::Internal | ErrorCategory::Timeout | ErrorCategory::Cancelled
        ),
        "categoria inattesa {:?}: {}",
        err.category,
        err.message
    );
    // Messaggio: Postgres emette "function st_geomfromtext(...) does not exist".
    let msg = err.message.to_lowercase();
    assert!(
        msg.contains("st_geomfromtext")
            || msg.contains("does not exist")
            || msg.contains("function"),
        "messaggio non identifica la funzione mancante: {}",
        err.message
    );

    let _ = Box::new(tx).rollback(&cancel).await;
}
