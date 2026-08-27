use super::*;

/// Il documento minimo ammesso dallo schema deve deserializzare.
///
/// E' lo stesso file che `scripts/phase0_validate.py` valida contro
/// `capabilities.schema.json`: una sola fonte, verificata da entrambi i
/// lati. Se qualcuno rende obbligatorio qui un campo che lo schema lascia
/// facoltativo, questo test lo dice subito invece di lasciarlo scoprire a
/// un consumatore.
/// Il confine fra cio che il contratto dichiara e cio che il prodotto
/// pretende, fissato dove sta.
///
/// `capabilities.schema.json` **non** esprime le relazioni fra capability:
/// un documento con `server_cursor` senza `streaming` e valido secondo la
/// major v2. Averlo fatto rifiutare da `validate()` — che sta sul percorso
/// di consumo di `prepare` — restringeva cio che la v2 accetta, e
/// restringere una major senza cambiarla e proprio quello che la regola 2
/// di AGENTS.md vieta.
///
/// Quindi: `validate()` lo accetta, `validate_coherence()` lo rifiuta, e la
/// conformita dei provider chiama la seconda. Se qualcuno riporta la
/// relazione in `validate()`, questo test lo dice.
#[test]
fn a_relation_the_contract_does_not_state_is_not_rejected_on_the_consumption_path() {
    let bytes = include_bytes!("../../../contracts/v2/examples/capabilities-minimal.json");
    let mut capabilities: ProviderCapabilities =
        serde_json::from_slice(bytes).expect("documento minimo");
    capabilities.reads.server_cursor = true;
    capabilities.reads.streaming = false;

    capabilities
        .validate()
        .expect("il contratto v2 non vieta questa combinazione");
    capabilities
        .validate_coherence()
        .expect_err("ma resta incoerente, e chi pubblica non deve emetterla");
}

/// Un limite che eccede `u64` e conforme allo schema v2 — che dice
/// `"type": "integer"` senza massimo — e resta illeggibile da questa
/// implementazione, che lo tiene in `u64`.
///
/// Il documento non viene rifiutato: non arriva neppure a esistere. Il
/// confine e la deserializzazione, e il messaggio di errore che ne esce
/// deve dire *quello*, non far credere a un contratto piu stretto di
/// quello pubblicato.
#[test]
fn a_limit_beyond_u64_is_within_the_contract_and_outside_this_reader() {
    let bytes = include_bytes!(
        "../../../contracts/v2/examples/unconsumable-capabilities-limit-over-u64.json"
    );
    serde_json::from_slice::<ProviderCapabilities>(bytes)
        .expect_err("un limite oltre u64 non e rappresentabile qui");
}

/// Cio che il contratto **dichiara** resta rifiutato dal percorso di
/// consumo: il confine si sposta in un verso solo.
#[test]
fn what_the_contract_states_is_still_rejected_on_the_consumption_path() {
    let bytes = include_bytes!("../../../contracts/v2/examples/capabilities-minimal.json");
    let mut capabilities: ProviderCapabilities =
        serde_json::from_slice(bytes).expect("documento minimo");

    // Lo schema ha `minimum: 1` su questo limite.
    capabilities.limits.max_batch_rows = Some(0);
    capabilities.validate().expect_err("limite a zero");

    // E `minLength: 1` sulla versione del provider.
    capabilities = serde_json::from_slice(bytes).expect("documento minimo");
    capabilities.provider_version = String::new();
    capabilities.validate().expect_err("versione vuota");
}

#[test]
fn the_minimal_contract_document_deserialises() {
    let bytes = include_bytes!("../../../contracts/v2/examples/capabilities-minimal.json");
    let capabilities: ProviderCapabilities =
        serde_json::from_slice(bytes).expect("il documento minimo del contratto deve caricare");

    // I default non sono neutri: sono la risposta conservativa.
    assert!(!capabilities.reads.server_cursor);
    assert!(!capabilities.reads.pagination);
    assert!(!capabilities.reads.resumable);
    assert!(!capabilities.writes.delete_by_keys);
    assert!(!capabilities.writes.bulk);
    assert!(!capabilities.writes.array_binding);
    assert!(!capabilities.writes.returning);
    assert!(!capabilities.writes.rollback_on_failure);
    assert!(!capabilities.transactions.single_transaction);
    assert!(!capabilities.transactions.savepoints);
    assert!(!capabilities.transactions.transactional_ddl);
    assert!(!capabilities.transactions.staged_swap);
    assert_eq!(capabilities.transactions.scope, TransactionScope::None);
    assert!(!capabilities.spatial.geometry);
    assert!(!capabilities.spatial.geography);
    assert!(!capabilities.spatial.spatial_index);
    assert!(!capabilities.spatial.mixed_geometry_types);
    assert!(capabilities.spatial.functions.is_empty());
    assert!(capabilities.extension_versions.is_empty());
    assert_eq!(capabilities.limits.max_identifier_bytes, None);
}

/// L'altra direzione: cio che questi tipi emettono contiene ogni campo
/// che lo schema richiede. I default rendono tollerante la lettura, non
/// reticente la scrittura.
#[test]
fn serialising_emits_every_field_the_schema_requires() {
    let bytes = include_bytes!("../../../contracts/v2/examples/capabilities-minimal.json");
    let capabilities: ProviderCapabilities =
        serde_json::from_slice(bytes).expect("documento minimo");
    let emitted = serde_json::to_value(&capabilities).expect("serializzabile");

    for field in [
        "schema_version",
        "provider",
        "provider_version",
        "reads",
        "writes",
        "transactions",
        "spatial",
        "limits",
    ] {
        assert!(emitted.get(field).is_some(), "manca `{field}`");
    }
    for field in ["streaming", "projection", "filter", "ordering"] {
        assert!(emitted["reads"].get(field).is_some(), "reads.{field}");
    }
    for field in [
        "create",
        "append",
        "truncate_insert",
        "update",
        "upsert",
        "replace",
    ] {
        assert!(emitted["writes"].get(field).is_some(), "writes.{field}");
    }
    for field in ["read_wkb", "write_wkb", "dimensions"] {
        assert!(emitted["spatial"].get(field).is_some(), "spatial.{field}");
    }
}
