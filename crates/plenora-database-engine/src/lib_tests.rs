use super::*;

/// Le quattro operazioni di inspect stanno in un solo sottoschema, dove
/// solo `id` e obbligatorio e `source` e ammesso per tutte.
///
/// Serde invece pretende `source` per `database.describe_object`: senza,
/// non c'e oggetto da descrivere. Un `{"id": "database.describe_object"}`
/// e percio un documento dentro il contratto e fuori da questo lettore.
///
/// Non si chiude allargando Serde — accettarlo vorrebbe dire rimandare a
/// runtime un errore rilevabile alla lettura — ne stringendo lo
/// schema, che e pubblicato e la major non si restringe. Il posto dove
/// separare i quattro sottoschemi e `contracts/v3/`.
#[test]
fn describe_object_without_a_source_is_within_the_contract_and_outside_this_reader() {
    let bytes = include_bytes!(
        "../../../contracts/v2/examples/unconsumable-plan-describe-without-source.json"
    );
    serde_json::from_slice::<Plan>(bytes)
        .expect_err("documento conforme allo schema e non consumabile");
}

/// L'altro verso **non** e una divergenza, ed e utile dirlo.
///
/// Un `database.list_catalogs` con `source` e ammesso dallo schema, e il
/// lettore lo accetta ignorando il campo: `deny_unknown_fields` non
/// raggiunge le varianti unitarie di un enum con tag interno. Non e una
/// falla — il contratto permette quel documento, e la variante non ha un
/// oggetto su cui operare — ma e un comportamento che si legge male dal
/// solo `deny_unknown_fields`, e resta scritto qui.
#[test]
fn list_catalogs_ignores_a_source_the_contract_allows() {
    let plan: Plan = serde_json::from_str(
        r#"{"schema_version":2,"connection_ref":"env:DSN","provider":"postgres",
            "operation":{"id":"database.list_catalogs",
                         "source":{"schema":"public","object":"eventi"}}}"#,
    )
    .expect("il contratto ammette il campo, e il lettore non lo rifiuta");
    assert_eq!(plan.operation, Operation::DatabaseListCatalogs);
}

/// Un limite oltre `u64` e conforme allo schema v2 — `minimum` senza
/// `maximum` — e illeggibile da questa implementazione.
///
/// E lo stesso confine gia fissato per il documento capability, e vale per
/// ogni intero del contratto: dopo il ripristino del dominio storico della
/// v2 non esiste piu alcun massimo dichiarato, quindi la divergenza fra
/// cio che il contratto ammette e cio che `u64` rappresenta e permanente
/// finche non arriva una major che la scriva.
#[test]
fn a_plan_limit_beyond_u64_is_within_the_contract_and_outside_this_reader() {
    let bytes =
        include_bytes!("../../../contracts/v2/examples/unconsumable-plan-limit-over-u64.json");
    serde_json::from_slice::<Plan>(bytes)
        .expect_err("un limite oltre u64 non e rappresentabile qui");
}

/// Una finestra senza ordinamento sta dentro il contratto e fuori da
/// questo lettore.
///
/// Lo schema non lega i due campi — potrebbe, con una condizione, ma la
/// regola non e sintattica: e la ragione per cui la finestra esiste. Due
/// letture consecutive di un risultato non ordinato possono rendere righe
/// diverse, quindi `row_offset` senza `order_by` descrive una pagina che
/// nessuno puo ripetere.
///
/// Il rifiuto e in `prepare` e non nei provider: riguarda il piano, e
/// `MySQL` lo applicava gia al tetto mentre `PostgreSQL` no. Metterlo qui
/// lo rende uno.
#[test]
fn an_offset_without_an_ordering_is_within_the_contract_and_refused_here() {
    let bytes = include_bytes!(
        "../../../contracts/v2/examples/unconsumable-plan-offset-without-order.json"
    );
    let validated = parse_and_validate(bytes).expect("il contratto ammette il documento");
    let error = prepare(validated, postgres_capabilities())
        .expect_err("una finestra senza ordinamento non e riproducibile");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::InvalidPlan
    );
    assert!(
        error.message.contains("row_offset richiede order_by"),
        "il messaggio non dice cosa manca: {}",
        error.message
    );
}

/// Una finestra chiesta a chi non la pubblica viene rifiutata.
///
/// E' cio che rende `reads.pagination` una bandiera invece di una
/// dichiarazione: prima nessuna riga la consultava, e un provider che
/// avesse pubblicato `false` avrebbe ricevuto l'offset lo stesso.
#[test]
fn an_offset_is_refused_by_a_provider_that_does_not_publish_pagination() {
    let bytes = include_bytes!(
        "../../../contracts/v2/examples/unconsumable-plan-offset-without-order.json"
    );
    let validated = parse_and_validate(bytes).expect("il contratto ammette il documento");
    let mut capabilities = postgres_capabilities();
    capabilities.reads.pagination = false;
    let error =
        prepare(validated, capabilities).expect_err("il provider non pubblica la paginazione");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Unsupported,
        "atteso un rifiuto di capability, non di piano: {}",
        error.message
    );
}

/// `maxLength: 256` conta code point. Un riferimento di 256 caratteri
/// accentati pesa 512 byte ed e dentro il contratto.
#[test]
fn a_connection_ref_of_256_characters_is_within_the_contract() {
    let reference = format!("env:{}", "e\u{300}".repeat(126));
    assert_eq!(reference.chars().count(), 256);
    assert!(
        reference.len() > 256,
        "il caso serve solo se i byte eccedono"
    );
    validate_connection_ref(&reference).expect("256 caratteri sono ammessi");
    enforce_connection_reference_policy(&reference).expect("prefisso indiretto");

    let too_long = format!("env:{}", "e\u{300}".repeat(127));
    validate_connection_ref(&too_long).expect_err("257 caratteri no");
}

/// La politica di questo runtime e piu stretta del contratto, e i due
/// giudizi restano distinti: il piano con la DSN in chiaro **e** un
/// documento v2 valido, e viene rifiutato lo stesso.
#[test]
fn an_inline_dsn_is_contract_valid_and_still_refused_here() {
    let inline = "postgres://user:password@host/db";
    validate_connection_ref(inline).expect("il contratto v2 lo ammette");
    enforce_connection_reference_policy(inline)
        .expect_err("questo runtime non esegue credenziali in chiaro");
}

#[test]
fn parses_the_postgres_contract_example() {
    let bytes = include_bytes!("../../../contracts/v2/examples/plan-postgres-read.json");
    let validated = parse_and_validate(bytes).expect("valid plan");
    assert_eq!(validated.fingerprint().len(), 64);
    assert_eq!(
        validated.plan().provider,
        plenora_database_core::plan::ProviderKind::Postgres
    );
}

// ------------------------------------------------------------------
//  prepare: la matrice piano -> capability
// ------------------------------------------------------------------

const CONTRACT_READ_PLAN: &[u8] =
    include_bytes!("../../../contracts/v2/examples/plan-postgres-read.json");

/// Spegne una capability. Il nome accompagna il veto perche il messaggio
/// di fallimento dica *quale* bandiera non ha morso.
type Veto = (&'static str, fn(&mut ProviderCapabilities));

fn postgres_capabilities() -> ProviderCapabilities {
    serde_json::from_slice(include_bytes!(
        "../../../contracts/v2/examples/capabilities-postgres.json"
    ))
    .expect("documento capability del contratto")
}

fn prepare_plan(plan: &[u8], capabilities: ProviderCapabilities) -> Result<PreparedPlan> {
    prepare(
        parse_and_validate(plan).expect("piano valido"),
        capabilities,
    )
}

#[test]
fn the_contract_plan_prepares_against_the_contract_capabilities() {
    prepare_plan(CONTRACT_READ_PLAN, postgres_capabilities())
        .expect("il piano del contratto deve preparare contro le capability del contratto");
}

/// Ogni capability che il piano usa deve poterlo fermare da sola.
///
/// Il test disabilita una bandiera per volta, cosi ogni capability usata
/// dal piano deve esercitare da sola il proprio veto.
#[test]
fn every_read_capability_the_plan_uses_can_veto_it() {
    let vetoes: [Veto; 4] = [
        ("streaming", |c| c.reads.streaming = false),
        ("projection", |c| c.reads.projection = false),
        ("filter", |c| c.reads.filter = false),
        ("ordering", |c| c.reads.ordering = false),
    ];
    for (name, veto) in vetoes {
        let mut capabilities = postgres_capabilities();
        veto(&mut capabilities);
        let error = prepare_plan(CONTRACT_READ_PLAN, capabilities).unwrap_err();
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::Unsupported,
            "`{name}` a false deve fermare il piano"
        );
    }
}

/// Il piano del contratto non usa `row_limit`: la relativa capability non
/// deve poterlo fermare. Una matrice che rifiuta troppo e sbagliata quanto
/// una che rifiuta troppo poco.
#[test]
fn a_capability_the_plan_does_not_use_does_not_veto_it() {
    let mut capabilities = postgres_capabilities();
    capabilities.reads.pagination = false;
    capabilities.spatial.functions.clear();
    prepare_plan(CONTRACT_READ_PLAN, capabilities)
        .expect("il piano non usa row_limit ne funzioni spatial");
}

/// Il piano di lettura del contratto, con le dichiarazioni di CRS chieste.
///
/// Passa per `parse_and_validate` come ogni altro piano di questi test: la
/// dichiarazione deve attraversare anche il contratto, non solo `prepare`,
/// e un campo che lo schema rifiutasse non arriverebbe mai qui.
fn plan_declaring(declarations: &[(&str, u32)]) -> Vec<u8> {
    let mut document: serde_json::Value =
        serde_json::from_slice(CONTRACT_READ_PLAN).expect("piano del contratto");
    document["operation"]["declared_crs"] = declarations
        .iter()
        .map(|(column, srid)| serde_json::json!({"column": column, "srid": srid}))
        .collect();
    serde_json::to_vec(&document).expect("piano serializzabile")
}

/// Un CRS dichiarato a chi non lo pretende viene rifiutato.
///
/// La direzione del rifiuto e quella meno ovvia, e vale la pena dirla: non
/// «il provider non sa leggere le geometrie», ma «il provider il CRS lo sa
/// gia». `PostgreSQL` lo legge da `geometry_columns`, `MySQL` da
/// `information_schema`; accettare li una dichiarazione vorrebbe dire
/// tenere due fonti per lo stesso fatto, e quando divergono nessuna delle
/// due e piu quella giusta.
///
/// Senza questa riga il campo sarebbe accettato ovunque e onorato da un
/// provider solo: il chiamante crederebbe di aver dichiarato qualcosa, e
/// nessuno gli direbbe di no.
#[test]
fn a_declared_crs_is_refused_by_a_provider_whose_catalog_knows_it() {
    let error = prepare_plan(&plan_declaring(&[("geom", 4326)]), postgres_capabilities())
        .expect_err("PostgreSQL il CRS lo legge dal catalogo");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Unsupported,
        "atteso un rifiuto di capability: {}",
        error.message
    );
}

/// Dove il provider lo pretende, la dichiarazione passa.
#[test]
fn a_declared_crs_is_accepted_where_the_catalog_stays_silent() {
    let mut capabilities = postgres_capabilities();
    capabilities.spatial.requires_declared_crs = true;
    prepare_plan(&plan_declaring(&[("geom", 4326)]), capabilities)
        .expect("il provider chiede di dichiararlo");
}

/// Zero non e un CRS: e l'assenza che il catalogo dice gia da solo.
///
/// Il rifiuto e `InvalidPlan` e non `Unsupported` perche non riguarda cosa
/// il provider sa fare: riguarda cosa quella dichiarazione afferma, che e
/// niente. Accettarlo darebbe al chiamante l'impressione di aver risolto
/// il problema per cui il campo esiste.
#[test]
fn a_declared_crs_of_zero_declares_nothing() {
    let mut capabilities = postgres_capabilities();
    capabilities.spatial.requires_declared_crs = true;
    let error = prepare_plan(&plan_declaring(&[("geom", 0)]), capabilities)
        .expect_err("zero e l'indefinito OGC, non un CRS");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::InvalidPlan
    );
}

/// La stessa colonna due volte e una contraddizione, non un rinforzo.
#[test]
fn a_column_declared_twice_is_refused() {
    let mut capabilities = postgres_capabilities();
    capabilities.spatial.requires_declared_crs = true;
    let error = prepare_plan(
        &plan_declaring(&[("geom", 4326), ("geom", 3003)]),
        capabilities,
    )
    .expect_err("due CRS per una colonna sola non ne fanno uno");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::InvalidPlan
    );
}

fn write_plan_with_mode(
    profile: &str,
    allow_partial: bool,
    spatial_index: bool,
    mode: &str,
) -> Vec<u8> {
    format!(
        concat!(
            r#"{{"schema_version": 2,"#,
            r#""connection_ref": "env:PLENORA_DATABASE_DSN","#,
            r#""provider": "postgres","#,
            r#""operation": {{"#,
            r#""id": "database.write","#,
            r#""target": {{"schema": "public", "object": "events"}},"#,
            r#""mode": "{}","#,
            r#""mapping_policy": "strict","#,
            r#""transaction_profile": "{}","#,
            r#""allow_partial": {},"#,
            r#""create_spatial_index": {}"#,
            r#"}}}}"#
        ),
        mode, profile, allow_partial, spatial_index
    )
    .into_bytes()
}

#[test]
fn every_write_capability_the_plan_uses_can_veto_it() {
    // `create`, non `append`: l'indice spaziale si chiede solo a chi crea
    // il target, e un piano che lo chiedesse altrove sarebbe fermato dalla
    // contraddizione interna prima di arrivare alle capability — cioe
    // questo test non proverebbe piu cio che dichiara.
    let plan = write_plan_with_mode("single_transaction", false, true, "create");
    prepare_plan(&plan, postgres_capabilities()).expect("capability complete");

    let vetoes: [Veto; 4] = [
        ("create", |c| c.writes.create = false),
        ("rollback_on_failure", |c| {
            c.writes.rollback_on_failure = false;
        }),
        ("single_transaction", |c| {
            // Il documento deve restare **valido**, altrimenti il test
            // misura la validazione invece del veto: `scope = transaction`
            // e `staged_swap` richiedono entrambi `single_transaction`, e
            // lasciarli accesi produrrebbe un documento contraddittorio
            // rifiutato prima del confronto col piano.
            c.transactions.single_transaction = false;
            c.transactions.savepoints = false;
            c.transactions.staged_swap = false;
            c.transactions.scope = TransactionScope::Statement;
        }),
        ("spatial_index", |c| c.spatial.spatial_index = false),
    ];
    for (name, veto) in vetoes {
        let mut capabilities = postgres_capabilities();
        veto(&mut capabilities);
        let error = prepare_plan(&plan, capabilities).unwrap_err();
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::Unsupported,
            "`{name}` a false deve fermare il piano"
        );
    }
}

/// Le contraddizioni interne al piano si chiudono senza capability.
///
/// Nessun documento capability puo renderle vere e nessun provider puo
/// eseguirle rispettando il lifecycle richiesto.
#[test]
fn a_write_that_contradicts_itself_is_rejected() {
    // (profilo, allow_partial, indice spaziale, mode, cosa si contraddice)
    let contradictions = [
        (
            "read_only",
            true,
            false,
            "append",
            "profilo di sola lettura",
        ),
        (
            "chunk_committed",
            false,
            false,
            "append",
            "commit intermedi con allow_partial=false",
        ),
        (
            "staged_swap",
            true,
            false,
            "append",
            "staged swap su una mode che non sostituisce",
        ),
    ];
    for (profile, allow_partial, spatial_index, mode, what) in contradictions {
        let plan = write_plan_with_mode(profile, allow_partial, spatial_index, mode);
        let error = prepare_plan(&plan, postgres_capabilities()).unwrap_err();
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::InvalidPlan,
            "{what} deve essere rifiutato dal piano, non dalle capability"
        );
    }
}

/// Le stesse forme, senza la contraddizione, restano ammesse.
#[test]
fn the_same_shapes_without_the_contradiction_still_prepare() {
    for (profile, allow_partial, spatial_index, mode) in [
        ("chunk_committed", true, false, "append"),
        ("staged_swap", true, false, "replace"),
        ("single_transaction", false, true, "create"),
    ] {
        let plan = write_plan_with_mode(profile, allow_partial, spatial_index, mode);
        let mut capabilities = postgres_capabilities();
        // L'esempio PostgreSQL pubblicato resta fedele all'implementazione e
        // non dichiara uno swap fisico. Qui si misura il validatore con una
        // capability sintetica esplicitamente aperta.
        if profile == "staged_swap" {
            capabilities.transactions.staged_swap = true;
        }
        prepare_plan(&plan, capabilities)
            .unwrap_or_else(|error| panic!("{profile}/{mode} respinto: {error:?}"));
    }
}

/// `prepare` non accetta piu un documento capability che il contratto
/// rifiuta: la validazione e quella completa del core, non due controlli
/// scelti a mano.
#[test]
fn a_capability_document_that_violates_the_contract_is_rejected() {
    let violations: [Veto; 4] = [
        ("major sbagliata", |c| c.schema_version = 1),
        // Vuota davvero: `minLength: 1` e un vincolo che lo schema
        // enuncia. Una versione di soli spazi lo supera, quindi non e una
        // violazione del contratto e non appartiene a questa lista.
        ("provider_version vuota", |c| {
            c.provider_version = String::new();
        }),
        ("limite esplicito a zero", |c| {
            c.limits.max_bind_parameters = Some(0);
        }),
        ("funzioni spatial duplicate", |c| {
            c.spatial.functions = vec![SpatialFunction::Intersects, SpatialFunction::Intersects];
        }),
    ];
    for (what, break_it) in violations {
        let mut capabilities = postgres_capabilities();
        break_it(&mut capabilities);
        let error = prepare_plan(CONTRACT_READ_PLAN, capabilities).unwrap_err();
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::InvalidPlan,
            "{what} deve fermare la preparazione"
        );
    }
}

#[test]
fn staged_swap_requires_the_staged_swap_capability() {
    // `replace`: lo staged swap sostituisce il contenuto del target, e su
    // una mode che non lo sostituisce e una contraddizione del piano.
    let plan = write_plan_with_mode("staged_swap", true, false, "replace");
    let mut capabilities = postgres_capabilities();
    capabilities.transactions.staged_swap = true;
    prepare_plan(&plan, capabilities).expect("capability sintetica completa");

    let mut capabilities = postgres_capabilities();
    capabilities.transactions.staged_swap = false;
    assert_eq!(
        prepare_plan(&plan, capabilities)
            .expect_err("staged_swap non pubblicizzato")
            .category,
        plenora_database_core::ErrorCategory::Unsupported
    );
}

/// Un piano che chiede una funzione spatial non pubblicizzata si ferma.
#[test]
fn an_unadvertised_spatial_function_vetoes_the_plan() {
    let plan = br#"{
      "schema_version": 2,
      "connection_ref": "env:PLENORA_DATABASE_DSN",
      "provider": "postgres",
      "operation": {
        "id": "database.read",
        "source": {"schema": "public", "object": "events"},
        "filter": {
          "op": "spatial",
          "function": "intersects",
          "field": "geom",
          "geometry_parameter": "reference"
        }
      }
    }"#;
    // Il documento capability del contratto non elenca funzioni spatial:
    // `functions` e opzionale e li e assente, quindi di suo non ne
    // garantisce nessuna. Il test dichiara cio che gli serve invece di
    // dipendere da quel documento.
    let mut advertising = postgres_capabilities();
    advertising.spatial.functions = vec![SpatialFunction::Intersects];
    prepare_plan(plan, advertising).expect("intersects pubblicizzata");

    let mut silent = postgres_capabilities();
    silent
        .spatial
        .functions
        .retain(|function| *function != SpatialFunction::Intersects);
    assert_eq!(
        prepare_plan(plan, silent)
            .expect_err("intersects non pubblicizzata")
            .category,
        plenora_database_core::ErrorCategory::Unsupported
    );
}

#[test]
fn fingerprint_is_stable() {
    let bytes = include_bytes!("../../../contracts/v2/examples/plan-postgres-read.json");
    let first = parse_and_validate(bytes).expect("first");
    let second = parse_and_validate(bytes).expect("second");
    assert_eq!(first.fingerprint(), second.fingerprint());
}

#[test]
fn rejects_inline_dsn() {
    let bytes = br#"{
      "schema_version": 2,
      "connection_ref": "postgres://user:password@host/db",
      "provider": "postgres",
      "operation": {"id": "database.test_connection"}
    }"#;
    let error = parse_and_validate(bytes).expect_err("inline DSN");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::InvalidPlan
    );
}
