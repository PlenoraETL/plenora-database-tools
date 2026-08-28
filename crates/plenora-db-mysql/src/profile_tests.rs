use super::{
    ProductProfile, COLUMN_ALIASES, INDEX_PART_ALIASES, MARIADB_PROFILE, MARIADB_STATEMENT_TIMEOUT,
    MEASURED_SERVER_CODES, MYSQL_PROFILE, OBJECT_ALIASES, SCHEMA_ALIASES, SECOND_PRODUCT_PROFILE,
};
use crate::types::MysqlColumnKind;
use crate::MysqlConfig;
use mysql_async::consts::ColumnType;
use mysql_async::Column;
use plenora_database_core::arrow::schema::{DataType, Field, Schema, SchemaRef};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{
    ComparisonOperator, ObjectRef, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
use plenora_database_core::query::{
    ColumnRef, QueryExpression, QueryOperation, QueryProjection, QuerySource,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::CancellationToken;
use plenora_database_core::{
    plan::ProviderKind, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition,
};

#[test]
fn the_profile_accepts_mysql_and_rejects_mariadb_from_either_string() {
    assert!(MYSQL_PROFILE
        .foreign_product_rejection("9.7.2", "MySQL Community Server - GPL")
        .is_none());
    // Il riferimento MariaDB 12.3.2 porta il marchio in `VERSION()`; la
    // 11.8.8 lo porta anche nel commento. Entrambe le letture bastano da
    // sole, e nessuna delle due si puo dare per scontata.
    for (version, comment) in [
        ("12.3.2-MariaDB", "MySQL Community Server - GPL"),
        ("11.8.8", "mariadb.org binary distribution"),
        ("12.3.2-MariaDB", "mariadb.org binary distribution"),
    ] {
        let rejection = MYSQL_PROFILE
            .foreign_product_rejection(version, comment)
            .unwrap_or_else(|| panic!("{version} / {comment} doveva essere rifiutato"));
        assert_eq!(rejection.category, ErrorCategory::Unsupported);
        assert_eq!(rejection.phase, ErrorPhase::Probe);
        assert_eq!(rejection.provider, Some(ProviderKind::Mysql));
        assert_eq!(rejection.remote_effect, RemoteEffect::None);
        assert!(rejection.message.contains("non qualificato per MariaDB"));
        assert!(rejection.message.contains(version));
    }
}

#[test]
fn the_statement_timeout_keeps_the_contract_unit() {
    // Il contratto parla in millisecondi e MySQL li accetta tali quali:
    // il numero che finisce nello statement e lo stesso che entra. Un
    // profilo che convertisse in secondi produrrebbe `5`, e questa
    // asserzione e cio che lo distingue.
    assert_eq!(
        MYSQL_PROFILE.statement_timeout_statement(5_000),
        "SET SESSION MAX_EXECUTION_TIME = 5000"
    );
    assert_eq!(
        MYSQL_PROFILE.statement_timeout_statement(1),
        "SET SESSION MAX_EXECUTION_TIME = 1"
    );
}

#[test]
fn no_other_module_writes_the_timeout_statement() {
    // La transazione emette il timeout ma non lo compone piu. Se il nome
    // della variabile tornasse a comparire li, un secondo profilo lo
    // cambierebbe in un posto solo e l'altro resterebbe MySQL.
    let variable = format!("MAX_EXECUTION{}TIME", "_");
    assert!(!include_str!("transaction.rs").contains(variable.as_str()));
}

#[test]
fn the_catalog_is_queried_only_through_the_profile() {
    // Una query rimasta nel catalogo sarebbe una query che un secondo
    // profilo non puo cambiare: verrebbe eseguita comunque, e fallirebbe
    // sul prodotto sbagliato invece di essere sostituita.
    let source = format!("FROM information{}schema", "_");
    assert!(!include_str!("catalog.rs").contains(source.as_str()));
}

#[test]
fn the_functional_index_flag_matches_the_query_that_supports_it() {
    // Il flag promette che le parti funzionali si vedono; a mostrarle e
    // la colonna selezionata dalla query. Separati, uno dei due mente.
    assert_eq!(
        MYSQL_PROFILE.reports_functional_index_parts(),
        MYSQL_PROFILE.object_indexes_query().contains("EXPRESSION")
    );
}

#[test]
fn the_query_module_no_longer_maps_wire_types() {
    // La mappatura vive nel profilo. Se `query.rs` tornasse a nominare i
    // tipi del protocollo fuori dai propri test, esisterebbero due
    // produzioni del `native_type` e un secondo profilo ne cambierebbe
    // una sola.
    let source = include_str!("query.rs");
    let production = source
        .split_once("#[cfg(test)]")
        .expect("query.rs ha un modulo di test")
        .0;
    let wire = format!("ColumnType::MYSQL{}TYPE", "_");
    assert!(!production.contains(wire.as_str()));
}

#[test]
fn the_wire_produces_the_native_type_that_diverged() {
    // Il caso che ADR 0014 ha misurato: dalla stessa DDL `document JSON`
    // MySQL manda MYSQL_TYPE_JSON e MariaDB MYSQL_TYPE_BLOB. Il nome che
    // ne esce e cio che finisce nei metadata Arrow, ed e questo il valore
    // che un secondo profilo dovra decidere di nuovo.
    let json = Column::new(ColumnType::MYSQL_TYPE_JSON)
        .with_name(b"document")
        .with_character_set(255);
    let spec = MYSQL_PROFILE.wire_column_spec(&json).expect("json");
    assert_eq!(spec.native_type, "json");
    assert_eq!(spec.kind, MysqlColumnKind::Utf8);

    let blob = Column::new(ColumnType::MYSQL_TYPE_BLOB)
        .with_name(b"document")
        .with_character_set(255);
    let spec = MYSQL_PROFILE.wire_column_spec(&blob).expect("blob");
    assert_eq!(spec.native_type, "text");
}

#[test]
fn the_spatial_types_carry_the_srid_rule_that_qualifies_them() {
    for spatial in [
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection",
        "geomcollection",
    ] {
        assert!(MYSQL_PROFILE.is_spatial_native_type(spatial), "{spatial}");
    }
    for scalar in ["blob", "json", "text", "geo", "geometryx", ""] {
        assert!(!MYSQL_PROFILE.is_spatial_native_type(scalar), "{scalar}");
    }
    // La regola che rende qualificata una colonna spatial: senza SRID
    // dichiarato si rifiuta. Un profilo che la spegnesse pubblicherebbe
    // geometrie con CRS ignoto, ed e la ragione per cui e una decisione
    // e non una costante.
    assert!(MYSQL_PROFILE.spatial_requires_declared_srid());
}

#[test]
fn the_expected_wkb_matches_the_projection_that_produces_it() {
    assert_eq!(
        MYSQL_PROFILE.geometry_projection("`geom`"),
        "ST_AsBinary(`geom`) AS `geom`"
    );
    // Cio che quella funzione produce: XY, nessun SRID incapsulato.
    assert!(!MYSQL_PROFILE.geometry_output_is_unexpected(None, "xy"));
    assert!(MYSQL_PROFILE.geometry_output_is_unexpected(Some(4_326), "xy"));
    assert!(MYSQL_PROFILE.geometry_output_is_unexpected(None, "xyz"));
}

#[test]
fn the_renderer_wraps_a_computed_geometry_as_the_profile_wraps_a_column() {
    // La forma dell'involucro e scritta in **due** posti: qui per una colonna, e nel
    // renderer condiviso per un'espressione. Devono restare la stessa
    // funzione, perche il controllo per valore in lettura e uno solo — e
    // `geometry_output_is_unexpected` valida cio che *quella* funzione
    // produce, non cio che ne produrrebbe un'altra.
    let wrapper = MYSQL_PROFILE
        .geometry_projection("`geom`")
        .split_once('(')
        .expect("la proiezione e una chiamata di funzione")
        .0
        .to_owned();
    let rendered = crate::types::mysql_renderer()
        .render_query(&plenora_database_core::query::QueryOperation {
            declared_crs: vec![plenora_database_core::plan::DeclaredCrs {
                column: "geom".to_owned(),
                srid: 4_326,
            }],
            common_table_expressions: Vec::new(),
            source: Some(plenora_database_core::query::QuerySource {
                object: plenora_database_core::plan::ObjectRef {
                    catalog: None,
                    schema: Some("app".to_owned()),
                    object: "shapes".to_owned(),
                },
                alias: None,
            }),
            derived_source: None,
            projection: vec![plenora_database_core::query::QueryProjection {
                alias: Some("hull".to_owned()),
                expression: plenora_database_core::query::QueryExpression::Spatial {
                    function: plenora_database_core::query::SpatialFunction::Envelope,
                    arguments: vec![plenora_database_core::query::QueryExpression::Column {
                        column: plenora_database_core::query::ColumnRef {
                            relation: None,
                            field: "geom".to_owned(),
                        },
                    }],
                },
            }],
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: None,
            row_offset: None,
            locking: None,
        })
        .expect("render");
    assert!(
        rendered.sql.contains(&format!("{wrapper}(ST_Envelope(")),
        "il renderer non incapsula la geometria calcolata come il profilo \
             incapsula la colonna: {}",
        rendered.sql
    );
}

#[test]
fn the_scalar_census_does_not_skip_what_is_already_published() {
    // La sonda delle scalari filtrava sulle due liste pubblicate, e cio la
    // rendeva una misura che **scadeva**: una funzione aperta su `MySQL`
    // smetteva di essere chiesta a `MariaDB`, dove non era aperta.
    // `HausdorffDistance` e `FrechetDistance` devono essere riverificate
    // sui riferimenti MariaDB, non semplicemente escluse da una lista.
    //
    // Il filtro sarebbe naturale da riscrivere — chiedere cio che si sa gia
    // sembra spreco — e costa una SELECT per funzione. E' il prezzo di una
    // misura ripetibile proprio dove i due prodotti divergono.
    let source = include_str!("mariadb_evidence.rs");
    let probe = source
        .split_once("async fn scalar_function_probe")
        .expect("la sonda delle scalari")
        .1;
    // Fino alla funzione successiva, e non fino alla prima graffa a inizio
    // riga: il file ha terminatori di riga Windows, e un `\n}\n` non ci
    // compare mai.
    let probe = probe
        .split_once("async fn ")
        .map_or(probe, |(body, _)| body);
    for list in ["VERIFIED_SPATIAL_FUNCTIONS", "MARIADB_VERIFIED_SPATIAL"] {
        assert!(
            !probe.contains(list),
            "la sonda delle scalari consulta {list}: la sua misura tornerebbe \
                 a scadere su cio che i due prodotti non condividono"
        );
    }
}

#[test]
fn no_other_module_writes_the_geometry_projection() {
    // La proiezione e l'attesa sul suo output sono due meta della stessa
    // decisione. Se `types.rs` tornasse a scrivere la funzione, un
    // secondo profilo ne cambierebbe una sola, e il controllo in lettura
    // validerebbe l'output di una funzione che non ha scelto.
    let production = include_str!("types.rs")
        .split_once("#[cfg(test)]")
        .expect("types.rs ha un modulo di test")
        .0;
    let function = format!("ST{}AsBinary", "_");
    assert!(!production.contains(function.as_str()));
}

#[test]
fn the_catalog_derived_specs_always_carry_a_profile() {
    // La forma pubblica di `from_catalog` ricade sul profilo statico. In
    // produzione deve restare solo la sua definizione: un consumatore che
    // la chiamasse validerebbe il target di un secondo prodotto con le
    // regole di questo, ed e esattamente cio che il preflight di
    // scrittura faceva.
    let needle = format!("from{}catalog(", "_");
    for (module, source, allowed) in [
        ("write.rs", include_str!("write.rs"), 0),
        ("read.rs", include_str!("read.rs"), 0),
        ("types.rs", include_str!("types.rs"), 1),
    ] {
        let production = source
            .split_once("#[cfg(test)]")
            .map_or(source, |(head, _)| head);
        assert_eq!(
            production.matches(needle.as_str()).count(),
            allowed,
            "{module} usa la forma senza profilo"
        );
    }
}

#[test]
fn no_module_signs_an_error_with_a_hardcoded_product() {
    // Il segnaposto ha un nome perche sia greppabile e perche il bordo lo
    // ristampi. Un literal scritto a mano non lo sarebbe: sopravvivrebbe
    // al bordo solo se qualcuno lo mettesse dove il bordo non passa, ed e
    // la ragione per cui qui non ne deve restare nessuno.
    let literal = format!("ProviderKind::{}sql", "My");
    // I corpi dei test vivono in file separati, quindi questi sorgenti
    // contengono soltanto il codice e gli helper locali che la guardia deve
    // presidiare.
    for (module, source) in [
        ("arrow.rs", include_str!("arrow.rs")),
        ("catalog.rs", include_str!("catalog.rs")),
        ("config.rs", include_str!("config.rs")),
        ("error.rs", include_str!("error.rs")),
        ("parameter.rs", include_str!("parameter.rs")),
        ("pool.rs", include_str!("pool.rs")),
        ("provider.rs", include_str!("provider.rs")),
        ("query.rs", include_str!("query.rs")),
        ("read.rs", include_str!("read.rs")),
        ("row_diagnostics.rs", include_str!("row_diagnostics.rs")),
        ("session.rs", include_str!("session.rs")),
        ("transaction.rs", include_str!("transaction.rs")),
        ("types.rs", include_str!("types.rs")),
        ("write.rs", include_str!("write.rs")),
    ] {
        let production = source;
        assert_eq!(
            production.matches(literal.as_str()).count(),
            0,
            "{module} firma un errore con il prodotto cablato"
        );
    }
}

#[test]
fn every_method_that_returns_a_future_restamps_the_attribution() {
    // Non un conteggio complessivo: quello lascerebbe compensare due
    // sbilanciamenti in metodi diversi. La verifica e per metodo, sui due
    // trait che restituiscono futuri — `Provider` e `TransactionScope` —
    // perche il segnaposto e sicuro solo dove il bordo lo copre.
    let boxed = format!("Box::{}(", "pin");
    let stamped = format!("crate::profile::{}", "attributed");
    let mut presidiati = 0;
    for (module, source) in GUARDED_MODULES {
        let mut inspected = 0;
        // Ogni `impl` dei due trait viene scoperto automaticamente, senza
        // un inventario parallelo da aggiornare.
        // Gli intestatori si compongono a runtime: scritti per intero
        // comparirebbero in questo file, e la guardia ispezionerebbe se
        // stessa trovando zero metodi.
        let trait_headers = [
            format!("impl {} for ", "Provider"),
            format!("impl {} for ", "TransactionScope"),
        ];
        let headers: Vec<&str> = trait_headers
            .iter()
            .flat_map(|header| source.match_indices(header.as_str()).map(|(at, _)| at))
            .collect::<Vec<_>>()
            .iter()
            .map(|at| &source[*at..])
            .collect();
        if headers.is_empty() {
            continue;
        }
        presidiati += 1;
        for tail in headers {
            let end = tail
                .find(format!("{}}}", '\n').as_str())
                .unwrap_or(tail.len());
            let block = &tail[..end];
            let mut methods = 0;
            for method in block.split(format!("{}    fn ", '\n').as_str()).skip(1) {
                let name = method.split(['(', '<']).next().unwrap_or("?");
                // Una delega pura non ristampa, e non deve: a ristampare e
                // il provider interno, costruito con il profilo del
                // prodotto. `MariadbProvider` e fatto cosi — un newtype
                // che inoltra tutto — e pretendere il timbro qui vorrebbe
                // dire timbrare due volte lo stesso bordo.
                //
                // L'eccezione e stretta apposta: vale solo se il corpo
                // inoltra **lo stesso metodo** al campo interno. Una
                // delega a un'operazione diversa, o a un altro oggetto,
                // non la soddisfa — e sarebbe proprio il caso in cui
                // l'attribuzione puo divergere senza che si veda.
                if method.contains(&format!("self.0{}{name}(", '.')) {
                    methods += 1;
                    continue;
                }
                if !method.contains(boxed.as_str()) {
                    continue;
                }
                methods += 1;
                assert!(
                    method.contains(stamped.as_str()),
                    "{module}::{name} restituisce un futuro senza ristampare l'attribuzione"
                );
            }
            assert!(
                methods >= 1,
                "{module}: nessun metodo ispezionato in un impl presidiato"
            );
            inspected += methods;
        }
        assert!(
            inspected >= 8,
            "{module}: solo {inspected} metodi ispezionati in totale"
        );
    }
    assert_eq!(
        presidiati, 2,
        "i due trait devono vivere in due moduli: trovati {presidiati}"
    );
}

#[tokio::test]
async fn a_second_profile_changes_what_the_caller_observes() {
    // La prova che le guardie strutturali non possono dare: con un solo
    // profilo ogni attribuzione e `Mysql`, e nessun test distingue il
    // profilo da un literal sopravvissuto. Con due, la differenza si
    // vede — e questo errore nasce nel renderer, con il segnaposto, e
    // arriva al chiamante con l'identita del provider.
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    );
    let provider = crate::MysqlProvider::with_profile(config, 2, &SECOND_PRODUCT_PROFILE)
        .expect("provider sul secondo profilo");
    assert_eq!(provider.kind(), ProviderKind::Mariadb);

    // Un identificatore oltre il limite fallisce nel rendering, prima di
    // qualunque connessione: e il percorso che usa `PROVISIONAL_KIND`.
    // Un identificatore oltre il limite fallisce nel rendering, prima
    // di qualunque connessione: e il percorso che usa
    // `PROVISIONAL_KIND`.
    let mut operation = oversized_identifier_query();
    operation.source = Some(QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("warehouse".to_owned()),
            object: "x".repeat(crate::MAX_IDENTIFIER_CHARACTERS + 1),
        },
        alias: None,
    });

    let outcome = provider
        .query(
            &SecretString::new("unique-secret"),
            &operation,
            &ParameterBag::default(),
            &ResourceBudget::new(ResourceLimits::default()).expect("budget"),
            &CancellationToken::new(),
        )
        .await;
    let Err(error) = outcome else {
        panic!("identificatore oltre il limite: doveva fallire");
    };
    assert_eq!(
        error.provider,
        Some(ProviderKind::Mariadb),
        "l'errore esce con l'attribuzione del provider, non con il segnaposto"
    );
}

#[test]
fn the_capability_table_is_built_only_by_the_profile() {
    // Il provider delega la tabella al profilo, cosi ogni prodotto
    // pubblica soltanto capability sostenute dalla propria evidenza.
    let source = include_str!("provider.rs");
    let production = source;
    for built in [
        "ProviderCapabilities",
        "SpatialCapabilities",
        "ProviderLimits",
    ] {
        let literal = format!("{built} {{");
        assert_eq!(
            production.matches(literal.as_str()).count(),
            0,
            "provider.rs costruisce {built} invece di chiederla al profilo"
        );
    }
    // Anche i valori pubblicati devono provenire dalla tabella del profilo.
    let published = MYSQL_PROFILE.capabilities("9.7.2".to_owned());
    assert_eq!(published.provider_version, "9.7.2");
    assert_eq!(published.provider, MYSQL_PROFILE.kind());
    assert!(published.spatial.read_wkb && published.spatial.write_wkb);
    assert!(published.spatial.spatial_index);
    assert_eq!(
        published.limits.max_bind_parameters,
        Some(crate::MAX_BIND_PARAMETERS as u64)
    );
}

#[test]
fn every_catalog_query_exposes_the_aliases_its_reader_requires() {
    // Per ogni profilo:
    // il contratto degli alias esiste proprio perche un prodotto a cui
    // manca una colonna la dichiari nulla invece di ometterla, e con un
    // profilo solo nell'elenco quella regola non sarebbe mai verificata
    // dove serve.
    for profile in [&MYSQL_PROFILE as &dyn ProductProfile, &MARIADB_PROFILE] {
        for (label, sql, aliases) in [
            ("schemi", profile.schemas_query(), SCHEMA_ALIASES),
            ("oggetti", profile.objects_query(), OBJECT_ALIASES),
            ("oggetto", profile.object_query(), OBJECT_ALIASES),
            ("colonne", profile.object_columns_query(), COLUMN_ALIASES),
            ("indici", profile.object_indexes_query(), INDEX_PART_ALIASES),
        ] {
            for alias in aliases {
                assert!(
                    sql.contains(format!("AS {alias}").as_str()),
                    "{}: la query {label} non espone {alias}",
                    profile.product()
                );
            }
        }
    }
}

#[test]
fn the_catalog_reads_no_alias_the_contract_does_not_declare() {
    // L'altra direzione: un alias letto e non dichiarato sarebbe un
    // requisito invisibile, che un secondo profilo scoprirebbe solo
    // fallendo. La probe resta fuori — legge variabili di sessione, non
    // il catalogo — e il taglio parte da dove il catalogo comincia.
    let source = include_str!("catalog.rs");
    let catalog = source
        .split_once("pub async fn list_schemas")
        .expect("il catalogo comincia da list_schemas")
        .1;
    let declared: Vec<&str> = SCHEMA_ALIASES
        .iter()
        .chain(OBJECT_ALIASES)
        .chain(COLUMN_ALIASES)
        .chain(INDEX_PART_ALIASES)
        .copied()
        .collect();
    let mut rest = catalog;
    while let Some((_, tail)) = rest.split_once("required(row, \"") {
        let alias = tail.split('"').next().unwrap_or_default();
        assert!(declared.contains(&alias), "alias non dichiarato: {alias}");
        rest = tail;
    }
    let mut rest = catalog;
    while let Some((_, tail)) = rest.split_once("optional(row, \"") {
        let alias = tail.split('"').next().unwrap_or_default();
        assert!(declared.contains(&alias), "alias non dichiarato: {alias}");
        rest = tail;
    }
}

fn oversized_identifier_query() -> QueryOperation {
    QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("warehouse".to_owned()),
                object: "events".to_owned(),
            },
            alias: None,
        }),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "event_id".to_owned(),
                },
            },
            alias: None,
        }],
        joins: Vec::new(),
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: None,
        row_offset: None,
        locking: None,
    }
}

#[test]
fn no_production_path_uses_a_profileless_entry_point() {
    // Ogni forma esportata ha un gemello `_with_profile`, e la forma
    // senza esiste solo per chi il profilo non ce l'ha. Chiamandola da
    // dentro il crate si perde silenziosamente il prodotto, ed e successo
    // due volte — con il pool e con la compilazione della scrittura.
    //
    // La verifica copre tutti i moduli di produzione, non il solo
    // provider: un secondo provider vivrebbe in un file nuovo, e una
    // guardia che nomina i file da ispezionare invecchia esattamente
    // quando serve.
    let entries = [
        format!("Mysql{}::new", "Pool"),
        format!("probe{}server", "_"),
        format!("list{}schemas", "_"),
        format!("list{}objects", "_"),
        format!("describe{}object", "_"),
        format!("read{}operation", "_"),
        format!("query{}operation", "_"),
        format!("MysqlReadPlan::{}", "compile"),
        format!("from{}catalog", "_"),
        format!("query{}result{}columns", "_", "_"),
    ];
    for (module, source) in GUARDED_MODULES {
        let production = source;
        for entry in &entries {
            let needle = format!("{entry}(");
            for at in production.match_indices(needle.as_str()).map(|(at, _)| at) {
                // Confine di parola: `validate_query_operation` finisce
                // con l'ago senza esserlo, e senza questo controllo la
                // guardia grida su una funzione che non c'entra.
                let head = &production[..at];
                if head
                    .chars()
                    .next_back()
                    .is_some_and(|c| c.is_alphanumeric() || c == '_')
                {
                    continue;
                }
                // La definizione non e una chiamata: e proprio li che la
                // forma senza profilo deve continuare a esistere.
                assert!(
                    head.trim_end().ends_with("fn"),
                    "{module} chiama {entry} senza profilo"
                );
            }
        }
    }
}

/// Il messaggio non deve limitarsi a non contraddire l'attribuzione:
/// deve nominare il prodotto servito. Asserire il solo `provider` e cio
/// che lascerebbe passare un residuo del pool attribuito al prodotto errato.
fn assert_names_the_second_product(error: &plenora_database_core::DatabaseError, what: &str) {
    assert_eq!(error.provider, Some(ProviderKind::Mariadb), "{what}");
    assert!(
        error.message.contains("SecondProduct"),
        "{what}: il messaggio non nomina il prodotto — {}",
        error.message
    );
    assert!(
        !error.message.contains("MySQL"),
        "{what}: il messaggio nomina ancora MySQL — {}",
        error.message
    );
}

/// Un errore che nasce dove il profilo non arriva non deve nominare
/// nessun prodotto: il bordo ne corregge l'attribuzione, non il testo.
fn assert_names_no_product(error: &plenora_database_core::DatabaseError, what: &str) {
    assert_eq!(error.provider, Some(ProviderKind::Mariadb), "{what}");
    assert!(
        !error.message.contains("MySQL"),
        "{what}: il messaggio nomina MySQL — {}",
        error.message
    );
    assert!(
        !error.message.contains("SecondProduct"),
        "{what}: il messaggio non puo nominare un prodotto che non conosce — {}",
        error.message
    );
    // E deve restare una frase. Togliere il nome del prodotto da un
    // messaggio ne ha lasciati alcuni senza soggetto o con la
    // punteggiatura sospesa: un errore pubblico degradato non e un
    // dettaglio di forma.
    assert!(
        !error.message.contains(" :")
            && !error.message.contains("  ")
            && !error.message.contains(" ,"),
        "{what}: punteggiatura sospesa — {}",
        error.message
    );
}

fn append_to_warehouse() -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("warehouse".to_owned()),
            object: "events".to_owned(),
        },
        mode: WriteMode::Append,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

/// Una query che dichiara un parametro: senza fornirlo, il binding
/// fallisce prima di qualunque connessione.
fn parameterized_query() -> QueryOperation {
    let mut query = oversized_identifier_query();
    query.source = Some(QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("warehouse".to_owned()),
            object: "events".to_owned(),
        },
        alias: None,
    });
    query.filter = Some(QueryExpression::Compare {
        left: Box::new(QueryExpression::Column {
            column: ColumnRef {
                relation: None,
                field: "event_id".to_owned(),
            },
        }),
        operator: ComparisonOperator::Eq,
        right: Box::new(QueryExpression::Parameter {
            name: "wanted".to_owned(),
        }),
    });
    query
}

fn schema_with(fields: Vec<Field>) -> SchemaRef {
    std::sync::Arc::new(Schema::new(fields))
}

fn second_product_config() -> MysqlConfig {
    MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    )
}

#[test]
fn the_constructor_attributes_its_own_failures_to_the_profile() {
    // I due errori che un consumatore puo vedere senza aver mai toccato
    // il server. Uscivano entrambi con il segnaposto, e il test
    // precedente non li vedeva perche usava solo configurazioni valide.
    let invalid = MysqlConfig::new("", "warehouse", "loader", SecretString::new("s"));
    let error = crate::MysqlProvider::with_profile(invalid, 2, &SECOND_PRODUCT_PROFILE)
        .expect_err("configurazione invalida");
    assert_names_the_second_product(&error, "configurazione invalida");

    let error =
        crate::MysqlProvider::with_profile(second_product_config(), 0, &SECOND_PRODUCT_PROFILE)
            .expect_err("pool a capacita zero");
    assert_names_the_second_product(&error, "pool a capacita zero");
}

#[test]
fn a_diverging_profile_changes_timeout_classification_and_spatial() {
    // Se il secondo profilo divergesse solo sull'identita, proverebbe il
    // transito dell'attribuzione e nient'altro: le altre decisioni
    // resterebbero indistinguibili da una tabella ereditata.

    // Timeout: nome e unita insieme, che e la forma della divergenza.
    let mysql = MYSQL_PROFILE.statement_timeout_statement(5_000);
    let second = SECOND_PRODUCT_PROFILE.statement_timeout_statement(5_000);
    assert_ne!(mysql, second);
    assert!(mysql.contains("5000"), "{mysql}");
    assert!(second.ends_with(" 5.000"), "{second}");
    // I due casi che una conversione approssimata sbaglierebbe in
    // direzioni opposte: la divisione intera porterebbe 200 ms a zero,
    // cioe a "nessun limite"; l'arrotondamento per eccesso li porterebbe
    // a un secondo, cioe a un timeout cinque volte piu lasco di quello
    // chiesto. La conversione esatta non perde nulla.
    assert!(
        SECOND_PRODUCT_PROFILE
            .statement_timeout_statement(200)
            .ends_with(" 0.200"),
        "sotto il secondo la conversione deve restare esatta"
    );
    assert!(
        SECOND_PRODUCT_PROFILE
            .statement_timeout_statement(1)
            .ends_with(" 0.001"),
        "un millisecondo non puo sparire"
    );

    // Classificazione: lo stesso codice, due significati.
    assert_eq!(
        MYSQL_PROFILE.classify_server_code(1_054).category,
        ErrorCategory::Schema
    );
    assert_eq!(
        SECOND_PRODUCT_PROFILE.classify_server_code(1_054).category,
        ErrorCategory::Unsupported
    );
    // Anche l'effetto remoto appartiene alla classificazione del profilo.
    assert_eq!(
        MYSQL_PROFILE.classify_server_code(1_213).remote_effect,
        Some(RemoteEffect::RolledBack)
    );
    assert_eq!(
        MYSQL_PROFILE.classify_server_code(1_062).remote_effect,
        None
    );

    // Spatial: una sola origine, e il profilo che non ha la prova la nega
    // in entrambi i posti.
    assert!(MYSQL_PROFILE.write_spatial_is_qualified());
    assert!(!SECOND_PRODUCT_PROFILE.write_spatial_is_qualified());
    for profile in [
        &MYSQL_PROFILE as &dyn ProductProfile,
        &SECOND_PRODUCT_PROFILE as &dyn ProductProfile,
    ] {
        assert_eq!(
            profile.capabilities("9.7.2".to_owned()).spatial.write_wkb,
            profile.write_spatial_is_qualified(),
            "capability e decisione devono avere una sola origine"
        );
    }
}

/// Ogni modulo di produzione del crate, con il proprio sorgente.
///
/// La lista e confrontata con le dichiarazioni `mod` di `lib.rs`, quindi
/// le guardie strutturali non possono omettere nuovi moduli in silenzio.
const GUARDED_MODULES: &[(&str, &str)] = &[
    ("arrow.rs", include_str!("arrow.rs")),
    ("catalog.rs", include_str!("catalog.rs")),
    ("config.rs", include_str!("config.rs")),
    ("error.rs", include_str!("error.rs")),
    ("parameter.rs", include_str!("parameter.rs")),
    ("pool.rs", include_str!("pool.rs")),
    ("profile.rs", include_str!("profile.rs")),
    ("provider.rs", include_str!("provider.rs")),
    ("query.rs", include_str!("query.rs")),
    ("read.rs", include_str!("read.rs")),
    ("row_diagnostics.rs", include_str!("row_diagnostics.rs")),
    ("session.rs", include_str!("session.rs")),
    ("transaction.rs", include_str!("transaction.rs")),
    ("types.rs", include_str!("types.rs")),
    ("write.rs", include_str!("write.rs")),
];

#[test]
fn no_literal_carries_a_collapsed_continuation() {
    // Una riga spezzata in un literal Rust si scrive con `\` a fine riga:
    // il compilatore toglie l'a capo **e** l'indentazione, e il messaggio
    // torna una frase sola. Se quel `\` si perde — succede scrivendo il
    // codice con uno strumento che lo mangia — la stringa resta valida e
    // compila, ma porta dentro l'indentazione: quello che il chiamante
    // legge diventa "la transazione                 non e cominciata".
    //
    // Non e un difetto di stile. Un messaggio d'errore e cio che qualcuno
    // legge alle tre di notte, e una riga sfondata da venti spazi lo
    // rende illeggibile proprio dove serve. Il compilatore non se ne
    // accorge, i test sul contenuto nemmeno — cercano sottostringhe corte
    // — quindi questa forma viene presidiata da una guardia automatica.
    //
    // Un `\n` esplicito seguito da indentazione e un'altra cosa: e SQL
    // scritto su piu righe, dove l'a capo appartiene alla stringa. Quello
    // resta legittimo, ed e l'unica eccezione.
    let sources: [(&str, &str); 9] = [
        ("arrow.rs", include_str!("arrow.rs")),
        ("catalog.rs", include_str!("catalog.rs")),
        ("evidence.rs", include_str!("evidence.rs")),
        ("live_tests.rs", include_str!("live_tests.rs")),
        ("mariadb_evidence.rs", include_str!("mariadb_evidence.rs")),
        ("profile.rs", include_str!("profile.rs")),
        ("session_evidence.rs", include_str!("session_evidence.rs")),
        ("transaction.rs", include_str!("transaction.rs")),
        ("types.rs", include_str!("types.rs")),
    ];
    let run = " ".repeat(4);
    for (module, source) in sources {
        for (at, line) in source.lines().enumerate() {
            let trimmed = line.trim_start();
            // Le righe di commento sono prosa: l'allineamento di una
            // tabella in un commento non finisce in nessun messaggio.
            if trimmed.starts_with("//") {
                continue;
            }
            let Some(quoted) = trimmed.split_once('"').map(|(_, rest)| rest) else {
                continue;
            };
            let Some(offset) = quoted.find(run.as_str()) else {
                continue;
            };
            if quoted[..offset].ends_with(r"\n") {
                continue;
            }
            assert!(
                !quoted[..offset]
                    .chars()
                    .next_back()
                    .is_some_and(
                        |character| character.is_alphanumeric() || ".,:;)'".contains(character)
                    ),
                "{module}:{}: literal con una continuazione persa — {trimmed}",
                at + 1
            );
        }
    }
}

#[test]
fn only_the_mariadb_provider_selects_the_mariadb_profile() {
    // La selezione del profilo deve restare una sola e dichiarata. Un
    // secondo punto che lo scegliesse sarebbe una selezione che nessuno
    // ha deciso, ed e
    // esattamente cio che ADR 0014 esclude quando dice «nessuna selezione
    // automatica».
    let declaration = format!("impl PublishedProfile for {}Provider", "Mariadb");
    for (module, source) in GUARDED_MODULES {
        let production = source;
        match *module {
            // Dove il profilo e definito: cercarlo qui vorrebbe dire
            // vietarne l'esistenza.
            "profile.rs" => {}
            // Dove e dichiarato, una volta sola e dentro l'`impl` che lo
            // pubblica. Il conteggio e la meta che conta: senza, una
            // seconda occorrenza altrove nel file passerebbe.
            "provider.rs" => {
                assert_eq!(
                    production.matches("MARIADB_PROFILE").count(),
                    1,
                    "provider.rs nomina il profilo MariaDB piu di una volta: \
                         la selezione non e piu una sola"
                );
                let header = production
                    .find(declaration.as_str())
                    .expect("provider.rs non dichiara il profilo di MariadbProvider");
                let selection = production
                    .find("MARIADB_PROFILE")
                    .expect("occorrenza gia contata");
                // Dentro la dichiarazione, non da qualche parte dopo: fra
                // l'intestazione e la costante ci sono poche decine di
                // caratteri, e una selezione piu in la nel file sarebbe
                // un secondo punto travestito da primo.
                assert!(
                    selection > header && selection - header < 200,
                    "provider.rs nomina il profilo MariaDB fuori dalla \
                         dichiarazione che lo pubblica"
                );
            }
            _ => {
                for needle in ["MARIADB_PROFILE", "MariadbProfile"] {
                    assert!(
                        !production.contains(needle),
                        "{module} seleziona il profilo MariaDB, che solo \
                             MariadbProvider deve dichiarare"
                    );
                }
            }
        }
    }
}

#[test]
fn the_guarded_module_list_covers_every_production_module() {
    // I moduli di solo test non contano: non esistono nel binario che il
    // consumatore riceve, ed e quello che le guardie presidiano.
    //
    // Il riconoscimento accetta qualunque visibilita — `mod x;`,
    // `pub(crate) mod x;`, `pub mod x;` — cosi nessuna forma di visibilita
    // resta esclusa dalla guardia.
    let source = include_str!("lib.rs");
    let lines: Vec<&str> = source.lines().collect();
    let mut declared = Vec::new();
    for (at, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        let Some(rest) = trimmed
            .strip_prefix("pub(crate) mod ")
            .or_else(|| trimmed.strip_prefix("pub mod "))
            .or_else(|| trimmed.strip_prefix("mod "))
        else {
            continue;
        };
        // Un modulo inline (`mod x {`) non ha un file da ispezionare, e
        // uno annidato sfuggirebbe comunque a questa lettura: entrambi
        // vanno vietati, non ignorati.
        assert!(
            rest.ends_with(';'),
            "lib.rs dichiara un modulo inline: le guardie leggono file, non blocchi — {trimmed}"
        );
        let name = rest.trim_end_matches(';');
        assert!(
            !name.contains("::"),
            "lib.rs dichiara un modulo annidato: {name}"
        );
        if lines[..at]
            .iter()
            .rev()
            .take_while(|line| line.trim().starts_with("#["))
            .any(|line| line.trim() == "#[cfg(test)]")
        {
            continue;
        }
        declared.push(format!("{name}.rs"));
    }
    assert!(declared.len() >= 15, "moduli dichiarati: {declared:?}");
    for module in &declared {
        assert!(
            GUARDED_MODULES.iter().any(|(name, _)| name == module),
            "{module} e dichiarato in lib.rs ma nessuna guardia lo ispeziona"
        );
    }
    // E nessun modulo dichiarato altrove: i sorgenti del crate sono i
    // file di `src`, e un file non dichiarato non compila comunque, ma
    // uno dichiarato in un sottomodulo si.
    for (module, source) in GUARDED_MODULES {
        let production = source;
        // Qualunque dichiarazione `mod`, non solo la forma nuda seguita
        // da graffa. Le guardie leggono file: un modulo che non ha
        // un file proprio, o che non e dichiarato in `lib.rs`, resta
        // fuori da ogni ispezione senza che nulla lo segnali.
        let lines: Vec<&str> = production.lines().collect();
        for (at, line) in lines.iter().enumerate() {
            let trimmed = line.trim();
            // Il riconoscimento non elenca piu le visibilita: `pub(self)`
            // e Rust valido e mancava, e un elenco di prefissi si elude
            // con uno spazio in piu fra i token. Si guarda la prima
            // parola dopo l'eventuale visibilita, qualunque essa sia.
            let rest = trimmed.strip_prefix("pub").map_or(trimmed, |after| {
                let after = after.trim_start();
                after.strip_prefix('(').map_or(after, |scoped| {
                    scoped
                        .find(')')
                        .map_or(after, |at| scoped[at + 1..].trim_start())
                })
            });
            let is_module_declaration = rest
                .strip_prefix("mod")
                .is_some_and(|after| after.starts_with(char::is_whitespace));
            let is_test_module = lines[..at]
                .iter()
                .rev()
                .take_while(|line| line.trim().starts_with("#["))
                .any(|line| line.trim() == "#[cfg(test)]");
            assert!(
                !is_module_declaration || is_test_module,
                "{module}:{} dichiara un modulo fuori da lib.rs: {trimmed}",
                at + 1
            );
        }
    }
}

#[test]
fn the_pool_keeps_naming_the_product_when_it_rebuilds_the_options() {
    // Il provider valida in costruzione, il pool ricostruisce le opzioni
    // al primo checkout: fra i due momenti la configurazione puo essere
    // diventata invalida, e l'errore che ne esce e il primo che il
    // consumatore vede. Passava per MySQL perche nessun test guardava il
    // testo, e il percorso non e raggiungibile dal costruttore.
    let error =
        crate::MysqlPool::new_with_profile(&second_product_config(), 0, &SECOND_PRODUCT_PROFILE)
            .expect_err("pool a capacita zero");
    assert_names_the_second_product(&error, "pool a capacita zero");

    // Una CA che non esiste: la validazione che il pool rifa al primo uso.
    // CA in memoria vuota invece di un percorso che si presume assente:
    // il test non deve dipendere da cosa esiste sul filesystem di chi lo
    // esegue, ne dai permessi con cui gira.
    let unreadable = second_product_config().with_private_ca_certificate_pem(Vec::new());
    let error = crate::MysqlPool::new_with_profile(&unreadable, 2, &SECOND_PRODUCT_PROFILE)
        .expect_err("CA vuota");
    assert_names_the_second_product(&error, "CA in memoria vuota");
}

#[test]
fn the_shared_paths_name_the_product_in_their_messages() {
    // I testi passano da costruttori verificabili, cosi la personalizzazione
    // per prodotto non dipende da rami non raggiungibili nei test.
    assert_names_the_second_product(
        &crate::transaction::query_timeout_error(&SECOND_PRODUCT_PROFILE),
        "timeout della query",
    );
    assert_names_the_second_product(
        &crate::transaction::conditional_update_mismatch(&SECOND_PRODUCT_PROFILE, 1, 0),
        "update condizionale",
    );
    assert_names_the_second_product(
        &crate::session::state_error(ErrorPhase::Write, &SECOND_PRODUCT_PROFILE),
        "sessione non riusabile",
    );

    // E gli stessi tre su MySQL restano quelli di prima.
    for (what, error) in [
        (
            "timeout",
            crate::transaction::query_timeout_error(&MYSQL_PROFILE),
        ),
        (
            "update condizionale",
            crate::transaction::conditional_update_mismatch(&MYSQL_PROFILE, 1, 0),
        ),
        (
            "sessione",
            crate::session::state_error(ErrorPhase::Write, &MYSQL_PROFILE),
        ),
    ] {
        assert!(error.message.contains("MySQL"), "{what}: {}", error.message);
        assert_eq!(error.provider, Some(ProviderKind::Mysql), "{what}");
    }
}

#[tokio::test]
async fn the_pure_paths_no_longer_contradict_the_attribution() {
    // I percorsi senza un profilo in portata sono il binding dei parametri,
    // l'AST non qualificato e il piano di scrittura invalido. Nessuno dei
    // tre ha un profilo in portata, e il bordo puo ristampare
    // l'attribuzione ma non riscrivere una frase — quindi la frase non
    // deve piu nominare un prodotto.
    let provider =
        crate::MysqlProvider::with_profile(second_product_config(), 2, &SECOND_PRODUCT_PROFILE)
            .expect("provider sul secondo profilo");
    let secret = SecretString::new("unique-secret");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let cancellation = CancellationToken::new();

    // 1. Binding: un parametro dichiarato e mai fornito.
    let outcome = provider
        .query(
            &secret,
            &parameterized_query(),
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await;
    let Err(error) = outcome else {
        panic!("parametro mancante: doveva fallire");
    };
    assert_names_no_product(&error, "binding invalido");

    // 2. AST: una forma che il dialetto non qualifica.
    let mut unsupported = oversized_identifier_query();
    unsupported.common_table_expressions =
        vec![plenora_database_core::query::CommonTableExpression {
            name: "recenti".to_owned(),
            recursive: false,
            query: Box::new(oversized_identifier_query()),
        }];
    let outcome = provider
        .query(
            &secret,
            &unsupported,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await;
    let Err(error) = outcome else {
        panic!("CTE non qualificata: doveva fallire");
    };
    assert_names_no_product(&error, "AST non supportato");

    // 3. Piano di scrittura. Lo schema vuoto non basta: quell'errore
    //    nasce nel ramo che il profilo ce l'ha, e non attraversa le
    //    validazioni neutralizzate. Servono errori che nascono dentro
    //    quelle validazioni.
    let error = crate::write::MysqlWritePlan::compile_with_profile(
        &std::sync::Arc::new(Schema::empty()),
        &append_to_warehouse(),
        "warehouse",
        &SECOND_PRODUCT_PROFILE,
    )
    .expect_err("schema Arrow vuoto");
    // Questo ramo il profilo ce l'ha, quindi appartiene alla prima
    // categoria: nomina il prodotto invece di tacerlo.
    assert_names_the_second_product(&error, "piano write, ramo product-aware");

    // 3a. `TruncateInsert`: modalita non qualificata dal dialetto.
    let mut truncate = append_to_warehouse();
    truncate.mode = WriteMode::TruncateInsert;
    let error = crate::write::MysqlWritePlan::compile_with_profile(
        &schema_with(vec![Field::new("id", DataType::Int64, false)]),
        &truncate,
        "warehouse",
        &SECOND_PRODUCT_PROFILE,
    )
    .expect_err("TruncateInsert non qualificata");
    assert_names_no_product(&error, "write TruncateInsert");

    // 3b. Tipo Arrow che il mapping non qualifica.
    let error = crate::write::MysqlWritePlan::compile_with_profile(
        &schema_with(vec![Field::new(
            "durata",
            DataType::Duration(plenora_database_core::arrow::schema::TimeUnit::Second),
            false,
        )]),
        &append_to_warehouse(),
        "warehouse",
        &SECOND_PRODUCT_PROFILE,
    )
    .expect_err("tipo Arrow non qualificato");
    assert_names_no_product(&error, "write tipo non qualificato");

    // 3c. Chiave primaria su un tipo che il motore rifiuta in chiave.
    let mut create = append_to_warehouse();
    create.mode = WriteMode::Create;
    create.keys = vec!["etichetta".to_owned()];
    let error = crate::write::MysqlWritePlan::compile_with_profile(
        &schema_with(vec![Field::new("etichetta", DataType::Utf8, false)]),
        &create,
        "warehouse",
        &SECOND_PRODUCT_PROFILE,
    )
    .expect_err("chiave primaria su Utf8");
    assert_names_no_product(&error, "write chiave primaria");
    // La punteggiatura sospesa si vede meccanicamente, il soggetto
    // perduto no: togliendo il nome del prodotto questa causa era
    // diventata "diventa TEXT e rifiuta TEXT", senza piu dire chi
    // rifiuta. Chi rifiuta va nominato.
    assert!(
        error.message.contains("il motore"),
        "il rifiuto deve dire chi rifiuta — {}",
        error.message
    );
}

// I riferimenti su cui ADR 0014 ha misurato, con le stringhe che i
// server hanno davvero esposto. Non sono esempi: sono le righe `probe.
// version` e `probe.version_comment` di `docs/mariadb/EVIDENCE.md`, ed e
// su queste che il riconoscimento deve partizionare.
const MEASURED_SERVERS: &[(&str, &str, bool)] = &[
    ("9.7.2", "MySQL Community Server - GPL", false),
    (
        "12.3.2-MariaDB-ubu2404",
        "mariadb.org binary distribution",
        true,
    ),
    (
        "11.8.8-MariaDB-ubu2404",
        "mariadb.org binary distribution",
        true,
    ),
];

#[test]
fn the_two_profiles_partition_the_servers_that_were_measured() {
    // Il riconoscimento e una partizione, non due filtri indipendenti:
    // ogni server misurato e accettato da uno solo dei due profili. Con
    // due letture separate delle stesse stringhe si potrebbe arrivare a
    // un server rifiutato da entrambi — nessun provider lo servirebbe — o
    // accettato da entrambi, che e il caso peggiore perche la scelta
    // diventerebbe l'ordine in cui qualcuno li prova.
    for (version, comment, is_mariadb) in MEASURED_SERVERS {
        let by_mysql = MYSQL_PROFILE.foreign_product_rejection(version, comment);
        let by_mariadb = MARIADB_PROFILE.foreign_product_rejection(version, comment);
        assert_ne!(
            by_mysql.is_some(),
            by_mariadb.is_some(),
            "{version} / {comment}: i due profili devono dare esiti opposti"
        );
        let (rejection, expected_kind) = if *is_mariadb {
            (
                by_mysql.expect("MySQL rifiuta MariaDB"),
                ProviderKind::Mysql,
            )
        } else {
            (
                by_mariadb.expect("MariaDB rifiuta cio che non lo e"),
                ProviderKind::Mariadb,
            )
        };
        // Un rifiuto vale quanto la sua attribuzione: senza, chi lo legge
        // non sa quale dei due profili ha deciso, e i due messaggi
        // parlano di prodotti diversi.
        assert_eq!(rejection.category, ErrorCategory::Unsupported);
        assert_eq!(rejection.phase, ErrorPhase::Probe);
        assert_eq!(rejection.remote_effect, RemoteEffect::None);
        assert_eq!(rejection.provider, Some(expected_kind));
        assert!(
            rejection.message.contains(version) && rejection.message.contains(comment),
            "il rifiuto non riporta cio che ha letto: {}",
            rejection.message
        );
    }
}

#[test]
fn the_version_is_a_second_question_after_the_product() {
    // Riconoscere il prodotto e qualificarne la versione sono due
    // domande, e la prima non risponde alla seconda: `contains("mariadb")`
    // e vero anche per una major che nessuno ha mai acceso, e sulla quale
    // tutto cio che il profilo afferma — quali colonne di catalogo
    // esistono, con quale codice arriva il timeout — non e stato misurato.
    //
    // La qualifica e per serie minor, che e la granularita con cui il
    // repository dichiara i riferimenti: `10.11` LTS, `11.8` LTS e `12.3`,
    // fissate per digest e aggiornate di patch in patch.
    //
    // L'elenco qualificato deriva dallo stesso catalogo dei riferimenti:
    // capability e prova devono muoversi insieme.
    for qualified in [
        "10.11.19-MariaDB-ubu2204",
        "11.8.8-MariaDB-ubu2404",
        "12.3.2-MariaDB-ubu2404",
        "10.11.0",
        "11.8.0",
        "12.3.19-MariaDB",
    ] {
        assert!(
            super::unqualified_version_rejection(&MARIADB_PROFILE, qualified).is_none(),
            "{qualified} e una versione misurata"
        );
    }
    for unqualified in [
        // Una LTS piu vecchia di quelle misurate: 10.6 e supportata fino
        // al 2026 e nessuno l'ha accesa.
        "10.6.21-MariaDB",
        "13.0.0-MariaDB",
        "12.4.0-MariaDB",
        "",
        "MariaDB",
    ] {
        let rejection = super::unqualified_version_rejection(&MARIADB_PROFILE, unqualified)
            .unwrap_or_else(|| panic!("{unqualified} non e fra le versioni misurate"));
        assert_eq!(rejection.category, ErrorCategory::Unsupported);
        assert_eq!(rejection.phase, ErrorPhase::Probe);
        assert_eq!(rejection.remote_effect, RemoteEffect::None);
        assert_eq!(rejection.provider, Some(ProviderKind::Mariadb));
        // Il messaggio deve dire cosa e successo davvero: non "questo
        // server non va", ma "su questo server non e stata fatta nessuna
        // prova". Sono due affermazioni diverse, e solo la seconda e vera.
        assert!(
            rejection.message.contains("non misurata")
                && rejection.message.contains("11.8")
                && rejection.message.contains("12.3"),
            "il rifiuto non dice cosa manca: {}",
            rejection.message
        );
    }

    // MySQL non dichiara un elenco, e qui non rifiuta nulla: la matrice
    // qualifica 9.7, 8.4 e 8.0, ma il provider non ha mai rifiutato le
    // altre, e trasformare quella matrice in un rifiuto e una modifica al
    // comportamento di un provider qualificato — non un effetto collaterale
    // dell'aggiunta di un secondo profilo.
    assert!(MYSQL_PROFILE.qualified_versions().is_none());
    for version in ["9.7.2", "8.4.11", "8.0.46", "9.8.0", "sconosciuta"] {
        assert!(
            super::unqualified_version_rejection(&MYSQL_PROFILE, version).is_none(),
            "{version}: il profilo MySQL non dichiara un limite di versione"
        );
    }

    // E le due domande restano separate: una MariaDB 10.11 e riconosciuta
    // come MariaDB da entrambi i profili — il primo la rifiuta perche non
    // e sua, il secondo la accetta come prodotto — e viene fermata dalla
    // qualifica, non dal riconoscimento.
    assert!(MYSQL_PROFILE
        .foreign_product_rejection("10.11.5-MariaDB", "mariadb.org binary distribution")
        .is_some());
    assert!(MARIADB_PROFILE
        .foreign_product_rejection("10.11.5-MariaDB", "mariadb.org binary distribution")
        .is_none());
}

#[test]
fn the_probe_asks_the_version_question_too() {
    // Il gate vive nel profilo, ma serve a qualcosa solo se il percorso
    // che apre una connessione lo attraversa. Senza questa riga il
    // profilo dichiarerebbe un elenco di versioni che nessuno consulta,
    // ed e la forma di fail-closed che non chiude niente.
    let production = include_str!("catalog.rs");
    assert!(
        production.contains("unqualified_version_rejection"),
        "la probe non verifica la qualifica della versione"
    );

    // E la verifica sta **fuori** dal bypass di test. Dentro, l'unico
    // percorso che accende il bypass — la misura di evidenza — sarebbe
    // anche l'unico a non attraversarla mai: il gate esisterebbe, e la
    // corsa che deve dimostrarlo lo salterebbe.
    let opening = "if !mariadb_rejection_bypassed() {";
    let at = production
        .find(opening)
        .expect("il bypass vive nella probe");
    let mut depth = 0_i32;
    let mut end = at;
    for (offset, character) in production[at..].char_indices() {
        match character {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = at + offset;
                    break;
                }
            }
            _ => {}
        }
    }
    assert!(depth == 0 && end > at, "blocco del bypass non delimitato");
    assert!(
        !production[at..=end].contains("unqualified_version_rejection"),
        "la qualifica della versione e dentro il bypass: la misura non la attraversa"
    );
}

#[test]
fn the_mariadb_timeout_diverges_in_name_and_in_unit() {
    // Le due meta della divergenza misurata: `MAX_EXECUTION_TIME` non
    // esiste su MariaDB (1193), e cio che la sostituisce non prende la
    // stessa unita. Un profilo che copiasse solo il nome emetterebbe
    // millisecondi come se fossero secondi, cioe un timeout mille volte
    // piu largo di quello chiesto.
    let variable = format!("MAX_EXECUTION{}TIME", "_");
    for milliseconds in [1_u64, 200, 999, 1_000, 1_500, 5_000, 60_000] {
        let mysql = MYSQL_PROFILE.statement_timeout_statement(milliseconds);
        let mariadb = MARIADB_PROFILE.statement_timeout_statement(milliseconds);
        assert_ne!(mysql, mariadb, "{milliseconds} ms");
        assert!(mysql.contains(variable.as_str()));
        assert!(!mariadb.contains(variable.as_str()));
        assert!(mariadb.contains("max_statement_time"));

        // La conversione si verifica rileggendola: secondi e millesimi
        // devono ricomporre esattamente il valore chiesto. Un
        // arrotondamento — in qualunque verso — rompe questa uguaglianza,
        // ed e cio che distingue una conversione da un'approssimazione.
        let value = mariadb
            .rsplit_once(" = ")
            .expect("lo statement porta un valore")
            .1;
        let (seconds, thousandths) = value.split_once('.').expect("secondi frazionari");
        assert_eq!(thousandths.len(), 3, "i millesimi restano tre cifre");
        let recomposed = seconds.parse::<u64>().expect("secondi") * 1_000
            + thousandths.parse::<u64>().expect("millesimi");
        assert_eq!(recomposed, milliseconds, "{mariadb}");
    }
    // Il caso che rende visibile l'arrotondamento: 200 ms non diventano
    // un secondo. Se lo diventassero, il timeout si allungherebbe da solo
    // proprio dove qualcuno lo stava stringendo.
    assert_eq!(
        MARIADB_PROFILE.statement_timeout_statement(200),
        "SET SESSION max_statement_time = 0.200"
    );
}

/// Le differenze **ammesse** fra le due query del catalogo, e la misura da
/// cui ciascuna discende.
///
/// La guardia sotto sostituisce questi frammenti nella query di `MySQL` e
/// pretende che ne esca, carattere per carattere, quella di `MariaDB`. Ogni
/// riga qui e percio una divergenza dichiarata: chi ne aggiunge una senza
/// passare da questa tabella fa fallire il confronto, ed e l'unico modo
/// perche il catalogo non si sdoppi in silenzio.
const DECLARED_CATALOG_DIVERGENCES: &[(&str, &str)] = &[
    // `SRS_ID` non esiste su MariaDB: 1054, misurato su entrambi i
    // riferimenti. La colonna si dichiara nulla, non si omette.
    ("SRS_ID AS srs_id", "NULL AS srs_id"),
    // `GENERATION_EXPRESSION` esiste, ma su MariaDB e **NULL** per le
    // colonne non generate, dove MySQL manda la stringa vuota. Il lettore
    // pretende una stringa, e "nessuna espressione" e la stringa vuota su
    // entrambi: la differenza e nella rappresentazione, non nel fatto.
    //
    // La sonda `provider.profile_describe_object` verifica questa
    // normalizzazione sui riferimenti live; la compilazione non basta.
    (
        "GENERATION_EXPRESSION AS generation_expression",
        "COALESCE(GENERATION_EXPRESSION, '') AS generation_expression",
    ),
    // `EXPRESSION` non esiste su MariaDB: 1054, misurato su entrambi.
    ("EXPRESSION AS expression", "NULL AS expression"),
];

#[test]
fn the_mariadb_catalog_differs_only_where_the_measure_says_so() {
    // Le due query divergono per le divergenze dichiarate, e per nient'altro.
    // Scritta cosi, la guardia regge anche le modifiche future: chi aggiunge
    // un filtro o una colonna a una sola delle due la fa fallire.
    for (mysql, mariadb) in [
        (
            MYSQL_PROFILE.object_columns_query(),
            MARIADB_PROFILE.object_columns_query(),
        ),
        (
            MYSQL_PROFILE.object_indexes_query(),
            MARIADB_PROFILE.object_indexes_query(),
        ),
    ] {
        let mut translated = mysql.to_owned();
        for (from, to) in DECLARED_CATALOG_DIVERGENCES {
            translated = translated.replace(from, to);
        }
        assert_eq!(translated, mariadb);
        // E la divergenza c'e davvero: senza questa riga le due
        // asserzioni sopra passerebbero anche con due query identiche.
        assert_ne!(mysql, mariadb);
    }
    // Le colonne che non esistono non compaiono da nessuna parte nelle
    // query di MariaDB, nemmeno in un filtro o in un ORDER BY.
    assert!(!MARIADB_PROFILE.object_columns_query().contains("SRS_ID AS"));
    assert!(!MARIADB_PROFILE
        .object_indexes_query()
        .contains("EXPRESSION AS"));
}

#[test]
fn the_catalog_queries_that_coincide_are_written_once() {
    // Dove l'evidenza non ha visto divergenze, il codice non ne inventa:
    // le tre query restano una costante sola.
    //
    // La guardia guarda la **sorgente**, non i valori, e non per gusto:
    // due `&'static str` con lo stesso contenuto sono spesso lo stesso
    // puntatore, perche il compilatore unifica i literal uguali. Un
    // confronto fra i valori — o fra gli indirizzi — passerebbe quindi
    // anche su due copie, cioe proprio nel caso da cui la costante
    // difende: la modifica fatta da una parte sola.
    let production = include_str!("profile.rs");
    for (label, fragment, expected) in [
        ("schemi", "SELECT SCHEMA_NAME AS schema_name", 1),
        // Due volte: la lista di uno schema e il singolo oggetto sono due
        // domande diverse con lo stesso `SELECT`, e ciascuna e scritta una
        // volta sola.
        ("oggetti", "SELECT TABLE_SCHEMA AS table_schema", 2),
    ] {
        assert_eq!(
            production.matches(fragment).count(),
            expected,
            "la query {label} non e piu scritta una volta per profilo condiviso"
        );
    }
    // E cio che i due profili restituiscono e davvero la stessa cosa.
    assert_eq!(
        MYSQL_PROFILE.schemas_query(),
        MARIADB_PROFILE.schemas_query()
    );
    assert_eq!(
        MYSQL_PROFILE.objects_query(),
        MARIADB_PROFILE.objects_query()
    );
    assert_eq!(MYSQL_PROFILE.object_query(), MARIADB_PROFILE.object_query());
}

#[test]
fn the_two_profiles_publish_distinct_metadata_namespaces() {
    // I metadata sono contratto pubblico: dicono al consumer cosa fosse la
    // colonna sul server. Con un namespace solo, un batch letto da MariaDB
    // arriverebbe annotato `plenora.mysql.*`, e chi lo legge dovrebbe
    // dedurre da un metadato che non lo dice quale tabella di tipi
    // applicare — mentre le due divergono davvero, `json` contro `text`
    // dalla stessa DDL.
    let mysql = MYSQL_PROFILE.metadata_keys();
    let mariadb = MARIADB_PROFILE.metadata_keys();
    assert_ne!(mysql.native_type, mariadb.native_type);
    assert_ne!(mysql.native_declaration, mariadb.native_declaration);
    assert_ne!(mysql.collation, mariadb.collation);
    for (profile, prefix) in [
        (&MYSQL_PROFILE as &dyn ProductProfile, "plenora.mysql."),
        (&MARIADB_PROFILE, "plenora.mariadb."),
    ] {
        let keys = profile.metadata_keys();
        for key in [keys.native_type, keys.native_declaration, keys.collation] {
            assert!(
                key.starts_with(prefix),
                "{}: la chiave {key} non e nel namespace del prodotto",
                profile.product()
            );
        }
    }

    // E la scelta arriva fino allo schema: dalla stessa colonna escono due
    // annotazioni con lo stesso valore e chiavi diverse.
    let spec = crate::MysqlColumnSpec {
        name: "document".to_owned(),
        native_type: "text".to_owned(),
        native_declaration: String::new(),
        nullable: true,
        collation: None,
        kind: MysqlColumnKind::Utf8,
        spatial_srid: None,
        spatial_srid_declared: false,
    };
    let by_mysql = spec.arrow_field_with_profile(&MYSQL_PROFILE);
    let by_mariadb = spec.arrow_field_with_profile(&MARIADB_PROFILE);
    assert_eq!(by_mysql.data_type(), by_mariadb.data_type());
    assert_eq!(
        by_mysql.metadata().get(mysql.native_type),
        by_mariadb.metadata().get(mariadb.native_type)
    );
    assert!(by_mariadb.metadata().get(mysql.native_type).is_none());
    assert!(by_mysql.metadata().get(mariadb.native_type).is_none());
    // L'API `arrow_field` conserva i metadata MySQL per compatibilità; i
    // percorsi di prodotto usano la variante che riceve il profilo.
    assert_eq!(
        spec.arrow_field().metadata().get(mysql.native_type),
        by_mysql.metadata().get(mysql.native_type)
    );
}

#[test]
fn no_production_module_writes_the_metadata_namespace_itself() {
    // Il namespace si sceglie in un posto solo. Un modulo che scrivesse
    // direttamente `protocol::MYSQL_NATIVE_TYPE` annoterebbe con MySQL
    // anche cio che ha letto da un altro prodotto, e lo farebbe in un
    // punto dove nessuno pensa di guardare: lo schema esce corretto nei
    // tipi e sbagliato nell'origine.
    for (module, source) in GUARDED_MODULES {
        if *module == "profile.rs" {
            continue;
        }
        let production = source;
        for needle in [
            "MYSQL_NATIVE_TYPE",
            "MYSQL_NATIVE_DECLARATION",
            "MYSQL_COLLATION",
            "MARIADB_NATIVE_TYPE",
        ] {
            assert!(
                !production.contains(needle),
                "{module} sceglie il namespace dei metadata invece di chiederlo al profilo"
            );
        }
    }
}

#[test]
fn the_wire_mapper_does_not_diverge_between_the_profiles() {
    // ADR 0014 ha misurato che dai metadata di `COM_STMT_PREPARE`
    // escono lo stesso `kind` e lo stesso `native_type` sui tre
    // riferimenti: a divergere e l'ingresso, non il mapper. La stessa
    // DDL `document JSON` arriva come `MYSQL_TYPE_JSON` da MySQL e come
    // `MYSQL_TYPE_BLOB` da MariaDB, dove `JSON` e un alias di `LONGTEXT`.
    for wire in [
        ColumnType::MYSQL_TYPE_JSON,
        ColumnType::MYSQL_TYPE_BLOB,
        ColumnType::MYSQL_TYPE_LONG,
        ColumnType::MYSQL_TYPE_NEWDECIMAL,
        ColumnType::MYSQL_TYPE_TIMESTAMP,
        ColumnType::MYSQL_TYPE_VAR_STRING,
    ] {
        let column = Column::new(wire)
            .with_name(b"document")
            .with_character_set(255);
        let by_mysql = MYSQL_PROFILE.wire_column_spec(&column);
        let by_mariadb = MARIADB_PROFILE.wire_column_spec(&column);
        match (by_mysql, by_mariadb) {
            (Ok(mysql), Ok(mariadb)) => {
                assert_eq!(mysql.native_type, mariadb.native_type, "{wire:?}");
                assert_eq!(mysql.kind, mariadb.kind, "{wire:?}");
                assert_eq!(mysql.nullable, mariadb.nullable, "{wire:?}");
            }
            // Anche i rifiuti coincidono: stesso verdetto, e ciascuno
            // nomina il profilo che lo ha prodotto. Un ramo che
            // pretendesse solo `Ok` lascerebbe fuori proprio i tipi che
            // il mapper non qualifica, che sono quelli su cui due copie
            // divergerebbero per prime.
            (Err(mysql), Err(mariadb)) => {
                assert_eq!(mysql.category, mariadb.category, "{wire:?}");
                assert_eq!(mysql.phase, mariadb.phase, "{wire:?}");
                assert_eq!(mysql.retry, mariadb.retry, "{wire:?}");
                assert!(
                    mysql.message.contains("MySQL") && mariadb.message.contains("MariaDB"),
                    "{wire:?}: i rifiuti non nominano chi ha rifiutato"
                );
            }
            (mysql, mariadb) => {
                panic!("{wire:?}: esiti diversi — {mysql:?} contro {mariadb:?}")
            }
        }
    }
    // La divergenza pubblicata resta quella dell'ingresso, e questo e il
    // suo test: due tipi wire diversi, due `native_type` diversi, dallo
    // stesso mapper.
    let json = Column::new(ColumnType::MYSQL_TYPE_JSON)
        .with_name(b"document")
        .with_character_set(255);
    let blob = Column::new(ColumnType::MYSQL_TYPE_BLOB)
        .with_name(b"document")
        .with_character_set(255);
    assert_eq!(
        MARIADB_PROFILE
            .wire_column_spec(&json)
            .expect("json")
            .native_type,
        "json"
    );
    assert_eq!(
        MARIADB_PROFILE
            .wire_column_spec(&blob)
            .expect("blob")
            .native_type,
        "text"
    );
}

#[test]
fn the_mapper_rejections_name_the_profile_that_refused() {
    // Il mapper e condiviso, l'attribuzione no: un rifiuto che dicesse
    // "MySQL" mentre a rifiutare e stato MariaDB manderebbe chi legge a
    // cercare sul server sbagliato.
    let unnamed = Column::new(ColumnType::MYSQL_TYPE_LONG).with_character_set(255);
    for profile in [&MYSQL_PROFILE as &dyn ProductProfile, &MARIADB_PROFILE] {
        let error = profile
            .wire_column_spec(&unnamed)
            .expect_err("una colonna senza nome si rifiuta");
        assert!(
            error.message.contains(profile.product()),
            "{}: il rifiuto non nomina chi ha rifiutato — {}",
            profile.product(),
            error.message
        );
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn the_mariadb_capabilities_open_only_where_a_probe_supports_them() {
    let published = MARIADB_PROFILE.capabilities("11.8.8-MariaDB".to_owned());
    assert_eq!(published.provider, ProviderKind::Mariadb);
    assert_eq!(published.provider_version, "11.8.8-MariaDB");

    // La lettura e aperta, e ognuna delle quattro ha una sonda che la
    // sostiene: valori, streaming, proiezione, filtro, ordinamento. Le
    // altre quattro restano chiuse perche il crate non le offre a
    // nessuno dei due prodotti — non perche siano state provate e
    // fallite.
    let reads = &published.reads;
    assert!(reads.streaming && reads.projection && reads.filter && reads.ordering);
    // `pagination` è sostenuta da `ReadOperation::row_offset`, compilato
    // dal piano di lettura e verificato dall'engine.
    assert!(reads.pagination);
    assert!(!reads.server_cursor);
    assert!(!reads.resumable);
    // E dove il crate non offre niente, i due prodotti dicono la stessa
    // cosa: una bandiera chiusa qui non e una divergenza di prodotto.
    let mysql_reads = &MYSQL_PROFILE.capabilities("9.7.2".to_owned()).reads;
    assert_eq!(reads.server_cursor, mysql_reads.server_cursor);
    assert_eq!(reads.pagination, mysql_reads.pagination);
    assert_eq!(reads.resumable, mysql_reads.resumable);

    // La scrittura procede una mode alla volta, ed e la differenza che
    // rende leggibile la tabella: non "tutto chiuso", ma "chiuso cio che
    // non e stato attraversato". Ogni modalita aperta ha sonde dedicate;
    // le altre restano chiuse.
    let writes = &published.writes;
    assert!(writes.append && writes.create);
    assert!(writes.update && writes.upsert);
    assert!(writes.replace && writes.delete_by_keys);
    // Sei mode su sette, come `MySQL`: la settima e `TruncateInsert`, e
    // resta chiusa su entrambi i profili per la stessa ragione
    // permanente. Le due righe qui sotto lo verificano insieme, perche e
    // proprio la coincidenza a dire che non e una lacuna di `MariaDB`.
    assert!(!writes.truncate_insert);
    assert!(
        !MYSQL_PROFILE
            .capabilities("9.7.2".to_owned())
            .writes
            .truncate_insert
    );
    // `bulk` coincide con quella di `MySQL` perche l'implementazione e la
    // stessa: dichiararla diversa sarebbe una divergenza inventata.
    // `array_binding` e `returning` restano chiuse su entrambi, e la
    // seconda per una ragione che sta a monte dei provider — `WriteOutcome`
    // conta righe e non le trasporta.
    let mysql_writes = &MYSQL_PROFILE.capabilities("9.7.2".to_owned()).writes;
    assert_eq!(writes.bulk, mysql_writes.bulk);
    assert!(writes.bulk);
    assert!(!writes.array_binding && !writes.returning);
    // `rollback_on_failure` e aperta: parla delle **righe** di ogni
    // scrittura che il profilo ammette, e le righe tornano indietro in
    // entrambe le mode aperte — le sonde girano con `allow_partial:
    // false` e lo misurano rileggendo da un'altra sessione.
    assert!(writes.rollback_on_failure);
    // Che il rollback non riporti indietro anche lo **schema** non lo
    // dice quel flag: lo dice `transactional_ddl`, chiuso. La tabella creata da `Create`
    // sopravvive al rollback su tutti e tre i riferimenti. Le due
    // bandiere parlano di due cose, e questa riga esiste perche restino
    // distinte.
    assert!(!published.transactions.transactional_ddl);
    // `truncate_insert` e chiusa su **entrambi** i profili, e per una
    // ragione che non e "non misurata": su questi due motori `TRUNCATE` e
    // DDL con commit implicito, quindi le righe sparirebbero prima
    // dell'INSERT e nessun rollback le riporterebbe indietro. E una
    // chiusura permanente finche quello resta vero, e va detta accanto
    // alla bandiera — non da qualche parte nel file, dove `TRUNCATE`
    // compare anche altrove.
    assert!(!published.writes.truncate_insert);
    assert!(
        !MYSQL_PROFILE
            .capabilities("9.7.2".to_owned())
            .writes
            .truncate_insert
    );
    // Solo la produzione: questo stesso test nomina la bandiera chiusa
    // per cercarla, e contando tutto il file conterebbe anche se stesso.
    let source = include_str!("profile.rs");
    let closed = "truncate_insert: false,";
    for (at, _) in source.match_indices(closed) {
        let start = source[..at].rfind("writes: WriteCapabilities").unwrap_or(0);
        assert!(
            source[start..at].contains("commit implicito"),
            "accanto alla bandiera chiusa non c'e scritto perche lo resta"
        );
    }
    assert_eq!(
        source.matches(closed).count(),
        2,
        "i due profili devono dichiararla entrambi, e chiusa"
    );
    assert!(writes.upsert && writes.replace && writes.delete_by_keys);

    // Le due bandiere della lettura spatial vanno lette insieme:
    // `geometry: true, requires_declared_crs: true` — perche la prima da
    // sola prometterebbe che una lettura semplice basti.
    let spatial = &published.spatial;
    assert!(spatial.read_wkb && spatial.geometry);
    assert!(spatial.requires_declared_crs);
    assert_eq!(
        spatial.dimensions,
        vec![plenora_database_core::geometry::Dimensions::Xy]
    );
    // La condizione non e una divergenza di prodotto: le stesse tre sonde
    // danno lo stesso esito su MySQL, dove una colonna `GEOMETRY` non
    // vincolata dalla DDL ha `SRS_ID` nullo esattamente come qui. E' la
    // riga che impedisce di leggere questa apertura come «MariaDB ha un
    // problema che MySQL non ha».
    assert_eq!(
        spatial.requires_declared_crs,
        MYSQL_PROFILE
            .capabilities("9.7.2".to_owned())
            .spatial
            .requires_declared_crs
    );
    // Cio che resta chiuso, resta chiuso: `geography` non esiste su questo
    // prodotto, e i tipi misti non sono mai stati letti.
    assert!(!spatial.geography);
    // Il profilo pubblica l'indice solo quando la stessa evidenza e
    // raggiungibile dal percorso di capability.
    assert!(spatial.spatial_index);
    assert_eq!(
        spatial.spatial_index,
        MYSQL_PROFILE
            .capabilities("9.7.2".to_owned())
            .spatial
            .spatial_index
    );
    // I tipi misti coincidono con MySQL: e la stessa colonna `GEOMETRY` che regge tipi diversi, e
    // le sonde lo misurano con lo stesso punto e lo stesso poligono.
    assert!(spatial.mixed_geometry_types);
    assert_eq!(
        spatial.mixed_geometry_types,
        MYSQL_PROFILE
            .capabilities("9.7.2".to_owned())
            .spatial
            .mixed_geometry_types
    );

    // Le funzioni verificate **non** coincidono con quelle di MySQL. La differenza e
    // `IsValid`, che la 12.3 esegue e la 11.8 LTS no — la prima divergenza
    // misurata fra le due major di questo prodotto.
    //
    // La lista pubblicata e la stessa consultata dal renderer.
    assert_eq!(
        spatial.functions,
        MARIADB_PROFILE.verified_spatial_functions().to_vec()
    );

    // Le due liste non sono piu una il sottoinsieme dell'altra, e la
    // guardia lo verifica in **entrambe** le direzioni: cercare solo cio
    // che manca a MariaDB lascerebbe passare in silenzio il giorno in cui
    // MySQL perdesse qualcosa che qui c'e.
    let mysql_functions = MYSQL_PROFILE.verified_spatial_functions();
    let only_mysql: Vec<_> = mysql_functions
        .iter()
        .filter(|function| !spatial.functions.contains(function))
        .copied()
        .collect();
    let only_mariadb: Vec<_> = spatial
        .functions
        .iter()
        .filter(|function| !mysql_functions.contains(function))
        .copied()
        .collect();
    assert_eq!(
        only_mysql,
        vec![
            plenora_database_core::query::SpatialFunction::IsValid,
            plenora_database_core::query::SpatialFunction::HausdorffDistance,
            plenora_database_core::query::SpatialFunction::FrechetDistance,
            // La quarta, e la prima che riguarda una funzione che rende
            // geometria: `raw.geometry_function_forms` ha trovato
            // `ST_Simplify` su `MySQL` e su nessuna major di `MariaDB` —
            // `4212` sulla 12.3, `1305` sulle due LTS.
            plenora_database_core::query::SpatialFunction::Simplify,
        ],
        "cio che MySQL ha e MariaDB no"
    );
    // `Relate` esiste sul server ma richiede tre argomenti, mentre il
    // contratto ne ammette anche due: non e quindi qualificabile per tutta
    // l'arieta. Le due differenze positive di MariaDB sono assenze reali
    // dall'altro prodotto: `ST_Boundary` e
    // `ST_PointOnSurface` rispondono `1305` su `MySQL` 9.7. Nella direzione
    // opposta viaggiano `ST_Transform` e `ST_SetSrid`, che su `MySQL` ci
    // sono e qui no — e non compaiono in nessuna delle due liste perche la
    // loro regola di CRS il provider non la sa propagare.
    assert_eq!(
        only_mariadb,
        vec![
            plenora_database_core::query::SpatialFunction::PointOnSurface,
            plenora_database_core::query::SpatialFunction::Boundary,
        ],
        "cio che MariaDB ha e MySQL no"
    );
    assert_eq!(
        spatial.write_wkb,
        MARIADB_PROFILE.write_spatial_is_qualified(),
        "la capability spatial e la decisione del piano devono avere una sola sorgente"
    );

    // Commit, rollback e isolamento coincidono sui
    // riferimenti. I savepoint no, e restano chiusi.
    let transactions = &published.transactions;
    assert!(transactions.single_transaction);
    // I savepoint sono implementati una volta sola per i due prodotti, e le sonde danno lo
    // stesso esito sui tre riferimenti. Il confronto sta qui perche una
    // divergenza inventata su una superficie condivisa e il difetto che
    // ADR 0010 ha nominato.
    assert!(transactions.savepoints);
    assert_eq!(
        transactions.savepoints,
        MYSQL_PROFILE
            .capabilities("9.7.2".to_owned())
            .transactions
            .savepoints
    );
    assert!(!transactions.transactional_ddl && !transactions.staged_swap);

    // I limiti non sono capability: dicono quanto il crate manda. `None`
    // si leggerebbe come "nessun limite dichiarato", che e la sola delle
    // due letture che puo far male.
    assert_eq!(
        published.limits.max_bind_parameters,
        Some(crate::MAX_BIND_PARAMETERS as u64)
    );
    assert_eq!(
        published.limits.max_batch_rows,
        Some(crate::MAX_BATCH_ROWS as u64)
    );

    // E il confronto che rende la chiusura osservabile: dove MySQL
    // dichiara qualificata la scrittura e lo spatial, MariaDB non lo fa.
    // La lettura invece ora coincide, ed e il primo punto in cui i due
    // prodotti dichiarano la stessa cosa perche entrambi l'hanno provata.
    let mysql = MYSQL_PROFILE.capabilities("9.7.2".to_owned());
    assert!(mysql.writes.append && mysql.spatial.read_wkb);
    assert!(!mysql.spatial.functions.is_empty());
    assert_eq!(mysql.reads, published.reads);
}

#[test]
fn the_shared_verdicts_are_shared_only_where_they_were_measured() {
    // Dove il codice e stato osservato su entrambi i prodotti, il verdetto
    // e lo stesso e cambia solo il nome di chi ha risposto.
    for code in MEASURED_SERVER_CODES {
        let mysql = MYSQL_PROFILE.classify_server_code(*code);
        let mariadb = MARIADB_PROFILE.classify_server_code(*code);
        assert_eq!(mysql.category, mariadb.category, "codice {code}");
        assert_eq!(mysql.retry, mariadb.retry, "codice {code}");
        assert_eq!(mysql.remote_effect, mariadb.remote_effect, "codice {code}");
        assert!(
            mysql.message.contains("MySQL") && mariadb.message.contains("MariaDB"),
            "codice {code}: i messaggi non nominano il prodotto che ha risposto"
        );
        assert_ne!(mysql.message, mariadb.message, "codice {code}");
    }

    // Dove non lo e, MariaDB non eredita. 1044 e 1049 non sono mai
    // arrivati dai riferimenti MariaDB, che restituiscono 1142 al loro
    // posto; 3024 e invece il timeout MySQL.
    //
    // La differenza non e cosmetica: su 1213 la tabella condivisa dichiara
    // `retry: Safe` e `remote_effect: RolledBack`. Un codice ereditato con
    // quelle due promesse direbbe al chiamante di rifare l'operazione, e
    // che non c'e niente da ripulire, su un motore che nessuno aveva
    // interrogato.
    for unmeasured in [1_044_u16, 1_049, 3_024] {
        let mysql = MYSQL_PROFILE.classify_server_code(unmeasured);
        let mariadb = MARIADB_PROFILE.classify_server_code(unmeasured);
        assert_ne!(
            mysql.category, mariadb.category,
            "codice {unmeasured}: MariaDB eredita una categoria che non ha misurato"
        );
        assert_eq!(mariadb.category, ErrorCategory::Execution);
        assert_eq!(mariadb.retry, RetryDisposition::Never);
        assert_eq!(mariadb.remote_effect, None);
        assert!(mariadb.message.contains("redatto"));
    }
    assert!(!MEASURED_SERVER_CODES.contains(&1_044));
    assert!(!MEASURED_SERVER_CODES.contains(&1_049));
    assert!(!MEASURED_SERVER_CODES.contains(&3_024));
}

#[test]
fn a_privilege_error_is_authorization_on_both_products() {
    // 1142 arriva ogni volta che il permesso manca su un comando o una
    // tabella, ed e il codice ricevuto **al posto** di 1044 e 1049: e la risposta piu comune del motore a una
    // richiesta che l'utente non puo fare.
    //
    // Restava fuori dalla tabella, quindi si classificava come esecuzione
    // generica: il chiamante leggeva un guasto dove c'era un permesso
    // mancante, e le due cose si risolvono in modi diversi — una si
    // ritenta, l'altra si concede. Il cambio tocca anche il provider
    // MySQL qualificato, ed e giusto che lo tocchi: la misura vale per
    // tutti e tre i riferimenti.
    for profile in [&MYSQL_PROFILE as &dyn ProductProfile, &MARIADB_PROFILE] {
        let verdict = profile.classify_server_code(1_142);
        assert_eq!(
            verdict.category,
            ErrorCategory::Authorization,
            "{}",
            profile.product()
        );
        assert_eq!(verdict.retry, RetryDisposition::Never);
        assert_eq!(verdict.remote_effect, None);
        assert!(
            verdict.message.contains("autorizzazione") && verdict.message.contains("1142"),
            "{}: {}",
            profile.product(),
            verdict.message
        );
    }
    // E vale per entrambi perche e stato misurato su entrambi: sta
    // nell'elenco dei codici osservati, non in un ramo scritto a mano nel
    // profilo che lo ha visto per primo.
    assert!(MEASURED_SERVER_CODES.contains(&1_142));
    // 1044 no: la sonda che lo cercava ha ricevuto 1142, quindi su
    // MariaDB quel codice resta non misurato e prende il verdetto
    // generico. Due codici della stessa famiglia, due stati di prova
    // diversi.
    assert!(!MEASURED_SERVER_CODES.contains(&1_044));
}

#[test]
fn the_statement_timeout_is_classified_as_a_timeout_on_both_products() {
    // Statement e classificazione devono concordare: il chiamante deve
    // distinguere un limite atteso da un guasto del server.
    let mysql = MYSQL_PROFILE.classify_server_code(3_024);
    let mariadb = MARIADB_PROFILE.classify_server_code(MARIADB_STATEMENT_TIMEOUT);
    for (product, verdict) in [("MySQL", &mysql), ("MariaDB", &mariadb)] {
        assert_eq!(verdict.category, ErrorCategory::Timeout, "{product}");
        assert_eq!(verdict.retry, RetryDisposition::Never, "{product}");
        assert!(verdict.message.contains("timeout"), "{product}");
        assert!(verdict.message.contains(product), "{product}");
    }
    // E i due numeri non si incrociano: il codice di un motore non
    // significa niente sull'altro, ed e la ragione per cui la riga vive
    // nel profilo e non nella tabella condivisa.
    assert_eq!(
        MYSQL_PROFILE
            .classify_server_code(MARIADB_STATEMENT_TIMEOUT)
            .category,
        ErrorCategory::Execution
    );
    assert_eq!(
        MARIADB_PROFILE.classify_server_code(3_024).category,
        ErrorCategory::Execution
    );
    // La conversione e il codice sono la stessa decisione vista due volte:
    // se l'istruzione tornasse a essere quella di MySQL, il codice 1969
    // non arriverebbe mai e questa riga sarebbe morta.
    assert!(MARIADB_PROFILE
        .statement_timeout_statement(200)
        .contains("max_statement_time"));
}

#[test]
fn mariadb_attributes_only_the_row_causes_it_did_not_infer() {
    // I tre codici che i due prodotti mandano dallo stesso tentativo.
    for shared in [1_048_u16, 1_062, 1_452] {
        assert_eq!(
            MYSQL_PROFILE.row_rejection_cause(shared),
            MARIADB_PROFILE.row_rejection_cause(shared),
            "codice {shared}"
        );
        assert!(MARIADB_PROFILE.row_rejection_cause(shared).is_some());
    }
    // Il CHECK diverge: lo stesso INSERT
    // che viola lo stesso vincolo arriva come 3819 da MySQL e come 4025 da
    // MariaDB. Ciascun profilo attribuisce il codice che ha ricevuto.
    assert!(MARIADB_PROFILE.row_rejection_cause(4_025).is_some());
    assert!(MYSQL_PROFILE.row_rejection_cause(3_819).is_some());
    assert!(
        MARIADB_PROFILE.row_rejection_cause(3_819).is_none(),
        "codice 3819: attribuito senza essere mai arrivato da MariaDB"
    );
}

#[test]
fn the_spatial_decisions_diverge_only_where_the_catalog_cannot_answer() {
    // Lettura: stessa funzione, stesso WKB atteso. `raw.spatial_functions`
    // ha misurato `POINT srid=4326` e 21 byte sui tre riferimenti.
    assert_eq!(
        MYSQL_PROFILE.geometry_projection("`geom`"),
        MARIADB_PROFILE.geometry_projection("`geom`")
    );
    for (srid, dimensions) in [(None, "xy"), (Some(4_326), "xy"), (None, "xyz")] {
        assert_eq!(
            MYSQL_PROFILE.geometry_output_is_unexpected(srid, dimensions),
            MARIADB_PROFILE.geometry_output_is_unexpected(srid, dimensions)
        );
    }
    for native in ["geometry", "point", "geomcollection", "blob", "json", ""] {
        assert_eq!(
            MYSQL_PROFILE.is_spatial_native_type(native),
            MARIADB_PROFILE.is_spatial_native_type(native),
            "{native}"
        );
    }

    // La divergenza sta dove il catalogo non risponde: su MariaDB
    // `srs_id` e sempre nullo, quindi la regola dell'SRID dichiarato
    // rifiuta ogni colonna geometrica. Non e "questa colonna non ha un
    // CRS": e "non c'e modo di saperlo".
    assert!(MARIADB_PROFILE.spatial_requires_declared_srid());
    assert!(MARIADB_PROFILE
        .object_columns_query()
        .contains("NULL AS srs_id"));

    // I tipi spatial scrivibili coincidono: sono nomi OGC, non una tabella
    // specifica di prodotto.
    assert!(MYSQL_PROFILE.write_spatial_is_qualified());
    assert!(MARIADB_PROFILE.write_spatial_is_qualified());
    for geometry in ["point", "linestring", "polygon", "multipoint"] {
        assert_eq!(
            MYSQL_PROFILE.writable_geometry_type(geometry),
            MARIADB_PROFILE.writable_geometry_type(geometry),
            "{geometry}"
        );
    }

    // E qui la divergenza vera della scrittura, che non e nella bandiera
    // ma nella **forma della colonna**. `MySQL` la vincola all'SRID;
    // `MariaDB` non puo — `raw.spatial_write_forms` ha misurato 1064 su
    // entrambe le major — e il CRS si sposta dentro i valori.
    assert_eq!(
        MYSQL_PROFILE.geometry_column_ddl(4_326),
        "GEOMETRY SRID 4326"
    );
    assert_eq!(MARIADB_PROFILE.geometry_column_ddl(4_326), "GEOMETRY");

    // Dove la colonna è vincolata il catalogo porta l'SRID; dove non può
    // esserlo, `None` indica l'assenza legittima del vincolo.
    assert!(MYSQL_PROFILE.geometry_target_srid_is_compatible(Some(4_326), 4_326));
    assert!(!MYSQL_PROFILE.geometry_target_srid_is_compatible(None, 4_326));
    assert!(MARIADB_PROFILE.geometry_target_srid_is_compatible(None, 4_326));
    assert!(!MARIADB_PROFILE.geometry_target_srid_is_compatible(Some(4_326), 4_326));
}

#[test]
fn mariadb_does_not_promise_index_parts_its_query_cannot_produce() {
    // Le due affermazioni devono restare vere insieme, per ogni profilo:
    // chi dichiara di pubblicare le parti funzionali deve selezionare la
    // colonna da cui si riconoscono, e chi non la seleziona non deve
    // dichiararlo. Su MariaDB quella colonna non esiste.
    for profile in [&MYSQL_PROFILE as &dyn ProductProfile, &MARIADB_PROFILE] {
        assert_eq!(
            profile.reports_functional_index_parts(),
            profile.object_indexes_query().contains("EXPRESSION AS"),
            "{}: la bandiera non corrisponde alla query che la sostiene",
            profile.product()
        );
    }
    assert!(!MARIADB_PROFILE.reports_functional_index_parts());
}

#[test]
fn the_profile_names_the_product_it_serves() {
    assert_eq!(MYSQL_PROFILE.product(), "MySQL");
    assert_eq!(MYSQL_PROFILE.kind(), ProviderKind::Mysql);
}
