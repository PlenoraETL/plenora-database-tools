use super::*;

/// Il provider che il costruttore pubblico restituisce e quello dichiarato.
///
/// La dichiarazione da sola non prova niente: `PublishedProfile::PROFILE`
/// potrebbe dire una cosa e `new` sceglierne un'altra. Qui si costruisce davvero, e
/// si confronta il prodotto servito con quello promesso.
///
/// E il lato Rust di una garanzia che `docs/STATO.md` riporta: quali
/// dichiarazioni di capability un consumatore puo raggiungere. Dedurlo
/// leggendo il sorgente da fuori non reggeva — una costante intermedia o
/// una delega bastavano a far sbagliare la deduzione.
#[test]
fn the_published_profile_is_the_one_the_constructor_selects() {
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    );
    let provider = MysqlProvider::new(config, 1).expect("provider");
    let declared = <MysqlProvider as PublishedProfile>::PROFILE;
    assert_eq!(provider.kind(), declared.kind());
    assert_eq!(provider.profile.product(), declared.product());
    assert_eq!(
        provider.profile.product(),
        crate::profile::MYSQL_PROFILE.product()
    );
}

/// Lo stesso, per il secondo provider del crate.
///
/// Il costruttore di `MariadbProvider` delega a `with_profile`, cioe passa
/// da un punto in cui il profilo e un argomento: la delega puo restare
/// giusta e l'argomento sbagliato, e il tipo non se ne accorgerebbe.
#[test]
fn the_mariadb_constructor_selects_the_mariadb_profile() {
    let config = MysqlConfig::new(
        "mariadb.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    );
    let provider = MariadbProvider::new(config, 1).expect("provider");
    let declared = <MariadbProvider as PublishedProfile>::PROFILE;
    assert_eq!(provider.kind(), declared.kind());
    assert_eq!(provider.kind(), ProviderKind::Mariadb);
    assert_eq!(
        declared.product(),
        crate::profile::MARIADB_PROFILE.product()
    );
}

/// I due provider si rifiutano a vicenda, e nessuno dei due si adatta.
///
/// E la meta di ADR 0014 che il codice deve rendere vera: «nessuna
/// selezione automatica». Un provider che accettasse l'altro motore
/// sceglierebbe per il consumatore nel punto in cui il consumatore non sta
/// guardando.
///
/// Il riconoscimento si esercita qui **senza rete**: e una funzione delle
/// due stringhe che il server manda, e chiedergliele dal vivo
/// misurerebbe anche la connessione. La corsa live che lo attraversa
/// davvero e `provider.profile_probe`, nella matrice dell'evidenza.
#[test]
fn neither_provider_adapts_to_the_other_product() {
    let mysql = ("9.7.2", "MySQL Community Server - GPL");
    let mariadb = ("11.8.8-MariaDB-ubu2404", "mariadb.org binary distribution");
    let mysql_profile = <MysqlProvider as PublishedProfile>::PROFILE;
    let mariadb_profile = <MariadbProvider as PublishedProfile>::PROFILE;

    assert!(
        mysql_profile
            .foreign_product_rejection(mysql.0, mysql.1)
            .is_none(),
        "il profilo MySQL rifiuta MySQL"
    );
    assert!(
        mariadb_profile
            .foreign_product_rejection(mariadb.0, mariadb.1)
            .is_none(),
        "il profilo MariaDB rifiuta MariaDB"
    );
    assert!(
        mysql_profile
            .foreign_product_rejection(mariadb.0, mariadb.1)
            .is_some(),
        "il provider MySQL accetta MariaDB: e una selezione automatica"
    );
    assert!(
        mariadb_profile
            .foreign_product_rejection(mysql.0, mysql.1)
            .is_some(),
        "il provider MariaDB accetta MySQL: la simmetria non regge"
    );
}
use plenora_database_core::plan::{ComparisonOperator, ObjectRef};
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::query::{
    ColumnRef, JoinKind, QueryExpression, QueryJoin, QueryLock, QueryLockStrength, QueryLockWait,
    QueryProjection, QuerySource, ScalarFunction,
};
use plenora_database_core::resource::ResourceLimits;
use std::collections::BTreeMap;

const fn assert_provider<T: Provider>() {}

fn parameterized_query() -> QueryOperation {
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
        filter: Some(QueryExpression::Compare {
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
        }),
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

#[tokio::test]
async fn query_renders_and_binds_before_reaching_the_network() {
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    );
    let provider = MysqlProvider::new(config, 1).expect("provider");
    let parameters = ParameterBag::new(BTreeMap::from([
        ("wanted".to_owned(), ParameterValue::I64(7)),
        ("unused".to_owned(), ParameterValue::I64(9)),
    ]));
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let cancellation = CancellationToken::new();
    let outcome = provider
        .query(
            &SecretString::new("unique-secret"),
            &parameterized_query(),
            &parameters,
            &budget,
            &cancellation,
        )
        .await;
    let Err(error) = outcome else {
        panic!("ParameterBag con parametro non usato accettato");
    };
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(error.phase, ErrorPhase::Prepare);
}

#[tokio::test]
async fn query_honours_cancellation_before_reaching_the_network() {
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    );
    let provider = MysqlProvider::new(config, 1).expect("provider");
    let parameters = ParameterBag::new(BTreeMap::from([(
        "wanted".to_owned(),
        ParameterValue::I64(7),
    )]));
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let outcome = provider
        .query(
            &SecretString::new("unique-secret"),
            &parameterized_query(),
            &parameters,
            &budget,
            &cancellation,
        )
        .await;
    let Err(error) = outcome else {
        panic!("token gia cancellato accettato");
    };
    assert_eq!(error.category, ErrorCategory::Cancelled);
}

#[tokio::test]
async fn query_keeps_unqualified_ast_fail_closed_before_the_network() {
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    );
    let provider = MysqlProvider::new(config, 1).expect("provider");
    let mut operation = parameterized_query();
    operation.locking = Some(QueryLock {
        strength: QueryLockStrength::Update,
        relations: Vec::new(),
        wait: QueryLockWait::NoWait,
    });
    let parameters = ParameterBag::new(BTreeMap::from([(
        "wanted".to_owned(),
        ParameterValue::I64(7),
    )]));
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let cancellation = CancellationToken::new();
    let outcome = provider
        .query(
            &SecretString::new("unique-secret"),
            &operation,
            &parameters,
            &budget,
            &cancellation,
        )
        .await;
    let Err(error) = outcome else {
        panic!("locking esplicito non qualificato accettato");
    };
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert_eq!(error.phase, ErrorPhase::Prepare);
}

/// Il bind di HAVING deve essere estratto e richiesto come ogni altro:
/// la mancanza del valore va vista prima di aprire la connessione.
#[tokio::test]
async fn query_demands_the_having_bind_before_reaching_the_network() {
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    );
    let provider = MysqlProvider::new(config, 1).expect("provider");
    let mut operation = parameterized_query();
    let events = QueryExpression::Scalar {
        function: ScalarFunction::Count,
        arguments: vec![QueryExpression::Column {
            column: ColumnRef {
                relation: None,
                field: "event_id".to_owned(),
            },
        }],
    };
    operation.projection = vec![
        QueryProjection {
            expression: QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "actor_id".to_owned(),
                },
            },
            alias: None,
        },
        QueryProjection {
            expression: events.clone(),
            alias: Some("events".to_owned()),
        },
    ];
    operation.group_by = vec![QueryExpression::Column {
        column: ColumnRef {
            relation: None,
            field: "actor_id".to_owned(),
        },
    }];
    operation.having = Some(QueryExpression::Compare {
        left: Box::new(events),
        operator: ComparisonOperator::Gte,
        right: Box::new(QueryExpression::Parameter {
            name: "floor".to_owned(),
        }),
    });
    let parameters = ParameterBag::new(BTreeMap::from([(
        "wanted".to_owned(),
        ParameterValue::I64(7),
    )]));
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let cancellation = CancellationToken::new();
    let outcome = provider
        .query(
            &SecretString::new("unique-secret"),
            &operation,
            &parameters,
            &budget,
            &cancellation,
        )
        .await;
    let Err(error) = outcome else {
        panic!("bind HAVING mancante accettato");
    };
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(error.phase, ErrorPhase::Prepare);
}

/// Il bind della clausola ON e posizionale come ogni altro e precede
/// quello di WHERE: la sua assenza deve essere vista prima di aprire la
/// connessione, non al `COM_STMT_EXECUTE`.
#[tokio::test]
async fn query_demands_the_join_on_bind_before_reaching_the_network() {
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    );
    let provider = MysqlProvider::new(config, 1).expect("provider");
    let qualified = |relation: &str, field: &str| QueryExpression::Column {
        column: ColumnRef {
            relation: Some(relation.to_owned()),
            field: field.to_owned(),
        },
    };
    let mut operation = parameterized_query();
    operation.source = Some(QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("warehouse".to_owned()),
            object: "events".to_owned(),
        },
        alias: Some("e".to_owned()),
    });
    operation.projection = vec![QueryProjection {
        expression: qualified("a", "name"),
        alias: Some("actor".to_owned()),
    }];
    operation.joins = vec![QueryJoin {
        kind: JoinKind::Inner,
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("warehouse".to_owned()),
                object: "actors".to_owned(),
            },
            alias: Some("a".to_owned()),
        }),
        derived_source: None,
        lateral: false,
        on: Some(QueryExpression::And {
            arguments: vec![
                QueryExpression::Compare {
                    left: Box::new(qualified("e", "actor_id")),
                    operator: ComparisonOperator::Eq,
                    right: Box::new(qualified("a", "actor_id")),
                },
                QueryExpression::Compare {
                    left: Box::new(qualified("a", "tier")),
                    operator: ComparisonOperator::Gte,
                    right: Box::new(QueryExpression::Parameter {
                        name: "tier".to_owned(),
                    }),
                },
            ],
        }),
    }];
    operation.filter = Some(QueryExpression::Compare {
        left: Box::new(qualified("e", "event_id")),
        operator: ComparisonOperator::Eq,
        right: Box::new(QueryExpression::Parameter {
            name: "wanted".to_owned(),
        }),
    });
    let parameters = ParameterBag::new(BTreeMap::from([(
        "wanted".to_owned(),
        ParameterValue::I64(7),
    )]));
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let cancellation = CancellationToken::new();
    let outcome = provider
        .query(
            &SecretString::new("unique-secret"),
            &operation,
            &parameters,
            &budget,
            &cancellation,
        )
        .await;
    let Err(error) = outcome else {
        panic!("bind della clausola ON mancante accettato");
    };
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(error.phase, ErrorPhase::Prepare);
}

fn append_write_operation() -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("warehouse".to_owned()),
            object: "events".to_owned(),
        },
        mode: plenora_database_core::plan::WriteMode::Append,
        mapping_policy: plenora_database_core::loss::MappingPolicy::Strict,
        transaction_profile: plenora_database_core::plan::TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

fn append_input_schema() -> SchemaRef {
    Arc::new(plenora_database_core::arrow::Schema::new_with_metadata(
        vec![plenora_database_core::arrow::Field::new(
            "id",
            plenora_database_core::arrow::DataType::Int64,
            false,
        )],
        BTreeMap::from([(
            plenora_database_core::protocol::CONTRACT_VERSION_KEY.to_owned(),
            plenora_database_core::protocol::CONTRACT_VERSION.to_owned(),
        )])
        .into_iter()
        .collect(),
    ))
}

struct EmptyBatchStream(SchemaRef);

impl BatchStream for EmptyBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.0)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a plenora_database_core::CancellationToken,
    ) -> plenora_database_core::provider::ProviderFuture<
        'a,
        Option<plenora_database_core::arrow::RecordBatch>,
    > {
        Box::pin(async { Ok(None) })
    }
}

#[test]
fn invalid_row_diagnostics_policy_is_rejected_before_transaction_setup() {
    let schema = append_input_schema();
    let policy = plenora_database_core::row_diagnostics::RowDiagnosticsPolicy::default();
    assert!(validate_diagnostic_input(&schema, 0, policy.clone()).is_err());

    let mut zero_examples = policy;
    zero_examples.examples_limit = 0;
    assert!(validate_diagnostic_input(&schema, 1, zero_examples).is_err());

    let missing_field = plenora_database_core::row_diagnostics::RowDiagnosticsPolicy {
        key_field: Some("missing_key".to_owned()),
        constraint_column: Some("missing_constraint_column".to_owned()),
        examples_limit: 10,
    };
    assert!(validate_diagnostic_input(&schema, 1, missing_field).is_err());

    let declared = plenora_database_core::row_diagnostics::RowDiagnosticsPolicy {
        key_field: Some("id".to_owned()),
        constraint_column: Some("id".to_owned()),
        examples_limit: 10,
    };
    let (_, validated) = validate_diagnostic_input(&schema, 1, declared)
        .expect("campi dichiarati presenti nello schema preparato");
    assert_eq!(validated.constraint_column.as_deref(), Some("id"));
}

fn prepared_write_for_test(budget: &ResourceBudget, input_schema: SchemaRef) -> PreparedWrite {
    PreparedWrite::new(
        append_write_operation(),
        input_schema,
        plenora_database_core::loss::LossReport {
            schema_version: 2,
            policy: plenora_database_core::loss::MappingPolicy::Strict,
            losses: Vec::new(),
        },
        budget.clone(),
        budget
            .try_lease(
                plenora_database_core::resource::ResourceKind::ConcurrentOperations,
                1,
            )
            .expect("lease operazione"),
        budget
            .try_lease(plenora_database_core::resource::ResourceKind::Columns, 1)
            .expect("lease colonne"),
    )
}

/// Il piano di scrittura e compilato prima di aprire la connessione: una
/// forma non qualificata non deve arrivare al server.
#[tokio::test]
async fn prepare_write_rejects_unqualified_operations_before_the_network() {
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    );
    let provider = MysqlProvider::new(config, 1).expect("provider");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut operation = append_write_operation();
    // `Lossy` resta Unsupported anche quando la modalita di scrittura e
    // qualificata, finche il relativo preflight non viene dimostrato.
    operation.mapping_policy = plenora_database_core::loss::MappingPolicy::Lossy;
    let outcome = provider
        .prepare_write(
            &SecretString::new("unique-secret"),
            &operation,
            append_input_schema(),
            &budget,
            &CancellationToken::new(),
        )
        .await;
    let Err(error) = outcome else {
        panic!("update MySQL non qualificato accettato");
    };
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert_eq!(error.phase, ErrorPhase::Prepare);
}

/// Un token gia cancellato chiude prima del checkout: non esiste una
/// connessione da quarantinare.
#[tokio::test]
async fn prepare_write_honours_cancellation_before_the_network() {
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    );
    let provider = MysqlProvider::new(config, 1).expect("provider");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let outcome = provider
        .prepare_write(
            &SecretString::new("unique-secret"),
            &append_write_operation(),
            append_input_schema(),
            &budget,
            &cancellation,
        )
        .await;
    let Err(error) = outcome else {
        panic!("token gia cancellato accettato");
    };
    assert_eq!(error.category, ErrorCategory::Cancelled);
}

/// Le lease di prepare non sono trasferibili: un budget diverso da quello
/// che ha prodotto il piano non puo eseguire la scrittura.
#[tokio::test]
async fn write_rejects_a_budget_that_did_not_prepare_it() {
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    );
    let provider = MysqlProvider::new(config, 1).expect("provider");
    let prepared_budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = prepared_write_for_test(&prepared_budget, append_input_schema());
    let foreign = ResourceBudget::new(ResourceLimits::default()).expect("budget estraneo");
    let error = provider
        .write(
            &SecretString::new("unique-secret"),
            prepared,
            Box::new(EmptyBatchStream(append_input_schema())),
            &foreign,
            &CancellationToken::new(),
        )
        .await
        .expect_err("budget estraneo");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
}

#[tokio::test]
async fn write_rejects_a_stream_schema_different_from_prepare() {
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    );
    let provider = MysqlProvider::new(config, 1).expect("provider");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = prepared_write_for_test(&budget, append_input_schema());
    let renamed = Arc::new(plenora_database_core::arrow::Schema::new_with_metadata(
        vec![plenora_database_core::arrow::Field::new(
            "renamed",
            plenora_database_core::arrow::DataType::Int64,
            false,
        )],
        BTreeMap::from([(
            plenora_database_core::protocol::CONTRACT_VERSION_KEY.to_owned(),
            plenora_database_core::protocol::CONTRACT_VERSION.to_owned(),
        )])
        .into_iter()
        .collect(),
    ));
    let error = provider
        .write(
            &SecretString::new("unique-secret"),
            prepared,
            Box::new(EmptyBatchStream(renamed)),
            &budget,
            &CancellationToken::new(),
        )
        .await
        .expect_err("schema stream diverso");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(error.phase, ErrorPhase::Write);
}

#[test]
fn provider_surface_is_typed_and_fail_closed() {
    assert_provider::<MysqlProvider>();
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    );
    let provider = MysqlProvider::new(config, 2).expect("provider");
    assert_eq!(provider.kind(), ProviderKind::Mysql);
    let rendered = format!("{provider:?}");
    assert!(!rendered.contains("unique-secret"));
}

#[test]
fn the_provider_is_always_built_through_a_profile() {
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("unique-secret"),
    );
    let provider = MysqlProvider::new(config, 2).expect("provider");
    assert_eq!(provider.profile.product(), "MySQL");
    assert_eq!(provider.profile.kind(), ProviderKind::Mysql);

    // La guardia strutturale: un secondo punto di costruzione sarebbe un
    // provider senza profilo dichiarato, e il test comportamentale sopra
    // non lo vedrebbe. Gli aghi si compongono a runtime perche scritti
    // per intero comparirebbero in questo stesso file, e la guardia si
    // troverebbe da sola.
    //
    // Due forme, perche un literal puo comparire direttamente o dentro un
    // `Ok` con turbofish; la guardia deve presidiare entrambe.
    let source = include_str!("provider.rs");
    let brace = " {";
    let by_self = format!("Self{brace}");
    // Un tipo di ritorno non e una costruzione: scontarlo tiene vero il
    // messaggio dell'asserzione invece di allargare la guardia a un caso
    // che non riguarda il profilo.
    let returns_self = format!("-> Self{brace}");
    let constructions =
        source.matches(by_self.as_str()).count() - source.matches(returns_self.as_str()).count();
    assert_eq!(
        constructions, 1,
        "il provider deve avere un solo punto di costruzione"
    );
    // La forma per nome compare anche in dichiarazione e negli `impl`:
    // li e legittima, altrove sarebbe una seconda costruzione.
    let by_name = format!("MysqlProvider{brace}");
    for at in source.match_indices(by_name.as_str()).map(|(at, _)| at) {
        let preceding = source[..at].trim_end();
        assert!(
            preceding.ends_with("struct")
                || preceding.ends_with("impl")
                || preceding.ends_with("for"),
            "costruzione di MysqlProvider per nome fuori da with_profile"
        );
    }
    let with_profile = source
        .find("fn with_profile(")
        .expect("with_profile deve esistere");
    let built = source
        .find(by_self.as_str())
        .expect("la costruzione deve esistere");
    assert!(
        built > with_profile,
        "l'unica costruzione deve stare dentro with_profile"
    );
}

#[test]
fn published_spatial_capabilities_match_generic_geometry_contract() {
    let capabilities = crate::profile::MYSQL_PROFILE
        .capabilities("9.7.2".to_owned())
        .spatial;
    assert!(capabilities.read_wkb);
    assert!(capabilities.write_wkb);
    assert!(capabilities.geometry);
    assert!(capabilities.mixed_geometry_types);
    assert_eq!(
        capabilities.dimensions,
        vec![plenora_database_core::geometry::Dimensions::Xy]
    );
    // L'indice si crea con la tabella: la clausola entra nella
    // `CREATE TABLE` della mode `Create`, e su una mode senza DDL il piano
    // la rifiuta — un `ALTER` separato sarebbe un secondo commit implicito,
    // e un fallimento a meta non saprebbe dire cosa e rimasto.
    assert!(capabilities.spatial_index);
}
