use super::*;
use crate::{MysqlColumn, MysqlObjectDescription, MysqlSchemaToken, MAX_BIND_PARAMETERS};
use chrono::NaiveDate;
use mysql_async::{Params, Value};
use plenora_database_core::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    StringArray, TimestampMicrosecondArray, UInt32Array,
};
use plenora_database_core::arrow::schema::{DataType, Field, Schema};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::outcome::{CertainPhase, WriteStatus};
use plenora_database_core::plan::{ObjectRef, ProviderKind, TransactionProfile, WriteMode};
use plenora_database_core::protocol;
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use std::collections::HashMap;
use std::sync::Arc;

fn schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ))
}

fn append_operation() -> WriteOperation {
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

fn append_plan(fields: Vec<Field>) -> MysqlWritePlan {
    MysqlWritePlan::compile_with_profile(
        &schema(fields),
        &append_operation(),
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect("piano append qualificato")
}

/// L'ordine delle colonne e quello dello schema Arrow, non un ordine
/// ricavato dal nome: e l'unico che resta allineato ai buffer di riga.
#[test]
fn insert_renders_qualified_quoted_columns_in_schema_order() {
    let plan = append_plan(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]);
    assert_eq!(
        plan.render_insert(2).expect("insert di due righe"),
        "INSERT INTO `warehouse`.`events` (`id`, `label`) VALUES (?, ?), (?, ?);"
    );

    let escaped = append_plan(vec![
        Field::new("zeta", DataType::Int64, false),
        Field::new("al`pha", DataType::Utf8, false),
    ]);
    assert_eq!(
        escaped.render_insert(1).expect("insert di una riga"),
        "INSERT INTO `warehouse`.`events` (`zeta`, `al``pha`) VALUES (?, ?);"
    );
}

/// Un INSERT senza righe non e una scrittura vuota: e una VALUES list
/// sintatticamente invalida che il server rifiuterebbe dopo la rete.
#[test]
fn insert_requires_at_least_one_row() {
    let error = append_plan(vec![Field::new("id", DataType::Int64, false)])
        .render_insert(0)
        .expect_err("insert senza righe");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(error.phase, ErrorPhase::Prepare);
}

/// Il tetto di 65.535 placeholder e del protocollo: superarlo va visto
/// prima del `COM_STMT_PREPARE`, non nell'errore del server.
#[test]
fn insert_stops_at_the_placeholder_ceiling_before_the_network() {
    let plan = append_plan(vec![Field::new("id", DataType::Int64, false)]);
    let sql = plan
        .render_insert(MAX_BIND_PARAMETERS)
        .expect("insert al tetto dei placeholder");
    assert_eq!(sql.matches('?').count(), MAX_BIND_PARAMETERS);
    let error = plan
        .render_insert(MAX_BIND_PARAMETERS + 1)
        .expect_err("insert oltre il tetto dei placeholder");
    assert_eq!(error.category, ErrorCategory::ResourceLimit);
    assert_eq!(error.phase, ErrorPhase::Prepare);
}

/// Il conteggio dei placeholder e un prodotto: senza controllo esplicito
/// un overflow lo riporterebbe dentro il tetto invece di rifiutarlo.
#[test]
fn insert_row_count_overflow_is_checked() {
    let plan = append_plan(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]);
    let error = plan
        .render_insert(usize::MAX / 2 + 1)
        .expect_err("prodotto righe per colonne in overflow");
    assert_eq!(error.category, ErrorCategory::ResourceLimit);
    assert_eq!(error.phase, ErrorPhase::Prepare);
}

#[test]
fn compile_accepts_supported_arrow_types_in_schema_order() {
    let plan = append_plan(vec![
        Field::new("flag", DataType::Boolean, false),
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(12, 2), true),
        Field::new(
            "created_at",
            DataType::Timestamp(
                plenora_database_core::arrow::schema::TimeUnit::Microsecond,
                None,
            ),
            false,
        ),
    ]);
    assert_eq!(plan.columns[0].kind, MysqlColumnKind::Bool);
    assert_eq!(plan.columns[1].kind, MysqlColumnKind::I64);
    assert_eq!(
        plan.columns[2].kind,
        MysqlColumnKind::Decimal {
            precision: 12,
            scale: 2,
        }
    );
    assert_eq!(plan.columns[3].kind, MysqlColumnKind::Timestamp);
    assert_eq!(plan.columns[0].name, "flag");
    assert!(!plan.columns[0].nullable);
    assert_eq!(plan.columns[2].quoted, "`amount`");
}

#[test]
fn compile_rejects_unqualified_operation_shapes_before_the_network() {
    let input = schema(vec![Field::new("id", DataType::Int64, false)]);
    let mut cases = Vec::new();

    // Ogni forma porta con se la categoria che le spetta, invece di
    // essere schiacciata su una sola: `Unsupported` significa "il
    // provider non lo fa", `InvalidPlan` significa "il piano descrive
    // qualcosa che la mode non significa". Sono risposte diverse e il
    // consumer le tratta diversamente.
    let mut operation = append_operation();
    operation.transaction_profile = TransactionProfile::ChunkCommitted;
    cases.push((operation, ErrorCategory::Unsupported));

    let mut operation = append_operation();
    operation.allow_partial = true;
    cases.push((operation, ErrorCategory::Unsupported));

    // Append non ha semantica di chiave: keys e update_columns non sono
    // una funzione mancante, sono un piano incoerente.
    let mut operation = append_operation();
    operation.keys.push("id".to_owned());
    cases.push((operation, ErrorCategory::InvalidPlan));

    let mut operation = append_operation();
    operation.update_columns.push("label".to_owned());
    cases.push((operation, ErrorCategory::InvalidPlan));

    let mut operation = append_operation();
    operation.create_spatial_index = true;
    cases.push((operation, ErrorCategory::Unsupported));

    let mut operation = append_operation();
    operation.mapping_policy = MappingPolicy::Lossy;
    cases.push((operation, ErrorCategory::Unsupported));

    for (operation, expected) in cases {
        let error = MysqlWritePlan::compile_with_profile(
            &input,
            &operation,
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect_err("forma write non qualificata");
        assert_eq!(error.category, expected, "{operation:?}");
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }
}

/// Le mode senza semantica di chiave rifiutano `keys` e `update_columns`, e
/// il messaggio nomina la mode effettiva. `Create` accetta chiavi opzionali e
/// le rende `PRIMARY KEY`, attraversando il relativo ramo del compilatore.
/// L'indice spaziale entra nella `CREATE TABLE`, e solo li.
///
/// Tre cose insieme, perche separate direbbero meno. La clausola compare
/// nella DDL della mode `Create`; una colonna geometrica nullable la fa
/// rifiutare **prima** del server — su questi motori la `CREATE TABLE` fa
/// commit implicito, quindi scoprirlo dal server significherebbe averlo
/// scoperto con la tabella gia in piedi; e uno schema senza geometrie fa
/// rifiutare la richiesta invece di eseguirla senza indice, che sarebbe un
/// piano onorato a meta.
///
/// La forma della colonna la decide il profilo — `MariaDB` non ammette il
/// vincolo di SRID — ma la clausola dell'indice e la stessa: e il vincolo
/// che diverge, non l'indice.
#[test]
fn a_spatial_index_belongs_to_the_create_ddl_and_wants_a_non_null_column() {
    let mut operation = append_operation();
    operation.mode = WriteMode::Create;
    operation.create_spatial_index = true;
    operation.srid_policy = Some(plenora_database_core::plan::SridPolicy::RequireMatch);

    // `NOT NULL` esplicito: l'aiuto rende una colonna nullable, e un
    // indice spaziale non la accetta — su nessuno dei due motori.
    let input = schema(vec![
        Field::new("id", DataType::Int64, false),
        spatial_field("geometry", 4_326).with_nullable(false),
    ]);
    for profile in [
        &crate::profile::MYSQL_PROFILE as &dyn crate::profile::ProductProfile,
        &crate::profile::MARIADB_PROFILE,
    ] {
        let ddl = build_create_table_sql(&input, &operation, "warehouse", profile)
            .expect("Create con indice spaziale");
        assert!(
            ddl.contains("SPATIAL INDEX (`geom`)"),
            "{}: la clausola non compare — {ddl}",
            profile.product()
        );
    }

    // La colonna nullable: rifiutata prima del server.
    let nullable = spatial_field("geometry", 4_326).with_nullable(true);
    let error = build_create_table_sql(
        &schema(vec![nullable]),
        &operation,
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect_err("indice spaziale su colonna nullable");
    assert_eq!(error.category, ErrorCategory::Unsupported);

    // Nessuna geometria: la richiesta e rifiutata, non ignorata.
    let error = build_create_table_sql(
        &schema(vec![Field::new("id", DataType::Int64, false)]),
        &operation,
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect_err("indice spaziale senza colonne geometriche");
    assert_eq!(error.category, ErrorCategory::Unsupported);
}

#[test]
fn create_accepts_keys_and_renders_them_as_a_primary_key() {
    let fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tenant", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ];
    let mut operation = append_operation();
    operation.mode = WriteMode::Create;
    operation.keys = vec!["id".to_owned(), "tenant".to_owned()];
    let input = schema(fields);

    MysqlWritePlan::compile_with_profile(
        &input,
        &operation,
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect("Create con keys");
    let ddl = build_create_table_sql(
        &input,
        &operation,
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect("DDL");
    assert!(
        ddl.contains("PRIMARY KEY (`id`, `tenant`)"),
        "PRIMARY KEY assente dalla DDL: {ddl}"
    );

    // Senza keys la tabella nasce senza chiave primaria: legittimo.
    let mut without = append_operation();
    without.mode = WriteMode::Create;
    let plain = build_create_table_sql(
        &input,
        &without,
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect("DDL");
    assert!(!plain.contains("PRIMARY KEY"), "{plain}");

    // Una key che non e nello schema Arrow non puo diventare PRIMARY KEY.
    let mut absent = append_operation();
    absent.mode = WriteMode::Create;
    absent.keys = vec!["mai_dichiarata".to_owned()];
    assert_eq!(
        MysqlWritePlan::compile_with_profile(
            &input,
            &absent,
            "warehouse",
            &crate::profile::MYSQL_PROFILE
        )
        .expect_err("key assente accettata")
        .category,
        ErrorCategory::InvalidPlan
    );

    // Una PRIMARY KEY nullable non esiste. MySQL la rifiuterebbe con
    // l'errore 1171, ma al server: il piano va fermato prima della rete.
    let nullable = schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]);
    let mut on_nullable = append_operation();
    on_nullable.mode = WriteMode::Create;
    on_nullable.keys = vec!["id".to_owned()];
    let error = MysqlWritePlan::compile_with_profile(
        &nullable,
        &on_nullable,
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect_err("PRIMARY KEY nullable accettata");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(error.phase, ErrorPhase::Prepare);
    assert!(error.message.contains("nullable"), "{}", error.message);

    // Una chiave ripetuta produrrebbe `PRIMARY KEY (id, id)`.
    let mut repeated = append_operation();
    repeated.mode = WriteMode::Create;
    repeated.keys = vec!["id".to_owned(), "id".to_owned()];
    assert_eq!(
        MysqlWritePlan::compile_with_profile(
            &input,
            &repeated,
            "warehouse",
            &crate::profile::MYSQL_PROFILE
        )
        .expect_err("chiave ripetuta accettata")
        .category,
        ErrorCategory::InvalidPlan
    );

    // `update_columns` non ha senso su Create: non aggiorna nulla.
    let mut updating = append_operation();
    updating.mode = WriteMode::Create;
    updating.update_columns = vec!["label".to_owned()];
    assert_eq!(
        MysqlWritePlan::compile_with_profile(
            &input,
            &updating,
            "warehouse",
            &crate::profile::MYSQL_PROFILE
        )
        .expect_err("update_columns accettate")
        .category,
        ErrorCategory::InvalidPlan
    );
}

/// I tipi che non possono stare in una PRIMARY KEY `MySQL` sono rifiutati
/// dal piano, non dal server.
///
/// Ciascuno di questi casi e stato verificato contro il riferimento e
/// produce un errore lato server: 1170 per TEXT/BLOB, 3728 per le colonne
/// spatial, 1070 oltre 16 parti. Arrivarci significa aver gia aperto la
/// sessione ed eseguito la DDL.
#[test]
fn primary_key_types_and_limits_are_refused_before_the_server() {
    let cases: [(&str, DataType, &str); 4] = [
        ("utf8", DataType::Utf8, "TEXT"),
        ("binary", DataType::Binary, "BLOB"),
        ("float32", DataType::Float32, "virgola mobile"),
        ("float64", DataType::Float64, "virgola mobile"),
    ];
    for (name, data_type, expected) in cases {
        let input = schema(vec![
            Field::new(name, data_type, false),
            Field::new("payload", DataType::Int64, false),
        ]);
        let mut operation = append_operation();
        operation.mode = WriteMode::Create;
        operation.keys = vec![name.to_owned()];
        let Err(error) = MysqlWritePlan::compile_with_profile(
            &input,
            &operation,
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        ) else {
            panic!("{name}: chiave accettata");
        };
        assert_eq!(error.category, ErrorCategory::InvalidPlan, "{name}");
        assert_eq!(error.phase, ErrorPhase::Prepare, "{name}");
        assert!(
            error.message.contains(expected),
            "{name}: messaggio senza la ragione: {}",
            error.message
        );
    }

    // Spatial: il motore la rifiuta con 3728.
    // La fixture spatial e nullable, e il controllo sulla nullability
    // scatterebbe per primo nascondendo quello sul tipo: qui serve una
    // colonna spatial **non** nullable, cosi la sola ragione del rifiuto e
    // che MySQL non ammette indici spatial come chiave.
    let nullable_spatial = spatial_field("point", 4_326);
    let spatial = schema(vec![
        Field::new(
            nullable_spatial.name(),
            nullable_spatial.data_type().clone(),
            false,
        )
        .with_metadata(nullable_spatial.metadata().clone()),
        Field::new("payload", DataType::Int64, false),
    ]);
    let mut operation = spatial_operation();
    operation.mode = WriteMode::Create;
    operation.keys = vec!["geom".to_owned()];
    let error = MysqlWritePlan::compile_with_profile(
        &spatial,
        &operation,
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect_err("chiave spatial accettata");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert!(error.message.contains("spatial"), "{}", error.message);

    // Oltre 16 parti: il motore risponde 1070.
    let wide_fields = (0..17)
        .map(|index| Field::new(format!("k{index}"), DataType::Int64, false))
        .collect::<Vec<_>>();
    let wide = schema(wide_fields);
    let mut too_many = append_operation();
    too_many.mode = WriteMode::Create;
    too_many.keys = (0..17).map(|index| format!("k{index}")).collect();
    let error = MysqlWritePlan::compile_with_profile(
        &wide,
        &too_many,
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect_err("17 parti accettate");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert!(error.message.contains("16"), "{}", error.message);

    // 16 parti esatte restano ammesse: il limite e un confine, non un veto.
    let bounded_fields = (0..16)
        .map(|index| Field::new(format!("k{index}"), DataType::Int64, false))
        .collect::<Vec<_>>();
    let bounded = schema(bounded_fields);
    let mut exact = append_operation();
    exact.mode = WriteMode::Create;
    exact.keys = (0..16).map(|index| format!("k{index}")).collect();
    MysqlWritePlan::compile_with_profile(
        &bounded,
        &exact,
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect("16 parti rifiutate");
}

#[test]
fn modes_without_key_semantics_reject_keys_and_update_columns() {
    for mode in [WriteMode::Append, WriteMode::Replace] {
        let mut operation = append_operation();
        operation.mode = mode;
        operation.keys = vec!["id".to_owned()];
        let error = MysqlWritePlan::compile_with_profile(
            &schema(vec![Field::new("id", DataType::Int64, false)]),
            &operation,
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect_err("keys accettate");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert!(
            error.message.contains(&format!("{mode:?}")),
            "il messaggio deve nominare la mode: {}",
            error.message
        );

        let mut operation = append_operation();
        operation.mode = mode;
        operation.update_columns = vec!["id".to_owned()];
        assert_eq!(
            MysqlWritePlan::compile_with_profile(
                &schema(vec![Field::new("id", DataType::Int64, false)]),
                &operation,
                "warehouse",
                &crate::profile::MYSQL_PROFILE,
            )
            .expect_err("update_columns accettate")
            .category,
            ErrorCategory::InvalidPlan
        );
    }
}

#[test]
fn compile_rejects_cross_database_and_layer_targets() {
    let input = schema(vec![Field::new("id", DataType::Int64, false)]);

    let mut cross_database = append_operation();
    cross_database.target.schema = Some("other_database".to_owned());
    let error = MysqlWritePlan::compile_with_profile(
        &input,
        &cross_database,
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect_err("target cross-database");
    assert_eq!(error.category, ErrorCategory::Unsupported);
}

#[test]
fn compile_rejects_empty_or_unqualified_arrow_schemas() {
    let error = MysqlWritePlan::compile_with_profile(
        &schema(Vec::new()),
        &append_operation(),
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect_err("schema vuoto");
    assert_eq!(error.category, ErrorCategory::Schema);

    let unsupported = schema(vec![Field::new(
        "created_at",
        DataType::Timestamp(
            plenora_database_core::arrow::schema::TimeUnit::Nanosecond,
            Some("UTC".into()),
        ),
        false,
    )]);
    let error = MysqlWritePlan::compile_with_profile(
        &unsupported,
        &append_operation(),
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect_err("timestamp con timezone non qualificato");
    assert_eq!(error.category, ErrorCategory::Unsupported);
}

/// Il contratto Arrow e parte del piano: una versione estranea non puo
/// essere interpretata e non deve arrivare al server.
#[test]
fn compile_rejects_a_foreign_contract_version() {
    // REJ-001: una versione ben formata non supportata e `unsupported`, una
    // malformata (`999.0`) e un difetto dei metadati dello schema.
    for (version, category) in [
        ("2", ErrorCategory::Unsupported),
        ("999.0", ErrorCategory::Schema),
    ] {
        let foreign = Arc::new(Schema::new_with_metadata(
            vec![Field::new("id", DataType::Int64, false)],
            HashMap::from([(
                protocol::CONTRACT_VERSION_KEY.to_owned(),
                version.to_owned(),
            )]),
        ));
        let error = MysqlWritePlan::compile_with_profile(
            &foreign,
            &append_operation(),
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect_err("contratto Arrow estraneo");
        assert_eq!(error.category, category, "{version}");
    }
}

fn server_column(name: &str, data_type: &str, declaration: &str, nullable: bool) -> MysqlColumn {
    MysqlColumn {
        name: name.to_owned(),
        ordinal: 1,
        data_type: data_type.to_owned(),
        native_declaration: declaration.to_owned(),
        nullable,
        default_expression: None,
        character_set: None,
        collation: None,
        numeric_precision: None,
        numeric_scale: None,
        datetime_precision: None,
        spatial_srid: None,
        extra: String::new(),
        generation_expression: String::new(),
    }
}

fn base_table(columns: Vec<MysqlColumn>) -> MysqlObjectDescription {
    base_table_with_indexes(columns, Vec::new())
}

fn base_table_with_indexes(
    columns: Vec<MysqlColumn>,
    indexes: Vec<crate::MysqlIndex>,
) -> MysqlObjectDescription {
    MysqlObjectDescription {
        schema: "warehouse".to_owned(),
        name: "events".to_owned(),
        kind: "BASE TABLE".to_owned(),
        engine: Some("InnoDB".to_owned()),
        columns,
        indexes,
        token: MysqlSchemaToken("token".to_owned()),
    }
}

fn unique_index(name: &str, columns: &[&str]) -> crate::MysqlIndex {
    crate::MysqlIndex {
        name: name.to_owned(),
        unique: true,
        column_backed: true,
        columns: columns.iter().map(|c| (*c).to_owned()).collect(),
        prefix: false,
    }
}

fn identity_plan() -> MysqlWritePlan {
    append_plan(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ])
}

fn identity_target() -> Vec<MysqlColumn> {
    vec![
        server_column("id", "bigint", "bigint", false),
        server_column("label", "varchar", "varchar(32)", true),
    ]
}

fn server_error(code: u16, message: &str) -> mysql_async::Error {
    mysql_async::Error::Server(mysql_async::ServerError {
        code,
        message: message.to_owned(),
        state: "HY000".to_owned(),
    })
}

/// Il chunk non dipende dai dati ma dal numero di colonne: due esecuzioni
/// della stessa append devono produrre esattamente gli stessi INSERT.
#[test]
fn chunk_size_is_deterministic_and_fits_the_placeholder_ceiling() {
    let single = append_plan(vec![Field::new("id", DataType::Int64, false)]);
    assert_eq!(single.rows_per_statement(), MAX_BIND_PARAMETERS);
    let pair = identity_plan();
    assert_eq!(pair.rows_per_statement(), MAX_BIND_PARAMETERS / 2);
    assert_eq!(
        pair.rows_per_statement(),
        identity_plan().rows_per_statement()
    );
    assert_eq!(
        pair.render_insert(pair.rows_per_statement())
            .expect("chunk al tetto")
            .matches('?')
            .count(),
        pair.rows_per_statement() * 2
    );
}

/// I valori viaggiano come bind del protocollo binario: il testo SQL resta
/// fatto di soli placeholder anche per testo, decimal e NULL.
#[test]
fn chunk_binding_is_positional_and_never_interpolates_values() {
    let fields = vec![
        Field::new("flag", DataType::Boolean, false),
        Field::new("id", DataType::Int64, false),
        Field::new("count", DataType::UInt32, false),
        Field::new("ratio", DataType::Float64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, true),
        Field::new("day", DataType::Date32, false),
        Field::new(
            "moment",
            DataType::Timestamp(
                plenora_database_core::arrow::schema::TimeUnit::Microsecond,
                None,
            ),
            false,
        ),
        Field::new("amount", DataType::Decimal128(12, 2), true),
    ];
    let plan = append_plan(fields.clone());
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let day = NaiveDate::from_ymd_opt(2026, 1, 2).expect("giorno");
    let days = i32::try_from(day.signed_duration_since(epoch).num_days()).expect("date32");
    let micros = day
        .and_hms_micro_opt(3, 4, 5, 123_456)
        .expect("istante")
        .and_utc()
        .timestamp_micros();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![true, false])),
        Arc::new(Int64Array::from(vec![7, -7])),
        Arc::new(UInt32Array::from(vec![4_000_000_000, 0])),
        Arc::new(Float64Array::from(vec![1.5, -2.25])),
        Arc::new(StringArray::from(vec![Some("reference"), None])),
        Arc::new(BinaryArray::from_opt_vec(vec![Some(&[1_u8, 2][..]), None])),
        Arc::new(Date32Array::from(vec![days, days])),
        Arc::new(TimestampMicrosecondArray::from(vec![micros, micros])),
        Arc::new(
            Decimal128Array::from(vec![Some(-105_i128), None])
                .with_precision_and_scale(12, 2)
                .expect("decimal"),
        ),
    ];
    let batch = RecordBatch::try_new(schema(fields), columns).expect("batch append");
    let Params::Positional(values) = plan.bind_chunk(&batch, 0, 2).expect("bind del chunk") else {
        panic!("bind MySQL non posizionale");
    };
    assert_eq!(values.len(), 18);
    assert_eq!(values[0], Value::Int(1));
    assert_eq!(values[1], Value::Int(7));
    assert_eq!(values[2], Value::UInt(4_000_000_000));
    assert_eq!(values[3], Value::Double(1.5));
    assert_eq!(values[4], Value::Bytes(b"reference".to_vec()));
    assert_eq!(values[5], Value::Bytes(vec![1, 2]));
    assert_eq!(values[6], Value::Date(2026, 1, 2, 0, 0, 0, 0));
    assert_eq!(values[7], Value::Date(2026, 1, 2, 3, 4, 5, 123_456));
    assert_eq!(values[8], Value::Bytes(b"-1.05".to_vec()));
    assert_eq!(values[9], Value::Int(0));
    assert_eq!(values[13], Value::NULL);
    assert_eq!(values[14], Value::NULL);
    assert_eq!(values[17], Value::NULL);

    let sql = plan.render_insert(2).expect("insert del chunk");
    assert!(!sql.contains("reference"), "{sql}");
    assert!(!sql.contains("1.05"), "{sql}");
    assert!(!sql.to_ascii_uppercase().contains("INFILE"), "{sql}");
}

/// Una cella NULL in una colonna dichiarata non nullable e un errore di
/// mapping locale: va vista prima di aprire la transazione.
#[test]
fn null_cells_in_non_nullable_columns_fail_before_the_network() {
    let plan = append_plan(vec![Field::new("id", DataType::Int64, false)]);
    let batch = RecordBatch::try_new(
        schema(vec![Field::new("id", DataType::Int64, true)]),
        vec![Arc::new(Int64Array::from(vec![None, Some(2)])) as ArrayRef],
    )
    .expect("batch con NULL");
    let error = plan
        .bind_chunk(&batch, 0, 2)
        .expect_err("NULL in colonna non nullable");
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert_eq!(error.phase, ErrorPhase::Prepare);
}

/// Il chunk deve restare dentro il batch: un intervallo fuori misura e un
/// errore esplicito, non una lettura oltre la fine dell'array.
#[test]
fn chunk_bounds_are_checked_against_the_batch() {
    let fields = vec![Field::new("id", DataType::Int64, false)];
    let plan = append_plan(fields.clone());
    let batch = RecordBatch::try_new(
        schema(fields),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef],
    )
    .expect("batch");
    assert_eq!(
        plan.bind_chunk(&batch, 1, 2)
            .expect_err("chunk oltre il batch")
            .category,
        ErrorCategory::InvalidPlan
    );
    assert_eq!(
        plan.bind_chunk(&batch, 0, 0)
            .expect_err("chunk vuoto")
            .category,
        ErrorCategory::InvalidPlan
    );
}

/// Lo schema del batch e quello dichiarato dallo stream: una deriva va
/// vista prima di convertire i valori.
#[test]
fn batch_schema_drift_is_rejected_before_binding() {
    let declared = schema(vec![Field::new("id", DataType::Int64, false)]);
    let stable = RecordBatch::try_new(
        Arc::clone(&declared),
        vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
    )
    .expect("batch stabile");
    validate_batch_schema(&stable, &declared).expect("schema stabile");

    let drifted = RecordBatch::try_new(
        schema(vec![Field::new("renamed", DataType::Int64, false)]),
        vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
    )
    .expect("batch deviato");
    let error = validate_batch_schema(&drifted, &declared).expect_err("schema deviato");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(error.phase, ErrorPhase::Write);
}

/// Strict puo dichiarare zero perdite solo dopo aver visto lo schema del
/// server: e il preflight, non il piano offline, a stabilirlo.
#[test]
fn server_preflight_reports_no_losses_only_for_a_compatible_table() {
    let report = identity_plan()
        .preflight(
            &base_table(vec![
                server_column("id", "bigint", "bigint", false),
                server_column("label", "varchar", "varchar(32)", true),
                server_column("noted_at", "datetime", "datetime(6)", true),
            ]),
            &crate::profile::MYSQL_PROFILE,
        )
        .expect("preflight compatibile");
    assert_eq!(report.schema_version, 2);
    assert_eq!(report.policy, MappingPolicy::Strict);
    assert!(report.losses.is_empty());
    assert!(report.permits_execution());
}

/// Ogni divergenza fra schema Arrow e schema server e una perdita che
/// Strict non ammette: nessuna transazione deve essere aperta.
#[test]
fn server_preflight_rejects_targets_that_strict_cannot_write() {
    let plan = identity_plan();
    let cases = vec![
        vec![server_column("id", "bigint", "bigint", false)],
        vec![
            server_column("id", "bigint", "bigint", false),
            server_column("label", "int", "int", true),
        ],
        vec![
            server_column("id", "bigint", "bigint", false),
            server_column("label", "varchar", "varchar(32)", false),
        ],
        vec![
            server_column("id", "bigint", "bigint", false),
            server_column("label", "varchar", "varchar(32)", true),
            server_column("mandatory", "int", "int", false),
        ],
    ];
    for columns in cases {
        let error = plan
            .preflight(&base_table(columns), &crate::profile::MYSQL_PROFILE)
            .expect_err("target incompatibile");
        assert_eq!(error.category, ErrorCategory::DataMapping);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    let mut generated = base_table(identity_target());
    generated.columns[1].generation_expression = "concat('x')".to_owned();
    assert_eq!(
        plan.preflight(&generated, &crate::profile::MYSQL_PROFILE)
            .expect_err("colonna generata")
            .category,
        ErrorCategory::DataMapping
    );

    let mut view = base_table(identity_target());
    view.kind = "VIEW".to_owned();
    assert_eq!(
        plan.preflight(&view, &crate::profile::MYSQL_PROFILE)
            .expect_err("target non tabella")
            .category,
        ErrorCategory::Unsupported
    );
}

/// Una colonna JSON, ENUM o BIT non e ancora un target di scrittura
/// qualificato anche se in lettura collassa su Utf8 o Binary.
#[test]
fn server_preflight_keeps_unqualified_write_targets_closed() {
    let plan = identity_plan();
    for (data_type, declaration) in [
        ("json", "json"),
        ("enum", "enum('alpha','beta')"),
        ("set", "set('read','write')"),
        ("char", "char(8)"),
    ] {
        let error = plan
            .preflight(
                &base_table(vec![
                    server_column("id", "bigint", "bigint", false),
                    server_column("label", data_type, declaration, true),
                ]),
                &crate::profile::MYSQL_PROFILE,
            )
            .expect_err("target non qualificato");
        assert_eq!(error.category, ErrorCategory::DataMapping);
    }

    let year = append_plan(vec![Field::new("year_value", DataType::Int16, false)]);
    assert_eq!(
        year.preflight(
            &base_table(vec![server_column("year_value", "year", "year", false,)]),
            &crate::profile::MYSQL_PROFILE
        )
        .expect_err("YEAR reinterpreta Int16")
        .category,
        ErrorCategory::DataMapping
    );

    let binary = append_plan(vec![Field::new("payload", DataType::Binary, true)]);
    for (data_type, declaration) in [("bit", "bit(16)"), ("binary", "binary(16)")] {
        assert_eq!(
            binary
                .preflight(
                    &base_table(vec![
                        server_column("payload", data_type, declaration, true,)
                    ]),
                    &crate::profile::MYSQL_PROFILE
                )
                .expect_err("target binary non qualificato")
                .category,
            ErrorCategory::DataMapping
        );
    }
}

/// Fino al 25/10 il preflight ammetteva soltanto `datetime(6)` e
/// `timestamp(6)`, per nome. La scala frazionaria e ora nella chiave della
/// regola A (`crate::write_admission`): il preflight lascia passare ogni
/// precisione e la misura decide (`datetime(3)` riscrive le cifre oltre il
/// millesimo, `FractionalSeconds`, e `Strict` rifiuta quei valori).
#[test]
fn server_preflight_leaves_the_temporal_precision_to_rule_a() {
    let plan = append_plan(vec![Field::new(
        "moment",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    )]);
    for (precision, declaration) in [
        (Some(0), "datetime"),
        (Some(3), "datetime(3)"),
        (Some(6), "datetime(6)"),
    ] {
        let mut column = server_column("moment", "datetime", declaration, true);
        column.datetime_precision = precision;
        assert!(plan
            .preflight(&base_table(vec![column]), &crate::profile::MYSQL_PROFILE)
            .is_ok());
    }
}

/// Un COMMIT interrotto non e un rollback: l'esito resta ignoto e non
/// autorizza retry automatico.
#[test]
fn commit_interruption_produces_an_unknown_outcome_without_automatic_retry() {
    let interrupted = crate::error::timeout_error(
        &crate::profile::MYSQL_PROFILE,
        ErrorPhase::Commit,
        RemoteEffect::None,
    );
    let outcome = commit_failure(interrupted, "mysql-test-1".to_owned(), 7)
        .expect("esito ignoto pubblicabile");
    outcome.validate().expect("outcome valido");
    assert_eq!(outcome.status, WriteStatus::OutcomeUnknown);
    assert_eq!(outcome.provider, ProviderKind::Mysql);
    assert_eq!(outcome.rows.received, 7);
    assert_eq!(outcome.rows.confirmed, 0);
    let recovery = outcome.recovery.expect("recovery obbligatoria");
    assert!(!recovery.automatic_retry_allowed);
    assert_eq!(recovery.last_certain_phase, CertainPhase::CommitRequested);
}

/// Il deadlock e l'unico esito che il server dichiara annullato: resta
/// `RolledBack` anche quando emerge al commit o senza rollback confermato.
#[test]
fn a_declared_deadlock_stays_rolled_back_instead_of_unknown() {
    let deadlock = crate::error::driver_error(
        &crate::profile::MYSQL_PROFILE,
        &server_error(1_213, "Deadlock found when trying to get lock"),
        ErrorPhase::Write,
        RemoteEffect::None,
    );
    assert_eq!(deadlock.remote_effect, RemoteEffect::RolledBack);

    let error = commit_failure(deadlock.clone(), "mysql-test-2".to_owned(), 3)
        .expect_err("deadlock dichiarato dal server");
    assert_eq!(error.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(error.execution_id.as_deref(), Some("mysql-test-2"));

    let shaped = rolled_back_error(deadlock, false, "mysql-test-2");
    assert_eq!(shaped.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(shaped.execution_id.as_deref(), Some("mysql-test-2"));
}

/// Un errore pre-commit puo dichiarare `RolledBack` solo dopo un ROLLBACK
/// confermato: altrimenti l'effetto remoto resta ignoto.
#[test]
fn pre_commit_errors_claim_rollback_only_when_it_is_confirmed() {
    let failure = crate::error::driver_error(
        &crate::profile::MYSQL_PROFILE,
        &server_error(1_062, "Duplicate entry"),
        ErrorPhase::Write,
        RemoteEffect::None,
    );
    let confirmed = rolled_back_error(failure.clone(), true, "mysql-test-3");
    assert_eq!(confirmed.category, ErrorCategory::Conflict);
    assert_eq!(confirmed.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(confirmed.retry, RetryDisposition::Never);

    let ambiguous = rolled_back_error(failure, false, "mysql-test-3");
    assert_eq!(ambiguous.remote_effect, RemoteEffect::Unknown);
    assert_eq!(ambiguous.retry, RetryDisposition::RequiresRecovery);
    // ERR-003: l'ultima fase avviata e il rollback.
    assert_eq!(ambiguous.phase, ErrorPhase::Rollback);
    assert_eq!(confirmed.phase, ErrorPhase::Write);
}

#[test]
fn an_already_quarantined_error_stays_non_retryable_when_rollback_is_unobservable() {
    let quarantined = DatabaseError {
        category: ErrorCategory::Protocol,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::Quarantine,
        provider: Some(ProviderKind::Mysql),
        execution_id: None,
        message: "conteggio righe MySQL incoerente".to_owned(),
        diagnostics: None,
    };

    let shaped = rolled_back_error(quarantined, false, "mysql-test-quarantine");
    assert_eq!(shaped.remote_effect, RemoteEffect::Unknown);
    assert_eq!(shaped.retry, RetryDisposition::Quarantine);
    assert!(!shaped.is_retryable());
    assert_eq!(
        shaped.execution_id.as_deref(),
        Some("mysql-test-quarantine")
    );
}

/// Il conteggio pubblicato deve superare la validazione del contratto e
/// non puo confermare piu righe di quante ne siano state ricevute.
/// Un `Create` fallito non e "come prima": la tabella creata dalla DDL
/// sopravvive al rollback, perche su MySQL il DDL fa commit implicito.
/// L'esito deve dirlo su **ogni** uscita, altrimenti un retry cieco
/// sbatte contro `Conflict` su un target che il chiamante crede assente.
#[test]
fn a_created_table_survives_the_rollback_and_every_outcome_says_so() {
    let failure = write_error(ErrorCategory::Protocol, "insert fallita");

    // Senza residuo l'esito passa invariato.
    let clean = stamp_ddl_residue(
        Err(rolled_back_error(failure.clone(), true, "mysql-create-1")),
        DdlResidue::None,
    )
    .expect_err("errore propagato");
    assert_eq!(clean.remote_effect, RemoteEffect::RolledBack);
    assert!(!clean.message.contains("commit implicito"));

    // Rollback confermato: righe annullate, schema no.
    let residual = stamp_ddl_residue(
        Err(rolled_back_error(failure.clone(), true, "mysql-create-2")),
        DdlResidue::CreatedTable,
    )
    .expect_err("errore propagato");
    assert_eq!(residual.remote_effect, RemoteEffect::Partial);
    assert_eq!(residual.retry, RetryDisposition::RequiresRecovery);
    assert!(residual.message.contains("commit implicito"));

    // Uscita che non ha mai aperto la transazione (`describe_object`,
    // preflight, START TRANSACTION): `None` e altrettanto falso.
    let untouched = stamp_ddl_residue(
        Err(write_error(ErrorCategory::Schema, "preflight cambiato")),
        DdlResidue::CreatedTable,
    )
    .expect_err("errore propagato");
    assert_eq!(untouched.remote_effect, RemoteEffect::Partial);
    assert_eq!(untouched.retry, RetryDisposition::RequiresRecovery);

    // Rollback non confermato: l'incertezza sulle righe resta.
    let ambiguous = stamp_ddl_residue(
        Err(rolled_back_error(failure, false, "mysql-create-3")),
        DdlResidue::CreatedTable,
    )
    .expect_err("errore propagato");
    assert_eq!(ambiguous.remote_effect, RemoteEffect::Unknown);

    // La quarantena e la disposizione piu forte e non viene declassata.
    let mut quarantined = write_error(ErrorCategory::Timeout, "timeout");
    quarantined.retry = RetryDisposition::Quarantine;
    let shaped = stamp_ddl_residue(Err(quarantined), DdlResidue::CreatedTable)
        .expect_err("errore propagato");
    assert_eq!(shaped.retry, RetryDisposition::Quarantine);

    // Commit ambiguo: l'esito resta `OutcomeUnknown`, ma la nota di
    // verifica dice che trovare la tabella non prova nulla sulle righe.
    let unknown = commit_failure(
        write_error(ErrorCategory::Timeout, "commit ambiguo"),
        "mysql-create-4".to_owned(),
        3,
    )
    .expect("outcome unknown");
    let stamped =
        stamp_ddl_residue(Ok(unknown), DdlResidue::CreatedTable).expect("outcome propagato");
    assert_eq!(stamped.status, WriteStatus::OutcomeUnknown);
    assert!(stamped
        .recovery
        .as_ref()
        .and_then(|recovery| recovery.verification_action.as_deref())
        .is_some_and(|action| action.contains("esiste comunque")));

    // Un commit riuscito non viene toccato: la tabella doveva esserci.
    let committed =
        committed_outcome_for_mode("mysql-create-5".to_owned(), 3, 3, WriteMode::Create)
            .expect("outcome committed");
    let stamped = stamp_ddl_residue(Ok(committed.clone()), DdlResidue::CreatedTable)
        .expect("outcome propagato");
    assert_eq!(stamped, committed);
}

/// Dopo un COMMIT riuscito nessun errore puo dire che il server e come
/// prima.
///
/// Se la validazione del documento fallisce, a essere incoerente e il
/// conteggio pubblicato, non lo stato remoto: le righe sono scritte. Un
/// esito `None`, `RolledBack` o `Partial` inviterebbe a un retry che le
/// raddoppierebbe.
#[test]
fn an_error_after_a_successful_commit_declares_the_rows_committed() {
    // `confirmed > received` e incoerente per contratto: il documento non
    // valida, ma il COMMIT e gia avvenuto.
    let error = committed_outcome_for_mode("mysql-post-1".to_owned(), 1, 5, WriteMode::Append)
        .expect_err("documento incoerente accettato");
    assert_eq!(error.remote_effect, RemoteEffect::Committed);
    assert_eq!(error.retry, RetryDisposition::Never);
    assert_eq!(error.category, ErrorCategory::Internal);
    assert!(error.message.contains("committed"));

    // Il residuo della DDL non lo tocca: la tabella c'e per forza, e
    // chiedere recupero suggerirebbe il retry che va evitato.
    let stamped = stamp_ddl_residue(Err(error.clone()), DdlResidue::CreatedTable)
        .expect_err("errore propagato");
    assert_eq!(stamped.remote_effect, RemoteEffect::Committed);
    assert_eq!(stamped.retry, RetryDisposition::Never);
    assert_eq!(
        stamped.message, error.message,
        "il residuo ha aggiunto rumore a un esito gia committed"
    );
}

#[test]
fn committed_outcome_row_counts_are_contract_valid() {
    let outcome = committed_outcome("mysql-test-4".to_owned(), 5, 5).expect("outcome committed");
    outcome.validate().expect("outcome valido");
    assert_eq!(outcome.status, WriteStatus::Committed);
    assert_eq!(outcome.provider, ProviderKind::Mysql);
    assert_eq!(outcome.rows.inserted, Some(5));
    assert_eq!(outcome.rows.updated, Some(0));
    assert_eq!(outcome.rows.deleted, Some(0));
    assert_eq!(outcome.rows.failed, 0);
    assert_eq!(outcome.rows.skipped, 0);
    assert!(outcome.recovery.is_none());

    assert_eq!(
        committed_outcome("mysql-test-5".to_owned(), 2, 3)
            .expect_err("conferme oltre le righe ricevute")
            .category,
        ErrorCategory::Internal
    );
}

fn spatial_column(native_type: &str, srid: u32) -> MysqlColumn {
    let mut column = server_column("geom", native_type, native_type, true);
    column.spatial_srid = Some(srid);
    column
}

/// Il campo come lo pubblica la lettura: `contract_schema` gli assegna il
/// field id, che il vocabolario chiede a ogni campo geometria.
fn spatial_field(native_type: &str, srid: u32) -> Field {
    let field = MysqlColumnSpec::from_catalog(&spatial_column(native_type, srid))
        .expect("colonna spatial qualificata")
        .arrow_field();
    let mut metadata = field.metadata().clone();
    metadata.insert(protocol::FIELD_ID.to_owned(), "0".to_owned());
    field.with_metadata(metadata)
}

fn spatial_operation() -> WriteOperation {
    let mut operation = append_operation();
    operation.srid_policy = Some(plenora_database_core::plan::SridPolicy::RequireMatch);
    operation
}

fn point_wkb(type_word: u32, srid: Option<u32>, ordinates: &[f64]) -> Vec<u8> {
    let mut bytes = vec![1_u8];
    bytes.extend_from_slice(&type_word.to_le_bytes());
    if let Some(srid) = srid {
        bytes.extend_from_slice(&srid.to_le_bytes());
    }
    for ordinate in ordinates {
        bytes.extend_from_slice(&ordinate.to_le_bytes());
    }
    bytes
}

#[test]
fn compile_and_preflight_qualify_only_xy_wkb_with_matching_srid() {
    let input = schema(vec![spatial_field("geometry", 4_326)]);
    let plan = MysqlWritePlan::compile_with_profile(
        &input,
        &spatial_operation(),
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect("piano spatial XY");
    assert_eq!(plan.columns[0].kind, MysqlColumnKind::Geometry);
    assert_eq!(plan.columns[0].spatial_srid, Some(4_326));
    assert_eq!(
            plan.render_insert(1).expect("insert geometry"),
            "INSERT INTO `warehouse`.`events` (`geom`) VALUES (ST_GeomFromWKB(CAST(? AS BINARY), 4326, 'axis-order=long-lat'));"
        );
    assert!(plan
        .preflight(
            &base_table(vec![spatial_column("geometry", 4_326)]),
            &crate::profile::MYSQL_PROFILE
        )
        .is_ok());
    assert_eq!(
        plan.preflight(
            &base_table(vec![spatial_column("geometry", 3_857)]),
            &crate::profile::MYSQL_PROFILE
        )
        .expect_err("SRID target diverso")
        .category,
        ErrorCategory::Crs
    );
}

#[test]
fn compile_rejects_dimensions_the_mysql_server_cannot_represent() {
    for dimensions in ["xyz", "xym", "xyzm"] {
        let mut metadata = spatial_field("geometry", 4_326).metadata().clone();
        metadata.insert(
            protocol::GEOMETRY_DIMENSIONS.to_owned(),
            dimensions.to_owned(),
        );
        let field = Field::new("geom", DataType::Binary, true).with_metadata(metadata);
        let error = MysqlWritePlan::compile_with_profile(
            &schema(vec![field]),
            &spatial_operation(),
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect_err("dimensione non rappresentabile da MySQL");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }
}

#[test]
fn spatial_batch_rejects_ewkb_srid_and_z_before_binding() {
    let input = schema(vec![spatial_field("geometry", 4_326)]);
    let plan = MysqlWritePlan::compile_with_profile(
        &input,
        &spatial_operation(),
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect("piano spatial XY");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget spatial");
    for payload in [
        point_wkb(0x2000_0001, Some(4_326), &[1.0, 2.0]),
        point_wkb(1_001, None, &[1.0, 2.0, 3.0]),
        point_wkb(2_001, None, &[1.0, 2.0, 3.0]),
        point_wkb(3_001, None, &[1.0, 2.0, 3.0, 4.0]),
    ] {
        let batch = RecordBatch::try_new(
            Arc::clone(&input),
            vec![Arc::new(BinaryArray::from(vec![payload.as_slice()])) as ArrayRef],
        )
        .expect("batch spatial non qualificato");
        assert_eq!(
            plan.validate_spatial_batch(&batch, &budget)
                .expect_err("payload spatial non qualificato")
                .category,
            ErrorCategory::DataMapping
        );
    }

    let xy = point_wkb(1, None, &[1.0, 2.0]);
    let batch = RecordBatch::try_new(
        input,
        vec![Arc::new(BinaryArray::from(vec![xy.as_slice()])) as ArrayRef],
    )
    .expect("batch spatial XY");
    assert_eq!(
        plan.validate_spatial_batch(&batch, &budget)
            .expect("WKB XY")
            .components,
        2
    );
    let Params::Positional(values) = plan
        .bind_chunk(&batch, 0, 1)
        .expect("bind WKB XY posizionale")
    else {
        panic!("bind MySQL non posizionale");
    };
    assert_eq!(values, vec![Value::Bytes(xy)]);
}

#[test]
fn spatial_batch_enforces_exact_type_and_cumulative_component_budget() {
    let input = schema(vec![spatial_field("linestring", 4_326)]);
    let plan = MysqlWritePlan::compile_with_profile(
        &input,
        &spatial_operation(),
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect("piano spatial exact");
    let point = point_wkb(1, None, &[1.0, 2.0]);
    let wrong_type = RecordBatch::try_new(
        Arc::clone(&input),
        vec![Arc::new(BinaryArray::from(vec![point.as_slice()])) as ArrayRef],
    )
    .expect("batch con tipo geometry errato");
    assert_eq!(
        plan.validate_spatial_batch(
            &wrong_type,
            &ResourceBudget::new(ResourceLimits::default()).expect("budget exact"),
        )
        .expect_err("tipo geometry diverso dal contratto")
        .category,
        ErrorCategory::DataMapping
    );

    let input = schema(vec![spatial_field("point", 4_326)]);
    let plan = MysqlWritePlan::compile_with_profile(
        &input,
        &spatial_operation(),
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect("piano point");
    let limits = ResourceLimits {
        geometry_components: 3,
        ..ResourceLimits::default()
    };
    let budget = ResourceBudget::new(limits).expect("budget componenti");
    let two_points = RecordBatch::try_new(
        Arc::clone(&input),
        vec![Arc::new(BinaryArray::from(vec![point.as_slice(), point.as_slice()])) as ArrayRef],
    )
    .expect("batch due point");
    assert_eq!(
        plan.validate_spatial_batch(&two_points, &budget)
            .expect_err("quattro componenti oltre il budget tre")
            .category,
        ErrorCategory::ResourceLimit
    );
    assert_eq!(budget.remaining(ResourceKind::GeometryComponents), 3);

    let one_point = RecordBatch::try_new(
        input,
        vec![Arc::new(BinaryArray::from(vec![point.as_slice()])) as ArrayRef],
    )
    .expect("batch un point");
    plan.validate_spatial_batch(&one_point, &budget)
        .expect("due componenti consumati");
    assert_eq!(budget.remaining(ResourceKind::GeometryComponents), 1);
    assert_eq!(
        plan.validate_spatial_batch(&one_point, &budget)
            .expect_err("budget cumulativo esaurito")
            .category,
        ErrorCategory::ResourceLimit
    );
    assert_eq!(budget.remaining(ResourceKind::GeometryComponents), 1);
}

// ============================ Upsert: rendering + policy indici ============

fn upsert_operation(keys: Vec<String>) -> WriteOperation {
    WriteOperation {
        mode: WriteMode::Upsert,
        keys,
        ..append_operation()
    }
}

fn upsert_plan(fields: Vec<Field>, keys: Vec<String>) -> MysqlWritePlan {
    MysqlWritePlan::compile_with_profile(
        &schema(fields),
        &upsert_operation(keys),
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect("piano upsert qualificato")
}

/// Un Upsert con colonne non-key rende `ON DUPLICATE KEY UPDATE` che
/// aggiorna esattamente le non-key con i VALUES della riga.
#[test]
fn upsert_renders_on_duplicate_update_for_non_key_columns() {
    let plan = upsert_plan(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ],
        vec!["id".to_owned()],
    );
    assert_eq!(
        plan.render_insert(1).expect("insert upsert"),
        "INSERT INTO `warehouse`.`events` (`id`, `label`) VALUES (?, ?) \
             ON DUPLICATE KEY UPDATE `label`=VALUES(`label`);"
    );
}

/// Un Upsert **keys-only** (schema di sole key) non deve degradare a un
/// INSERT nudo che erra sul primo conflitto: rende una clausola no-op
/// `k=k` per ottenere semantica insert-or-ignore idempotente.
#[test]
fn upsert_keys_only_renders_noop_on_duplicate_clause() {
    let plan = upsert_plan(
        vec![Field::new("id", DataType::Int64, false)],
        vec!["id".to_owned()],
    );
    assert_eq!(
        plan.render_insert(2).expect("insert upsert keys-only"),
        "INSERT INTO `warehouse`.`events` (`id`) VALUES (?), (?) \
             ON DUPLICATE KEY UPDATE `id`=`id`;"
    );
}

/// Le keys devono corrispondere a un PK/UNIQUE index reale.
#[test]
fn upsert_preflight_accepts_keys_matching_a_unique_index() {
    let plan = upsert_plan(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ],
        vec!["id".to_owned()],
    );
    let target = base_table_with_indexes(identity_target(), vec![unique_index("PRIMARY", &["id"])]);
    assert!(plan
        .preflight(&target, &crate::profile::MYSQL_PROFILE)
        .is_ok());
}

/// Un unique index **aggiuntivo** diverso dalle keys rende l'Upsert
/// non sicuro: ON DUPLICATE KEY potrebbe colpire la riga sbagliata.
#[test]
fn upsert_preflight_rejects_a_conflicting_extra_unique_index() {
    let plan = upsert_plan(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ],
        vec!["id".to_owned()],
    );
    let target = base_table_with_indexes(
        identity_target(),
        vec![
            unique_index("PRIMARY", &["id"]),
            unique_index("uq_label", &["label"]),
        ],
    );
    let error = plan
        .preflight(&target, &crate::profile::MYSQL_PROFILE)
        .expect_err("unique index in conflitto");
    assert_eq!(error.category, ErrorCategory::Schema);
    assert_eq!(error.phase, ErrorPhase::Prepare);
    assert!(error
        .message
        .ends_with(plenora_database_core::write_keys::UPSERT_WITH_OTHER_UNIQUE_INDEX));
}

/// S6: un unique index su un **prefisso** delle keys entra in conflitto fra
/// valori interi diversi con lo stesso prefisso: l'Upsert si rifiuta, anche
/// quando e l'unico indice e le sue colonne sono le keys.
#[test]
fn upsert_preflight_rejects_a_unique_index_on_a_key_prefix() {
    let plan = upsert_plan(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ],
        vec!["id".to_owned()],
    );
    let mut prefix = unique_index("prefisso", &["id"]);
    prefix.prefix = true;
    for profile in [
        &crate::profile::MYSQL_PROFILE as &dyn crate::profile::ProductProfile,
        &crate::profile::MARIADB_PROFILE,
    ] {
        let target = base_table_with_indexes(identity_target(), vec![prefix.clone()]);
        let error = plan
            .preflight(&target, profile)
            .expect_err("unique index su un prefisso");
        assert_eq!(error.category, ErrorCategory::Schema);
        assert_eq!(error.provider, Some(profile.kind()));
        assert!(error
            .message
            .ends_with(plenora_database_core::write_keys::UPSERT_WITH_OTHER_UNIQUE_INDEX));
    }
}

/// Nessun unique index sulle keys → l'Upsert inserirebbe duplicati.
#[test]
fn upsert_preflight_rejects_keys_without_a_backing_unique_index() {
    let plan = upsert_plan(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ],
        vec!["id".to_owned()],
    );
    // Solo un indice non-unique su id: non ancora l'ancora richiesta.
    let non_unique = crate::MysqlIndex {
        name: "idx_id".to_owned(),
        unique: false,
        column_backed: true,
        columns: vec!["id".to_owned()],
        prefix: false,
    };
    let target = base_table_with_indexes(identity_target(), vec![non_unique]);
    let error = plan
        .preflight(&target, &crate::profile::MYSQL_PROFILE)
        .expect_err("nessun unique index sulle keys");
    assert_eq!(error.category, ErrorCategory::Unsupported);
}

/// Un unique index funzionale (espressione) non è confrontabile con le
/// keys → fail-closed.
#[test]
fn upsert_preflight_rejects_a_functional_unique_index() {
    let plan = upsert_plan(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ],
        vec!["id".to_owned()],
    );
    let functional = crate::MysqlIndex {
        name: "uq_expr".to_owned(),
        unique: true,
        column_backed: false,
        columns: Vec::new(),
        prefix: false,
    };
    let target = base_table_with_indexes(
        identity_target(),
        vec![unique_index("PRIMARY", &["id"]), functional],
    );
    let error = plan
        .preflight(&target, &crate::profile::MYSQL_PROFILE)
        .expect_err("unique index funzionale");
    assert_eq!(error.category, ErrorCategory::Schema);
}

/// Un unique index composito ridondante sulle **stesse** colonne delle
/// keys (stesso insieme) è ammesso: colpisce sempre la stessa riga.
#[test]
fn upsert_preflight_accepts_a_redundant_unique_index_on_the_same_keys() {
    let plan = upsert_plan(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ],
        vec!["id".to_owned()],
    );
    let target = base_table_with_indexes(
        identity_target(),
        vec![
            unique_index("PRIMARY", &["id"]),
            unique_index("uq_id_dup", &["id"]),
        ],
    );
    assert!(plan
        .preflight(&target, &crate::profile::MYSQL_PROFILE)
        .is_ok());
}

/// Un campo geometria del catalogo con alcuni metadati sostituiti o tolti.
fn spatial_field_with(changes: &[(&str, Option<&str>)]) -> Field {
    let field = spatial_field("point", 4_326);
    let mut metadata = field.metadata().clone();
    for &(key, value) in changes {
        match value {
            Some(value) => metadata.insert(key.to_owned(), value.to_owned()),
            None => metadata.remove(key),
        };
    }
    field.with_metadata(metadata)
}

/// GEO-001: un campo `lat_lon` non si scrive su un bersaglio che vuole la
/// longitudine per prima, ne su `MySQL` ne su `MariaDB`; il rifiuto e
/// `unsupported` e arriva dalla compilazione, prima di ogni effetto. Fino al
/// 24/10 nessun adattatore leggeva `axis_order` in ingresso.
#[test]
fn lat_lon_axis_order_is_unsupported_before_any_effect() {
    let input = schema(vec![spatial_field_with(&[(
        protocol::GEOMETRY_AXIS_ORDER,
        Some("lat_lon"),
    )])]);
    for profile in [
        &crate::profile::MYSQL_PROFILE as &dyn crate::profile::ProductProfile,
        &crate::profile::MARIADB_PROFILE,
    ] {
        let error = MysqlWritePlan::compile_with_profile(
            &input,
            &spatial_operation(),
            "warehouse",
            profile,
        )
        .expect_err("lat_lon non rappresentabile");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }
    // `unknown` resta ammesso, con l'hazard dichiarato.
    let unknown = schema(vec![spatial_field("point", 4_326)]);
    MysqlWritePlan::compile_with_profile(
        &unknown,
        &spatial_operation(),
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect("unknown ammesso");
}

/// GEO-008: uno SRID derivato da `EPSG:<n>` si scrive su `MySQL` solo dopo la
/// verifica del catalogo (il piano lo segnala), e su `MariaDB`, che un
/// catalogo non ha, non si deriva. Il rendering di `MariaDB` resta senza
/// l'opzione degli assi, che non accetta.
#[test]
fn derived_srid_is_verified_on_mysql_and_refused_on_mariadb() {
    let input = schema(vec![spatial_field_with(&[
        (protocol::GEOMETRY_SRID, None),
        (protocol::GEOMETRY_CRS_ID, Some("EPSG:4326")),
        (protocol::GEOMETRY_AXIS_ORDER, Some("lon_lat")),
    ])]);
    let plan = MysqlWritePlan::compile_with_profile(
        &input,
        &spatial_operation(),
        "warehouse",
        &crate::profile::MYSQL_PROFILE,
    )
    .expect("SRID derivato su MySQL");
    assert_eq!(plan.derived_srids(), vec![4_326]);
    let error = MysqlWritePlan::compile_with_profile(
        &input,
        &spatial_operation(),
        "warehouse",
        &crate::profile::MARIADB_PROFILE,
    )
    .expect_err("SRID derivato su MariaDB");
    assert_eq!(error.category, ErrorCategory::Crs);

    let declared = schema(vec![spatial_field("point", 4_326)]);
    let plan = MysqlWritePlan::compile_with_profile(
        &declared,
        &spatial_operation(),
        "warehouse",
        &crate::profile::MARIADB_PROFILE,
    )
    .expect("SRID dichiarato su MariaDB");
    assert!(plan.derived_srids().is_empty());
    assert_eq!(
        plan.render_insert(1).expect("insert geometry"),
        "INSERT INTO `warehouse`.`events` (`geom`) VALUES (ST_GeomFromWKB(CAST(? AS BINARY), 4326));"
    );
}

/// La regola K: Update e `DeleteByKeys` vogliono un indice unico fatto di
/// sole colonne, con le colonne uguali (come insieme) alle chiavi.
#[test]
fn la_regola_k_vuole_un_indice_unico_sulle_chiavi() {
    let kind = plenora_database_core::plan::ProviderKind::Mysql;
    let functional = crate::MysqlIndex {
        name: "uq_expr".to_owned(),
        unique: true,
        column_backed: false,
        columns: vec!["id".to_owned()],
        prefix: false,
    };
    let non_unique = crate::MysqlIndex {
        name: "idx_id".to_owned(),
        unique: false,
        column_backed: true,
        columns: vec!["id".to_owned()],
        prefix: false,
    };
    for mode in [WriteMode::Update, WriteMode::DeleteByKeys] {
        let mut operation = append_operation();
        operation.mode = mode;
        operation.keys = vec!["id".to_owned()];
        for (name, indexes, admitted) in [
            ("primaria", vec![unique_index("PRIMARY", &["id"])], true),
            ("non unico", vec![non_unique.clone()], false),
            ("funzionale", vec![functional.clone()], false),
            (
                "piu largo",
                vec![unique_index("uq", &["id", "label"])],
                false,
            ),
            ("nessuno", Vec::new(), false),
        ] {
            let target = base_table_with_indexes(identity_target(), indexes);
            let outcome =
                super::validate_unique_keys(&target, &operation, kind, ErrorPhase::Prepare);
            assert_eq!(outcome.is_ok(), admitted, "{mode:?} {name}");
            if let Err(error) = outcome {
                assert_eq!(error.category, ErrorCategory::Schema);
                assert!(error
                    .message
                    .ends_with(plenora_database_core::write_keys::KEYS_WITHOUT_UNIQUE_INDEX));
            }
        }
    }
    let mut composite = append_operation();
    composite.mode = WriteMode::Update;
    composite.keys = vec!["label".to_owned(), "id".to_owned()];
    let target = base_table_with_indexes(
        identity_target(),
        vec![unique_index("uq", &["id", "label"])],
    );
    assert!(super::validate_unique_keys(&target, &composite, kind, ErrorPhase::Prepare).is_ok());
    let target = base_table_with_indexes(identity_target(), Vec::new());
    assert!(
        super::validate_unique_keys(&target, &append_operation(), kind, ErrorPhase::Prepare)
            .is_ok(),
        "Append non e una mode della regola K"
    );
}

fn text_column(data_type: &str, declaration: &str) -> crate::MysqlColumn {
    crate::MysqlColumn {
        name: "label".to_owned(),
        ordinal: 1,
        data_type: data_type.to_owned(),
        native_declaration: declaration.to_owned(),
        nullable: true,
        default_expression: None,
        character_set: Some("utf8mb4".to_owned()),
        collation: Some("utf8mb4_0900_ai_ci".to_owned()),
        numeric_precision: None,
        numeric_scale: None,
        datetime_precision: None,
        spatial_srid: None,
        extra: String::new(),
        generation_expression: String::new(),
    }
}

/// La regola P: su 1b02559 `abc` con tre spazi verso `VARCHAR(3)` diventava
/// `abc` con il COMMIT (il server tronca gli spazi finali anche con
/// `STRICT_TRANS_TABLES`). Ora un testo piu lungo della colonna si rifiuta.
#[test]
fn la_regola_p_rifiuta_il_testo_piu_lungo_della_colonna() {
    let varchar = text_limit(&text_column("varchar", "varchar(3)"));
    assert_eq!(varchar, Some(TextLimit::Chars(3)));
    assert!(check_text(varchar, "abc   ").is_err());
    assert!(check_text(varchar, "ab ").is_ok());
    // I caratteri, non i byte, per `VARCHAR(n)`.
    assert!(check_text(varchar, "\u{416}\u{20ac}\u{4e2d}").is_ok());
    let tiny = text_limit(&text_column("tinytext", "tinytext"));
    assert_eq!(tiny, Some(TextLimit::Bytes(255)));
    assert!(check_text(tiny, &"\u{20ac}".repeat(86)).is_err());
    assert!(check_text(tiny, &"\u{20ac}".repeat(85)).is_ok());
    assert_eq!(text_limit(&text_column("longtext", "longtext")), None);
    let error = check_text(varchar, "abcd").expect_err("rifiuto");
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert!(error.message.ends_with(LOSSY_VALUE));
}

#[test]
fn le_chiavi_testuali_senza_collation_si_rifiutano_in_ogni_mode() {
    let plan = append_plan(vec![Field::new("label", DataType::Utf8, true)]);
    for mode in [
        WriteMode::Upsert,
        WriteMode::Update,
        WriteMode::DeleteByKeys,
    ] {
        let mut operation = append_operation();
        operation.mode = mode;
        operation.keys = vec!["label".to_owned()];
        for missing_charset in [false, true] {
            let mut text = text_column("varchar", "varchar(10)");
            if missing_charset {
                text.character_set = None;
            } else {
                text.collation = None;
            }
            let target = base_table(vec![text]);
            let error = plan
                .key_collation_guard(&operation, &target, "warehouse", "test")
                .expect_err("confronto della chiave non provato");
            assert_eq!(error.category, ErrorCategory::Unsupported);
        }
    }
}

#[test]
fn lo_staging_non_sostituisce_metadati_testuali_assenti_o_invalidi() {
    let schema = Arc::new(Schema::new(vec![Field::new("label", DataType::Utf8, true)]));
    for profile in [
        &crate::profile::MYSQL_PROFILE as &dyn crate::profile::ProductProfile,
        &crate::profile::MARIADB_PROFILE,
    ] {
        for invalid in [None, Some(""), Some("utf8mb4; DROP")] {
            for charset in [false, true] {
                let mut text = text_column("varchar", "varchar(10)");
                if charset {
                    text.character_set = invalid.map(ToOwned::to_owned);
                } else {
                    text.collation = invalid.map(ToOwned::to_owned);
                }
                let error = build_temp_staging_sql(
                    &schema,
                    "staging",
                    "warehouse",
                    profile,
                    &base_table(vec![text]),
                )
                .expect_err("nessun default implicito per il testo");
                assert_eq!(error.category, ErrorCategory::Unsupported);
            }
        }
        let sql = build_temp_staging_sql(
            &schema,
            "staging",
            "warehouse",
            profile,
            &base_table(vec![text_column("varchar", "varchar(10)")]),
        )
        .expect("metadati completi");
        assert!(sql.contains("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"));
    }
}
