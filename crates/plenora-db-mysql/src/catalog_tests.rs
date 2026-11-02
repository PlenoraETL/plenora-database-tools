use super::*;

/// Le sei colonne che entrambe le query di indice espongono, nello stesso
/// ordine in cui il contratto degli alias le dichiara.
fn index_row(name: &str, sequence: i64, column: Value, expression: Value) -> Row {
    index_row_with_sub_part(name, sequence, column, expression, Value::NULL)
}

fn index_row_with_sub_part(
    name: &str,
    sequence: i64,
    column: Value,
    expression: Value,
    sub_part: Value,
) -> Row {
    let wire: std::sync::Arc<[mysql_async::Column]> = [
        "index_name",
        "non_unique",
        "seq_in_index",
        "column_name",
        "expression",
        "sub_part",
    ]
    .into_iter()
    .map(|alias| {
        mysql_async::Column::new(mysql_async::consts::ColumnType::MYSQL_TYPE_VAR_STRING)
            .with_name(alias.as_bytes())
    })
    .collect();
    mysql_common::row::new_row(
        vec![
            Value::Bytes(name.as_bytes().to_vec()),
            Value::Int(1),
            Value::Int(sequence),
            column,
            expression,
            sub_part,
        ],
        wire,
    )
}

#[test]
fn a_functional_index_is_described_where_the_product_publishes_its_parts() {
    // MySQL popola `EXPRESSION`, quindi la parte funzionale si riconosce:
    // l'indice esiste, ma non e piu confrontabile per colonne, ed e
    // esattamente cio che il preflight Upsert deve sapere.
    let rows = vec![index_row(
        "idx_lower_name",
        1,
        Value::NULL,
        Value::Bytes(b"lower(`name`)".to_vec()),
    )];
    let indexes = build_indexes(&rows, &crate::profile::MYSQL_PROFILE).expect("indici MySQL");
    assert_eq!(indexes.len(), 1);
    assert!(!indexes[0].column_backed);
    assert!(indexes[0].columns.is_empty());
}

/// S6: una parte su un prefisso di colonna (`SUB_PART`, `UNIQUE (code(2))`)
/// segna l'indice: per l'Upsert non e la chiave intera.
#[test]
fn a_prefix_part_marks_the_index() {
    for profile in [
        &crate::profile::MYSQL_PROFILE as &dyn crate::profile::ProductProfile,
        &crate::profile::MARIADB_PROFILE,
    ] {
        let rows = vec![
            index_row_with_sub_part(
                "prefisso",
                1,
                Value::Bytes(b"code".to_vec()),
                Value::NULL,
                Value::Int(2),
            ),
            index_row("intero", 1, Value::Bytes(b"code".to_vec()), Value::NULL),
        ];
        let indexes = build_indexes(&rows, profile).expect("indici");
        assert!(indexes[0].prefix && indexes[0].column_backed);
        assert_eq!(indexes[0].columns, ["code"]);
        assert!(!indexes[1].prefix);
        // Nel documento il campo compare soltanto quando e vero.
        let document = serde_json::to_value(&indexes).expect("documento");
        assert_eq!(document[0]["prefix"], true);
        assert!(document[1].get("prefix").is_none());
    }
}

#[test]
fn a_part_without_a_column_is_refused_where_the_product_cannot_describe_it() {
    // Lo stesso indice visto da MariaDB, dove `EXPRESSION` non esiste e la
    // query la dichiara nulla: la parte arriva senza colonna **e** senza
    // espressione, e non c'e modo di dire cosa indicizzi.
    //
    // Il rifiuto e la fine della catena che comincia nel profilo — la
    // colonna assente, la bandiera a `false` — e questo test e il punto in
    // cui quella catena si osserva invece di dedurla. Dichiarare l'indice
    // confrontabile per colonne, con la lista vuota, lo farebbe passare
    // per un indice su nessuna colonna: un upsert lo confronterebbe con le
    // sue keys e non troverebbe nulla da opporre.
    let rows = vec![index_row("idx_lower_name", 1, Value::NULL, Value::NULL)];
    let error = build_indexes(&rows, &crate::profile::MARIADB_PROFILE)
        .expect_err("una parte senza colonna ne espressione si rifiuta");
    // `DataMapping`, non `Schema`: lo schema del server e coerente — e la
    // riga che ne descrive un indice a non essere interpretabile.
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert!(
        error.message.contains("MariaDB"),
        "il rifiuto non nomina chi ha rifiutato: {}",
        error.message
    );
    // E le parti normali continuano a descriversi: il rifiuto riguarda la
    // parte che non si sa leggere, non l'indice per il fatto di esistere.
    let ordinary = vec![index_row(
        "PRIMARY",
        1,
        Value::Bytes(b"id".to_vec()),
        Value::NULL,
    )];
    let indexes =
        build_indexes(&ordinary, &crate::profile::MARIADB_PROFILE).expect("indici MariaDB");
    assert_eq!(indexes[0].columns, vec!["id".to_owned()]);
    assert!(indexes[0].column_backed);
}

#[test]
fn schema_token_is_stable_and_sensitive() {
    let column = MysqlColumn {
        name: "id".to_owned(),
        ordinal: 1,
        data_type: "int".to_owned(),
        native_declaration: "int".to_owned(),
        nullable: false,
        default_expression: None,
        character_set: None,
        collation: None,
        numeric_precision: Some(10),
        numeric_scale: Some(0),
        datetime_precision: None,
        spatial_srid: None,
        extra: String::new(),
        generation_expression: String::new(),
    };
    let pk = MysqlIndex {
        name: "PRIMARY".to_owned(),
        unique: true,
        column_backed: true,
        columns: vec!["id".to_owned()],
        prefix: false,
    };
    let first = schema_token(
        "data",
        "items",
        "BASE TABLE",
        Some("InnoDB"),
        std::slice::from_ref(&column),
        std::slice::from_ref(&pk),
    )
    .expect("token");
    let same = schema_token(
        "data",
        "items",
        "BASE TABLE",
        Some("InnoDB"),
        std::slice::from_ref(&column),
        std::slice::from_ref(&pk),
    )
    .expect("same token");
    let changed = schema_token(
        "data",
        "items",
        "BASE TABLE",
        Some("InnoDB"),
        &[MysqlColumn {
            nullable: true,
            ..column.clone()
        }],
        std::slice::from_ref(&pk),
    )
    .expect("changed token");
    // Una modifica agli indici (aggiunta di un unique index) cambia il token.
    let index_changed = schema_token(
        "data",
        "items",
        "BASE TABLE",
        Some("InnoDB"),
        std::slice::from_ref(&column),
        &[
            pk,
            MysqlIndex {
                name: "uq_code".to_owned(),
                unique: true,
                column_backed: true,
                columns: vec!["code".to_owned()],
                prefix: false,
            },
        ],
    )
    .expect("index changed token");
    assert_eq!(first, same);
    assert_ne!(first, changed);
    assert_ne!(first, index_changed);
}

/// La regola Q su `MySQL`/`MariaDB`: ogni relazione di catalogo che la
/// libreria interroga porta lo schema (`information_schema.`,
/// `performance_schema.`, `mysql.`). Una tabella dello schema corrente con lo
/// stesso nome (`dataflow_test.columns`) non deve poter rispondere al posto
/// del catalogo.
///
/// Le funzioni no: una funzione memorizzata con il nome di una funzione del
/// server si chiama soltanto qualificata (`db.database()`), e la chiamata
/// senza schema resta quella del server. Misurato il 25/10 su `MySQL` 9.7 e
/// `MariaDB` 12.3 con `CREATE FUNCTION database()` e `concat_ws(a, b, c)`
/// nello schema corrente: `DATABASE()` e `CONCAT_WS(...)` restituivano i
/// valori del server.
#[test]
fn le_relazioni_di_catalogo_sono_qualificate() {
    const SOURCES: &[(&str, &str)] = &[
        ("catalog.rs", include_str!("catalog.rs")),
        ("profile.rs", include_str!("profile.rs")),
        ("provider.rs", include_str!("provider.rs")),
        ("read.rs", include_str!("read.rs")),
        ("query.rs", include_str!("query.rs")),
        ("session.rs", include_str!("session.rs")),
        ("transaction.rs", include_str!("transaction.rs")),
        ("types.rs", include_str!("types.rs")),
        ("write.rs", include_str!("write.rs")),
        ("row_diagnostics.rs", include_str!("row_diagnostics.rs")),
        ("pool.rs", include_str!("pool.rs")),
    ];
    const CATALOG: &[&str] = &[
        "columns",
        "statistics",
        "tables",
        "schemata",
        "triggers",
        "user_privileges",
        "schema_privileges",
        "table_privileges",
        "column_privileges",
        "geometry_columns",
        "st_spatial_reference_systems",
        "st_geometry_columns",
        "routines",
        "views",
        "key_column_usage",
        "table_constraints",
        "referential_constraints",
        "check_constraints",
        "processlist",
        "threads",
        "prepared_statements_instances",
        "events_statements_current",
        "user",
        "db",
        "spatial_ref_sys",
    ];
    let mut unqualified = Vec::new();
    for (file, source) in SOURCES {
        let lower = source.to_ascii_lowercase();
        for keyword in ["from ", "join "] {
            for (at, _) in lower.match_indices(keyword) {
                let rest = &lower[at + keyword.len()..];
                let word: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '.')
                    .collect();
                if CATALOG.contains(&word.as_str()) {
                    let line = lower[..at].matches('\n').count() + 1;
                    unqualified.push(format!("{file}:{line}: {word}"));
                }
            }
        }
    }
    assert!(unqualified.is_empty(), "{}", unqualified.join("\n"));
}

/// La tabella delle famiglie di `MySQL` e `MariaDB` (regola F) e le colonne
/// che il server calcola (regola B per colonna, rilettura 13).
#[test]
fn ogni_tipo_ha_la_famiglia_della_tabella_e_le_colonne_generate_sono_derivate() {
    use plenora_database_core::spatial_operand::OperandFamily::{
        Binary, Geometry, Numeric, Other, Temporal, Text,
    };
    for profile in [
        &crate::profile::MysqlProfile as &dyn crate::profile::ProductProfile,
        &crate::profile::MariadbProfile,
    ] {
        for (declared, family) in [
            ("geometry", Geometry),
            ("point", Geometry),
            ("varbinary(100)", Binary),
            ("binary(16)", Binary),
            ("longblob", Binary),
            ("varchar(10)", Text),
            ("char(1)", Text),
            ("longtext", Text),
            ("enum('a','b')", Text),
            ("set('a')", Text),
            ("int unsigned", Numeric),
            ("bigint", Numeric),
            ("decimal(10,2)", Numeric),
            ("double", Numeric),
            ("bit(1)", Numeric),
            ("tinyint(1)", Numeric),
            ("year", Numeric),
            ("date", Temporal),
            ("datetime(6)", Temporal),
            ("timestamp", Temporal),
            ("time", Temporal),
            ("json", Other),
        ] {
            assert_eq!(type_family(declared, profile), family, "{declared}");
        }
    }
    for (extra, expression, computed) in [
        ("VIRTUAL GENERATED", "", true),
        ("STORED GENERATED", "", true),
        ("DEFAULT_GENERATED", "", true),
        ("PERSISTENT", "", true),
        ("", "st_asbinary(`g`)", true),
        ("auto_increment", "", false),
        ("on update CURRENT_TIMESTAMP", "", false),
        ("INVISIBLE", "", false),
        ("", "", false),
    ] {
        assert_eq!(is_computed_column(extra, expression), computed, "{extra}");
    }
}

/// Su `MariaDB` ogni default presente si giudica calcolato (regola B): il
/// catalogo non distingue `DEFAULT (ST_AsBinary(g))` da un letterale.
#[test]
fn un_default_di_mariadb_si_giudica_calcolato() {
    for (default, computed) in [
        (None, false),
        (Some("NULL"), false),
        (Some("null"), false),
        (Some("st_asbinary(`g`)"), true),
        (Some("'x'"), true),
        (Some("7"), true),
    ] {
        assert_eq!(
            mariadb_default_may_compute(default),
            computed,
            "{default:?}"
        );
    }
}

/// Le relazioni derivate: la descrizione della lettura e la lettura del
/// catalogo degli operandi (`VIEW_SQL`) guardano gli stessi motori.
#[test]
fn le_relazioni_derivate_sono_le_stesse_nei_due_percorsi() {
    for engine in DERIVED_ENGINES {
        assert!(VIEW_SQL.contains(&format!("'{engine}'")), "{engine}");
        assert!(is_derived_relation(
            "BASE TABLE",
            Some(&engine.to_lowercase())
        ));
    }
    assert!(is_derived_relation("BASE TABLE", Some("MRG_MyISAM")));
    assert!(is_derived_relation("VIEW", None));
    assert!(is_derived_relation("SYSTEM VIEW", None));
    assert!(!is_derived_relation("BASE TABLE", Some("InnoDB")));
    assert!(!is_derived_relation("BASE TABLE", None));
}
