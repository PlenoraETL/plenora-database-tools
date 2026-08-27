use super::*;

/// Le cinque colonne che entrambe le query di indice espongono, nello
/// stesso ordine in cui il contratto degli alias le dichiara.
fn index_row(name: &str, sequence: i64, column: Value, expression: Value) -> Row {
    let wire: std::sync::Arc<[mysql_async::Column]> = [
        "index_name",
        "non_unique",
        "seq_in_index",
        "column_name",
        "expression",
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
            },
        ],
    )
    .expect("index changed token");
    assert_eq!(first, same);
    assert_ne!(first, changed);
    assert_ne!(first, index_changed);
}
