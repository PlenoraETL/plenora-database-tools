//! Regressione audit 3: precisione, valore e identita delle chiavi.
use super::*;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::transaction::{Statement, TransactionOptions};

async fn snapshot(provider: &dyn Provider, secret: &SecretString, sql: &str) -> Vec<String> {
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let mut tx = provider
        .begin_transaction(
            secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .unwrap();
    let rows = tx.query(&Statement::new(sql), &cancellation).await.unwrap();
    tx.rollback(&cancellation).await.unwrap();
    rows.iter()
        .map(|row| match row.values().first().unwrap() {
            ParameterValue::String(value) => value.clone(),
            other => panic!("proiezione ASCII attesa: {other:?}"),
        })
        .collect()
}

#[tokio::test]
#[ignore = "richiede Db2 live"]
async fn live_sospetti_timestamp_precision_identity() {
    let (provider, secret) = provider(&database());
    let table = "PLN_S900_TS";
    let qualified = format!("{SCHEMA}.{table}");
    for precision in [0, 3, 6] {
        for (mode, keys, id) in [
            (WriteMode::Append, &[][..], 30),
            (WriteMode::Upsert, &["ID"][..], 10),
            (WriteMode::Upsert, &["V"][..], 30),
        ] {
            drop_table(&provider, &secret, table).await;
            let sql = format!("CREATE TABLE {qualified} (ID BIGINT NOT NULL PRIMARY KEY, V TIMESTAMP({precision}) NOT NULL UNIQUE)");
            ddl(&provider, &secret, &sql).await;
            // Due vicini: una chiave troncata/arrotondata non deve aggiornare ID=10.
            for (key, fraction) in [(10, "123000"), (20, "999000")] {
                let fraction = if precision == 0 {
                    "000000"
                } else if precision == 6 && key == 10 && keys == ["V"] {
                    "123456"
                } else {
                    fraction
                };
                let sql = format!("INSERT INTO {qualified} VALUES ({key}, TIMESTAMP('2026-01-01-00.00.00.{fraction}'))");
                let sql = if key == 20 {
                    sql.replace("00.00.00", "00.00.02")
                        .replace("00:00:00", "00:00:02")
                } else {
                    sql
                };
                ddl(&provider, &secret, &sql).await;
            }
            let projection =
                format!("SELECT VARCHAR(ID) || ':' || VARCHAR(V) FROM {qualified} ORDER BY ID");
            let before = snapshot(&provider, &secret, &projection).await;
            assert_eq!(before.len(), 2, "due righe sentinella presenti");
            let value = Arc::new(TimestampMicrosecondArray::from(vec![
                1_767_225_600_123_456_i64,
            ]));
            let result = write_declared(
                &provider,
                &secret,
                table,
                mode,
                keys,
                ids(vec![id]),
                (value, false),
            )
            .await;
            let after = snapshot(&provider, &secret, &projection).await;
            if precision < 6 {
                let error = result.expect_err("riduzione temporale rifiutata");
                assert_refusal(
                    &error,
                    ErrorCategory::DataMapping,
                    ErrorPhase::Prepare,
                    "timestamp oltre la precisione dell'operando: il server la arrotonderebbe",
                );
                // La precisione temporale ha un diagnostico specifico; il writer
                // conferma il rollback anche se il rifiuto nasce in Prepare.
                assert_eq!(error.remote_effect, RemoteEffect::RolledBack);
                assert_eq!(
                    after, before,
                    "rifiuto senza alterazioni, precisione {precision}"
                );
            } else {
                assert_eq!(result.unwrap().status, WriteStatus::Committed);
                let expected_id = if mode == WriteMode::Upsert && keys == ["ID"] {
                    10
                } else {
                    30
                };
                let mut expected = before.clone();
                if mode == WriteMode::Upsert {
                    expected.remove(0);
                }
                expected.push(format!("{expected_id}:2026-01-01-00.00.00.123456"));
                expected.sort();
                assert_eq!(after, expected, "identita e valori di tutte le righe");
            }
        }
    }
    drop_table(&provider, &secret, table).await;
}

#[tokio::test]
#[ignore = "richiede Db2 live"]
#[allow(clippy::too_many_lines)]
async fn live_sospetti_numeric_scalar_array_bits() {
    use plenora_database_core::transaction::TransactionScope;
    let (provider, secret) = provider(&database());
    let table = "PLN_S900_NUM";
    drop_table(&provider, &secret, table).await;
    ddl(
        &provider,
        &secret,
        &format!(
            "CREATE TABLE {SCHEMA}.{table} (ID BIGINT NOT NULL PRIMARY KEY, F DOUBLE, D DECIMAL(31,15))"
        ),
    )
    .await;
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let values = [
        1.234_567_890_123_456_7,
        f64::from_bits(1.234_567_890_123_456_7_f64.to_bits() + 1),
        0.0,
        f64::MIN_POSITIVE,
        f64::MAX,
    ];
    for array in [false, true] {
        let mut tx = crate::transaction::Db2Transaction::begin(
            provider.config(),
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .unwrap();
        let sql = format!("INSERT INTO {SCHEMA}.{table} VALUES (?, ?, ?)");
        let rows: Vec<_> = values
            .iter()
            .enumerate()
            .map(|(i, value)| {
                vec![
                    ParameterValue::I64(i64::try_from(i).unwrap()),
                    ParameterValue::F64(*value),
                    ParameterValue::Decimal("9999999999999999.999999999999999".into()),
                ]
            })
            .collect();
        if array {
            tx.execute_parameter_array(sql.clone(), rows, &cancellation)
                .await
                .unwrap();
        } else {
            for row in rows {
                tx.execute(&Statement::new(&sql).with_params(row), &cancellation)
                    .await
                    .unwrap();
            }
        }
        let observed = tx
            .query(
                &Statement::new(format!(
                    "SELECT F, HEX(F), VARCHAR(D) FROM {SCHEMA}.{table} ORDER BY ID"
                )),
                &cancellation,
            )
            .await
            .unwrap();
        assert_eq!(observed.len(), values.len());
        for (row, value) in observed.iter().zip(values) {
            let expected = hex(&value.to_le_bytes());
            assert_eq!(row.values()[1], ParameterValue::String(expected));
            let ParameterValue::F64(read) = row.values()[0] else {
                panic!("DOUBLE attendu");
            };
            assert_eq!(read.to_bits(), value.to_bits());
            assert_eq!(
                row.values()[2],
                ParameterValue::String("9999999999999999.999999999999999".into())
            );
        }
        // I rifiuti non devono inserire la riga sentinella, in nessun trasporto.
        for value in [-0.0, f64::from_bits(1), -f64::from_bits(1)] {
            let row = vec![
                ParameterValue::I64(99),
                ParameterValue::F64(value),
                ParameterValue::Decimal("0".into()),
            ];
            let result = if array {
                tx.execute_parameter_array(sql.clone(), vec![row], &cancellation)
                    .await
            } else {
                tx.execute(&Statement::new(&sql).with_params(row), &cancellation)
                    .await
            };
            let error = result.expect_err("double non rappresentabile");
            let message = if value.is_subnormal() {
                "double subnormale fuori dal dominio DOUBLE Db2"
            } else {
                "double -0.0: Db2 conserva lo zero senza segno"
            };
            assert_refusal(
                &error,
                ErrorCategory::DataMapping,
                ErrorPhase::Prepare,
                message,
            );
        }
        for decimal in [
            "10000000000000000.000000000000000",
            "0.0000000000000001",
            "-10000000000000000.000000000000000",
        ] {
            let row = vec![
                ParameterValue::I64(99),
                ParameterValue::F64(0.0),
                ParameterValue::Decimal(decimal.into()),
            ];
            let result = if array {
                tx.execute_parameter_array(sql.clone(), vec![row], &cancellation)
                    .await
            } else {
                tx.execute(&Statement::new(&sql).with_params(row), &cancellation)
                    .await
            };
            let error = result.expect_err("limite decimal non esatto");
            assert_refusal(
                &error,
                ErrorCategory::DataMapping,
                ErrorPhase::Prepare,
                "parametro non rappresentabile esatto nel tipo dell'operando",
            );
        }
        let rows = tx
            .query(
                &Statement::new(format!("SELECT ID FROM {SCHEMA}.{table} WHERE ID=99")),
                &cancellation,
            )
            .await
            .unwrap();
        assert!(rows.is_empty());
        Box::new(tx).rollback(&cancellation).await.unwrap();
    }
    // Inserimento SQL indipendente dal writer; il decoder deve rendere gli stessi bit.
    ddl(
        &provider,
        &secret,
        &format!(
            "INSERT INTO {SCHEMA}.{table} VALUES (1, CAST('1.2345678901234567' AS DOUBLE), 0)"
        ),
    )
    .await;
    assert_eq!(
        snapshot(
            &provider,
            &secret,
            &format!("SELECT HEX(F) FROM {SCHEMA}.{table}")
        )
        .await,
        [hex(&values[0].to_le_bytes())]
    );
    let batches = arrow_read(&provider, &secret, table, &["F"]).await.unwrap();
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<plenora_database_core::arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(column.value(0).to_bits(), values[0].to_bits());
    drop_table(&provider, &secret, table).await;
}

#[tokio::test]
#[ignore = "richiede Db2 live"]
#[allow(clippy::too_many_lines)]
async fn live_sospetti_bit_data_catalog_and_bytes() {
    use plenora_database_core::transaction::TransactionScope;
    let (provider, secret) = provider(&database());
    let table = "PLN_S900_BIT";
    drop_table(&provider, &secret, table).await;
    ddl(&provider, &secret, &format!("CREATE TABLE {SCHEMA}.{table} (ID BIGINT NOT NULL PRIMARY KEY, T VARCHAR(16), B VARCHAR(16) FOR BIT DATA)")).await;
    let (connection, timeout) = open_connection(provider.config(), &secret).unwrap();
    let description = crate::catalog::describe_object(&connection, timeout, SCHEMA, table).unwrap();
    assert_eq!(description.columns[1].data_type, "VARCHAR");
    assert_eq!(description.columns[2].data_type, "VARCHAR FOR BIT DATA");
    assert_eq!(
        crate::Db2ColumnSpec::from_catalog(&description.columns[1], 1024)
            .unwrap()
            .kind,
        crate::Db2ColumnKind::Utf8
    );
    let error = crate::Db2ColumnSpec::from_catalog(&description.columns[2], 1024).unwrap_err();
    assert_refusal(
        &error,
        ErrorCategory::Unsupported,
        ErrorPhase::Prepare,
        "tipo Db2 non ancora qualificato per Arrow",
    );
    // Arrow binario resta non qualificato: nessuna promozione implicita a Utf8.
    drop(connection);
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let mut tx = crate::transaction::Db2Transaction::begin(
        provider.config(),
        &secret,
        &TransactionOptions::default(),
        &budget,
        &cancellation,
    )
    .await
    .unwrap();
    let values: [&[u8]; 4] = [b"\0", &[0xAB, 0x01], &[0xFF, 0x80], b"AB01"];
    for (id, bytes) in values.iter().enumerate() {
        tx.execute(
            &Statement::new(format!("INSERT INTO {SCHEMA}.{table} VALUES (?, ?, ?)")).with_params(
                vec![
                    ParameterValue::I64(i64::try_from(id).unwrap()),
                    ParameterValue::String("AB01".into()),
                    ParameterValue::Bytes(bytes.to_vec()),
                ],
            ),
            &cancellation,
        )
        .await
        .unwrap();
    }
    let rows = tx
        .query(
            &Statement::new(format!(
                "SELECT HEX(T), HEX(B), LENGTH(B), B FROM {SCHEMA}.{table} ORDER BY ID"
            )),
            &cancellation,
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), values.len());
    for (row, bytes) in rows.iter().zip(values) {
        assert_eq!(row.values()[0], ParameterValue::String("41423031".into()));
        assert_eq!(row.values()[1], ParameterValue::String(hex(bytes)));
        assert_eq!(
            row.values()[2],
            ParameterValue::I32(i32::try_from(bytes.len()).unwrap())
        );
        assert_eq!(row.values()[3], ParameterValue::Bytes(bytes.to_vec()));
    }
    Box::new(tx).rollback(&cancellation).await.unwrap();
    ddl(
        &provider,
        &secret,
        &format!("INSERT INTO {SCHEMA}.{table} VALUES (9, 'AB01', X'AB01')"),
    )
    .await;
    let mut tx = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .unwrap();
    let rows = tx
        .query(
            &Statement::new(format!("SELECT B FROM {SCHEMA}.{table} WHERE ID=9")),
            &cancellation,
        )
        .await
        .unwrap();
    assert_eq!(rows[0].values()[0], ParameterValue::Bytes(vec![0xAB, 1]));
    tx.rollback(&cancellation).await.unwrap();
    let batches = arrow_read(&provider, &secret, table, &["T"]).await.unwrap();
    assert_eq!(batches[0].column(0).data_type(), &DataType::Utf8);
    let error = arrow_read(&provider, &secret, table, &["B"])
        .await
        .unwrap_err();
    assert_refusal(
        &error,
        ErrorCategory::Unsupported,
        ErrorPhase::Prepare,
        "tipo Db2 non ancora qualificato per Arrow",
    );
    drop_table(&provider, &secret, table).await;
    ddl(
        &provider,
        &secret,
        &format!(
            "CREATE TABLE {SCHEMA}.{table} (ID BIGINT NOT NULL PRIMARY KEY, V VARCHAR(16) FOR BIT DATA)"
        ),
    )
    .await;
    let bytes = Arc::new(plenora_database_core::arrow::array::BinaryArray::from(
        vec![&[0xAB, 1][..]],
    ));
    let error = write(
        &provider,
        &secret,
        table,
        WriteMode::Append,
        &[],
        ids(vec![1]),
        bytes,
    )
    .await
    .unwrap_err();
    assert_refusal(
        &error,
        ErrorCategory::Unsupported,
        ErrorPhase::Prepare,
        "tipo Arrow non qualificato per write Db2",
    );
    assert!(snapshot(
        &provider,
        &secret,
        &format!("SELECT HEX(V) FROM {SCHEMA}.{table}")
    )
    .await
    .is_empty());
    drop_table(&provider, &secret, table).await;
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut text, byte| {
        write!(text, "{byte:02X}").unwrap();
        text
    })
}

async fn arrow_read(
    provider: &Db2Provider,
    secret: &SecretString,
    table: &str,
    columns: &[&str],
) -> Result<Vec<RecordBatch>, DatabaseError> {
    let operation = plenora_database_core::plan::ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some(SCHEMA.into()),
            object: table.into(),
        },
        projection: columns.iter().map(|column| (*column).into()).collect(),
        order_by: Vec::new(),
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let mut stream = provider
        .read(
            secret,
            &operation,
            &plenora_database_core::provider::ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next_batch(&cancellation).await? {
        batches.push(batch);
    }
    Ok(batches)
}

fn assert_refusal(
    error: &DatabaseError,
    category: ErrorCategory,
    phase: ErrorPhase,
    message: &str,
) {
    assert_eq!(error.category, category, "{error:?}");
    assert_eq!(error.phase, phase, "{error:?}");
    assert_eq!(error.message, message, "{error:?}");
}
