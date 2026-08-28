use super::*;

/// Un `Update` su chiavi non univoche conferma piu righe di quante ne ha
/// ricevute: una riga in ingresso ne tocca molte nel target. Il contratto
/// lo rifiuta, e il rifiuto deve arrivare **prima** del commit.
///
/// Il documento e ora costruito e validato mentre il rollback e ancora
/// possibile: dopo il commit non resta alcuna operazione fallibile, quindi
/// non esiste il caso "errore su dati gia scritti" da classificare.
#[test]
fn a_document_confirming_more_rows_than_received_is_rejected_before_the_commit() {
    // 3 righe in ingresso, 7 righe aggiornate: chiavi non univoche.
    let invalid = committed_outcome(WriteMode::Update, "pg-update-1", 3, 7);
    let error = invalid
        .validate()
        .expect_err("documento incoerente accettato");
    let shaped = contract_violation(error, "pg-update-1");
    assert_eq!(shaped.category, ErrorCategory::Internal);
    assert_eq!(shaped.phase, ErrorPhase::Write);
    assert_eq!(shaped.execution_id.as_deref(), Some("pg-update-1"));
    // Nessuna dichiarazione di effetto remoto: il rollback che segue la
    // stabilisce, e a quel punto e `RolledBack` — non dati scritti.
    assert_eq!(shaped.remote_effect, RemoteEffect::None);

    // Il caso coerente resta valido per ogni mode.
    for (mode, confirmed) in [
        (WriteMode::Append, 3),
        (WriteMode::Create, 3),
        (WriteMode::Replace, 3),
        (WriteMode::TruncateInsert, 3),
        (WriteMode::Update, 2),
        (WriteMode::Upsert, 3),
        (WriteMode::DeleteByKeys, 3),
    ] {
        committed_outcome(mode, "pg-ok", 3, confirmed)
            .validate()
            .unwrap_or_else(|error| panic!("{mode:?} rifiutata: {error:?}"));
    }
}

fn decode_numeric_binary(payload: &[u8]) -> (bool, u128, u16) {
    assert!(payload.len() >= 8);
    let digits = usize::from(u16::from_be_bytes([payload[0], payload[1]]));
    let weight = i16::from_be_bytes([payload[2], payload[3]]);
    let negative = u16::from_be_bytes([payload[4], payload[5]]) == 0x4000;
    let scale = u16::from_be_bytes([payload[6], payload[7]]);
    assert_eq!(payload.len(), 8 + digits * 2);
    let groups = payload[8..]
        .as_chunks::<2>()
        .0
        .iter()
        .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
        .collect::<Vec<_>>();
    assert!(groups.iter().all(|group| *group < 10_000));
    if groups.is_empty() {
        return (false, 0, scale);
    }
    let group_at = |exponent: i16| -> u16 {
        let index = i32::from(weight) - i32::from(exponent);
        usize::try_from(index)
            .ok()
            .and_then(|index| groups.get(index))
            .copied()
            .unwrap_or(0)
    };
    let mut text = String::new();
    if weight < 0 {
        text.push('0');
    } else {
        for exponent in (0..=weight).rev() {
            let group = group_at(exponent);
            if exponent == weight {
                write!(&mut text, "{group}").expect("format integer group");
            } else {
                write!(&mut text, "{group:04}").expect("format integer group");
            }
        }
    }
    let fractional_groups = usize::from(scale).div_ceil(4);
    for index in 0..fractional_groups {
        let exponent = -i16::try_from(index).expect("fraction index") - 1;
        write!(&mut text, "{:04}", group_at(exponent)).expect("format fraction group");
    }
    text.truncate(text.len() - fractional_groups * 4 + usize::from(scale));
    (
        negative,
        text.parse::<u128>().expect("decoded numeric"),
        scale,
    )
}

#[test]
fn batch_schema_drift_is_rejected_before_encoding() {
    let declared = Arc::new(arrow_schema::Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        true,
    )]));
    let matching = RecordBatch::try_new(
        Arc::clone(&declared),
        vec![Arc::new(Int32Array::from(vec![Some(1)]))],
    )
    .expect("matching batch");
    validate_batch_schema(&matching, &declared).expect("stable schema");

    let equivalent = RecordBatch::try_new(
        Arc::new(declared.as_ref().clone()),
        vec![Arc::new(Int32Array::from(vec![Some(1)]))],
    )
    .expect("equivalent batch");
    validate_batch_schema(&equivalent, &declared).expect("structurally stable schema");

    let drifted = Arc::new(arrow_schema::Schema::new(vec![Field::new(
        "renamed",
        DataType::Int32,
        true,
    )]));
    let drifted_batch =
        RecordBatch::try_new(drifted, vec![Arc::new(Int32Array::from(vec![Some(1)]))])
            .expect("drifted batch");
    assert_eq!(
        validate_batch_schema(&drifted_batch, &declared)
            .expect_err("schema drift")
            .category,
        ErrorCategory::InvalidPlan
    );
}

#[test]
fn arrow_temporal_extremes_return_mapping_errors_without_panicking() {
    let date_field = Field::new("date_value", DataType::Date32, true);
    for extreme in [i32::MIN, i32::MAX] {
        let date = Date32Array::from(vec![Some(extreme)]);
        let mut text = String::new();
        let plan = WriteColumnPlan::compile(&date_field).expect("date plan");
        let date_text_error =
            encode_copy_value(&mut text, &date, &plan, 0).expect_err("date text range");
        assert_eq!(date_text_error.category, ErrorCategory::DataMapping);
        let date_prepared_error = arrow_value(&date, &plan, 0).expect_err("date prepared range");
        assert_eq!(date_prepared_error.category, ErrorCategory::DataMapping);
    }

    for extreme in [i64::MIN, i64::MAX] {
        let timestamp = TimestampMicrosecondArray::from(vec![Some(extreme)]);
        for timezone in [None, Some("UTC".into())] {
            let field = Field::new(
                "timestamp_value",
                DataType::Timestamp(TimeUnit::Microsecond, timezone),
                true,
            );
            let plan = WriteColumnPlan::compile(&field).expect("timestamp plan");
            let mut text = String::new();
            let text_error = encode_copy_value(&mut text, &timestamp, &plan, 0)
                .expect_err("timestamp text range");
            assert_eq!(text_error.category, ErrorCategory::DataMapping);
            let error = arrow_value(&timestamp, &plan, 0).expect_err("timestamp prepared range");
            assert_eq!(error.category, ErrorCategory::DataMapping);
        }
    }
}

#[test]
fn ewkb_header_must_match_spatial_contract() {
    let field = Field::new("geom", DataType::Binary, false).with_metadata(
        std::collections::HashMap::from([
            (
                "ARROW:extension:name".to_owned(),
                GEOARROW_WKB_EXTENSION_NAME.to_owned(),
            ),
            ("plenora.geometry_type".to_owned(), "Point".to_owned()),
            ("plenora.dimensions".to_owned(), "xyz".to_owned()),
            ("plenora.srid".to_owned(), "4326".to_owned()),
        ]),
    );
    let mut point_z_4326 = vec![1_u8];
    point_z_4326.extend_from_slice(&0xa000_0001_u32.to_le_bytes());
    point_z_4326.extend_from_slice(&4326_u32.to_le_bytes());
    point_z_4326.extend_from_slice(&[0_u8; 24]);
    let plan = WriteColumnPlan::compile(&field).expect("spatial plan");
    let inspection = inspect_ewkb_detailed(&point_z_4326, 10, 1).expect("valid point Z EWKB");
    validate_ewkb_contract(inspection.root, &plan).expect("matching contract");

    let mut wrong_srid = point_z_4326.clone();
    wrong_srid[5..9].copy_from_slice(&3857_u32.to_le_bytes());
    let inspection = inspect_ewkb_detailed(&wrong_srid, 10, 1).expect("valid wrong-SRID EWKB");
    assert_eq!(
        validate_ewkb_contract(inspection.root, &plan)
            .expect_err("SRID mismatch")
            .category,
        ErrorCategory::DataMapping
    );

    let mut point_xy_4326 = vec![1_u8];
    point_xy_4326.extend_from_slice(&0x2000_0001_u32.to_le_bytes());
    point_xy_4326.extend_from_slice(&4326_u32.to_le_bytes());
    point_xy_4326.extend_from_slice(&[0_u8; 16]);
    let inspection = inspect_ewkb_detailed(&point_xy_4326, 10, 1).expect("valid point XY EWKB");
    assert_eq!(
        validate_ewkb_contract(inspection.root, &plan)
            .expect_err("dimension mismatch")
            .category,
        ErrorCategory::DataMapping
    );
    assert!(inspect_ewkb_detailed(&[2, 0, 0, 0, 1], 10, 1).is_err());
}

#[test]
fn decimal_format_is_exact() {
    assert_eq!(decimal_string(12_345, 2), "123.45");
    assert_eq!(decimal_string(-1, 2), "-0.01");
    assert_eq!(decimal_string(7, 0), "7");
    assert_eq!(decimal_string(123, -2), "12300");
}

#[test]
fn numeric_binary_codec_is_deterministic_at_boundaries() {
    let cases = [
        (0, 0),
        (1, 0),
        (-1, 0),
        (12_345, 2),
        (-987_654_321, 6),
        (1, 18),
        (999_999_999_999_999_999, 0),
        (123, -2),
        (-123, -4),
        (i128::MIN, 0),
        (i128::MAX, 0),
    ];
    for (value, scale) in cases {
        let mut first = BytesMut::new();
        encode_numeric_binary(value, scale, &mut first).expect("numeric encoding");
        let mut second = BytesMut::new();
        encode_numeric_binary(value, scale, &mut second).expect("numeric encoding");
        assert_eq!(first, second);

        let (negative, decoded, decoded_scale) = decode_numeric_binary(&first);
        let expected_scale = u16::from(scale.max(0).unsigned_abs());
        let expected = if scale < 0 {
            value
                .unsigned_abs()
                .checked_mul(10_u128.pow(u32::from(scale.unsigned_abs())))
                .expect("scaled expected value")
        } else {
            value.unsigned_abs()
        };
        assert_eq!(decoded_scale, expected_scale);
        assert_eq!(decoded, expected);
        assert_eq!(negative, value < 0);
    }
}

#[test]
fn binary_copy_jsonb_accepts_canonical_native_type_metadata() {
    let field = Field::new("payload", DataType::Utf8, true).with_metadata(
        std::collections::HashMap::from([(
            protocol::POSTGRES_NATIVE_TYPE.to_owned(),
            "jsonb".to_owned(),
        )]),
    );
    let array = StringArray::from(vec![Some(r#"{"safe":true}"#)]);
    let plan = WriteColumnPlan::compile(&field).expect("JSONB plan");
    let value = binary_copy_value(&array, &plan, &Type::JSONB, 0).expect("JSONB binary value");
    let mut encoded = BytesMut::new();
    assert!(matches!(
        value
            .to_sql_checked(&Type::JSONB, &mut encoded)
            .expect("JSONB binary encoding"),
        IsNull::No
    ));
    assert_eq!(encoded.first(), Some(&1));
}

#[test]
fn prepared_placeholders_accept_canonical_native_metadata() {
    let jsonb = Field::new("payload", DataType::Utf8, true).with_metadata(
        std::collections::HashMap::from([(
            protocol::POSTGRES_NATIVE_TYPE.to_owned(),
            "jsonb".to_owned(),
        )]),
    );
    let jsonb_plan = WriteColumnPlan::compile(&jsonb).expect("JSONB plan");
    assert_eq!(placeholder_expression(&jsonb_plan, 1), "$1::text::jsonb");

    let domain =
        Field::new("code", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([
            (
                protocol::POSTGRES_NATIVE_DECLARATION.to_owned(),
                "public.safe_code".to_owned(),
            ),
            (protocol::POSTGRES_TYPE_KIND.to_owned(), "d".to_owned()),
        ]));
    let domain_plan = WriteColumnPlan::compile(&domain).expect("domain plan");
    assert_eq!(
        placeholder_expression(&domain_plan, 2),
        "$2::text::\"public\".\"safe_code\""
    );
}

#[test]
fn numeric_text_parser_rejects_ambiguous_input() {
    assert_eq!(
        parse_numeric_components("+.5").expect("leading dot"),
        (5, 1)
    );
    assert_eq!(
        parse_numeric_components("1.").expect("trailing dot"),
        (1, 0)
    );
    for invalid in ["", "-", "+", "--1", "++1", "+-1", "1.2.3", " 1", "1e2"] {
        assert!(
            parse_numeric_components(invalid).is_err(),
            "accepted invalid numeric: {invalid}"
        );
    }
}

#[test]
fn postgres_range_and_composite_escaping_is_exact() {
    let mut encoded = String::new();
    append_quoted_postgres_value(&mut encoded, "a,\"b\\c\n'tè");
    assert_eq!(encoded, "\"a,\\\"b\\\\c\n'tè\"");
}

#[test]
fn interval_text_is_portable_across_postgres_versions() {
    assert_eq!(
        interval_text(&PostgresIntervalBinary {
            months: 0,
            days: 2,
            microseconds: 11_045_000_000,
        }),
        "0 mons 2 days 03:04:05.000000"
    );
    assert_eq!(
        interval_text(&PostgresIntervalBinary {
            months: -1,
            days: 0,
            microseconds: -3_723_000_004,
        }),
        "-1 mons 0 days -01:02:03.000004"
    );
}

#[test]
fn cancellation_reports_verified_rollback_or_requires_recovery() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let rolled_back = cancelled_write_error(&cancellation, true);
    assert_eq!(rolled_back.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(rolled_back.retry, RetryDisposition::Never);

    let unknown = cancelled_write_error(&cancellation, false);
    assert_eq!(unknown.remote_effect, RemoteEffect::Unknown);
    assert_eq!(unknown.retry, RetryDisposition::RequiresRecovery);
}

#[test]
fn deadline_is_a_timeout_with_the_same_rollback_guarantees() {
    let deadline = CancellationToken::new();
    deadline.cancel_due_to_deadline();
    let rolled_back = cancelled_write_error(&deadline, true);
    assert_eq!(rolled_back.category, ErrorCategory::Timeout);
    assert_eq!(rolled_back.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(rolled_back.retry, RetryDisposition::Never);

    let commit = commit_interruption_error(&deadline, "pg-test-1");
    assert_eq!(commit.category, ErrorCategory::Timeout);
    assert_eq!(commit.phase, ErrorPhase::Commit);
    assert_eq!(commit.remote_effect, RemoteEffect::Unknown);
    assert_eq!(commit.retry, RetryDisposition::RequiresRecovery);
    assert_eq!(commit.execution_id.as_deref(), Some("pg-test-1"));
}

#[test]
fn resource_failure_reports_verified_rollback_or_requires_recovery() {
    let cause = DatabaseError::resource_limit("budget esaurito");
    let rolled_back = resource_write_error(&cause, true);
    assert_eq!(rolled_back.category, ErrorCategory::ResourceLimit);
    assert_eq!(rolled_back.phase, ErrorPhase::Write);
    assert_eq!(rolled_back.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(rolled_back.retry, RetryDisposition::Never);

    let unknown = resource_write_error(&cause, false);
    assert_eq!(unknown.remote_effect, RemoteEffect::Unknown);
    assert_eq!(unknown.retry, RetryDisposition::RequiresRecovery);
}

#[test]
fn unknown_write_outcome_is_valid_and_never_retryable() {
    let outcome = unknown_write_outcome(
        "pg-test-unknown".to_owned(),
        7,
        "verificare lo stato remoto",
    );
    outcome.validate().expect("valid unknown outcome");
    assert_eq!(outcome.status, WriteStatus::OutcomeUnknown);
    assert_eq!(outcome.rows.received, 7);
    assert_eq!(outcome.rows.confirmed, 0);
    assert!(
        !outcome
            .recovery
            .as_ref()
            .expect("recovery")
            .automatic_retry_allowed
    );
}

#[test]
fn divergent_canonical_and_legacy_metadata_is_rejected() {
    let field = Field::new("geom", DataType::Binary, false).with_metadata(
        [
            (protocol::GEOMETRY_DIMENSIONS.to_owned(), "xy".to_owned()),
            ("plenora.dimensions".to_owned(), "xyz".to_owned()),
        ]
        .into_iter()
        .collect(),
    );
    let error = FieldContract::parse(&field).expect_err("metadata divergence");
    assert_eq!(error.category, ErrorCategory::DataMapping);
}

#[test]
fn incoherent_crs_metadata_is_rejected_before_preflight() {
    let base = [
        (
            protocol::GEOARROW_EXTENSION_NAME.to_owned(),
            GEOARROW_WKB_EXTENSION_NAME.to_owned(),
        ),
        (
            protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
            "resolved".to_owned(),
        ),
        (protocol::GEOMETRY_SRID.to_owned(), "4326".to_owned()),
        (protocol::GEOMETRY_CRS_ID.to_owned(), "EPSG:4326".to_owned()),
    ]
    .into_iter()
    .collect();
    let resolved_without_id = Field::new("geom", DataType::Binary, false).with_metadata(base);
    assert!(FieldContract::parse(&resolved_without_id).is_err());

    let missing_with_srid = Field::new("geom", DataType::Binary, false).with_metadata(
        [
            (
                protocol::GEOARROW_EXTENSION_NAME.to_owned(),
                GEOARROW_WKB_EXTENSION_NAME.to_owned(),
            ),
            (
                protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
                "missing".to_owned(),
            ),
            (protocol::GEOMETRY_SRID.to_owned(), "4326".to_owned()),
        ]
        .into_iter()
        .collect(),
    );
    assert!(FieldContract::parse(&missing_with_srid).is_err());
}
