use super::*;
use crate::plan::{ObjectRef, TransactionProfile, WriteMode};
use arrow_array::{ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray};
use arrow_schema::{Field, Schema};
use std::sync::Arc;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
        Field::new("b", DataType::Binary, true),
        Field::new("f", DataType::Float64, true),
    ]))
}

fn batch(v: Option<&str>, b: Option<&[u8]>, f: Option<f64>) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(StringArray::from(vec![v])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![b])) as ArrayRef,
            Arc::new(Float64Array::from(vec![f])) as ArrayRef,
        ],
    )
    .expect("batch")
}

fn operation(mode: WriteMode, policy: MappingPolicy, keys: &[&str]) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: None,
            object: "t".to_owned(),
        },
        mode,
        mapping_policy: policy,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: keys.iter().map(|key| (*key).to_owned()).collect(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

fn columns() -> Vec<ColumnRewrites> {
    vec![
        ColumnRewrites {
            field: 1,
            target_type: "VARCHAR2".to_owned(),
            rewrites: vec![
                StoreRewrite::EmptyToNull,
                StoreRewrite::PadText { chars: 3 },
            ],
        },
        ColumnRewrites {
            field: 2,
            target_type: "RAW".to_owned(),
            rewrites: vec![StoreRewrite::EmptyToNull],
        },
        ColumnRewrites {
            field: 3,
            target_type: "FLOAT".to_owned(),
            rewrites: vec![
                StoreRewrite::Underflow { min_abs: 1e-129 },
                StoreRewrite::NegativeZero,
            ],
        },
    ]
}

fn guard(policy: MappingPolicy, keys: &[&str]) -> RewriteGuard {
    let mode = if keys.is_empty() {
        WriteMode::Append
    } else {
        WriteMode::Upsert
    };
    RewriteGuard::new(
        ProviderKind::Oracle,
        &schema(),
        &operation(mode, policy, keys),
        columns(),
    )
    .expect("guard")
}

#[test]
fn con_strict_ogni_valore_riscritto_si_rifiuta() {
    let strict = guard(MappingPolicy::Strict, &[]);
    for rewritten in [
        batch(Some(""), None, None),
        batch(Some("ab"), None, None),
        batch(None, Some(b""), None),
        batch(None, None, Some(1e-200)),
        batch(None, None, Some(-0.0)),
    ] {
        let error = strict.check(&rewritten).expect_err("riscritto");
        assert_eq!(error.category, ErrorCategory::DataMapping);
        assert!(error.message.contains(REWRITTEN_VALUE));
    }
    for intact in [
        batch(Some("abc"), Some(b"\0"), Some(0.0)),
        batch(None, None, Some(1e-100)),
        batch(Some("abcd"), Some(b"x"), Some(-1.0)),
    ] {
        strict.check(&intact).expect("intatto");
    }
    assert!(strict.losses().is_empty());
}

#[test]
fn con_le_altre_politiche_la_perdita_si_dichiara_e_il_valore_passa() {
    for policy in [
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        let lenient = guard(policy, &[]);
        lenient
            .check(&batch(Some(""), Some(b""), Some(-0.0)))
            .expect("dichiarato");
        let losses = lenient.losses();
        assert_eq!(losses.len(), 5);
        assert!(losses.iter().all(
            |loss| loss.severity == LossSeverity::DataLoss && loss.reason.contains("regola S")
        ));
        assert_eq!(
            losses
                .iter()
                .filter(|loss| loss.category == LossCategory::Nullability)
                .map(|loss| loss.field_id)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }
}

#[test]
fn una_chiave_riscritta_si_rifiuta_con_ogni_politica() {
    for policy in [
        MappingPolicy::Strict,
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        let keyed = guard(policy, &["v"]);
        let error = keyed
            .check(&batch(Some(""), None, None))
            .expect_err("chiave vuota");
        assert!(error.message.contains(REWRITTEN_KEY));
        keyed
            .check(&batch(Some("abc"), None, None))
            .expect("chiave intatta");
        // La chiave non ha perdite: il suo valore riscritto si rifiuta.
        assert!(keyed.losses().iter().all(|loss| loss.field_id != 1));
    }
}

#[test]
fn una_riscrittura_su_un_tipo_estraneo_non_passa_in_silenzio() {
    let error = RewriteGuard::new(
        ProviderKind::Oracle,
        &schema(),
        &operation(WriteMode::Append, MappingPolicy::Strict, &[]),
        vec![ColumnRewrites {
            field: 0,
            target_type: "NUMBER".to_owned(),
            rewrites: vec![StoreRewrite::EmptyToNull],
        }],
    )
    .expect_err("intero");
    assert_eq!(error.category, ErrorCategory::Internal);
    RewriteGuard::new(
        ProviderKind::Oracle,
        &schema(),
        &operation(WriteMode::Append, MappingPolicy::Strict, &[]),
        vec![ColumnRewrites {
            field: 9,
            target_type: "NUMBER".to_owned(),
            rewrites: vec![StoreRewrite::EmptyToNull],
        }],
    )
    .expect_err("fuori dallo schema");
}

#[test]
fn le_riscritture_dei_binari_e_degli_spazi() {
    let text = StringArray::from(vec!["a ", "a", ""]);
    let bytes = BinaryArray::from(vec![&b"a\0"[..], &b"a"[..], &b""[..]]);
    assert!(StoreRewrite::StripTrailing.rewrites(&text, 0));
    assert!(!StoreRewrite::StripTrailing.rewrites(&text, 1));
    assert!(!StoreRewrite::StripTrailing.rewrites(&text, 2));
    assert!(StoreRewrite::StripTrailing.rewrites(&bytes, 0));
    assert!(!StoreRewrite::StripTrailing.rewrites(&bytes, 1));
    assert!(StoreRewrite::PadBinary { bytes: 2 }.rewrites(&bytes, 1));
    assert!(!StoreRewrite::PadBinary { bytes: 2 }.rewrites(&bytes, 0));
    // I caratteri, non i byte.
    let accented = StringArray::from(vec!["éé"]);
    assert!(!StoreRewrite::PadText { chars: 2 }.rewrites(&accented, 0));
    assert!(StoreRewrite::PadText { chars: 3 }.rewrites(&accented, 0));
}

#[test]
fn le_classi_della_regola_a_riconoscono_i_valori_che_si_riscrivono() {
    use arrow_array::TimestampMicrosecondArray;
    let instants = TimestampMicrosecondArray::from(vec![0, 123_000, 123_456, -1, 999_999]);
    let three = StoreRewrite::FractionalSeconds { digits: 3 };
    for (row, rewritten) in [false, false, true, true, true].iter().enumerate() {
        assert_eq!(three.rewrites(&instants, row), *rewritten, "riga {row}");
    }
    let six = StoreRewrite::FractionalSeconds { digits: 6 };
    assert!((0..5).all(|row| !six.rewrites(&instants, row)));

    let text = StringArray::from(vec!["abc", "é", "😀"]);
    assert!(!StoreRewrite::NonAscii.rewrites(&text, 0));
    assert!(StoreRewrite::NonAscii.rewrites(&text, 1));
    assert!(StoreRewrite::NonAscii.rewrites(&text, 2));

    let documents = StringArray::from(vec![
        r#"{"a": 1, "b": [0, -12, 123456789012345]}"#,
        r#"{"a": 1.0}"#,
        r#"{"a": 1e2}"#,
        r#"{"a": -0}"#,
        r#"{"a": 1234567890123456}"#,
        r#"{"a": "1.0 dentro una stringa \" 1e2"}"#,
        "[01]",
    ]);
    for (row, rewritten) in [false, true, true, true, true, false, true]
        .iter()
        .enumerate()
    {
        assert_eq!(
            StoreRewrite::JsonNumbers.rewrites(&documents, row),
            *rewritten,
            "riga {row}"
        );
    }
    let nullable = StringArray::from(vec![None, Some("")]);
    assert!(!StoreRewrite::Every.rewrites(&nullable, 0));
    assert!(StoreRewrite::Every.rewrites(&nullable, 1));
    assert!(StoreRewrite::Every.admits(&DataType::Decimal128(5, 2)));
    assert!(!StoreRewrite::FractionalSeconds { digits: 3 }.admits(&DataType::Utf8));
}
