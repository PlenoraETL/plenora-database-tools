use super::*;

fn outcomes_for(names: &[String], rewritten: &[&str], refused: &[&str]) -> Vec<ValueOutcome> {
    names
        .iter()
        .map(|name| {
            if rewritten.contains(&name.as_str()) {
                ValueOutcome::Rewritten
            } else if refused.contains(&name.as_str()) {
                ValueOutcome::Refused
            } else {
                ValueOutcome::Exact
            }
        })
        .collect()
}

#[test]
fn timestamp_3_si_spiega_con_le_cifre_frazionarie() {
    let spec = ProbeSpec::new(
        "TIMESTAMP(3)",
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    let battery = battery(&spec).expect("batteria");
    // TIMESTAMP(3): ogni istante con cifre oltre il millesimo si riscrive.
    let rewritten = battery
        .names
        .iter()
        .enumerate()
        .filter(|(row, _)| {
            !battery.values.is_null(*row)
                && subsecond_ns(battery.values.as_ref(), *row).expect("istante") % 1_000_000 != 0
        })
        .map(|(_, name)| name.as_str())
        .collect::<Vec<_>>();
    assert!(rewritten.contains(&"cifre_4") && !rewritten.contains(&"cifre_3"));
    let outcomes = outcomes_for(&battery.names, &rewritten, &[]);
    let (classes, excess) = classify(
        &spec,
        TargetDimensions::default(),
        battery.values.as_ref(),
        &outcomes,
    );
    assert_eq!(classes, vec![MeasuredRewrite::FrazioneSecondi { cifre: 3 }]);
    assert!(excess.is_empty());
}

#[test]
fn testo_vuoto_e_riempimento_si_spiegano_senza_eccesso() {
    let spec = ProbeSpec::new("CHAR(4 CHAR)", DataType::Utf8).with_dimension(4);
    let battery = battery(&spec).expect("batteria");
    let dimensions = TargetDimensions {
        chars: Some(4),
        bytes: Some(16),
    };
    let short = battery
        .names
        .iter()
        .enumerate()
        .filter(|(row, _)| {
            let array = battery.values.as_string::<i32>();
            !array.is_null(*row) && array.value(*row).chars().count() < 4
        })
        .map(|(_, name)| name.as_str())
        .collect::<Vec<_>>();
    let outcomes = outcomes_for(&battery.names, &short, &["oltre"]);
    // Oracle: il testo vuoto diventa NULL, i piu corti si completano.
    let to_null = battery
        .names
        .iter()
        .map(|name| name == "vuoto")
        .collect::<Vec<_>>();
    let (classes, excess) = classify_with(
        &spec,
        dimensions,
        battery.values.as_ref(),
        &outcomes,
        &to_null,
    );
    assert_eq!(
        classes,
        vec![MeasuredRewrite::RiempieTesto, MeasuredRewrite::VuotoANull]
    );
    assert!(excess.is_empty());
    // SQL Server: il testo vuoto si completa con spazi, come gli altri: un
    // riempimento, non un NULL, a ogni lunghezza.
    let (classes, _) = classify(&spec, dimensions, battery.values.as_ref(), &outcomes);
    assert_eq!(classes, vec![MeasuredRewrite::RiempieTesto]);
}

#[test]
fn una_riscrittura_che_nessuna_classe_spiega_e_ogni() {
    let spec = ProbeSpec::new("DOUBLE(10,2)", DataType::Float64);
    let battery = battery(&spec).expect("batteria");
    let outcomes = outcomes_for(&battery.names, &["terzo", "zero_negativo"], &[]);
    let (classes, excess) = classify(
        &spec,
        TargetDimensions::default(),
        battery.values.as_ref(),
        &outcomes,
    );
    assert_eq!(classes, vec![MeasuredRewrite::Ogni]);
    assert!(!excess.is_empty());
}

#[test]
fn underflow_e_zero_negativo_dal_minimo_esatto() {
    let spec = ProbeSpec::new("FLOAT", DataType::Float64);
    let battery = battery(&spec).expect("batteria");
    let tiny = [
        "zero_negativo",
        "normale_minimo",
        "subnormale_minimo",
        "subnormale",
        "e_meno_300",
        "e_meno_200",
        "e_meno_150",
        "e_meno_131",
        "meno_e_meno_200",
    ];
    let outcomes = outcomes_for(&battery.names, &tiny, &["infinito", "meno_infinito", "nan"]);
    let (classes, excess) = classify(
        &spec,
        TargetDimensions::default(),
        battery.values.as_ref(),
        &outcomes,
    );
    assert_eq!(
        classes,
        vec![
            MeasuredRewrite::Underflow { min_abs: 1e-130 },
            MeasuredRewrite::ZeroNegativo
        ]
    );
    assert!(excess.is_empty());
}

#[test]
fn i_documenti_si_confrontano_come_documenti() {
    let written = StringArray::from(vec![r#"{"b":1,"a":"é"}"#, r#"{"a":1.0}"#]);
    let read = StringArray::from(vec![r#"{"a":"é","b":1}"#, r#"{"a":1}"#]);
    assert!(same_value(&written, 0, &read, 0, true));
    assert!(!same_value(&written, 1, &read, 1, true));
    // Come testo, l'ordine conta.
    assert!(!same_value(&written, 0, &read, 0, false));
    // Una chiave ripetuta resta.
    let repeated = StringArray::from(vec![r#"{"a":1,"a":2}"#, r#"{"a":2}"#]);
    assert!(!same_value(&repeated, 0, &repeated, 1, true));
}

#[test]
fn i_numeri_si_confrontano_per_valore_e_i_float_per_bit() {
    let small = Int16Array::from(vec![7_i16]);
    let wide = Int64Array::from(vec![7_i64]);
    assert!(same_value(&small, 0, &wide, 0, false));
    let decimal = Decimal128Array::from(vec![150_i128])
        .with_precision_and_scale(5, 2)
        .expect("decimale");
    let other = Decimal128Array::from(vec![15_i128])
        .with_precision_and_scale(38, 1)
        .expect("decimale");
    assert!(same_value(&decimal, 0, &other, 0, false));
    let negative = Float32Array::from(vec![-0.0_f32]);
    let positive = Float64Array::from(vec![0.0_f64]);
    assert!(!same_value(&negative, 0, &positive, 0, false));
    let widened = Float64Array::from(vec![-0.0_f64]);
    assert!(same_value(&negative, 0, &widened, 0, false));
    let nulls = Float64Array::from(vec![None, Some(1.0)]);
    assert!(same_value(&nulls, 0, &nulls, 0, false));
    assert!(!same_value(&nulls, 0, &nulls, 1, false));
}

#[test]
fn gli_istanti_con_offset_si_confrontano_per_istante_e_offset() {
    let spec = ProbeSpec::new("datetimeoffset(7)", DataType::Utf8)
        .with_text_domain(TextDomain::OffsetInstant);
    let texts = StringArray::from(vec![
        "2026-01-01T10:00:00+00:00",
        "2026-01-01T10:00:00Z",
        "2026-01-01T10:00:00.1000000+02:00",
        "2026-01-01T10:00:00.1+02:00",
        "2026-01-01T08:00:00.1Z",
        "2016-12-31T23:59:60Z",
        "2017-01-01T00:00:00Z",
        "2026-02-30T10:00:00Z",
        "1969-12-31T23:59:59.999999-03:00",
        "1970-01-01T02:59:59.999999Z",
    ]);
    let same = |left, right| same_value_in(&spec, &texts, left, &texts, right);
    assert!(same(0, 1), "Z e +00:00");
    assert!(same(2, 3), "gli zeri finali della frazione");
    assert!(!same(3, 4), "lo stesso istante con un altro offset");
    assert!(!same(5, 6), "il secondo intercalare non e il secondo dopo");
    assert!(!same(7, 1), "una data non valida resta testo");
    assert!(!same(8, 9), "lo stesso istante con offset diversi");
    let instant = |text| match parse_offset_instant(text) {
        Some(Canon::OffsetInstant(ns, offset, _)) => Some((ns, offset)),
        _ => None,
    };
    assert_eq!(
        instant("1969-12-31T23:59:59.999999-03:00"),
        Some((10_799_999_999_000, -10_800))
    );
    assert_eq!(
        instant("1970-01-01T02:59:59.999999Z"),
        Some((10_799_999_999_000, 0))
    );
    assert_eq!(days_from_civil(1970, 1, 1), 0);
    assert_eq!(days_from_civil(1, 1, 1), -719_162);
    assert_eq!(days_from_civil(9999, 12, 31), 2_932_896);
}

#[test]
fn gli_uuid_si_confrontano_per_valore() {
    let spec =
        ProbeSpec::new("uniqueidentifier", DataType::Utf8).with_text_domain(TextDomain::Uuid);
    let texts = StringArray::from(vec![
        "6f9619ff-8b86-d011-b42d-00c04fc964ff",
        "6F9619FF-8B86-D011-B42D-00C04FC964FF",
        "{6f9619ff-8b86-d011-b42d-00c04fc964ff}",
        "6f9619ff-8b86-d011-b42d-00c04fc964ffxyz",
        "6f9619ff8b86d011b42d00c04fc964ff",
    ]);
    let same = |left, right| same_value_in(&spec, &texts, left, &texts, right);
    assert!(same(0, 1));
    assert!(same(0, 2));
    assert!(same(0, 4));
    assert!(!same(3, 0), "la coda non e lo stesso UUID");
    let free = ProbeSpec::new("nvarchar(max)", DataType::Utf8);
    assert!(!same_value_in(&free, &texts, 0, &texts, 1));
}

#[test]
fn le_batterie_di_tinyint_time_e_dei_domini_esistono() {
    for spec in [
        ProbeSpec::new("tinyint", DataType::UInt8),
        ProbeSpec::new("time(7)", DataType::Time64(TimeUnit::Microsecond)),
        ProbeSpec::new("datetimeoffset(7)", DataType::Utf8)
            .with_text_domain(TextDomain::OffsetInstant),
        ProbeSpec::new("uniqueidentifier", DataType::Utf8).with_text_domain(TextDomain::Uuid),
    ] {
        let battery = battery(&spec).expect("batteria");
        assert_eq!(battery.names.len(), battery.values.len());
        assert_eq!(battery.names.last().map(String::as_str), Some("nullo"));
    }
}

#[test]
fn oracolo_distingue_rifiuto_riscrittura_e_lettura_non_qualificata() {
    use plenora_database_core::provider::ParameterValue;
    let spec = ProbeSpec::new("RAW(10)", DataType::Binary).with_dimension(10);
    let battery = Battery {
        names: vec!["vuoto".to_owned(), "uno".to_owned()],
        values: Arc::new(BinaryArray::from(vec![b"".as_slice(), b"a".as_slice()])),
    };
    let key = AdmissionKey {
        arrow: "binary".to_owned(),
        nativo: "RAW(n)".to_owned(),
        dimensione: Some(10),
    };
    let read = (
        BTreeMap::from([(0, (0, 0)), (1, (1, 0))]),
        vec![
            vec![direct_value(&ParameterValue::Null {
                type_name: "raw".to_owned(),
            })],
            vec![direct_value(&ParameterValue::Bytes(b"a".to_vec()))],
        ],
    );
    let measured = verdict(
        &spec,
        &battery,
        key.clone(),
        TargetDimensions::default(),
        &[false, false],
        Some(&read),
        0,
    );
    assert_eq!(measured.esito, MeasuredOutcome::Riscrive);
    assert_eq!(measured.riscritture, vec![MeasuredRewrite::VuotoANull]);
    assert_eq!(measured.riscritti, vec!["vuoto"]);
    assert!(measured.rifiutati.is_empty());
    let unreadable = verdict(
        &spec,
        &battery,
        key.clone(),
        TargetDimensions::default(),
        &[false, false],
        None,
        0,
    );
    assert_eq!(unreadable.esito, MeasuredOutcome::Illeggibile);
    let refused = verdict(
        &spec,
        &battery,
        key,
        TargetDimensions::default(),
        &[true, true],
        None,
        0,
    );
    assert_eq!(refused.esito, MeasuredOutcome::Rifiuta);
    assert_eq!(unreadable.rifiutati, refused.rifiutati);
    assert_ne!(
        unreadable, refused,
        "l'ammissione non sostituisce la misura"
    );
}

#[test]
#[should_panic(expected = "errore di trasporto")]
fn errore_di_rete_dell_oracolo_non_diventa_rifiuto_del_server() {
    let error = plenora_database_core::DatabaseError::new(
        plenora_database_core::ErrorCategory::Io,
        plenora_database_core::ErrorPhase::Read,
        None,
        "connessione interrotta",
    );
    let _ = direct_error(&error, true);
}
