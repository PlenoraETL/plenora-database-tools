use super::*;

fn fixture(keys: &str) -> String {
    format!(
        r#"{{"schema_version": 1, "provider": "oracle", "misure": [{{"server_major": "23",
        "server_version": "23.26.2", "immagine": "img@sha256:0", "batteria": "1",
        "chiavi": [{keys}]}}]}}"#
    )
}

const EXACT: &str = r#"{"arrow": "float64", "nativo": "BINARY_DOUBLE", "esito": "esatto",
    "riscritture": [], "riscritti": [], "rifiutati": [], "per_eccesso": []}"#;

fn key(arrow: &str, native: &str, dimension: Option<u64>) -> AdmissionKey {
    AdmissionKey {
        arrow: arrow.to_owned(),
        nativo: native.to_owned(),
        dimensione: dimension,
    }
}

fn table(keys: &str) -> AdmissionTable {
    AdmissionTable::parse(ProviderKind::Oracle, fixture(keys).as_bytes()).expect("fixture")
}

#[test]
fn una_coppia_misurata_esatta_si_ammette_senza_riscritture() {
    let table = table(EXACT);
    let server = table.server("23").expect("major");
    assert_eq!(
        server
            .admit(
                &key("float64", "BINARY_DOUBLE", None),
                TargetDimensions::default()
            )
            .expect("ammessa"),
        Vec::new()
    );
}

#[test]
fn una_coppia_o_una_major_non_misurate_si_rifiutano_senza_dati() {
    let table = table(EXACT);
    let error = table.server("19").expect_err("major non misurata");
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert_eq!(error.message, UNPROVEN_SERVER);
    let server = table.server("23").expect("major");
    for unmeasured in [
        key("float32", "BINARY_DOUBLE", None),
        key("float64", "BINARY_FLOAT", None),
        key("float64", "BINARY_DOUBLE", Some(4)),
    ] {
        let error = server
            .admit(&unmeasured, TargetDimensions::default())
            .expect_err("non misurata");
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
        assert_eq!(error.message, UNPROVEN_PAIR);
    }
}

#[test]
fn le_riscritture_misurate_prendono_i_parametri_del_target() {
    let keys = r#"{"arrow": "utf8", "nativo": "CHAR(n CHAR)", "dimensione": 1, "esito": "riscrive",
        "riscritture": [{"classe": "vuoto_a_null"}, {"classe": "riempie_testo"}],
        "riscritti": ["vuoto", "corto"], "rifiutati": [], "per_eccesso": []},
        {"arrow": "utf8", "nativo": "CHAR(n CHAR)", "dimensione": 2000, "esito": "riscrive",
        "riscritture": [{"classe": "vuoto_a_null"}, {"classe": "riempie_testo"}],
        "riscritti": ["vuoto", "corto"], "rifiutati": [], "per_eccesso": []}"#;
    let table = table(keys);
    let server = table.server("23").expect("major");
    let dimensions = TargetDimensions {
        chars: Some(7),
        bytes: Some(28),
    };
    assert_eq!(
        server
            .admit(&key("utf8", "CHAR(n CHAR)", Some(7)), dimensions)
            .expect("ammessa"),
        vec![
            StoreRewrite::EmptyToNull,
            StoreRewrite::PadText { chars: 7 }
        ]
    );
    // Fuori dalle lunghezze misurate: non provata.
    let error = server
        .admit(&key("utf8", "CHAR(n CHAR)", Some(2001)), dimensions)
        .expect_err("fuori dalle misure");
    assert_eq!(error.message, UNPROVEN_PAIR);
    // Una chiave con la dimensione chiesta senza dimensione: non provata.
    let error = server
        .admit(&key("utf8", "CHAR(n CHAR)", None), dimensions)
        .expect_err("senza dimensione");
    assert_eq!(error.message, UNPROVEN_PAIR);
    // Senza la lunghezza del target la classe non si istanzia.
    let error = server
        .admit(
            &key("utf8", "CHAR(n CHAR)", Some(7)),
            TargetDimensions::default(),
        )
        .expect_err("senza lunghezza");
    assert_eq!(error.category, ErrorCategory::Internal);
}

#[test]
fn le_classi_di_una_chiave_sono_l_unione_delle_sue_lunghezze() {
    let keys = r#"{"arrow": "utf8", "nativo": "VARCHAR(n)", "dimensione": 1, "esito": "esatto",
        "riscritture": [], "riscritti": [], "rifiutati": [], "per_eccesso": []},
        {"arrow": "utf8", "nativo": "VARCHAR(n)", "dimensione": 100, "esito": "riscrive",
        "riscritture": [{"classe": "spazi_oltre"}], "riscritti": ["spazi"], "rifiutati": [],
        "per_eccesso": []}"#;
    let dimensions = TargetDimensions {
        chars: Some(10),
        bytes: Some(10),
    };
    let measured = table(keys);
    let server = measured.server("23").expect("major");
    assert_eq!(
        server
            .admit(&key("utf8", "VARCHAR(n)", Some(10)), dimensions)
            .expect("unione"),
        vec![StoreRewrite::ExcessSpaces { bytes: 10 }]
    );
    // Una lunghezza rifiutata non prova la chiave.
    let refused = keys.replacen("\"esito\": \"esatto\"", "\"esito\": \"rifiuta\"", 1);
    let error = table(&refused)
        .server("23")
        .expect("major")
        .admit(&key("utf8", "VARCHAR(n)", Some(10)), dimensions)
        .expect_err("rifiutata");
    assert_eq!(error.message, REFUSED_PAIR);
}

#[test]
fn una_coppia_misurata_come_rifiutata_si_rifiuta_nel_preflight() {
    let keys = r#"{"arrow": "timestamp[us,UTC]", "nativo": "TIMESTAMP(6) WITH LOCAL TIME ZONE",
        "esito": "rifiuta", "riscritture": [], "riscritti": [], "rifiutati": ["epoca"],
        "per_eccesso": []}"#;
    let error = table(keys)
        .server("23")
        .expect("major")
        .admit(
            &key(
                "timestamp[us,UTC]",
                "TIMESTAMP(6) WITH LOCAL TIME ZONE",
                None,
            ),
            TargetDimensions::default(),
        )
        .expect_err("rifiutata");
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert_eq!(error.message, REFUSED_PAIR);
}

#[test]
fn un_fixture_incoerente_non_ammette_niente() {
    let incoherent = [
        // riscrive senza classi
        r#"{"arrow": "utf8", "nativo": "X", "esito": "riscrive", "riscritture": [],
            "riscritti": ["a"], "rifiutati": [], "per_eccesso": []}"#
            .to_owned(),
        // esatto con valori riscritti
        r#"{"arrow": "utf8", "nativo": "X", "esito": "esatto", "riscritture": [],
            "riscritti": ["a"], "rifiutati": [], "per_eccesso": []}"#
            .to_owned(),
        // chiave d'ammissione ripetuta
        format!("{EXACT}, {EXACT}"),
    ];
    for keys in incoherent {
        let error = AdmissionTable::parse(ProviderKind::Oracle, fixture(&keys).as_bytes())
            .expect_err("incoerente");
        assert_eq!(error.message, INVALID_FIXTURE);
    }
    let other_provider = fixture(EXACT).replace("\"oracle\"", "\"db2\"");
    assert!(AdmissionTable::parse(ProviderKind::Oracle, other_provider.as_bytes()).is_err());
    let other_battery = fixture(EXACT).replace("\"batteria\": \"1\"", "\"batteria\": \"0\"");
    assert!(AdmissionTable::parse(ProviderKind::Oracle, other_battery.as_bytes()).is_err());
    let repeated_member = fixture(EXACT).replace(
        "\"esito\": \"esatto\"",
        "\"esito\": \"esatto\", \"esito\": \"esatto\"",
    );
    assert!(AdmissionTable::parse(ProviderKind::Oracle, repeated_member.as_bytes()).is_err());
}

#[test]
fn la_chiave_arrow_porta_unita_zona_e_precisione() {
    assert_eq!(
        arrow_key(&DataType::Timestamp(TimeUnit::Microsecond, None)),
        "timestamp[us]"
    );
    assert_eq!(
        arrow_key(&DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("UTC".into())
        )),
        "timestamp[ns,UTC]"
    );
    assert_eq!(arrow_key(&DataType::Decimal128(10, 2)), "decimal128(10,2)");
    assert_eq!(arrow_key(&DataType::Float32), "float32");
}
