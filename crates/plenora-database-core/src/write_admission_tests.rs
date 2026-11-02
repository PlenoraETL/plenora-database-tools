use super::*;

/// Il log ricostruisce la fonte precedente; la misura nuova non apre il prodotto.
/// Le sole differenze ammesse sono le restrizioni documentate del writer;
/// nessuna coppia prima chiusa puo essere aperta.
#[test]
#[allow(clippy::too_many_lines)]
fn server_measurements_do_not_change_qualified_admission() {
    macro_rules! sources {
        ($name:literal) => {
            (
                include_bytes!(concat!(
                    "../../../tests/fixtures/ammissione_scrittura/",
                    $name,
                    ".json"
                ))
                .as_slice(),
                include_bytes!(concat!(
                    "../../../tests/fixtures/scrittura_server_",
                    $name,
                    ".json"
                ))
                .as_slice(),
                include_bytes!(concat!(
                    "../../../tests/fixtures/write_probe_evidence/",
                    $name,
                    ".json"
                ))
                .as_slice(),
            )
        };
    }
    for (provider, (qualified, measured, evidence)) in [
        (ProviderKind::Db2, sources!("db2")),
        (ProviderKind::Oracle, sources!("oracle")),
        (ProviderKind::Postgres, sources!("postgres")),
        (ProviderKind::Mysql, sources!("mysql")),
        (ProviderKind::Mariadb, sources!("mariadb")),
        (ProviderKind::Sqlserver, sources!("sqlserver")),
    ] {
        let after = AdmissionTable::parse(provider, qualified).expect("profilo qualificato");
        let mut previous: AdmissionFixture =
            serde_json::from_slice(measured).expect("misura server");
        let evidence: serde_json::Value = serde_json::from_slice(evidence).expect("log importato");
        for change in evidence["changes"].as_array().expect("differenze") {
            let before: MeasuredKey = serde_json::from_value(change["before"].clone()).unwrap();
            let measurement = previous
                .misure
                .iter_mut()
                .find(|m| m.server_major == change["major"].as_str().unwrap())
                .unwrap();
            let entry = measurement
                .chiavi
                .iter_mut()
                .find(|k| {
                    k.arrow == before.arrow
                        && k.nativo == before.nativo
                        && k.dimensione == before.dimensione
                })
                .unwrap();
            *entry = before;
        }
        let restrictions: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../../../tests/fixtures/ammissione_scrittura/writer_restrictions.json"
        ))
        .unwrap();
        let changes = restrictions["changes"].as_array().unwrap();
        let mut restored = after.fixture().clone();
        for change in changes
            .iter()
            .filter(|c| c["provider"] == provider_name(provider))
        {
            let old: MeasuredKey = serde_json::from_value(change["before"].clone()).unwrap();
            let new: MeasuredKey = serde_json::from_value(change["after"].clone()).unwrap();
            let entry = restored
                .misure
                .iter_mut()
                .find(|m| m.server_major == change["major"].as_str().unwrap())
                .unwrap()
                .chiavi
                .iter_mut()
                .find(|k| {
                    k.arrow == old.arrow && k.nativo == old.nativo && k.dimensione == old.dimensione
                })
                .unwrap();
            assert_eq!(*entry, new);
            assert!(old.rifiutati.iter().all(|n| new.rifiutati.contains(n)));
            *entry = old;
        }
        assert_eq!(restored, previous, "baseline ricostruito: {provider:?}");
        let before =
            AdmissionTable::parse(provider, &serde_json::to_vec(&previous).unwrap()).unwrap();
        let mut accepted = [0; 2];
        let mut refused = [0; 2];
        let mut rewrites_changed = 0;
        let mut widened = 0;
        for measurement in &previous.misure {
            let old = before.server(&measurement.server_major).unwrap();
            let new = after.server(&measurement.server_major).unwrap();
            for measured in &measurement.chiavi {
                let key = AdmissionKey {
                    arrow: measured.arrow.clone(),
                    nativo: measured.nativo.clone(),
                    dimensione: measured.dimensione,
                };
                let dimensions = TargetDimensions {
                    chars: measured.dimensione,
                    bytes: measured.dimensione,
                };
                let old = old.admit(&key, dimensions).map_err(|e| e.category);
                let new = new.admit(&key, dimensions).map_err(|e| e.category);
                widened += usize::from(old.is_err() && new.is_ok());
                rewrites_changed += usize::from(matches!((&old, &new), (Ok(a), Ok(b)) if a != b));
                for (index, decision) in [&old, &new].iter().enumerate() {
                    if decision.is_ok() {
                        accepted[index] += 1;
                    } else {
                        refused[index] += 1;
                    }
                }
                if old != new {
                    assert!(
                        changes
                            .iter()
                            .any(|c| c["provider"] == provider_name(provider)
                                && c["major"] == measurement.server_major
                                && c["before"]["arrow"] == key.arrow
                                && c["before"]["nativo"] == key.nativo),
                        "variazione senza provenienza"
                    );
                }
            }
        }
        eprintln!("{provider:?}: prima/dopo A {accepted:?} ammesse, {refused:?} rifiutate; S diverse {rewrites_changed}; allargamenti {widened}");
        assert_eq!(widened, 0);
    }
}

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
        let error =
            crate::admission_gate::write_column(&server, &unmeasured, TargetDimensions::default())
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

/// Il rifiuto dell'intera coppia non dipende dal valore del batch.
#[test]
fn coppie_chiuse_hanno_lo_stesso_errore_per_tutti_i_provider() {
    for provider in [
        ProviderKind::Postgres,
        ProviderKind::Mysql,
        ProviderKind::Mariadb,
        ProviderKind::Sqlserver,
        ProviderKind::Oracle,
        ProviderKind::Db2,
    ] {
        for outcome in ["rifiuta", "illeggibile"] {
            let keys = EXACT.replace("esatto", outcome);
            let source = fixture(&keys).replace("oracle", provider_name(provider));
            let table = AdmissionTable::parse(provider, source.as_bytes()).expect("fixture");
            let server = table.server("23").expect("major");
            let error = crate::admission_gate::write_column(
                &server,
                &key("float64", "BINARY_DOUBLE", None),
                TargetDimensions::default(),
            )
            .expect_err("coppia chiusa");
            assert_eq!(error.category, ErrorCategory::Unsupported);
            assert_eq!(error.phase, ErrorPhase::Prepare);
            assert_eq!(error.provider, Some(provider));
            assert_eq!(error.remote_effect, crate::RemoteEffect::None);
            assert_eq!(error.message, REFUSED_PAIR);
        }
        // Alcuni valori rifiutati dal codec non chiudono una coppia
        // misurata esatta: la rappresentabilita resta un controllo per valore.
        let keys = EXACT.replace("\"rifiutati\": []", "\"rifiutati\": [\"limite\"]");
        let source = fixture(&keys).replace("oracle", provider_name(provider));
        let table = AdmissionTable::parse(provider, source.as_bytes()).expect("fixture");
        assert!(table
            .server("23")
            .expect("major")
            .admit(
                &key("float64", "BINARY_DOUBLE", None),
                TargetDimensions::default(),
            )
            .expect("coppia ammessa")
            .is_empty());
    }
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

/// Le coppie JSON native non ereditano la prova dei tipi testuali.
#[test]
fn json_nativo_e_misurato_con_guardia_o_resta_chiuso() {
    for (provider, source, native, measured) in [
        (
            ProviderKind::Oracle,
            include_bytes!("../../../tests/fixtures/ammissione_scrittura/oracle.json").as_slice(),
            "JSON",
            false,
        ),
        (
            ProviderKind::Postgres,
            include_bytes!("../../../tests/fixtures/ammissione_scrittura/postgres.json").as_slice(),
            "scalare:jsonb;UTF8",
            true,
        ),
        (
            ProviderKind::Mysql,
            include_bytes!("../../../tests/fixtures/ammissione_scrittura/mysql.json").as_slice(),
            "json;utf8mb4",
            false,
        ),
        (
            ProviderKind::Sqlserver,
            include_bytes!("../../../tests/fixtures/ammissione_scrittura/sqlserver.json")
                .as_slice(),
            "json",
            false,
        ),
        (
            ProviderKind::Db2,
            include_bytes!("../../../tests/fixtures/ammissione_scrittura/db2.json").as_slice(),
            "JSON",
            false,
        ),
    ] {
        let table = AdmissionTable::parse(provider, source).expect("fixture");
        for measurement in &table.fixture().misure {
            let server = table.server(&measurement.server_major).expect("major");
            let result = crate::admission_gate::write_column(
                &server,
                &key("utf8", native, None),
                TargetDimensions::default(),
            );
            if measured {
                assert_eq!(result.expect("misurato"), vec![StoreRewrite::JsonNumbers]);
            } else {
                let error = result.expect_err("nessuna prova della coppia JSON nativa");
                assert_eq!(error.category, ErrorCategory::Unsupported);
                assert_eq!(error.phase, ErrorPhase::Prepare);
                assert_eq!(error.remote_effect, crate::RemoteEffect::None);
            }
        }
    }
}
