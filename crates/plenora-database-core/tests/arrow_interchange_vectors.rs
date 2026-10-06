//! Vettori `arrow-v1` della revisione adottata di `plenora-contracts`.
//!
//! Il manifest di adozione dichiara `plenora-arrow-interchange-v1`
//! conforme: questi vettori sono la prova eseguibile di quella
//! dichiarazione. Le copie stanno in `contracts/upstream/arrow-v1`, byte per
//! byte, con lo SHA-256 in `contracts/upstream/source.json`; il job
//! `public-contract` le confronta con il checkout fissato e rifiuta un
//! vettore del pin che qui manca.
//!
//! Un vettore `valid` deve superare `validate_schema_contract` e, campo per
//! campo, `FieldContract::parse` e `validate_current`; uno `invalid` deve
//! essere rifiutato da `validate_schema_contract`.

use plenora_database_core::arrow::{DataType, Field, Schema};
use plenora_database_core::field_contract::{validate_schema_contract, FieldContract};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

fn contracts_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../contracts")
}

fn json(path: &PathBuf) -> Value {
    let bytes = fs::read(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    serde_json::from_slice(&bytes).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut text, byte| {
            write!(text, "{byte:02x}").expect("scrittura su String");
            text
        })
}

/// I file `arrow-v1` dichiarati in `source.json`, verificati contro il pin
/// di adozione e contro il proprio SHA-256.
fn pinned_arrow_vectors() -> Vec<(String, Value)> {
    let source = json(&contracts_root().join("upstream/source.json"));
    let adoption = json(&contracts_root().join("adoption-source.json"));
    assert_eq!(
        source["revision"], adoption["contracts_source"]["revision"],
        "le copie upstream vengono da una revisione diversa dal pin di adozione"
    );
    let files = source["files"].as_object().expect("files");
    let mut vectors = Vec::new();
    for (name, entry) in files {
        if !name.starts_with("arrow-v1/") {
            continue;
        }
        let path = contracts_root().join("upstream").join(name);
        let bytes = fs::read(&path).unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            sha256_hex(&bytes),
            entry["sha256"].as_str().expect("sha256"),
            "{name}: copia diversa dal vettore fissato"
        );
        let vector: Value = serde_json::from_slice(&bytes).expect("vettore JSON");
        vectors.push((name.clone(), vector));
    }
    vectors
}

fn data_type(name: &str) -> DataType {
    match name {
        "binary" => DataType::Binary,
        "large_binary" => DataType::LargeBinary,
        "int64" => DataType::Int64,
        other => panic!("tipo Arrow del vettore non mappato: {other}"),
    }
}

fn string_map(value: &Value) -> HashMap<String, String> {
    value
        .as_object()
        .expect("metadati come oggetto")
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                value.as_str().expect("metadato stringa").to_owned(),
            )
        })
        .collect()
}

fn schema(vector: &Value) -> Schema {
    let fields = vector["fields"]
        .as_array()
        .expect("fields")
        .iter()
        .map(|field| {
            Field::new(
                field["name"].as_str().expect("name"),
                data_type(field["type"].as_str().expect("type")),
                field["nullable"].as_bool().expect("nullable"),
            )
            .with_metadata(string_map(&field["metadata"]))
        })
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, string_map(&vector["schema_metadata"]))
}

#[test]
fn ogni_vettore_arrow_v1_ha_l_esito_dichiarato() {
    let vectors = pinned_arrow_vectors();
    let mut outcomes = (0, 0);
    for (name, vector) in &vectors {
        assert_eq!(vector["contract"], "plenora-arrow-metadata-vector-v1");
        let schema = schema(vector);
        match vector["expect"].as_str() {
            Some("valid") => {
                validate_schema_contract(&schema)
                    .unwrap_or_else(|error| panic!("{name}: rifiutato: {error}"));
                for field in schema.fields() {
                    FieldContract::parse(field)
                        .and_then(FieldContract::validate_current)
                        .unwrap_or_else(|error| panic!("{name}: campo rifiutato: {error}"));
                }
                outcomes.0 += 1;
            }
            Some("invalid") => {
                assert!(
                    validate_schema_contract(&schema).is_err(),
                    "{name}: vettore invalido accettato"
                );
                outcomes.1 += 1;
            }
            other => panic!("{name}: esito atteso sconosciuto {other:?}"),
        }
    }
    // Un elenco vuoto o tutto da una parte proverebbe solo meta del
    // contratto: il pin ne ha due per lato.
    assert_eq!(
        outcomes,
        (2, 2),
        "vettori arrow-v1 attesi: 2 validi e 2 invalidi"
    );
}
