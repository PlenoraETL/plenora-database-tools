//! Registro eseguibile dei consumer; gira nel gate cargo test del workspace.
use std::collections::BTreeMap;
use std::path::Path;

fn scan(root: &Path, directory: &Path, found: &mut BTreeMap<String, usize>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            scan(root, &path, found);
        } else if path.extension().is_some_and(|ext| ext == "rs")
            && !path.ends_with("prepared_consumer_inventory.rs")
        {
            let text = std::fs::read_to_string(&path).unwrap();
            let sites =
                text.matches(".prepare_write(").count() + text.matches("PreparedWrite").count();
            if sites != 0 {
                found.insert(
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/"),
                    sites,
                );
            }
            if !path.ends_with("plenora-database-core/src/provider.rs") {
                assert!(
                    !text.contains("fn write_with_consent<'a>"),
                    "non sovrascrivere il confine comune: {}",
                    path.display()
                );
            }
        }
    }
}

#[test]
fn every_preparation_consumer_is_registered_and_public_consumers_use_consent() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let registry: BTreeMap<String, serde_json::Value> =
        serde_json::from_str(include_str!("prepared_consumers.json")).unwrap();
    let mut found = BTreeMap::new();
    scan(root, root, &mut found);
    let expected = registry
        .iter()
        .map(|(path, entry)| {
            (
                path.clone(),
                usize::try_from(entry["sites"].as_u64().unwrap()).unwrap(),
            )
        })
        .collect();
    assert_eq!(
        found, expected,
        "classificare ogni nuovo consumer di prepare_write"
    );
    let mut public = 0;
    for (path, entry) in registry {
        if entry["role"] == "public" {
            public += 1;
            let text = std::fs::read_to_string(root.join(path)).unwrap();
            for site in text.split(".prepare_write(").skip(1) {
                let next = site
                    .find(".write_with_consent(")
                    .expect("consumer pubblico fuori dal confine");
                assert!(
                    !site[..next].contains(".write("),
                    "esecuzione prima del consenso"
                );
            }
        }
    }
    assert_eq!(public, 5);
}
