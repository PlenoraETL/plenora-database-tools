//! Snapshot di comportamento: in caso di divergenza salva soltanto l'osservato in target.
pub fn check(root: &std::path::Path, name: &str, observed: &serde_json::Value) {
    let path = root
        .join("tests/fixtures/mysql_permissive")
        .join(format!("{name}.json"));
    let expected = std::fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok());
    if expected.as_ref() != Some(observed) {
        let directory = root.join("target/p1-observed");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join(format!("{name}.json")),
            serde_json::to_vec_pretty(observed).unwrap(),
        )
        .unwrap();
        panic!("baseline {name} diversa; osservato in target/p1-observed, non aggiornare le attese per assorbire regressioni");
    }
}
