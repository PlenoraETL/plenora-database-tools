use super::{compiled_commands, unknown_command, usage, COMMAND_CATALOGUE};

/// I nomi dei rami del `match` del dispatch, con la feature che li porta.
///
/// Letti dal sorgente: un `match` non si enumera a runtime, e la
/// alternativa — una tabella di puntatori a funzione — sposterebbe il
/// problema senza risolverlo, perche resterebbe da provare che la tabella
/// e il `match` dicano la stessa cosa.
fn dispatch_arms() -> Vec<(String, Option<String>)> {
    let source = include_str!("main.rs");
    let start = source
        .find("let command = args.next()")
        .expect("inizio del dispatch");
    let end = source[start..]
        .find("_ => Err(unknown_command(")
        .expect("fine del dispatch")
        + start;
    let mut pending: Option<String> = None;
    let mut arms = Vec::new();
    for line in source[start..end].lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("#[cfg(feature = \"") {
            if let Some(feature) = rest.split('"').next() {
                pending = Some(feature.to_owned());
            }
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix('"') {
            if let Some(name) = rest.split('"').next() {
                if rest[name.len()..]
                    .trim_start_matches('"')
                    .trim_start()
                    .starts_with("=>")
                {
                    arms.push((name.to_owned(), pending.take()));
                    continue;
                }
            }
        }
        pending = None;
    }
    arms
}

#[test]
fn the_catalogue_matches_the_dispatch() {
    let mut from_dispatch: Vec<(String, Option<String>)> = dispatch_arms();
    from_dispatch.sort();
    assert!(
        from_dispatch.len() >= 3,
        "dispatch non riconosciuto: {from_dispatch:?}"
    );
    let mut from_catalogue: Vec<(String, Option<String>)> = COMMAND_CATALOGUE
        .iter()
        .map(|(name, feature)| {
            (
                (*name).to_owned(),
                feature.map(std::string::ToString::to_string),
            )
        })
        .collect();
    from_catalogue.sort();
    assert_eq!(
        from_dispatch, from_catalogue,
        "il catalogo e il dispatch non elencano gli stessi comandi"
    );
}

#[test]
fn usage_lists_every_compiled_command_and_only_those() {
    let text = usage();
    for name in compiled_commands() {
        assert!(
            documents(&text, name),
            "comando compilato e non documentato: {name}"
        );
    }
    let compiled = compiled_commands();
    for (name, _) in COMMAND_CATALOGUE {
        if compiled.contains(name) {
            continue;
        }
        assert!(
            !documents(&text, name),
            "l'aiuto elenca {name}, che questo binario non ha compilato"
        );
    }
}

/// Se l'aiuto presenta `name` **come comando**.
///
/// Il confronto e per riga e non per sottostringa: `execute-sql` compare
/// dentro `mysql-execute-sql`, quindi un `contains` direbbe che un binario
/// `MySQL`-only documenta i comandi `PostgreSQL`.
fn documents(text: &str, name: &str) -> bool {
    text.lines().any(|line| {
        line.strip_prefix("  ")
            .and_then(|rest| rest.strip_prefix(name))
            .is_some_and(|rest| rest.is_empty() || rest.starts_with(' '))
    })
}

#[test]
fn usage_declares_only_the_providers_this_binary_can_build() {
    let text = usage();
    let line = text
        .lines()
        .find(|line| line.contains("provider compilati in questo binario"))
        .expect("riga dei provider");
    for (feature, name) in [
        (cfg!(feature = "postgres"), "postgres"),
        (cfg!(feature = "mysql"), "mysql"),
        (cfg!(feature = "sqlserver"), "sqlserver"),
    ] {
        assert_eq!(
            line.contains(name),
            feature,
            "la riga dei provider non riflette le feature: {line}"
        );
    }
    // L'affermazione che ha reso necessaria questa guardia.
    assert!(!text.contains("--features full"));
}

#[test]
fn an_uncompiled_command_is_told_apart_from_one_that_does_not_exist() {
    let missing = COMMAND_CATALOGUE
        .iter()
        .find(|(name, feature)| feature.is_some() && !compiled_commands().contains(name))
        .map(|(name, _)| *name);
    if let Some(name) = missing {
        let message = format!("{:?}", unknown_command(name));
        assert!(
            message.contains("non compilato in questo binario"),
            "comando non compilato trattato come inesistente: {message}"
        );
    }
    let message = format!("{:?}", unknown_command("comando-che-non-esiste"));
    assert!(
        !message.contains("non compilato in questo binario"),
        "un comando inesistente non va spacciato per non compilato"
    );
}
