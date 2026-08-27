/// I campi che questo file non consulta, e perche. Il motivo non e
/// ornamentale: e la differenza fra una scelta e una dimenticanza.
const DESCRIPTIVE: &[(&str, &str)] = &[
        (
            "server_cursor",
            "nessun piano chiede un cursore nominato: aprirlo vorrebbe dire prima              un'operazione nel contratto che lo domandi",
        ),
        (
            "resumable",
            "riprendere richiede un punto di ripresa che il contratto non ha",
        ),
        (
            "bulk",
            "la forma della scrittura la sceglie il provider, non il chiamante: non              esiste un piano che chieda l'una o l'altra",
        ),
        (
            "array_binding",
            "nessuna forma di piano lega un array a un parametro solo",
        ),
        (
            "returning",
            "`WriteOutcome` conta righe e non le trasporta: aprirlo sarebbe una major              del contratto, non una bandiera",
        ),
        (
            "savepoints",
            "un savepoint non si chiede in un piano: lo usa chi tiene lo scope in mano",
        ),
        (
            "transactional_ddl",
            "descrive cosa resta dopo un rollback, e il chiamante lo riceve nell'esito              — `Partial` invece di `RolledBack` — non come rifiuto in prepare",
        ),
    ];

/// I campi dichiarati dalle tre strutture, letti dal contratto.
fn declared_fields() -> Vec<(&'static str, &'static str)> {
    let source = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../plenora-database-core/src/capabilities.rs"
    ));
    let mut fields = Vec::new();
    for structure in [
        "ReadCapabilities",
        "WriteCapabilities",
        "TransactionCapabilities",
    ] {
        let head = format!("pub struct {structure} {{");
        let at = source
            .find(head.as_str())
            .unwrap_or_else(|| panic!("{structure} non dichiarata nel contratto"));
        let body = &source[at + head.len()..];
        let end = body
            .find(
                "
}",
            )
            .unwrap_or(body.len());
        for line in body[..end].lines() {
            if let Some(rest) = line.trim().strip_prefix("pub ") {
                if let Some((name, _)) = rest.split_once(':') {
                    fields.push((
                        structure,
                        Box::leak(name.trim().to_owned().into_boxed_str()) as &'static str,
                    ));
                }
            }
        }
    }
    assert!(fields.len() >= 20, "lettura dei campi fallita: {fields:?}");
    fields
}

#[test]
fn every_capability_flag_is_enforced_or_declared_descriptive() {
    // Il file si legge da se: cio che conta e se il **codice** nomina il
    // campo, e la parte di test di questo file non e codice che gira in
    // produzione.
    let source = include_str!("lib.rs");
    let production = source
        .split_once(
            "
#[cfg(test)]",
        )
        .map_or(source, |(head, _)| head);
    // Le occorrenze nei commenti non costituiscono consultazioni: soltanto
    // il codice di produzione può applicare una capability.
    let code: String = production
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    let declared: Vec<&str> = DESCRIPTIVE.iter().map(|(name, _)| *name).collect();

    for (structure, field) in declared_fields() {
        // `scope` non e un booleano e si consulta per confronto: il
        // riconoscimento e lo stesso, cerca il nome del campo.
        let enforced = code.contains(&format!(".{field}"));
        let described = declared.contains(&field);
        assert!(
            enforced || described,
            "{structure}::{field} non e consultata da nessuna riga dell'engine \
                 e non dichiara di essere descrittiva: e una promessa che nessun \
                 controllo fa rispettare"
        );
        assert!(
            !(enforced && described),
            "{structure}::{field} e dichiarata descrittiva ma l'engine la \
                 consulta: la dichiarazione e scaduta"
        );
    }
}

#[test]
fn the_descriptive_declaration_does_not_outlive_the_fields() {
    let fields: Vec<&str> = declared_fields().into_iter().map(|(_, f)| f).collect();
    for (name, reason) in DESCRIPTIVE {
        assert!(
            fields.contains(name),
            "{name} e dichiarata descrittiva ma non esiste piu nel contratto"
        );
        assert!(
            reason.len() > 30,
            "{name}: la dichiarazione deve dire il motivo, non ripetere il nome"
        );
    }
}
