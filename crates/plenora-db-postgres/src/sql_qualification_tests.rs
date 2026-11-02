//! La regola Q, per costruzione: lo SQL che la libreria emette nomina le
//! funzioni, le relazioni di catalogo e i tipi di `pg_catalog` qualificati, e
//! i nomi di `PostGIS` con lo schema dell'estensione.
//!
//! La prova legge i sorgenti dei moduli che emettono SQL e, dentro ogni
//! letterale stringa che contiene SQL, cerca le forme non qualificate di un
//! elenco chiuso di nomi: le funzioni di `pg_catalog` che la libreria usa, le
//! relazioni di catalogo e i tipi dopo `::`. Un nome nuovo fuori elenco
//! passa da qui senza essere visto, e lo copre la guardia del `search_path`
//! (`write::visibility::guard_search_path`), che rifiuta ogni sessione in cui
//! uno schema prima di `pg_catalog` ne oscura un nome.
//!
//! Fuori dalla prova: `spatial.rs` e `spatial_probe.rs` (le letture con
//! predicati spatial, limite dichiarato in `postgis_schema`).
//!
//! La regola Q2 (`crate::search_path`) chiude il percorso per lo SQL della
//! libreria nella scrittura e nel dry run; dove resta quello della sessione
//! (introspezione, preflight, letture) la prova degli interi
//! ([`bare_integer_operators`]) cerca gli operatori che una conversione
//! implicita del letterale lascerebbe a uno schema qualsiasi del percorso, e
//! la prova di `write::sql` (`write_tests`) che gli statement sul target non
//! nominino tipi od operatori risolti dalla sessione.

/// I sorgenti che emettono SQL della libreria.
const SOURCES: &[(&str, &str)] = &[
    ("age.rs", include_str!("age.rs")),
    ("catalog.rs", include_str!("catalog.rs")),
    (
        "catalog/capabilities.rs",
        include_str!("catalog/capabilities.rs"),
    ),
    ("catalog/listing.rs", include_str!("catalog/listing.rs")),
    ("catalog/schema.rs", include_str!("catalog/schema.rs")),
    ("field_identity.rs", include_str!("field_identity.rs")),
    ("lib.rs", include_str!("lib.rs")),
    ("postgis_schema.rs", include_str!("postgis_schema.rs")),
    ("preflight.rs", include_str!("preflight.rs")),
    ("query_execution.rs", include_str!("query_execution.rs")),
    ("search_path.rs", include_str!("search_path.rs")),
    ("transaction/mod.rs", include_str!("transaction/mod.rs")),
    ("types.rs", include_str!("types.rs")),
    ("write.rs", include_str!("write.rs")),
    ("write/canonical.rs", include_str!("write/canonical.rs")),
    ("write/dry_run.rs", include_str!("write/dry_run.rs")),
    (
        "write/row_diagnostics.rs",
        include_str!("write/row_diagnostics.rs"),
    ),
    ("write/sql.rs", include_str!("write/sql.rs")),
    ("write/target.rs", include_str!("write/target.rs")),
    ("write/visibility.rs", include_str!("write/visibility.rs")),
];

/// Le funzioni di `pg_catalog` che lo SQL della libreria chiama.
const FUNCTIONS: &[&str] = &[
    "acldefault",
    "aclexplode",
    "array_agg",
    "bool_or",
    "col_description",
    "count",
    "current_database",
    "current_schemas",
    "current_setting",
    "format_type",
    "has_column_privilege",
    "has_schema_privilege",
    "has_table_privilege",
    "isempty",
    "jsonb_agg",
    "jsonb_build_array",
    "jsonb_build_object",
    "lower",
    "lower_inc",
    "lower_inf",
    "obj_description",
    "pg_function_is_visible",
    "pg_get_constraintdef",
    "pg_get_expr",
    "pg_get_indexdef",
    "pg_get_partkeydef",
    "pg_get_userbyid",
    "pg_get_viewdef",
    "pg_has_role",
    "pg_opclass_is_visible",
    "pg_relation_is_updatable",
    "pg_relation_size",
    "pg_type_is_visible",
    "quote_ident",
    "quote_literal",
    "row_security_active",
    "set_config",
    "to_jsonb",
    "to_regclass",
    "unnest",
    "upper",
    "upper_inc",
    "upper_inf",
];

/// Le relazioni di catalogo.
const RELATIONS: &[&str] = &[
    "pg_am",
    "pg_attrdef",
    "pg_attribute",
    "pg_class",
    "pg_collation",
    "pg_constraint",
    "pg_database",
    "pg_depend",
    "pg_enum",
    "pg_extension",
    "pg_index",
    "pg_inherits",
    "pg_namespace",
    "pg_opclass",
    "pg_operator",
    "pg_policy",
    "pg_proc",
    "pg_amop",
    "pg_range",
    "pg_rewrite",
    "pg_roles",
    "pg_trigger",
    "pg_type",
];

/// I tipi di `pg_catalog` che non sono parole chiave della grammatica (quelle,
/// come `int` o `bigint`, il parser le risolve in `pg_catalog` da se).
const TYPES: &[&str] = &[
    "bytea",
    "cidr",
    "date",
    "daterange",
    "inet",
    "int2",
    "int4",
    "int8",
    "jsonb",
    "json",
    "name",
    "oid",
    "regclass",
    "regnamespace",
    "regtype",
    "text",
    "timestamptz",
    "tsrange",
    "tstzrange",
    "uuid",
];

/// I nomi di `PostGIS`, che passano da `postgis_schema::qualify` o dal
/// prefisso del piano (`WriteColumnPlan::postgis`): ammessi soltanto nei
/// sorgenti che li qualificano cosi, e in `types.rs` (le proiezioni delle
/// letture, limite dichiarato).
const POSTGIS: &[&str] = &[
    "spatial_ref_sys",
    "postgis_typmod_srid",
    "postgis_typmod_dims",
    "postgis_typmod_type",
    "ST_GeomFromEWKB",
    "ST_AsEWKB",
];
const POSTGIS_QUALIFIED_AT_RUNTIME: &[&str] = &[
    "catalog/schema.rs",
    "postgis_schema.rs",
    "preflight.rs",
    "query_execution.rs",
    "types.rs",
];

/// I letterali stringa di un sorgente Rust (normali e raw), con la riga.
fn literals(source: &str) -> Vec<(usize, String)> {
    let bytes = source.as_bytes();
    let mut out = Vec::new();
    let mut index = 0;
    let mut line = 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\n' => line += 1,
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
                continue;
            }
            b'\'' => {
                // Un carattere (`'"'`) o una lifetime: si salta il carattere.
                if bytes.get(index + 2) == Some(&b'\'') {
                    index += 3;
                    continue;
                }
                if bytes.get(index + 1) == Some(&b'\\') && bytes.get(index + 3) == Some(&b'\'') {
                    index += 4;
                    continue;
                }
            }
            b'r' if matches!(bytes.get(index + 1), Some(b'"' | b'#'))
                && (index == 0 || !bytes[index - 1].is_ascii_alphanumeric()) =>
            {
                let hashes = bytes[index + 1..]
                    .iter()
                    .take_while(|b| **b == b'#')
                    .count();
                let open = index + 1 + hashes;
                if bytes.get(open) == Some(&b'"') {
                    let close = format!("\"{}", "#".repeat(hashes));
                    if let Some(end) = source[open + 1..].find(&close) {
                        let text = &source[open + 1..open + 1 + end];
                        out.push((line, text.to_owned()));
                        line += text.matches('\n').count();
                        index = open + 1 + end + close.len();
                        continue;
                    }
                }
            }
            b'"' => {
                let start = index + 1;
                let mut end = start;
                let mut text = String::new();
                while end < bytes.len() && bytes[end] != b'"' {
                    if bytes[end] == b'\\' {
                        // `\` a fine riga: continuazione, gli spazi iniziali
                        // della riga dopo non fanno parte del letterale.
                        if bytes.get(end + 1) == Some(&b'\n') {
                            end += 2;
                            line += 1;
                            while end < bytes.len() && bytes[end].is_ascii_whitespace() {
                                if bytes[end] == b'\n' {
                                    line += 1;
                                }
                                end += 1;
                            }
                            continue;
                        }
                        text.push(char::from(bytes[end + 1]));
                        end += 2;
                        continue;
                    }
                    if bytes[end] == b'\n' {
                        line += 1;
                    }
                    let character = source[end..].chars().next().unwrap_or_default();
                    text.push(character);
                    end += character.len_utf8().max(1);
                }
                out.push((line, text));
                index = end + 1;
                continue;
            }
            _ => {}
        }
        index += 1;
    }
    out
}

fn is_sql(text: &str) -> bool {
    ["SELECT ", "FROM ", "JOIN ", "WITH ", "WHERE ", "::"]
        .iter()
        .any(|marker| text.contains(marker))
}

fn word_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// Le occorrenze di `name` come parola in `text`, con il carattere che la
/// precede (o `None` a inizio testo) e cio che segue.
fn occurrences<'a>(text: &'a str, name: &str) -> Vec<(Option<u8>, &'a str)> {
    let bytes = text.as_bytes();
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(offset) = text[from..].find(name) {
        let start = from + offset;
        let end = start + name.len();
        let before = start.checked_sub(1).map(|index| bytes[index]);
        let after_ok = bytes.get(end).is_none_or(|byte| !word_char(*byte));
        if before.is_none_or(|byte| !word_char(byte)) && after_ok {
            found.push((before, &text[end..]));
        }
        from = end;
    }
    found
}

#[test]
fn lo_sql_della_libreria_qualifica_pg_catalog() {
    let mut unqualified = Vec::new();
    for (file, source) in SOURCES {
        for (line, text) in literals(source) {
            if !is_sql(&text) {
                continue;
            }
            for name in FUNCTIONS {
                for (before, after) in occurrences(&text, name) {
                    if after.trim_start().starts_with('(') && before != Some(b'.') {
                        unqualified.push(format!("{file}:{line}: funzione {name}"));
                    }
                }
            }
            for name in RELATIONS {
                for (before, after) in occurrences(&text, name) {
                    // Il nome dentro una stringa SQL (`'pg_catalog.pg_class'`)
                    // e qualificato dal testo stesso; `pg_class.oid` no.
                    let referenced = !after.starts_with('\'') && before != Some(b'\'');
                    if referenced && before != Some(b'.') {
                        unqualified.push(format!("{file}:{line}: relazione {name}"));
                    }
                }
            }
            let mut rest = text.as_str();
            while let Some(position) = rest.find("::") {
                let cast = rest[position + 2..].trim_start();
                let word = cast
                    .split(|character: char| {
                        !(character.is_ascii_alphanumeric() || character == '_')
                    })
                    .next()
                    .unwrap_or_default();
                if TYPES.contains(&word.to_ascii_lowercase().as_str()) {
                    unqualified.push(format!("{file}:{line}: tipo ::{word}"));
                }
                rest = &rest[position + 2..];
            }
            if !POSTGIS_QUALIFIED_AT_RUNTIME.contains(file) {
                for name in POSTGIS {
                    for (before, _) in occurrences(&text, name) {
                        if !matches!(before, Some(b'.' | b'}')) {
                            unqualified.push(format!("{file}:{line}: PostGIS {name}"));
                        }
                    }
                }
            }
        }
    }
    assert!(unqualified.is_empty(), "{}", unqualified.join("\n"));
}

/// La prova vede davvero le forme non qualificate.
#[test]
fn la_prova_della_regola_q_riconosce_le_forme_non_qualificate() {
    let source = r#"const A: &str = "SELECT current_setting('x'), c.oid::text FROM pg_class c";
        const B: &str = "SELECT pg_catalog.current_setting('x'), c.oid::pg_catalog.text \
                         FROM pg_catalog.pg_class c";"#;
    let found = literals(source);
    assert_eq!(found.len(), 2);
    assert!(found[1]
        .1
        .contains("x'), c.oid::pg_catalog.text FROM pg_catalog"));
    let (_, unsafe_sql) = &found[0];
    assert_eq!(occurrences(unsafe_sql, "current_setting").len(), 1);
    assert_eq!(occurrences(unsafe_sql, "pg_class")[0].0, Some(b' '));
}

/// Le colonne di catalogo di tipo `int2`, `int4` o `int8` che lo SQL della
/// libreria confronta con un intero senza cast: `pg_catalog` ha gli operatori
/// di confronto esatti per ogni coppia di questi tipi (`int24gt`, ...), che
/// precedono ogni schema del percorso. Per `oid`, `"char"` o un operatore
/// bit a bit (`int2 & int4` non c'e) il letterale va portato al tipo esatto.
const INTEGER_COLUMNS: &[&str] = &[
    "amopstrategy",
    "attnum",
    "atttypmod",
    "depth",
    "position",
    "pronargs",
    "srid",
    "typmod",
];

const COMPARISONS: &[&str] = &["<>", "<=", ">=", "!=", "=", "<", ">"];
const BITWISE: &[&str] = &["&", "|", "#"];

/// La parola prima di `end` in `text`, saltando gli spazi.
fn word_before(text: &str, end: usize) -> &str {
    let head = text[..end].trim_end();
    let start = head
        .rfind(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .map_or(0, |index| index + 1);
    &head[start..]
}

/// Ogni operatore fra un'espressione e un intero senza cast (`y.typelem <>
/// 0`, `t.tgtype & 64`), dove la conversione implicita del letterale lascia
/// vincere un operatore esatto di un altro schema (F1 della terza
/// rilettura: `public.<>(oid, integer)`).
fn bare_integer_operators(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut found = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let rest = &text[index..];
        let operator = COMPARISONS
            .iter()
            .chain(BITWISE)
            .find(|operator| rest.starts_with(**operator));
        let Some(operator) = operator else {
            index += rest.chars().next().map_or(1, char::len_utf8);
            continue;
        };
        let after = rest[operator.len()..].trim_start();
        let digits = after.bytes().take_while(u8::is_ascii_digit).count();
        let literal = digits > 0
            && !after[digits..].starts_with("::")
            && after[digits..]
                .bytes()
                .next()
                .is_none_or(|byte| !(byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'.'));
        // Un operatore preceduto da un altro carattere d'operatore (`<<`,
        // `->>`) o da `::tipo` esplicito non e di questo caso.
        let previous = text[..index].trim_end();
        let typed = previous.ends_with(')')
            || previous
                .rsplit_once("::")
                .is_some_and(|(_, tail)| !tail.contains(char::is_whitespace));
        let column = word_before(text, index);
        let allowed = !BITWISE.contains(operator) && INTEGER_COLUMNS.contains(&column);
        let in_operator = previous
            .bytes()
            .last()
            .is_some_and(|byte| b"<>=!&|#-+*/".contains(&byte));
        if literal && !typed && !allowed && !in_operator && !column.is_empty() {
            found.push(format!("{column} {operator} {}", &after[..digits]));
        }
        index += operator.len();
    }
    found
}

#[test]
fn nessun_operatore_dipende_dalla_conversione_di_un_intero() {
    let mut unqualified = Vec::new();
    for (file, source) in SOURCES {
        for (line, text) in literals(source) {
            if !is_sql(&text) {
                continue;
            }
            for hit in bare_integer_operators(&text) {
                unqualified.push(format!("{file}:{line}: {hit}"));
            }
        }
    }
    assert!(unqualified.is_empty(), "{}", unqualified.join("\n"));
}

/// La prova degli interi vede le forme della terza rilettura e lascia le
/// forme esatte.
#[test]
fn la_prova_degli_interi_riconosce_le_forme_dirottabili() {
    assert_eq!(
        bare_integer_operators("WHERE y.typelem <> 0 AND (t.tgtype & 64) <> 0"),
        vec!["typelem <> 0".to_owned(), "tgtype & 64".to_owned()]
    );
    assert_eq!(
        bare_integer_operators("CASE WHEN role_oid = 0 THEN 'PUBLIC' END"),
        vec!["role_oid = 0".to_owned()]
    );
    assert!(bare_integer_operators(
        "WHERE y.typelem <> 0::pg_catalog.oid AND (g.tgtype::int & 64) <> 0 \
         AND a.attnum > 0 AND a.atttypmod >= 0 AND ((x - 4) & 2047) >= 1024"
    )
    .is_empty());
}
