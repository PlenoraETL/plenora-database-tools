use super::*;

const WGS84_WKT2: &str = r#"GEOGCRS["WGS 84",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],CS[ellipsoidal,2],AXIS["latitude",north,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],AXIS["longitude",east,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]],ID["EPSG",4326]]"#;
const MONTE_MARIO_WKT1: &str = r#"PROJCS["Monte Mario / Italy zone 1",GEOGCS["Monte Mario",DATUM["Monte_Mario",SPHEROID["International 1924",6378388,297]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4265"]],PROJECTION["Transverse_Mercator"],UNIT["metre",1],AUTHORITY["EPSG","3003"]]"#;

#[test]
fn top_level_identifier_satisfies_and_contradicts() {
    assert_eq!(
        contradicts(Some("EPSG:4326"), WGS84_WKT2, DefinitionFormat::Wkt2),
        Ok(false)
    );
    assert_eq!(
        contradicts(Some("epsg:4326"), WGS84_WKT2, DefinitionFormat::Wkt2),
        Ok(false)
    );
    assert_eq!(
        contradicts(Some("EPSG:3857"), WGS84_WKT2, DefinitionFormat::Wkt2),
        Ok(true)
    );
    // Un'altra autorita non si confronta.
    assert_eq!(
        contradicts(Some("OGC:CRS84"), WGS84_WKT2, DefinitionFormat::Wkt2),
        Ok(false)
    );
}

/// GEO-005: l'`AUTHORITY` del `GEOGCS` dentro un `PROJCS` non soddisfa e
/// non contraddice.
#[test]
fn nested_identifiers_never_decide() {
    assert_eq!(
        contradicts(Some("EPSG:3003"), MONTE_MARIO_WKT1, DefinitionFormat::Wkt),
        Ok(false)
    );
    assert_eq!(
        contradicts(Some("EPSG:4265"), MONTE_MARIO_WKT1, DefinitionFormat::Wkt),
        Ok(true)
    );
}

#[test]
fn ascii_case_insensitive_authority_only() {
    // `ſ` (U+017F) diventa `S` con le maiuscole Unicode: per GEO-005 resta se
    // stessa, e l'identificatore e di un'altra autorita.
    let definition = WGS84_WKT2.replace(r#"ID["EPSG",4326]"#, r#"ID["EPſG",3857]"#);
    assert_eq!(
        contradicts(Some("EPSG:4326"), &definition, DefinitionFormat::Wkt2),
        Ok(false)
    );
}

#[test]
fn malformed_definitions() {
    for (text, format) in [
        ("GEOGCRS[\"x\"", DefinitionFormat::Wkt2),
        ("GEOGCRS[\"x\")", DefinitionFormat::Wkt2),
        ("GEOGCS[\"x\"]", DefinitionFormat::Wkt2),
        ("GEOGCRS[\"x\"]", DefinitionFormat::Wkt),
        ("GEOGCRS[\"x\",ID[\"EPSG\",٤٣٢٦]]", DefinitionFormat::Wkt2),
        (
            "{\"id\":{\"authority\":\"EPSG\",\"code\":4326},\"id\":{}}",
            DefinitionFormat::Projjson,
        ),
        ("[]", DefinitionFormat::Projjson),
        (
            "{\"id\":{\"authority\":\"EPSG\",\"code\":4326.0}}",
            DefinitionFormat::Projjson,
        ),
    ] {
        assert_eq!(contradicts(None, text, format), Err(Malformed), "{text}");
    }
}

#[test]
fn projjson_identifiers_are_read_at_the_root() {
    let text = r#"{"type":"GeographicCRS","name":"WGS 84","base_crs":{"id":{"authority":"EPSG","code":1}},"id":{"authority":"EPSG","code":4326}}"#;
    assert_eq!(
        contradicts(Some("EPSG:4326"), text, DefinitionFormat::Projjson),
        Ok(false)
    );
    assert_eq!(
        contradicts(Some("EPSG:1"), text, DefinitionFormat::Projjson),
        Ok(true)
    );
}

#[test]
fn crs_id_grammar_matches_the_whole_value() {
    for good in ["EPSG:4326", "POSTGIS:4326", "OGC:CRS84", "a.b-c_d:x"] {
        assert!(is_well_formed_crs_id(good), "{good}");
    }
    for bad in [
        "",
        "EPSG",
        ":4326",
        "1EPSG:4326",
        "EPSG:",
        "EPSG:43 26",
        "EPSG:4326\n",
        "EPSG:a:b",
    ] {
        assert!(!is_well_formed_crs_id(bad), "{bad:?}");
    }
}

#[test]
fn local_authorities_are_the_closed_list() {
    for local in [
        "POSTGIS:1",
        "postgis:1",
        "MSSQL:4326",
        "Oracle:8307",
        "DB2:1",
        "SQLITE:1",
    ] {
        assert!(is_local_authority(local), "{local}");
    }
    for registry in ["EPSG:4326", "SQLSERVER:4326", "poſtgis:1"] {
        assert!(!is_local_authority(registry), "{registry}");
    }
}

// ---------------------------------------------------------------------------
// Le prove PROJJSON di IO-tools (`plenora-io-model::crs_definizione::tests`,
// alla 6.0.0 candidata), portate con i nomi di questo crate. Il modulo
// `projjson` e lo stesso, portato da li: le stesse definizioni devono avere
// lo stesso verdetto nei due componenti, e il verdetto e quello dell'oracolo
// (`tools/arrow_data.py`, `json.loads` con i suoi ganci).
// ---------------------------------------------------------------------------

fn pair(authority: &str, code: &str) -> (DefinitionText, DefinitionText) {
    (
        DefinitionText::Unicode(authority.to_owned()),
        DefinitionText::Unicode(code.to_owned()),
    )
}

#[test]
fn projjson_root_identifiers_in_order() {
    let text = r#"{"type":"GeographicCRS","ids":[{"authority":"EPSG","code":4326},{"authority":"OGC","code":"CRS84"}]}"#;
    assert_eq!(
        identifiers(text, DefinitionFormat::Projjson),
        Ok(vec![pair("EPSG", "4326"), pair("OGC", "CRS84")])
    );
}

#[test]
fn projjson_malformed_like_the_io_suite() {
    for text in [
        r#"{"id":{"authority":"EPSG","code":4326},"id":{"authority":"EPSG","code":3857}}"#,
        r#"{"id":{"authority":"EPSG","code":4326},"ids":[]}"#,
        r#"{"id":{"authority":"EPSG","code":true}}"#,
        r#"{"id":{"authority":"EPSG","code":4326.0}}"#,
        "[]",
    ] {
        assert_eq!(
            identifiers(text, DefinitionFormat::Projjson),
            Err(Malformed),
            "{text}"
        );
        assert_eq!(
            contradicts(None, text, DefinitionFormat::Projjson),
            Err(Malformed),
            "{text}"
        );
    }
}

/// I PROJJSON che `json.loads` legge e `serde_json` (senza le sue feature
/// opzionali) rifiuterebbe: un numero oltre `f64`, un surrogato isolato, piu
/// di 128 livelli, un codice `-0` o oltre `u64`.
#[test]
fn projjson_as_the_oracle_reads_it() {
    let with = |member: &str, code: &str| {
        format!(r#"{{"type":"GeographicCRS",{member}"id":{{"authority":"EPSG","code":{code}}}}}"#)
    };
    for text in [
        with(r#""x":1e400,"#, "4326"),
        with(r#""x":-1e-400,"#, "4326"),
        with(r#""name":"\ud800","#, "4326"),
        with(r#""name":"\udc00\ud800x","#, "4326"),
        with(
            &format!(r#""x":{}{},"#, "[".repeat(2000), "]".repeat(2000)),
            "4326",
        ),
        with(
            &format!(r#""x":{}1{},"#, r#"{"a":"#.repeat(500), "}".repeat(500)),
            "4326",
        ),
        // Caratteri crudi di due, tre e quattro byte.
        with("\"name\":\"Citt\u{e0} \u{2713} \u{1f600}\",", "4326"),
    ] {
        assert_eq!(
            identifiers(&text, DefinitionFormat::Projjson),
            Ok(vec![pair("EPSG", "4326")]),
            "{text:.80}"
        );
    }
    // `-0` e l'intero 0: `str(int)` lo scrive `0`.
    let zero = with("", "-0");
    assert_eq!(
        identifiers(&zero, DefinitionFormat::Projjson),
        Ok(vec![pair("EPSG", "0")])
    );
    assert_eq!(
        contradicts(Some("EPSG:0"), &zero, DefinitionFormat::Projjson),
        Ok(false)
    );
    assert_eq!(
        contradicts(Some("EPSG:-0"), &zero, DefinitionFormat::Projjson),
        Ok(true)
    );
    let large = with("", "184467440737095516160000");
    assert_eq!(
        identifiers(&large, DefinitionFormat::Projjson),
        Ok(vec![pair("EPSG", "184467440737095516160000")])
    );
    let negative = with("", "-4326");
    assert_eq!(
        contradicts(Some("EPSG:-4326"), &negative, DefinitionFormat::Projjson),
        Ok(false)
    );
}

/// I surrogati: una coppia si unisce, un isolato si conserva e non e uguale a
/// nessun codice; una chiave scritta con un escape e la stessa chiave.
#[test]
fn projjson_surrogates_and_escapes() {
    let joined = r#"{"id":{"authority":"EPSG","code":"😀"}}"#;
    assert_eq!(
        identifiers(joined, DefinitionFormat::Projjson),
        Ok(vec![pair("EPSG", "\u{1f600}")])
    );
    let lone = r#"{"id":{"authority":"EPSG","code":"\ud800"}}"#;
    assert_eq!(
        identifiers(lone, DefinitionFormat::Projjson),
        Ok(vec![(
            DefinitionText::Unicode("EPSG".to_owned()),
            DefinitionText::LoneSurrogates
        )])
    );
    assert_eq!(
        contradicts(Some("EPSG:\u{fffd}"), lone, DefinitionFormat::Projjson),
        Ok(true),
        "un surrogato isolato non e il carattere di sostituzione"
    );
    let escaped = r#"{"id":{"authority":"EPSG","code":4326}}"#;
    assert_eq!(
        identifiers(escaped, DefinitionFormat::Projjson),
        Ok(vec![pair("EPSG", "4326")])
    );
}

/// Cio che `json.loads`, con i ganci dell'oracolo, rifiuta.
#[test]
fn projjson_malformed_as_for_the_oracle() {
    let id = r#""id":{"authority":"EPSG","code":4326}"#;
    for text in [
        format!(r#"{{"x":NaN,{id}}}"#),
        format!(r#"{{"x":Infinity,{id}}}"#),
        format!(r#"{{"x":-Infinity,{id}}}"#),
        format!(r#"{{"x":{{"a":1,"a":2}},{id}}}"#),
        format!(r#"{{"id":1,{id}}}"#),
        format!("{{\"name\":\"a\tb\",{id}}}"),
        format!(r#"{{"name":"a\xb",{id}}}"#),
        format!(r#"{{"name":"\u12",{id}}}"#),
        format!(r#"{{"x":01,{id}}}"#),
        format!(r#"{{"x":1.,{id}}}"#),
        format!(r#"{{"x":.5,{id}}}"#),
        format!(r#"{{"x":1e,{id}}}"#),
        format!(r#"{{"x":+1,{id}}}"#),
        format!(r#"{{"x":[1,],{id}}}"#),
        format!(r#"{{"x":tru,{id}}}"#),
        format!("{{{id}}}\u{b}"),
        format!("\u{feff}{{{id}}}"),
        format!("{{{id}}} {{}}"),
        format!("{{{id},}}"),
        r#"{"id":{"authority":"EPSG","code":4326e0}}"#.to_owned(),
        r#"{"id":{"authority":"EPSG","code":null}}"#.to_owned(),
        r#"{"id":{"authority":"EPSG"}}"#.to_owned(),
        r#"{"id":{"authority":1,"code":4326}}"#.to_owned(),
        r#"{"id":[{"authority":"EPSG","code":4326}]}"#.to_owned(),
        r#"{"ids":{"authority":"EPSG","code":4326}}"#.to_owned(),
        r#"{"ids":[1]}"#.to_owned(),
        "\"EPSG\"".to_owned(),
        String::new(),
        "{".repeat(100),
    ] {
        assert_eq!(
            identifiers(&text, DefinitionFormat::Projjson),
            Err(Malformed),
            "{text:.80}"
        );
    }
}

/// Gli spazi di JSON sono quattro, e soltanto quattro.
#[test]
fn projjson_json_whitespace() {
    let text = " \t\n\r{ \"id\" :\r{\"authority\" : \"EPSG\" , \"code\" : 4326 } }\n";
    assert_eq!(
        identifiers(text, DefinitionFormat::Projjson),
        Ok(vec![pair("EPSG", "4326")])
    );
}

/// L'autorita si confronta ASCII case-insensitive (GEO-005, GEO-020): la s
/// lunga U+017F e la i senza punto U+0131 non sono `S` e `I`.
#[test]
fn projjson_authority_compares_in_ascii_only() {
    let text = "{\"id\":{\"authority\":\"EP\u{17f}G\",\"code\":3857}}";
    assert_eq!(
        contradicts(Some("EPSG:4326"), text, DefinitionFormat::Projjson),
        Ok(false)
    );
    let ascii = "{\"id\":{\"authority\":\"epsg\",\"code\":3857}}";
    assert_eq!(
        contradicts(Some("EPSG:4326"), ascii, DefinitionFormat::Projjson),
        Ok(true)
    );
    assert!(!is_local_authority("POSTG\u{131}S:4326"));
    assert!(is_local_authority("postgis:4326"));
}

/// Il tetto dei byte vale anche per il lettore PROJJSON, per difesa: oltre
/// [`MAX_DEFINITION_BYTES`] la definizione e malformata.
#[test]
fn projjson_beyond_the_byte_ceiling_is_malformed() {
    let padding = " ".repeat(MAX_DEFINITION_BYTES);
    let text = format!(r#"{{"id":{{"authority":"EPSG","code":4326}}}}{padding}"#);
    assert!(!within_size_limit(&text));
    assert_eq!(
        identifiers(&text, DefinitionFormat::Projjson),
        Err(Malformed)
    );
}
