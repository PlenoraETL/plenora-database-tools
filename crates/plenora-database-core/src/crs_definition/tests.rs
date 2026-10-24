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
