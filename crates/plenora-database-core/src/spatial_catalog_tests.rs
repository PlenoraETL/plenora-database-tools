use super::*;

#[test]
fn embedded_catalog_is_unique_and_complete() {
    let catalog = spatial_function_catalog().expect("catalog fixture");
    assert_eq!(catalog.schema_version, 1);
    assert_eq!(
        catalog.functions.len(),
        crate::query::SpatialFunction::ALL.len()
    );
    let unique = catalog
        .functions
        .iter()
        .map(|function| function.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(unique.len(), catalog.functions.len());
}

#[test]
fn every_geometry_result_declares_where_it_lands_and_no_scalar_does() {
    let catalog = spatial_function_catalog().expect("catalog fixture");
    let disagreeing = catalog
        .functions
        .iter()
        .filter(|function| (function.returns == "geometry") != function.crs.is_some())
        .map(|function| function.id.as_str())
        .collect::<Vec<_>>();
    assert!(
        disagreeing.is_empty(),
        "regola di CRS e tipo di ritorno non coincidono: {disagreeing:?}"
    );
}

#[test]
fn two_geometries_in_means_the_frame_is_not_derivable() {
    let catalog = spatial_function_catalog().expect("catalog fixture");
    // Il risultato conserva il frame soltanto se i due ingressi lo
    // condividono, e il contratto non lo sa: è una condizione da
    // dimostrare a runtime, non una regola da dichiarare.
    let overclaiming = catalog
        .functions
        .iter()
        .filter(|function| {
            function
                .arguments
                .iter()
                .filter(|a| *a == "geometry")
                .count()
                > 1
                && function.crs.is_some_and(|rule| rule != CrsRule::Undefined)
        })
        .map(|function| function.id.as_str())
        .collect::<Vec<_>>();
    assert!(
        overclaiming.is_empty(),
        "prendono due geometrie e dichiarano di sapere dove cade il risultato: {overclaiming:?}"
    );
}

/// Il ponte fra il nome sul filo e la voce del catalogo.
///
/// Serve a due prove, e nessuna delle due può fidarsi dell'ordine: `ALL` e
/// `functions` sono elenchi scritti a mano in due file diversi.
fn spec_of(function: crate::query::SpatialFunction) -> &'static SpatialFunctionSpec {
    let catalog = spatial_function_catalog().expect("catalog fixture");
    let wire = serde_json::to_value(function)
        .expect("serialize spatial function")
        .as_str()
        .expect("spatial function string")
        .to_owned();
    catalog
        .functions
        .iter()
        .find(|spec| spec.id == wire)
        .unwrap_or_else(|| panic!("{wire} non e nel catalogo"))
}

#[test]
fn geometry_returning_functions_match_the_versioned_catalog() {
    // `returns_geometry` decide se una projection viene incapsulata prima
    // di finire sul filo, e il catalogo dichiara la stessa cosa a chi legge
    // il contratto. Erano due elenchi scritti a mano che nessuno incrociava.
    let disagreeing = crate::query::SpatialFunction::ALL
        .iter()
        .filter(|function| {
            (spec_of(**function).returns == "geometry") != function.returns_geometry()
        })
        .collect::<Vec<_>>();
    assert!(
        disagreeing.is_empty(),
        "il codice e il catalogo non concordano su cosa rende geometria: {disagreeing:?}"
    );
}

#[test]
fn crs_rules_match_the_versioned_catalog() {
    let disagreeing = crate::query::SpatialFunction::ALL
        .iter()
        .filter(|function| spec_of(**function).crs_rule() != function.crs_rule())
        .collect::<Vec<_>>();
    assert!(
        disagreeing.is_empty(),
        "il codice e il catalogo non concordano su dove cade il risultato: {disagreeing:?}"
    );
}

#[test]
fn spatial_function_wire_names_match_the_versioned_catalog() {
    let catalog = spatial_function_catalog().expect("catalog fixture");
    let catalog_ids = catalog
        .functions
        .iter()
        .map(|function| function.id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let wire_ids = crate::query::SpatialFunction::ALL
        .iter()
        .map(|function| {
            serde_json::to_value(function)
                .expect("serialize spatial function")
                .as_str()
                .expect("spatial function string")
                .to_owned()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(wire_ids, catalog_ids);
}
