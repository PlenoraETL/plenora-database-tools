//! Il giudizio del catalogo Oracle su un catalogo finto: la risoluzione dei
//! nomi (schema corrente, sinonimi privati e pubblici, catene, cicli e
//! database link), le viste e le viste materializzate (regola B), il tipo
//! (`MDSYS`, anche attraverso il sinonimo pubblico; un omonimo di un altro
//! schema non e una geometria).

use crate::operand_catalog::{
    ensure_scalar_read_fields, operand_catalog, ColumnRow, ObjectRow, OracleCatalog, QualifiedName,
    SynonymRow, MAX_SYNONYM_DEPTH,
};
use plenora_database_core::plan::{FilterExpression, ObjectRef, ProviderKind, ReadOperation};
use plenora_database_core::relational::QueryOperation;
use plenora_database_core::spatial_operand::{scalar_operands, GEOMETRY_OPERAND_UNRESOLVED};
use plenora_database_core::spatial_probe::GEOMETRY_SCALAR_OPERAND;
use plenora_database_core::Result;
use std::collections::BTreeMap;

fn name(owner: &str, object: &str) -> QualifiedName {
    (owner.to_owned(), object.to_owned())
}

#[derive(Default)]
struct Finto {
    current: String,
    objects: BTreeMap<QualifiedName, Vec<String>>,
    synonyms: BTreeMap<QualifiedName, SynonymRow>,
    columns: BTreeMap<QualifiedName, Vec<ColumnRow>>,
}

impl Finto {
    fn new() -> Self {
        Self {
            current: "PLENORA".to_owned(),
            ..Self::default()
        }
    }

    fn relation(
        &mut self,
        owner: &str,
        object: &str,
        kinds: &[&str],
        columns: &[(&str, &str, Option<&str>)],
    ) {
        self.objects.insert(
            name(owner, object),
            kinds.iter().map(|kind| (*kind).to_owned()).collect(),
        );
        self.columns.insert(
            name(owner, object),
            columns
                .iter()
                .map(|(column, data_type, owner)| ColumnRow {
                    name: (*column).to_owned(),
                    data_type: (*data_type).to_owned(),
                    data_type_owner: owner.map(str::to_owned),
                })
                .collect(),
        );
    }

    fn synonym(&mut self, owner: &str, object: &str, target: Option<(&str, &str)>) {
        self.objects
            .insert(name(owner, object), vec!["SYNONYM".to_owned()]);
        self.synonyms.insert(
            name(owner, object),
            SynonymRow {
                target: target.map(|(owner, object)| name(owner, object)),
            },
        );
    }
}

impl OracleCatalog for Finto {
    fn relation(
        &mut self,
        owner: Option<&str>,
        object: &str,
    ) -> impl std::future::Future<Output = Result<Vec<ObjectRow>>> {
        let owner = owner.map_or_else(|| self.current.clone(), str::to_owned);
        let key = name(&owner, object);
        let mut rows = Vec::new();
        for kind in self.objects.get(&key).cloned().unwrap_or_default() {
            let columns = if matches!(kind.as_str(), "TABLE" | "VIEW") {
                self.columns.get(&key).cloned().unwrap_or_default()
            } else {
                Vec::new()
            };
            if columns.is_empty() {
                rows.push(ObjectRow {
                    owner: owner.clone(),
                    kind,
                    column: None,
                });
            } else {
                rows.extend(columns.into_iter().map(|column| ObjectRow {
                    owner: owner.clone(),
                    kind: kind.clone(),
                    column: Some(column),
                }));
            }
        }
        std::future::ready(Ok(rows))
    }

    fn synonym(
        &mut self,
        owner: &str,
        object: &str,
    ) -> impl std::future::Future<Output = Result<Option<SynonymRow>>> {
        std::future::ready(Ok(self.synonyms.get(&name(owner, object)).cloned()))
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match future
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("il finto e sincrono"),
    }
}

/// `SELECT T.ID FROM <schema>.<object> T WHERE T.<campo> = ?`
fn confronto(schema: Option<&str>, object: &str, campo: &str) -> QueryOperation {
    serde_json::from_value(serde_json::json!({
        "source": {"object": {"catalog": null, "schema": schema, "object": object}, "alias": "T"},
        "projection": [{"expression": {"kind": "column", "column": {"relation": "T", "field": "ID"}}, "alias": "ID"}],
        "filter": {"kind": "compare", "left": {"kind": "column", "column": {"relation": "T", "field": campo}}, "operator": "eq", "right": {"kind": "parameter", "name": "p"}},
        "having": null,
        "row_limit": null
    }))
    .expect("piano")
}

fn giudizio(
    finto: &mut Finto,
    schema: Option<&str>,
    object: &str,
    campo: &str,
) -> std::result::Result<(), String> {
    let operation = confronto(schema, object, campo);
    let operands = scalar_operands(ProviderKind::Oracle, &operation).expect("visita");
    let catalog = block_on(operand_catalog(finto, operands.relations())).expect("catalogo");
    operands.judge(&catalog).map_err(|error| error.message)
}

fn rifiuto(motivo: &str) -> std::result::Result<(), String> {
    Err(motivo.to_owned())
}

/// La tabella `PLENORA.T` e la vista `PLENORA.V` con le forme misurate il
/// 25/10: `G` `SDO_GEOMETRY` di `MDSYS`, `W` (`SDO_UTIL.TO_WKBGEOMETRY`) e
/// `W2` (`GET_WKB`) `BLOB`, `R` (`DBMS_LOB.SUBSTR`) `RAW`, `X`
/// (`TO_WKTGEOMETRY`) `CLOB`.
fn base() -> Finto {
    let mut finto = Finto::new();
    let colonne = [
        ("ID", "NUMBER", None),
        ("NAME", "VARCHAR2", None),
        ("G", "SDO_GEOMETRY", Some("MDSYS")),
        ("W", "BLOB", None),
        ("W2", "BLOB", None),
        ("R", "RAW", None),
        ("L", "LONG RAW", None),
        ("X", "CLOB", None),
    ];
    finto.relation("PLENORA", "T", &["TABLE"], &colonne);
    finto.relation("PLENORA", "V", &["VIEW"], &colonne);
    finto.relation("PLENORA", "MV", &["TABLE", "MATERIALIZED VIEW"], &colonne);
    finto
}

#[test]
fn una_colonna_binaria_di_una_vista_si_giudica_geometrica() {
    let mut finto = base();
    for object in ["T", "V", "MV"] {
        let derivata = object != "T";
        assert_eq!(
            giudizio(&mut finto, Some("PLENORA"), object, "G"),
            rifiuto(GEOMETRY_SCALAR_OPERAND),
            "{object}"
        );
        for campo in ["W", "W2", "R", "L"] {
            let atteso = if derivata {
                rifiuto(GEOMETRY_SCALAR_OPERAND)
            } else {
                Ok(())
            };
            assert_eq!(
                giudizio(&mut finto, Some("PLENORA"), object, campo),
                atteso,
                "{object}.{campo}"
            );
        }
        for campo in ["ID", "NAME", "X"] {
            assert_eq!(
                giudizio(&mut finto, Some("PLENORA"), object, campo),
                Ok(()),
                "{object}.{campo}"
            );
        }
    }
}

#[test]
fn il_proprietario_del_tipo_decide_la_geometria() {
    let mut finto = Finto::new();
    finto.relation(
        "PLENORA",
        "T",
        &["TABLE"],
        &[
            ("ID", "NUMBER", None),
            // Il catalogo rende il proprietario gia risolto attraverso il
            // sinonimo pubblico (`COLUMNS_SQL`).
            ("G", "SDO_GEOMETRY", Some("MDSYS")),
            ("TOPO", "SDO_TOPO_GEOMETRY", Some("MDSYS")),
            ("OMONIMO", "SDO_GEOMETRY", Some("ALTRO")),
            ("NON_RISOLTO", "SDO_GEOMETRY", Some("PUBLIC")),
            ("RAWDI", "RAW", Some("ALTRO")),
        ],
    );
    for campo in ["G", "TOPO"] {
        assert_eq!(
            giudizio(&mut finto, Some("PLENORA"), "T", campo),
            rifiuto(GEOMETRY_SCALAR_OPERAND),
            "{campo}"
        );
    }
    for campo in ["ID", "OMONIMO", "NON_RISOLTO", "RAWDI"] {
        assert_eq!(
            giudizio(&mut finto, Some("PLENORA"), "T", campo),
            Ok(()),
            "{campo}"
        );
    }
}

#[test]
fn senza_schema_vale_lo_schema_corrente_poi_il_sinonimo_pubblico() {
    let mut finto = base();
    assert_eq!(giudizio(&mut finto, None, "T", "ID"), Ok(()));
    assert_eq!(
        giudizio(&mut finto, None, "V", "W"),
        rifiuto(GEOMETRY_SCALAR_OPERAND)
    );
    finto.current = "ALTRO".to_owned();
    // Lo schema corrente non ha il nome, il sinonimo pubblico non c'e.
    assert_eq!(giudizio(&mut finto, None, "V", "W"), Ok(()));
    finto.synonym("PUBLIC", "V", Some(("PLENORA", "V")));
    assert_eq!(
        giudizio(&mut finto, None, "V", "W"),
        rifiuto(GEOMETRY_SCALAR_OPERAND)
    );
    assert_eq!(giudizio(&mut finto, None, "V", "ID"), Ok(()));
    // Lo schema corrente nasconde il sinonimo pubblico.
    finto.relation(
        "ALTRO",
        "V",
        &["TABLE"],
        &[("ID", "NUMBER", None), ("W", "BLOB", None)],
    );
    assert_eq!(giudizio(&mut finto, None, "V", "W"), Ok(()));
    // Con lo schema esplicito il sinonimo pubblico non vale.
    assert_eq!(giudizio(&mut finto, Some("NESSUNO"), "V", "W"), Ok(()));
}

#[test]
fn i_sinonimi_si_seguono_fino_all_oggetto() {
    let mut finto = base();
    finto.synonym("PLENORA", "S", Some(("PLENORA", "V")));
    finto.synonym("PLENORA", "SS", Some(("PLENORA", "S")));
    for synonym in ["S", "SS"] {
        assert_eq!(
            giudizio(&mut finto, Some("PLENORA"), synonym, "W"),
            rifiuto(GEOMETRY_SCALAR_OPERAND),
            "{synonym}"
        );
        assert_eq!(
            giudizio(&mut finto, Some("PLENORA"), synonym, "ID"),
            Ok(()),
            "{synonym}"
        );
    }
}

#[test]
fn un_sinonimo_che_non_arriva_a_una_relazione_non_si_conosce() {
    let mut finto = base();
    finto.synonym("PLENORA", "LINK", None);
    finto.synonym("PLENORA", "ASSENTE", Some(("PLENORA", "NESSUNO")));
    finto.synonym("PLENORA", "C1", Some(("PLENORA", "C2")));
    finto.synonym("PLENORA", "C2", Some(("PLENORA", "C1")));
    finto
        .objects
        .insert(name("PLENORA", "VUOTA"), vec!["VIEW".to_owned()]);
    for object in ["LINK", "ASSENTE", "C1", "VUOTA"] {
        assert_eq!(
            giudizio(&mut finto, Some("PLENORA"), object, "ID"),
            rifiuto(GEOMETRY_OPERAND_UNRESOLVED),
            "{object}"
        );
    }
    // Assente: la query fallira sul server, il piano non ha geometrie.
    assert_eq!(
        giudizio(&mut finto, Some("PLENORA"), "NESSUNO", "G"),
        Ok(())
    );
}

#[test]
fn una_catena_di_sinonimi_oltre_il_limite_non_si_conosce() {
    let mut finto = base();
    for index in 0..MAX_SYNONYM_DEPTH {
        let next = format!("S{}", index + 1);
        finto.synonym("PLENORA", &format!("S{index}"), Some(("PLENORA", &next)));
    }
    finto.synonym(
        "PLENORA",
        &format!("S{MAX_SYNONYM_DEPTH}"),
        Some(("PLENORA", "T")),
    );
    assert_eq!(
        giudizio(&mut finto, Some("PLENORA"), "S0", "ID"),
        rifiuto(GEOMETRY_OPERAND_UNRESOLVED)
    );
    assert_eq!(giudizio(&mut finto, Some("PLENORA"), "S1", "ID"), Ok(()));
}

#[test]
fn i_nomi_si_confrontano_per_byte() {
    let mut finto = base();
    assert!(giudizio(&mut finto, Some("PLENORA"), "V", "w").is_err());
    // Una relazione nominata in minuscolo e un'altra relazione.
    assert_eq!(giudizio(&mut finto, Some("plenora"), "V", "W"), Ok(()));
}

fn lettura(object: &str, field: &str) -> ReadOperation {
    ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: object.to_owned(),
        },
        projection: vec!["ID".to_owned()],
        order_by: Vec::new(),
        row_limit: None,
        row_offset: None,
        filter: Some(FilterExpression::Eq {
            field: field.to_owned(),
            parameter: "p".to_owned(),
        }),
        declared_crs: Vec::new(),
    }
}

/// La lettura del provider giudica sullo stesso catalogo: la vista
/// materializzata, che la descrizione dice `TABLE`, compresa.
#[test]
fn la_lettura_giudica_sullo_stesso_catalogo() {
    let mut finto = base();
    for (object, field, rifiutata) in [
        ("T", "W", false),
        ("T", "G", true),
        ("V", "W", true),
        ("V", "R", true),
        ("MV", "W", true),
        ("V", "ID", false),
        ("MV", "X", false),
    ] {
        let outcome = block_on(ensure_scalar_read_fields(
            &mut finto,
            "PLENORA",
            object,
            &lettura(object, field),
        ));
        if rifiutata {
            let error = outcome.expect_err(field);
            assert_eq!(error.message, GEOMETRY_SCALAR_OPERAND, "{object}.{field}");
        } else {
            outcome.unwrap_or_else(|error| panic!("{object}.{field}: {error}"));
        }
    }
    // Una relazione che il catalogo non descrive: ogni campo si rifiuta.
    finto.synonym("PLENORA", "LINK", None);
    assert!(block_on(ensure_scalar_read_fields(
        &mut finto,
        "PLENORA",
        "LINK",
        &lettura("LINK", "ID")
    ))
    .is_err());
}
