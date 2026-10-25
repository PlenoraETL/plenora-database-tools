//! Il giudizio del catalogo Db2 su un catalogo finto: la risoluzione dei
//! nomi (schema corrente, alias pubblici, catene e cicli di alias, nickname,
//! `SESSION`) e il giudizio del tipo (spatial di `SYSIBM` e `DB2GSE`, tipi
//! distinti e strutturati che vi arrivano, omonimi di un altro schema, tipi
//! che il catalogo non descrive).

use crate::operand_catalog::{
    geometric_columns, operand_catalog, ColumnRow, Db2Catalog, QualifiedName, RelationRow, TypeRow,
    MAX_ALIAS_DEPTH, MAX_TYPE_DEPTH,
};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::relational::QueryOperation;
use plenora_database_core::spatial_operand::scalar_operands;
use plenora_database_core::spatial_probe::GEOMETRY_SCALAR_OPERAND;
use plenora_database_core::Result;
use std::collections::BTreeMap;

fn name(schema: &str, object: &str) -> QualifiedName {
    (schema.to_owned(), object.to_owned())
}

#[derive(Default)]
struct Finto {
    current: String,
    relations: BTreeMap<QualifiedName, RelationRow>,
    columns: BTreeMap<QualifiedName, Vec<ColumnRow>>,
    types: BTreeMap<QualifiedName, TypeRow>,
    supertypes: BTreeMap<QualifiedName, QualifiedName>,
    letture: usize,
}

impl Finto {
    fn new() -> Self {
        Self {
            current: "DB2INST1".to_owned(),
            ..Self::default()
        }
    }

    fn table(&mut self, schema: &str, object: &str, columns: &[(&str, &str, &str)]) {
        self.relations.insert(
            name(schema, object),
            RelationRow {
                kind: "T".to_owned(),
                base: None,
            },
        );
        self.columns.insert(
            name(schema, object),
            columns
                .iter()
                .map(|(column, type_schema, type_name)| ColumnRow {
                    name: (*column).to_owned(),
                    type_name: name(type_schema, type_name),
                })
                .collect(),
        );
    }

    fn kind(&mut self, schema: &str, object: &str, kind: &str, base: Option<(&str, &str)>) {
        self.relations.insert(
            name(schema, object),
            RelationRow {
                kind: kind.to_owned(),
                base: base.map(|(schema, object)| name(schema, object)),
            },
        );
    }

    fn distinct(&mut self, schema: &str, object: &str, source: (&str, &str)) {
        self.types.insert(
            name(schema, object),
            TypeRow {
                metatype: "T".to_owned(),
                source: Some(name(source.0, source.1)),
                source_module: false,
            },
        );
    }

    fn structured(&mut self, schema: &str, object: &str, supertype: Option<(&str, &str)>) {
        self.types.insert(
            name(schema, object),
            TypeRow {
                metatype: "R".to_owned(),
                source: None,
                source_module: false,
            },
        );
        if let Some((super_schema, super_name)) = supertype {
            self.supertypes
                .insert(name(schema, object), name(super_schema, super_name));
        }
    }
}

impl Db2Catalog for Finto {
    fn current_schema(&mut self) -> Result<String> {
        self.letture += 1;
        Ok(self.current.clone())
    }

    fn relation(&mut self, schema: &str, object: &str) -> Result<Option<RelationRow>> {
        self.letture += 1;
        Ok(self.relations.get(&name(schema, object)).cloned())
    }

    fn columns(&mut self, schema: &str, object: &str) -> Result<Vec<ColumnRow>> {
        self.letture += 1;
        Ok(self
            .columns
            .get(&name(schema, object))
            .cloned()
            .unwrap_or_default())
    }

    fn data_type(&mut self, schema: &str, object: &str) -> Result<Option<TypeRow>> {
        self.letture += 1;
        Ok(self.types.get(&name(schema, object)).cloned())
    }

    fn supertype(&mut self, schema: &str, object: &str) -> Result<Option<QualifiedName>> {
        self.letture += 1;
        Ok(self.supertypes.get(&name(schema, object)).cloned())
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

/// Il giudizio del piano sul catalogo finto: `Ok`, o il motivo del rifiuto.
fn giudizio(
    finto: &mut Finto,
    schema: Option<&str>,
    object: &str,
    campo: &str,
) -> std::result::Result<(), String> {
    let operation = confronto(schema, object, campo);
    let operands = scalar_operands(ProviderKind::Db2, &operation).expect("visita");
    let catalog = operand_catalog(finto, operands.relations()).expect("catalogo");
    operands.judge(&catalog).map_err(|error| error.message)
}

fn geometrica(finto: &mut Finto, schema: &str, object: &str, campo: &str) -> bool {
    giudizio(finto, Some(schema), object, campo) == Err(GEOMETRY_SCALAR_OPERAND.to_owned())
}

fn base() -> Finto {
    let mut finto = Finto::new();
    finto.table(
        "PLENORA",
        "T",
        &[
            ("ID", "SYSIBM", "INTEGER"),
            ("NAME", "SYSIBM", "VARCHAR"),
            ("G", "SYSIBM", "ST_GEOMETRY"),
            ("P", "SYSIBM", "ST_POINT"),
        ],
    );
    finto
}

#[test]
fn le_colonne_scalari_si_ammettono_e_le_geometriche_si_rifiutano() {
    let mut finto = base();
    assert_eq!(giudizio(&mut finto, Some("PLENORA"), "T", "ID"), Ok(()));
    assert_eq!(giudizio(&mut finto, Some("PLENORA"), "T", "NAME"), Ok(()));
    assert!(geometrica(&mut finto, "PLENORA", "T", "G"));
    assert!(geometrica(&mut finto, "PLENORA", "T", "P"));
}

#[test]
fn i_tipi_spatial_di_db2gse_sono_geometrici() {
    let mut finto = Finto::new();
    finto.table(
        "PLENORA",
        "T",
        &[
            ("ID", "SYSIBM", "INTEGER"),
            ("G", "DB2GSE", "ST_MULTIPOLYGON"),
        ],
    );
    assert!(geometrica(&mut finto, "PLENORA", "T", "G"));
    assert_eq!(giudizio(&mut finto, Some("PLENORA"), "T", "ID"), Ok(()));
}

#[test]
fn un_omonimo_di_un_altro_schema_si_giudica_dalla_sua_fonte() {
    let mut finto = Finto::new();
    finto.distinct("M", "ST_GEOMETRY", ("SYSIBM", "INTEGER"));
    finto.distinct("M", "MONEY", ("SYSIBM", "DECIMAL"));
    finto.table(
        "PLENORA",
        "T",
        &[("ID", "M", "ST_GEOMETRY"), ("PRICE", "M", "MONEY")],
    );
    assert_eq!(giudizio(&mut finto, Some("PLENORA"), "T", "ID"), Ok(()));
    assert_eq!(giudizio(&mut finto, Some("PLENORA"), "T", "PRICE"), Ok(()));
}

#[test]
fn i_tipi_dell_utente_che_arrivano_a_una_geometria_sono_geometrici() {
    let mut finto = Finto::new();
    finto.distinct("M", "DGEO", ("SYSIBM", "ST_GEOMETRY"));
    finto.distinct("M", "DDGEO", ("M", "DGEO"));
    finto.structured("M", "SGEO", Some(("DB2GSE", "ST_POINT")));
    finto.structured("M", "SSGEO", Some(("M", "SGEO")));
    finto.structured("M", "SBASE", None);
    finto.table(
        "PLENORA",
        "T",
        &[
            ("ID", "SYSIBM", "INTEGER"),
            ("A", "M", "DGEO"),
            ("B", "M", "DDGEO"),
            ("C", "M", "SGEO"),
            ("D", "M", "SSGEO"),
            ("E", "M", "SBASE"),
        ],
    );
    for campo in ["A", "B", "C", "D"] {
        assert!(geometrica(&mut finto, "PLENORA", "T", campo), "{campo}");
    }
    assert_eq!(giudizio(&mut finto, Some("PLENORA"), "T", "E"), Ok(()));
}

#[test]
fn un_tipo_che_il_catalogo_non_descrive_si_giudica_geometrico() {
    let mut finto = Finto::new();
    // Un ciclo di fonti, un metatipo che il giudizio non conosce, una fonte
    // di modulo, un tipo assente.
    finto.distinct("M", "C1", ("M", "C2"));
    finto.distinct("M", "C2", ("M", "C1"));
    finto.types.insert(
        name("M", "ARR"),
        TypeRow {
            metatype: "A".to_owned(),
            source: None,
            source_module: false,
        },
    );
    finto.types.insert(
        name("M", "MOD"),
        TypeRow {
            metatype: "T".to_owned(),
            source: Some(name("M", "X")),
            source_module: true,
        },
    );
    finto.table(
        "PLENORA",
        "T",
        &[
            ("ID", "SYSIBM", "INTEGER"),
            ("A", "M", "C1"),
            ("B", "M", "ARR"),
            ("C", "M", "MOD"),
            ("D", "M", "ASSENTE"),
        ],
    );
    for campo in ["A", "B", "C", "D"] {
        assert!(geometrica(&mut finto, "PLENORA", "T", campo), "{campo}");
    }
    assert_eq!(giudizio(&mut finto, Some("PLENORA"), "T", "ID"), Ok(()));
}

#[test]
fn una_catena_di_tipi_oltre_il_limite_si_giudica_geometrica() {
    let mut finto = Finto::new();
    for index in 0..=MAX_TYPE_DEPTH {
        let next = format!("D{}", index + 1);
        finto.distinct("M", &format!("D{index}"), ("M", &next));
    }
    finto.distinct(
        "M",
        &format!("D{}", MAX_TYPE_DEPTH + 1),
        ("SYSIBM", "INTEGER"),
    );
    finto.table(
        "PLENORA",
        "T",
        &[("ID", "SYSIBM", "INTEGER"), ("A", "M", "D0")],
    );
    assert!(geometrica(&mut finto, "PLENORA", "T", "A"));
    // La stessa catena entro il limite arriva a INTEGER.
    finto.table(
        "PLENORA",
        "U",
        &[("ID", "SYSIBM", "INTEGER"), ("A", "M", "D2")],
    );
    assert_eq!(giudizio(&mut finto, Some("PLENORA"), "U", "A"), Ok(()));
}

#[test]
fn senza_schema_vale_current_schema_poi_l_alias_pubblico() {
    let mut finto = base();
    finto.current = "PLENORA".to_owned();
    assert_eq!(giudizio(&mut finto, None, "T", "ID"), Ok(()));
    assert!(giudizio(&mut finto, None, "T", "G").is_err());
    // Un alias pubblico, soltanto se lo schema corrente non ha il nome.
    finto.current = "ALTRO".to_owned();
    finto.kind("SYSPUBLIC", "T", "A", Some(("PLENORA", "T")));
    assert_eq!(giudizio(&mut finto, None, "T", "ID"), Ok(()));
    assert_eq!(
        giudizio(&mut finto, None, "T", "G"),
        Err(GEOMETRY_SCALAR_OPERAND.to_owned())
    );
    finto.table(
        "ALTRO",
        "T",
        &[("ID", "SYSIBM", "INTEGER"), ("G", "SYSIBM", "BLOB")],
    );
    assert_eq!(giudizio(&mut finto, None, "T", "G"), Ok(()));
    // Con lo schema esplicito l'alias pubblico non vale.
    assert_eq!(giudizio(&mut finto, Some("NESSUNO"), "T", "G"), Ok(()));
}

#[test]
fn una_relazione_assente_non_ha_geometrie_e_session_non_si_conosce() {
    let mut finto = base();
    // Assente: la query fallira sul server; il piano non ha geometrie.
    assert_eq!(
        giudizio(&mut finto, Some("PLENORA"), "ASSENTE", "G"),
        Ok(())
    );
    // SESSION: una tabella temporanea dichiarata, che SYSCAT non descrive.
    finto.table("SESSION", "T", &[("ID", "SYSIBM", "INTEGER")]);
    assert!(giudizio(&mut finto, Some("SESSION"), "T", "ID").is_err());
    finto.current = "SESSION".to_owned();
    assert!(giudizio(&mut finto, None, "T", "ID").is_err());
}

#[test]
fn gli_alias_si_seguono_fino_alla_base() {
    let mut finto = base();
    finto.kind("PLENORA", "A", "A", Some(("PLENORA", "T")));
    finto.kind("PLENORA", "AA", "A", Some(("PLENORA", "A")));
    for alias in ["A", "AA"] {
        assert_eq!(giudizio(&mut finto, Some("PLENORA"), alias, "ID"), Ok(()));
        assert!(geometrica(&mut finto, "PLENORA", alias, "G"), "{alias}");
    }
}

#[test]
fn un_alias_che_non_arriva_a_una_relazione_non_si_conosce() {
    let mut finto = base();
    finto.kind("PLENORA", "AX", "A", Some(("PLENORA", "NONESISTE")));
    finto.kind("PLENORA", "AN", "A", None);
    finto.kind("PLENORA", "C1", "A", Some(("PLENORA", "C2")));
    finto.kind("PLENORA", "C2", "A", Some(("PLENORA", "C1")));
    finto.kind("PLENORA", "NICK", "N", None);
    finto.kind("PLENORA", "ANICK", "A", Some(("PLENORA", "NICK")));
    finto.kind("PLENORA", "ALTRO", "?", None);
    for object in ["AX", "AN", "C1", "NICK", "ANICK", "ALTRO"] {
        assert!(
            giudizio(&mut finto, Some("PLENORA"), object, "ID").is_err(),
            "{object}"
        );
    }
}

#[test]
fn una_catena_di_alias_oltre_il_limite_non_si_conosce() {
    let mut finto = base();
    for index in 0..MAX_ALIAS_DEPTH {
        let next = format!("A{}", index + 1);
        finto.kind(
            "PLENORA",
            &format!("A{index}"),
            "A",
            Some(("PLENORA", &next)),
        );
    }
    finto.kind(
        "PLENORA",
        &format!("A{MAX_ALIAS_DEPTH}"),
        "A",
        Some(("PLENORA", "T")),
    );
    assert!(giudizio(&mut finto, Some("PLENORA"), "A0", "ID").is_err());
    assert_eq!(giudizio(&mut finto, Some("PLENORA"), "A1", "ID"), Ok(()));
}

#[test]
fn le_viste_e_gli_altri_tipi_di_relazione_portano_le_colonne() {
    let mut finto = base();
    for kind in ["V", "S", "G", "U", "W", "H", "L"] {
        finto.table(
            "PLENORA",
            kind,
            &[("ID", "SYSIBM", "INTEGER"), ("G", "SYSIBM", "ST_GEOMETRY")],
        );
        finto.kind("PLENORA", kind, kind, None);
        assert_eq!(
            giudizio(&mut finto, Some("PLENORA"), kind, "ID"),
            Ok(()),
            "{kind}"
        );
        assert!(geometrica(&mut finto, "PLENORA", kind, "G"), "{kind}");
    }
    // Una relazione senza colonne nel catalogo non si conosce.
    finto.kind("PLENORA", "VUOTA", "V", None);
    assert!(giudizio(&mut finto, Some("PLENORA"), "VUOTA", "ID").is_err());
}

#[test]
fn i_nomi_si_confrontano_per_byte() {
    let mut finto = base();
    // Un nome del piano che incontra la colonna soltanto senza maiuscole.
    assert!(giudizio(&mut finto, Some("PLENORA"), "T", "id").is_err());
    assert!(giudizio(&mut finto, Some("PLENORA"), "T", "g").is_err());
    // Una relazione nominata in minuscolo e un'altra relazione.
    assert_eq!(giudizio(&mut finto, Some("plenora"), "T", "G"), Ok(()));
}

#[test]
fn il_tipo_si_legge_una_volta_sola_per_relazioni() {
    let mut finto = Finto::new();
    finto.distinct("M", "DGEO", ("SYSIBM", "ST_GEOMETRY"));
    finto.table("PLENORA", "T", &[("A", "M", "DGEO"), ("B", "M", "DGEO")]);
    let geometric = geometric_columns(&mut finto, "PLENORA", "T").expect("colonne");
    assert_eq!(geometric, Some(vec!["A".to_owned(), "B".to_owned()]));
    // relation, columns e un solo data_type (poi la fonte SYSIBM, senza
    // lettura).
    assert_eq!(finto.letture, 3);
    // Una relazione che non si conosce non ha un elenco.
    finto.kind("PLENORA", "NICK", "N", None);
    assert_eq!(
        geometric_columns(&mut finto, "PLENORA", "NICK").expect("nickname"),
        None
    );
}

/// Misurato: una colonna di vista resa da `ST_CENTROID(G)` e `SYSIBM.BLOB`
/// nel catalogo e porta il formato interno della geometria. In una vista,
/// una MQT o una vista tipizzata un `BLOB` si giudica geometrico; in una
/// tabella resta il tipo dichiarato.
#[test]
fn un_blob_calcolato_da_una_vista_si_giudica_geometrico() {
    let mut finto = Finto::new();
    for kind in ["T", "V", "S", "W", "G"] {
        finto.table(
            "PLENORA",
            kind,
            &[
                ("ID", "SYSIBM", "INTEGER"),
                ("C", "SYSIBM", "BLOB"),
                ("X", "SYSIBM", "CLOB"),
            ],
        );
        finto.kind("PLENORA", kind, kind, None);
        let computed = matches!(kind, "V" | "S" | "W");
        assert_eq!(
            geometrica(&mut finto, "PLENORA", kind, "C"),
            computed,
            "{kind}"
        );
        let geometric = geometric_columns(&mut finto, "PLENORA", kind).expect("colonne");
        assert_eq!(
            geometric,
            Some(if computed {
                vec!["C".to_owned()]
            } else {
                Vec::new()
            }),
            "{kind}"
        );
        // Le altre colonne restano scalari.
        assert_eq!(
            giudizio(&mut finto, Some("PLENORA"), kind, "X"),
            Ok(()),
            "{kind}"
        );
        assert_eq!(
            giudizio(&mut finto, Some("PLENORA"), kind, "ID"),
            Ok(()),
            "{kind}"
        );
    }
}
