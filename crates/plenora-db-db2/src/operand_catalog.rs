//! Il catalogo degli operandi geometrici di Db2
//! (`plenora_database_core::spatial_operand`), letto nella transazione e
//! nella lettura del provider.
//!
//! Db2 12.1 non protegge gli operandi geometrici: `SYSIBM.ST_GEOMETRY` e un
//! tipo distinto su `BLOB`, e `=`, `<`, `IN`, `BETWEEN`, `CAST`, `COALESCE`
//! confrontano i byte del formato interno senza errore (misurato,
//! `tests/fixtures/operandi_server_guard.json`). Il giudizio quindi legge le
//! colonne di ogni relazione nominata dal piano, con il loro tipo, da
//! `SYSCAT` (misurato su Db2 12.1.5, l'immagine di
//! `docker/db2-client/Dockerfile`, il 25/10):
//!
//! - **il nome.** Il renderer quota ogni identificatore: il nome del piano e
//!   il nome del catalogo, per byte (`NameRule::Exact`). Il server ignora gli
//!   spazi finali di un identificatore (`"G"` lega una colonna `"G "`, `"t"`
//!   e `"t "` sono lo stesso nome) e completa con spazi il confronto delle
//!   stringhe: una relazione che il catalogo trova soltanto cosi non si
//!   conosce, una colonna e la forma (d) della regola N. Una relazione senza
//!   schema si risolve come la risolve il server: `CURRENT SCHEMA` della
//!   connessione, poi un alias pubblico (`SYSPUBLIC`). Lo schema `SESSION`
//!   nomina anche le tabelle temporanee dichiarate, che `SYSCAT` non
//!   descrive: non si conosce;
//! - **la relazione.** Una tabella, una vista, una MQT, una tabella
//!   temporanea creata, una tabella o una vista tipizzata: le sue colonne da
//!   `SYSCAT.COLUMNS`. Un alias (`SYSCAT.TABLES.TYPE = 'A'`, anche pubblico)
//!   si segue fino alla sua base (`BASE_TABSCHEMA`, `BASE_TABNAME`), anche
//!   attraverso altri alias; un alias che non arriva a una relazione (base
//!   assente, ciclo, catena oltre [`MAX_ALIAS_DEPTH`]) e un nickname
//!   (`TYPE = 'N'`, un'altra fonte federata) non si conoscono: nessun
//!   riferimento si risolve;
//! - **il tipo.** Una colonna e geometrica se il suo tipo e un tipo spatial
//!   di `SYSIBM` (`ST_GEOMETRY`, `ST_POINT`, ..., `ST_SURFACE`, `ST_RASTER`:
//!   tutti distinti su `BLOB`) o di `DB2GSE` (lo Spatial Extender delle
//!   versioni precedenti), o un tipo dell'utente che vi arriva: un tipo
//!   distinto attraverso la sua fonte (`SYSCAT.DATATYPES`), un tipo
//!   strutturato attraverso il suo supertipo (`SYSCAT.HIERARCHIES`). Su 12.1
//!   il server non ne crea (misurato: un distinto su `ST_GEOMETRY`, SQL20441N;
//!   `UNDER ST_GEOMETRY`, SQL20026N), e la regola resta per le versioni con
//!   tipi strutturati. Un omonimo di un altro schema (`M.ST_GEOMETRY` su
//!   `INTEGER`) si giudica dalla sua fonte. Un tipo che il catalogo non
//!   descrive, o una catena oltre [`MAX_TYPE_DEPTH`], si giudica geometrico:
//!   a mondo chiuso non si prova scalare;
//! - **la colonna calcolata.** In una vista, una MQT o una vista tipizzata
//!   una colonna binaria si giudica geometrica (la regola B,
//!   `spatial_operand::catalog_column_is_geometric`): `BLOB`, `BINARY`,
//!   `VARBINARY`, `CHAR`/`VARCHAR`/`LONG VARCHAR FOR BIT DATA`
//!   (`CODEPAGE` 0), o un tipo distinto la cui catena di fonti vi arriva. Il
//!   catalogo dice `BLOB` per una colonna resa da una funzione spatial
//!   (misurato: `ST_CENTROID(G)` in una vista e `SYSIBM.BLOB`, porta il
//!   formato interno della geometria e `V.C = T.G` passa senza errore), e
//!   `VARCHAR FOR BIT DATA` o `VARBINARY` per `CAST(G AS ...)` e
//!   `CAST(ST_ASBINARY(G) AS ...)` (misurato il 25/10: due poligoni uguali con
//!   i vertici in ordine diverso non sono uguali sotto `=`).

use crate::connection::query_text;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::spatial_operand::{
    catalog_column_is_geometric, ColumnCarrier, OperandCatalog, RelationColumns, RelationOrigin,
};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::BTreeMap;

/// Oltre questa catena di alias la relazione non si conosce.
pub const MAX_ALIAS_DEPTH: usize = 32;

/// Oltre questa catena di tipi (fonte di un tipo distinto, supertipo di un
/// tipo strutturato) il tipo si giudica geometrico.
pub const MAX_TYPE_DEPTH: usize = 32;

/// Gli schemi dei tipi spatial del prodotto: `SYSIBM` su Db2 12.1 (misurato),
/// `DB2GSE` sullo Spatial Extender delle versioni precedenti.
const SPATIAL_SCHEMAS: [&str; 2] = ["SYSIBM", "DB2GSE"];

/// Lo schema degli alias pubblici, dove il server cerca un nome senza schema
/// che `CURRENT SCHEMA` non ha.
const PUBLIC_SCHEMA: &str = "SYSPUBLIC";

/// Lo schema delle tabelle temporanee dichiarate (`DECLARE GLOBAL TEMPORARY
/// TABLE`), che `SYSCAT` non descrive.
const SESSION_SCHEMA: &str = "SESSION";

/// Il tipo di una relazione che il catalogo uguaglia al nome soltanto
/// completandolo con spazi: non si conosce.
const UNRESOLVED_KIND: &str = "?";

const CURRENT_SCHEMA_SQL: &str = "SELECT CURRENT SCHEMA FROM SYSIBM.SYSDUMMY1";

/// Una relazione del catalogo per nome, con il nome com'e nel catalogo: il
/// confronto di Db2 completa con spazi la stringa piu corta, e il nome
/// esatto si confronta qui.
const RELATION_SQL: &str = "SELECT TABSCHEMA, TABNAME, TYPE, BASE_TABSCHEMA, BASE_TABNAME \
    FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ?";

const COLUMNS_SQL: &str = "SELECT TABSCHEMA, TABNAME, COLNAME, TYPESCHEMA, TYPENAME, \
    CASE WHEN CODEPAGE = 0 THEN 'Y' ELSE 'N' END \
    FROM SYSCAT.COLUMNS WHERE TABSCHEMA = ? AND TABNAME = ? ORDER BY COLNO";

const DATATYPE_SQL: &str = "SELECT TYPESCHEMA, TYPENAME, METATYPE, SOURCESCHEMA, SOURCENAME, \
    SOURCEMODULENAME, CASE WHEN CODEPAGE = 0 THEN 'Y' ELSE 'N' END FROM SYSCAT.DATATYPES \
    WHERE TYPESCHEMA = ? AND TYPENAME = ? AND TYPEMODULENAME IS NULL";

/// I tipi binari di `SYSIBM`; i tipi carattere lo sono con `FOR BIT DATA`.
const BINARY_TYPES: [&str; 3] = ["BLOB", "BINARY", "VARBINARY"];
const BIT_DATA_TYPES: [&str; 3] = ["CHARACTER", "VARCHAR", "LONG VARCHAR"];

const SUPERTYPE_SQL: &str = "SELECT SUB_SCHEMA, SUB_NAME, SUPER_SCHEMA, SUPER_NAME \
    FROM SYSCAT.HIERARCHIES WHERE METATYPE = 'R' AND SUB_SCHEMA = ? AND SUB_NAME = ?";

/// Un nome qualificato del catalogo: schema, nome.
pub type QualifiedName = (String, String);

/// Una relazione di `SYSCAT.TABLES`: il tipo e, per un alias, la base.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationRow {
    pub kind: String,
    pub base: Option<QualifiedName>,
}

/// Una colonna di `SYSCAT.COLUMNS`: nome, tipo e `FOR BIT DATA`
/// (`CODEPAGE` 0).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRow {
    pub name: String,
    pub type_name: QualifiedName,
    pub bit_data: bool,
}

/// Un tipo dell'utente di `SYSCAT.DATATYPES`: il metatipo e, per un tipo
/// distinto, la fonte; `source_module`: la fonte e di un modulo;
/// `bit_data`: la fonte e `FOR BIT DATA` (`CODEPAGE` 0).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeRow {
    pub metatype: String,
    pub source: Option<QualifiedName>,
    pub source_module: bool,
    pub bit_data: bool,
}

/// Le letture del catalogo su cui si regge il giudizio. Ogni nome si
/// confronta per byte con quello del catalogo.
pub trait Db2Catalog {
    /// `CURRENT SCHEMA` della connessione.
    ///
    /// # Errors
    ///
    /// Gli errori del driver.
    fn current_schema(&mut self) -> Result<String>;

    /// La relazione con quel nome esatto, se c'e.
    ///
    /// # Errors
    ///
    /// Gli errori del driver.
    fn relation(&mut self, schema: &str, name: &str) -> Result<Option<RelationRow>>;

    /// Le colonne della relazione, nell'ordine del catalogo.
    ///
    /// # Errors
    ///
    /// Gli errori del driver.
    fn columns(&mut self, schema: &str, name: &str) -> Result<Vec<ColumnRow>>;

    /// Il tipo dell'utente con quel nome esatto, fuori da un modulo.
    ///
    /// # Errors
    ///
    /// Gli errori del driver.
    fn data_type(&mut self, schema: &str, name: &str) -> Result<Option<TypeRow>>;

    /// Il supertipo di un tipo strutturato, se ne ha uno.
    ///
    /// # Errors
    ///
    /// Gli errori del driver.
    fn supertype(&mut self, schema: &str, name: &str) -> Result<Option<QualifiedName>>;
}

/// Il catalogo delle relazioni (schema, oggetto) come il piano le nomina.
///
/// # Errors
///
/// Gli errori del catalogo.
pub fn operand_catalog(
    catalog: &mut dyn Db2Catalog,
    relations: &[(Option<String>, String)],
) -> Result<OperandCatalog> {
    let mut types = TypeJudge::default();
    let mut current = None;
    let mut result = OperandCatalog::new(ProviderKind::Db2);
    for (schema, object) in relations {
        let columns =
            relation_columns(catalog, &mut types, &mut current, schema.as_deref(), object)?;
        result.insert(schema.clone(), object.clone(), columns);
    }
    Ok(result)
}

/// I nomi delle colonne geometriche della relazione con schema esplicito
/// (la lettura del provider, che descrive tabelle e viste), con lo stesso
/// giudizio; `None`: la relazione non si conosce.
///
/// # Errors
///
/// Gli errori del catalogo.
pub fn geometric_columns(
    catalog: &mut dyn Db2Catalog,
    schema: &str,
    object: &str,
) -> Result<Option<Vec<String>>> {
    let mut types = TypeJudge::default();
    let mut current = None;
    let columns = relation_columns(catalog, &mut types, &mut current, Some(schema), object)?;
    Ok(match columns {
        RelationColumns::Known(columns) => Some(
            columns
                .into_iter()
                .filter_map(|(name, geometric)| geometric.then_some(name))
                .collect(),
        ),
        _ => None,
    })
}

fn relation_columns(
    catalog: &mut dyn Db2Catalog,
    types: &mut TypeJudge,
    current: &mut Option<String>,
    schema: Option<&str>,
    object: &str,
) -> Result<RelationColumns> {
    let mut name = if let Some(schema) = schema {
        (schema.to_owned(), object.to_owned())
    } else {
        if current.is_none() {
            *current = Some(catalog.current_schema()?);
        }
        (current.clone().unwrap_or_default(), object.to_owned())
    };
    let mut first = true;
    let mut row = None;
    for _ in 0..=MAX_ALIAS_DEPTH {
        if name.0 == SESSION_SCHEMA {
            return Ok(RelationColumns::Unknown);
        }
        let found = match catalog.relation(&name.0, &name.1)? {
            None if first && schema.is_none() => {
                // Senza schema il server cerca poi un alias pubblico.
                name = (PUBLIC_SCHEMA.to_owned(), object.to_owned());
                catalog.relation(&name.0, &name.1)?
            }
            found => found,
        };
        let Some(found) = found else {
            // Il nome del piano non c'e: la query fallira sul server. La base
            // di un alias che non c'e non si conosce.
            return Ok(if first {
                RelationColumns::Absent
            } else {
                RelationColumns::Unknown
            });
        };
        first = false;
        if found.kind.trim_end() == "A" {
            let Some(base) = found.base else {
                return Ok(RelationColumns::Unknown);
            };
            name = base;
            continue;
        }
        row = Some(found);
        break;
    }
    let Some(row) = row else {
        // Una catena di alias oltre il limite, o un ciclo.
        return Ok(RelationColumns::Unknown);
    };
    // Tabella, vista, MQT, tabella temporanea creata, tabella o vista
    // tipizzata, gerarchia, tabella staccata: le colonne in SYSCAT.COLUMNS.
    // Un nickname (`N`) e un'altra fonte, e il resto non si conosce.
    let kind = row.kind.trim_end();
    if !matches!(kind, "T" | "V" | "S" | "G" | "U" | "W" | "H" | "L") {
        return Ok(RelationColumns::Unknown);
    }
    // Vista, MQT, vista tipizzata: colonne calcolate.
    let origin = if matches!(kind, "V" | "S" | "W") {
        RelationOrigin::Derived
    } else {
        RelationOrigin::Base
    };
    let rows = catalog.columns(&name.0, &name.1)?;
    if rows.is_empty() {
        return Ok(RelationColumns::Unknown);
    }
    let mut columns = Vec::with_capacity(rows.len());
    for column in rows {
        let carrier = types.carrier(catalog, &column.type_name, column.bit_data)?;
        columns.push((column.name, catalog_column_is_geometric(origin, carrier)));
    }
    Ok(RelationColumns::Known(columns))
}

/// Il giudizio del tipo di una colonna, con i tipi gia giudicati.
#[derive(Default)]
struct TypeJudge {
    judged: BTreeMap<(QualifiedName, bool), ColumnCarrier>,
}

impl TypeJudge {
    /// Il tipo, seguito fino a un tipo di `SYSIBM` o spatial: geometrico,
    /// binario (anche `FOR BIT DATA`), scalare. Un tipo che il catalogo non
    /// descrive, o una catena oltre [`MAX_TYPE_DEPTH`], e geometrico: a mondo
    /// chiuso non si prova scalare.
    fn carrier(
        &mut self,
        catalog: &mut dyn Db2Catalog,
        start: &QualifiedName,
        bit_data: bool,
    ) -> Result<ColumnCarrier> {
        let key = (start.clone(), bit_data);
        if let Some(judged) = self.judged.get(&key) {
            return Ok(*judged);
        }
        let mut current = start.clone();
        let mut bit_data = bit_data;
        let mut verdict = ColumnCarrier::Geometry;
        for _ in 0..=MAX_TYPE_DEPTH {
            let (schema, name) = (current.0.trim_end(), current.1.as_str());
            if SPATIAL_SCHEMAS.contains(&schema) && name.starts_with("ST_") {
                verdict = ColumnCarrier::Geometry;
                break;
            }
            if schema == "SYSIBM" {
                verdict = if BINARY_TYPES.contains(&name)
                    || (bit_data && BIT_DATA_TYPES.contains(&name))
                {
                    ColumnCarrier::Binary
                } else {
                    ColumnCarrier::Scalar
                };
                break;
            }
            let Some(row) = catalog.data_type(schema, name)? else {
                break;
            };
            let next = match row.metatype.trim_end() {
                // Distinto: la sua fonte, con il suo `FOR BIT DATA`.
                "T" if !row.source_module => {
                    bit_data = row.bit_data;
                    row.source
                }
                // Strutturato: il supertipo (la fonte di un tipo strutturato e
                // la rappresentazione del riferimento, non un supertipo);
                // senza supertipo, un tipo dell'utente che non arriva a una
                // geometria.
                "R" => {
                    let Some(supertype) = catalog.supertype(schema, name)? else {
                        verdict = ColumnCarrier::Scalar;
                        break;
                    };
                    Some(supertype)
                }
                _ => None,
            };
            let Some(next) = next else {
                break;
            };
            current = next;
        }
        self.judged.insert(key, verdict);
        Ok(verdict)
    }
}

/// Il catalogo letto sulla connessione ODBC.
pub struct OdbcCatalog<'c, 'e> {
    connection: &'c odbc_api::Connection<'e>,
    timeout: usize,
}

impl<'c, 'e> OdbcCatalog<'c, 'e> {
    pub const fn new(connection: &'c odbc_api::Connection<'e>, timeout: usize) -> Self {
        Self {
            connection,
            timeout,
        }
    }

    fn rows(
        &self,
        sql: &str,
        parameters: &[&str],
        columns: usize,
    ) -> Result<Vec<Vec<Option<String>>>> {
        query_text(
            self.connection,
            sql,
            parameters,
            self.timeout,
            columns,
            1024,
        )
    }
}

const COLUMN_TYPES_SQL: &str = "SELECT COLNAME, TYPENAME, VARCHAR(LENGTH), VARCHAR(SCALE), \
    VARCHAR(CODEPAGE) FROM SYSCAT.COLUMNS WHERE TABSCHEMA = ? AND TABNAME = ? ORDER BY COLNO";

impl OdbcCatalog<'_, '_> {
    /// I tipi delle colonne di una relazione (`SYSCAT.COLUMNS`), con
    /// lunghezza, scala e `FOR BIT DATA`, per l'ammissione dei letterali
    /// portable (`plenora_database_core::operand_admission`). `None` per una
    /// relazione che il catalogo non conosce.
    ///
    /// # Errors
    ///
    /// Gli errori del catalogo.
    pub fn column_types(
        &mut self,
        schema: Option<&str>,
        name: &str,
    ) -> Result<Option<plenora_database_core::operand_admission::RelationTypes>> {
        let schema = match schema {
            Some(schema) => schema.to_owned(),
            None => self.current_schema()?,
        };
        let rows = self.rows(COLUMN_TYPES_SQL, &[&schema, name], 5)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let number = |row: &[Option<String>], index: usize| -> Result<u64> {
            cell(row, index)
                .map(str::trim)
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| malformed("numero del catalogo Db2 non leggibile"))
        };
        rows.iter()
            .map(|row| {
                let column = cell(row, 0)
                    .ok_or_else(|| malformed("colonna Db2 senza nome"))?
                    .to_owned();
                let type_name = cell(row, 1)
                    .ok_or_else(|| malformed("colonna Db2 senza tipo"))?
                    .trim_end()
                    .to_owned();
                Ok((
                    column,
                    catalog_type(
                        &type_name,
                        number(row, 2)?,
                        i64::try_from(number(row, 3)?).unwrap_or(i64::MAX),
                        number(row, 4)? == 0,
                    ),
                ))
            })
            .collect::<Result<_>>()
            .map(Some)
    }
}

/// Il tipo del catalogo Db2: `LENGTH` e la precisione dei decimali e la
/// lunghezza dei testi e dei binari, `SCALE` la scala o le cifre frazionarie
/// di `TIMESTAMP`; un testo con `CODEPAGE` 0 e `FOR BIT DATA`, cioe binario.
#[must_use]
pub fn catalog_type(
    type_name: &str,
    length: u64,
    scale: i64,
    bit_data: bool,
) -> plenora_database_core::operand_admission::CatalogType {
    let name = match (type_name.to_ascii_uppercase().as_str(), bit_data) {
        ("CHARACTER" | "CHAR", true) => "BINARY".to_owned(),
        ("VARCHAR", true) => "VARBINARY".to_owned(),
        (name, _) => name.to_owned(),
    };
    plenora_database_core::operand_admission::CatalogType {
        name,
        declaration: None,
        length: Some(length),
        precision: Some(length),
        scale: Some(scale),
        datetime_precision: None,
    }
}

/// Le righe il cui nome (le prime due colonne) e quello esatto.
fn exact<'r>(
    rows: &'r [Vec<Option<String>>],
    schema: &str,
    name: &str,
) -> std::vec::IntoIter<&'r Vec<Option<String>>> {
    rows.iter()
        .filter(|row| cell(row, 0).map(str::trim_end) == Some(schema) && cell(row, 1) == Some(name))
        .collect::<Vec<_>>()
        .into_iter()
}

fn cell(row: &[Option<String>], index: usize) -> Option<&str> {
    row.get(index).and_then(|value| value.as_deref())
}

fn qualified(row: &[Option<String>], schema: usize, name: usize) -> Option<QualifiedName> {
    Some((
        cell(row, schema)?.trim_end().to_owned(),
        cell(row, name)?.to_owned(),
    ))
}

/// Un flag `Y`/`N` reso dal catalogo; un altro valore e un catalogo che non
/// si riconosce.
fn flag(row: &[Option<String>], index: usize, message: &'static str) -> Result<bool> {
    match cell(row, index).map(str::trim_end) {
        Some("Y") => Ok(true),
        Some("N") => Ok(false),
        _ => Err(malformed(message)),
    }
}

fn malformed(message: &'static str) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Protocol,
        ErrorPhase::Prepare,
        Some(ProviderKind::Db2),
        message,
    )
}

impl Db2Catalog for OdbcCatalog<'_, '_> {
    fn current_schema(&mut self) -> Result<String> {
        let rows = self.rows(CURRENT_SCHEMA_SQL, &[], 1)?;
        match rows.as_slice() {
            [row] => cell(row, 0)
                .map(|schema| schema.trim_end().to_owned())
                .ok_or_else(|| malformed("CURRENT SCHEMA Db2 assente")),
            _ => Err(malformed("CURRENT SCHEMA Db2 non univoco")),
        }
    }

    fn relation(&mut self, schema: &str, name: &str) -> Result<Option<RelationRow>> {
        let rows = self.rows(RELATION_SQL, &[schema, name], 5)?;
        let mut found = exact(&rows, schema, name);
        let row = found.next();
        if found.next().is_some() {
            return Err(malformed("relazione Db2 non univoca nel catalogo"));
        }
        if row.is_none() && !rows.is_empty() {
            // Il server ha uguagliato il nome a uno che differisce per gli
            // spazi finali: non si sa quale relazione legherebbe la query.
            return Ok(Some(RelationRow {
                kind: UNRESOLVED_KIND.to_owned(),
                base: None,
            }));
        }
        row.map(|row| {
            Ok(RelationRow {
                kind: cell(row, 2)
                    .ok_or_else(|| malformed("relazione Db2 senza tipo"))?
                    .to_owned(),
                base: qualified(row, 3, 4),
            })
        })
        .transpose()
    }

    fn columns(&mut self, schema: &str, name: &str) -> Result<Vec<ColumnRow>> {
        let rows = self.rows(COLUMNS_SQL, &[schema, name], 6)?;
        exact(&rows, schema, name)
            .map(|row| {
                Ok(ColumnRow {
                    name: cell(row, 2)
                        .ok_or_else(|| malformed("colonna Db2 senza nome"))?
                        .to_owned(),
                    type_name: qualified(row, 3, 4)
                        .ok_or_else(|| malformed("colonna Db2 senza tipo"))?,
                    bit_data: flag(row, 5, "colonna Db2 senza CODEPAGE")?,
                })
            })
            .collect()
    }

    fn data_type(&mut self, schema: &str, name: &str) -> Result<Option<TypeRow>> {
        let rows = self.rows(DATATYPE_SQL, &[schema, name], 7)?;
        let mut found = exact(&rows, schema, name);
        let row = found.next();
        if found.next().is_some() {
            return Err(malformed("tipo Db2 non univoco nel catalogo"));
        }
        row.map(|row| {
            Ok(TypeRow {
                metatype: cell(row, 2)
                    .ok_or_else(|| malformed("tipo Db2 senza metatipo"))?
                    .to_owned(),
                source: qualified(row, 3, 4),
                source_module: cell(row, 5).is_some(),
                bit_data: flag(row, 6, "tipo Db2 senza CODEPAGE")?,
            })
        })
        .transpose()
    }

    fn supertype(&mut self, schema: &str, name: &str) -> Result<Option<QualifiedName>> {
        let rows = self.rows(SUPERTYPE_SQL, &[schema, name], 4)?;
        let mut found = exact(&rows, schema, name);
        let row = found.next();
        if found.next().is_some() {
            return Err(malformed("supertipo Db2 non univoco nel catalogo"));
        }
        Ok(row.and_then(|row| qualified(row, 2, 3)))
    }
}
