//! Chiavi e helper che applicano il contratto Arrow sul bordo pubblico.

pub const CONTRACT_VERSION: &str = "1";
use crate::arrow::{Field, Schema, SchemaRef};
use std::collections::HashMap;
use std::sync::Arc;

pub const CONTRACT_VERSION_KEY: &str = "plenora.contract.version";

/// Costruisce lo schema Arrow sul bordo pubblico applicando sempre la
/// versione del contratto corrente.
///
/// Tenerlo nel core evita che i provider possano divergere silenziosamente
/// sulla metadata obbligatoria dello schema.
///
/// # Field id
///
/// Ogni campo esce con `plenora.field_id` (ARROW-VOCABULARY §2, e §4 per le
/// geometrie). Un id gia dichiarato resta, qualunque intero decimale non
/// negativo sia: il vocabolario non pone un massimo. Il provider dichiara
/// l'id quando conosce l'origine della colonna (PostgreSQL: `attrelid` e
/// `attnum`, vedi [`origin_field_id`]), cosi una rinomina o un alias non lo
/// cambiano. Per gli altri campi — espressioni, e gli adapter che non
/// espongono l'origine — l'id si deriva dal nome: e un limite dichiarato,
/// perche una rinomina di quei campi cambia l'id. La posizione non entra mai.
///
/// Gli id generati sono FNV-1a a 32 bit ridotto a 31 bit, interi non negativi
/// anche per chi li legge con segno.
///
/// # Errors
///
/// `Schema` se un id dichiarato non e un intero decimale non negativo, o se
/// due campi hanno lo stesso id — nomi ripetuti, collisione dell'hash, id
/// dichiarati uguali (anche con zeri iniziali diversi): l'unicita non si
/// ripara rinumerando.
pub fn contract_schema(fields: Vec<Field>) -> crate::Result<SchemaRef> {
    let mut seen = std::collections::HashSet::new();
    let fields = fields
        .into_iter()
        .map(|field| {
            let declared = field.metadata().get(FIELD_ID).cloned();
            let id = match &declared {
                Some(value) => canonical_field_id(value).ok_or_else(|| {
                    field_id_error("field_id dichiarato non e un intero decimale non negativo")
                })?,
                None => name_field_id(field.name()).to_string(),
            };
            if !seen.insert(id.clone()) {
                return Err(field_id_error(
                    "field_id duplicato nello schema: nomi di campo ripetuti, id uguali o \
                     collisione",
                ));
            }
            if declared.is_some() {
                return Ok(field);
            }
            let mut metadata = field.metadata().clone();
            metadata.insert(FIELD_ID.to_owned(), id);
            Ok(field.with_metadata(metadata))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([(CONTRACT_VERSION_KEY.to_owned(), CONTRACT_VERSION.to_owned())]),
    )))
}

/// La forma canonica di un field id: cifre decimali senza zeri iniziali.
/// `None` se il valore non e un intero decimale non negativo.
#[must_use]
pub fn canonical_field_id(value: &str) -> Option<String> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let trimmed = value.trim_start_matches('0');
    Some(if trimmed.is_empty() { "0" } else { trimmed }.to_owned())
}

/// L'id di un campo che non ne dichiara uno: FNV-1a 32 bit del nome, 31 bit.
#[must_use]
pub fn name_field_id(name: &str) -> u32 {
    fnv31(&[name.as_bytes()])
}

/// L'id di una colonna dalla sua origine nel database.
///
/// Relazione e colonna, nei byte che il provider sceglie (per PostgreSQL
/// `attrelid` e `attnum`): stabile finche la colonna e la stessa, qualunque
/// nome abbia.
#[must_use]
pub fn origin_field_id(relation: &[u8], column: &[u8]) -> u32 {
    fnv31(&[b"origin", relation, column])
}

/// FNV-1a 32 bit su parti separate, ridotto a 31 bit.
fn fnv31(parts: &[&[u8]]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            hash ^= 0xff;
            hash = hash.wrapping_mul(0x0100_0193);
        }
        for byte in *part {
            hash ^= u32::from(*byte);
            hash = hash.wrapping_mul(0x0100_0193);
        }
    }
    hash & 0x7fff_ffff
}

fn field_id_error(message: &str) -> crate::DatabaseError {
    crate::DatabaseError::new(
        crate::ErrorCategory::Schema,
        crate::ErrorPhase::Read,
        None,
        message,
    )
}

pub const GEOMETRY_ENCODING: &str = "plenora.geometry.encoding";
pub const GEOMETRY_DIMENSIONS: &str = "plenora.geometry.dimensions";
pub const GEOMETRY_TYPES: &str = "plenora.geometry.types";
pub const GEOMETRY_TYPES_DECLARATION: &str = "plenora.geometry.types_declaration";
pub const GEOMETRY_SRID: &str = "plenora.geometry.srid";
pub const GEOMETRY_CRS_RESOLUTION: &str = "plenora.geometry.crs_resolution";
pub const GEOMETRY_CRS_ID: &str = "plenora.geometry.crs_id";
pub const GEOMETRY_CRS_DEFINITION: &str = "plenora.geometry.crs_definition";
pub const GEOMETRY_CRS_DEFINITION_FORMAT: &str = "plenora.geometry.crs_definition_format";
pub const GEOMETRY_AXIS_ORDER: &str = "plenora.geometry.axis_order";
pub const GEOMETRY_SPATIAL_SEMANTICS: &str = "plenora.geometry.spatial_semantics";
pub const GEOMETRY_PRECISION: &str = "plenora.geometry.precision";
pub const FIELD_ID: &str = "plenora.field_id";

pub const POSTGRES_NATIVE_TYPE: &str = "plenora.postgres.native_type";
pub const POSTGRES_NATIVE_DECLARATION: &str = "plenora.postgres.native_declaration";
pub const POSTGRES_TYPE_KIND: &str = "plenora.postgres.type_kind";
pub const POSTGRES_ENUM_LABELS: &str = "plenora.postgres.enum_labels";
pub const POSTGRES_DOMAIN_BASE_TYPE: &str = "plenora.postgres.domain_base_type";
pub const POSTGRES_DOMAIN_CONSTRAINTS: &str = "plenora.postgres.domain_constraints";
pub const POSTGRES_COLLATION: &str = "plenora.postgres.collation";

pub const SQLSERVER_NATIVE_TYPE: &str = "plenora.sqlserver.native_type";
pub const SQLSERVER_NATIVE_DECLARATION: &str = "plenora.sqlserver.native_declaration";
pub const SQLSERVER_COLLATION: &str = "plenora.sqlserver.collation";

/// Tipo nativo osservato sul percorso di lettura.
///
/// Il catalogo descrive la dichiarazione SQL; il risultato di una query
/// descrive il tipo wire. JSON e LONGTEXT di MariaDB non sono distinguibili
/// dai soli metadata wire: per la dichiarazione SQL occorre il catalogo.
pub const MYSQL_NATIVE_TYPE: &str = "plenora.mysql.native_type";

/// La dichiarazione SQL completa, quando il provider l'ha vista.
///
/// Vuota — cioe assente dai metadata — sul path query: il prepare descrive il
/// tipo del protocollo e non conserva lunghezza, precisione frazionaria,
/// collation o il tipo di un'espressione. Ricostruirla darebbe una stringa
/// plausibile e non fedele, che e il modo in cui un metadato smette di essere
/// verificabile.
pub const MYSQL_NATIVE_DECLARATION: &str = "plenora.mysql.native_declaration";
pub const MYSQL_COLLATION: &str = "plenora.mysql.collation";

/// Come [`MYSQL_NATIVE_TYPE`], per `MariaDB`.
///
/// Namespace proprio, e non e una formalita. Il contratto usa gia un
/// namespace per prodotto — `plenora.postgres.*`, `plenora.sqlserver.*` — e
/// un consumatore che leggesse `plenora.mysql.native_type` da un server
/// `MariaDB` dovrebbe indovinare, da un metadato che non lo dice, quale delle
/// due tabelle di tipi applicare. Sono tabelle che divergono davvero: dalla
/// stessa DDL `document JSON` esce `json` da `MySQL` e `text` da `MariaDB`.
///
/// Il protocollo condiviso non e un argomento per condividere il namespace:
/// un metadato dichiara chi ha risposto, non come gli si e parlato.
pub const MARIADB_NATIVE_TYPE: &str = "plenora.mariadb.native_type";

/// Come [`MYSQL_NATIVE_DECLARATION`], per `MariaDB`. Vedi
/// [`MARIADB_NATIVE_TYPE`] per la scelta del namespace.
pub const MARIADB_NATIVE_DECLARATION: &str = "plenora.mariadb.native_declaration";

/// Come [`MYSQL_COLLATION`], per `MariaDB`. Vedi [`MARIADB_NATIVE_TYPE`] per
/// la scelta del namespace.
pub const MARIADB_COLLATION: &str = "plenora.mariadb.collation";

pub const GEOARROW_EXTENSION_NAME: &str = "ARROW:extension:name";

#[cfg(test)]
#[path = "protocol_tests.rs"]
mod tests;
