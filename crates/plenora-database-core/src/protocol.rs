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
/// geometrie). Un id gia dichiarato resta. Gli altri si derivano dal **nome**
/// del campo, che per un database e l'identita della colonna sorgente: lo
/// stesso campo ha lo stesso id letto da solo, in un'altra proiezione o in un
/// altro ordine, come chiede §2 («independent of field name and ordinal
/// position» vale fra campi diversi; per lo stesso campo invariato l'id resta
/// quello). La posizione non entra: con la posizione, `b` valeva 1 in `[a, b]`
/// e 0 in `[b]`.
///
/// L'id e FNV-1a a 32 bit del nome UTF-8, ridotto a 31 bit perche resti un
/// intero non negativo anche per chi lo legge con segno.
///
/// # Errors
///
/// `Schema` se un id dichiarato non e un intero a 32 bit, o se due campi
/// hanno lo stesso id — nomi ripetuti, o una collisione dell'hash: l'unicita
/// non si ripara rinumerando.
pub fn contract_schema(fields: Vec<Field>) -> crate::Result<SchemaRef> {
    let mut seen = std::collections::HashSet::new();
    let fields = fields
        .into_iter()
        .map(|field| {
            let id = match field.metadata().get(FIELD_ID) {
                Some(value) => value
                    .parse::<u32>()
                    .map_err(|_| field_id_error("field_id dichiarato non e un intero a 32 bit"))?,
                None => name_field_id(field.name()),
            };
            if !seen.insert(id) {
                return Err(field_id_error(
                    "field_id duplicato nello schema: nomi di campo ripetuti o collisione",
                ));
            }
            if field.metadata().contains_key(FIELD_ID) {
                return Ok(field);
            }
            let mut metadata = field.metadata().clone();
            metadata.insert(FIELD_ID.to_owned(), id.to_string());
            Ok(field.with_metadata(metadata))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([(CONTRACT_VERSION_KEY.to_owned(), CONTRACT_VERSION.to_owned())]),
    )))
}

/// L'id di un campo che non ne dichiara uno: FNV-1a 32 bit del nome, 31 bit.
#[must_use]
pub fn name_field_id(name: &str) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in name.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
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
