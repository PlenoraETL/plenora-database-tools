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
/// Ogni campo esce con `plenora.field_id` (ARROW-VOCABULARY §2, e §4 per le
/// geometrie, che lo richiedono): quello che il campo porta gia resta, gli
/// altri ricevono la posizione nello schema, oppure — se qualche campo ne
/// porta gia uno — il primo numero libero dopo il massimo, cosi gli id
/// restano unici.
#[must_use]
pub fn contract_schema(fields: Vec<Field>) -> SchemaRef {
    let declared = fields
        .iter()
        .filter_map(|field| field.metadata().get(FIELD_ID))
        .filter_map(|value| value.parse::<u64>().ok())
        .max();
    let mut next = declared.map_or(0, |max| max.saturating_add(1));
    let fields = fields
        .into_iter()
        .enumerate()
        .map(|(index, field)| {
            if field.metadata().contains_key(FIELD_ID) {
                return field;
            }
            let id = if declared.is_some() {
                let id = next;
                next = next.saturating_add(1);
                id
            } else {
                u64::try_from(index).unwrap_or(u64::MAX)
            };
            let mut metadata = field.metadata().clone();
            metadata.insert(FIELD_ID.to_owned(), id.to_string());
            field.with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([(CONTRACT_VERSION_KEY.to_owned(), CONTRACT_VERSION.to_owned())]),
    ))
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
