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
#[must_use]
pub fn contract_schema(fields: Vec<Field>) -> SchemaRef {
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

/// Il tipo nativo della colonna, come il provider `MySQL` lo ha osservato.
///
/// Annota **cio che il provider ha letto**, e le due strade da cui puo
/// arrivare non dicono la stessa cosa:
///
/// * sul path catalogo il nome viene da `information_schema.columns`, cioe
///   dalla dichiarazione;
/// * sul path query viene dai metadata di `COM_STMT_PREPARE`, cioe dal
///   **filo**, che e l'unica cosa che il protocollo porta.
///
/// La distinzione non e teorica: ADR 0014 l'ha misurata. Dalla stessa DDL
/// `document JSON` esce `json` su `MySQL` e `text` su `MariaDB`, perche li `JSON`
/// e un alias di `LONGTEXT` e sul filo le due dichiarazioni sono
/// indistinguibili. Entrambe le annotazioni sono corrette, ed e per questo che
/// il contratto le dichiara come cio che sono: il tipo osservato, non la DDL
/// ricostruita. Un consumer che deve sapere se una colonna e davvero `JSON`
/// legge il catalogo, non il risultato di una query.
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
