//! Validazione del contratto Arrow trasversale.
//!
//! Questo modulo giudica soltanto le chiavi canoniche e `GeoArrow`. I metadati
//! nativi di un provider restano nel relativo crate.

use crate::arrow::schema::{DataType, Field, Schema};
use crate::geometry::GEOARROW_WKB_EXTENSION_NAME;
use crate::protocol;
use crate::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use arrow_schema::Metadata;

const LEGACY_DIMENSIONS: &str = "plenora.dimensions";
const LEGACY_SRID: &str = "plenora.srid";
const LEGACY_SPATIAL_SEMANTICS: &str = "plenora.spatial_semantics";
const LEGACY_GEOMETRY_TYPE: &str = "plenora.geometry_type";

const CANONICAL_GEOMETRY_KEYS: [&str; 12] = [
    protocol::GEOMETRY_ENCODING,
    protocol::GEOMETRY_DIMENSIONS,
    protocol::GEOMETRY_TYPES,
    protocol::GEOMETRY_TYPES_DECLARATION,
    protocol::GEOMETRY_SRID,
    protocol::GEOMETRY_CRS_RESOLUTION,
    protocol::GEOMETRY_CRS_ID,
    protocol::GEOMETRY_CRS_DEFINITION,
    protocol::GEOMETRY_CRS_DEFINITION_FORMAT,
    protocol::GEOMETRY_AXIS_ORDER,
    protocol::GEOMETRY_SPATIAL_SEMANTICS,
    protocol::GEOMETRY_PRECISION,
];

const GEOMETRY_TYPES: [&str; 16] = [
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
    "geometrycollection",
    "circularstring",
    "compoundcurve",
    "curvepolygon",
    "multicurve",
    "multisurface",
    "polyhedralsurface",
    "tin",
    "triangle",
    "unknown",
];

/// Vista validata dei metadati canonici di un campo Arrow.
#[derive(Debug, Clone, Copy)]
pub struct FieldContract<'a> {
    pub field: &'a Field,
    /// Il field id dichiarato, gia verificato come intero decimale non
    /// negativo; la forma canonica la da `protocol::canonical_field_id`.
    pub field_id: Option<&'a str>,
    pub encoding: Option<&'a str>,
    pub geometry_types: Option<&'a str>,
    pub types_declaration: Option<&'a str>,
    pub dimensions: Option<&'a str>,
    pub spatial_semantics: Option<&'a str>,
    pub srid: Option<u32>,
    pub crs_resolution: Option<&'a str>,
    pub crs_id: Option<&'a str>,
    pub crs_definition: Option<&'a str>,
    pub crs_definition_format: Option<&'a str>,
    pub axis_order: Option<&'a str>,
    pub precision: Option<&'a str>,
    pub spatial: bool,
}

impl<'a> FieldContract<'a> {
    /// Analizza un campo senza attribuirgli default impliciti.
    ///
    /// Accetta ingressi legacy incompleti, ma rifiuta valori presenti non
    /// validi e rappresentazioni canoniche/legacy/GeoArrow divergenti.
    ///
    /// # Errors
    ///
    /// Restituisce un errore categorizzato quando tipo Arrow, metadati,
    /// enumerazioni o rappresentazioni CRS sono incoerenti.
    pub fn parse(field: &'a Field) -> Result<Self> {
        let metadata = field.metadata();
        let dimensions = coherent_value(
            metadata,
            protocol::GEOMETRY_DIMENSIONS,
            LEGACY_DIMENSIONS,
            false,
        )?;
        let raw_srid = coherent_value(metadata, protocol::GEOMETRY_SRID, LEGACY_SRID, false)?;
        let spatial_semantics = coherent_value(
            metadata,
            protocol::GEOMETRY_SPATIAL_SEMANTICS,
            LEGACY_SPATIAL_SEMANTICS,
            false,
        )?;
        let geometry_types = coherent_value(
            metadata,
            protocol::GEOMETRY_TYPES,
            LEGACY_GEOMETRY_TYPE,
            true,
        )?;
        let extension = metadata
            .get(protocol::GEOARROW_EXTENSION_NAME)
            .map(String::as_str);
        let has_canonical_geometry = CANONICAL_GEOMETRY_KEYS
            .iter()
            .any(|key| metadata.contains_key(key));
        let spatial = has_canonical_geometry
            || dimensions.is_some()
            || raw_srid.is_some()
            || spatial_semantics.is_some()
            || geometry_types.is_some()
            || extension == Some(GEOARROW_WKB_EXTENSION_NAME);

        if spatial && extension.is_some_and(|value| value != GEOARROW_WKB_EXTENSION_NAME) {
            return Err(contract_error(
                ErrorCategory::Schema,
                "metadati geometrici divergenti dall'estensione GeoArrow",
            ));
        }
        if spatial && !matches!(field.data_type(), DataType::Binary | DataType::LargeBinary) {
            return Err(contract_error(
                ErrorCategory::Schema,
                "il contratto geometrico WKB richiede un campo Arrow binary",
            ));
        }

        let field_id = metadata
            .get(protocol::FIELD_ID)
            .map(|value| {
                protocol::canonical_field_id(value)
                    .map(|_| value.as_str())
                    .ok_or_else(|| {
                        contract_error(
                            ErrorCategory::Schema,
                            "field_id deve essere un intero decimale non negativo",
                        )
                    })
            })
            .transpose()?;
        let srid = parse_srid(raw_srid)?;
        let contract = Self {
            field,
            field_id,
            encoding: metadata
                .get(protocol::GEOMETRY_ENCODING)
                .map(String::as_str),
            geometry_types,
            types_declaration: metadata
                .get(protocol::GEOMETRY_TYPES_DECLARATION)
                .map(String::as_str),
            dimensions,
            spatial_semantics,
            srid,
            crs_resolution: metadata
                .get(protocol::GEOMETRY_CRS_RESOLUTION)
                .map(String::as_str),
            crs_id: metadata.get(protocol::GEOMETRY_CRS_ID).map(String::as_str),
            crs_definition: metadata
                .get(protocol::GEOMETRY_CRS_DEFINITION)
                .map(String::as_str),
            crs_definition_format: metadata
                .get(protocol::GEOMETRY_CRS_DEFINITION_FORMAT)
                .map(String::as_str),
            axis_order: metadata
                .get(protocol::GEOMETRY_AXIS_ORDER)
                .map(String::as_str),
            precision: metadata
                .get(protocol::GEOMETRY_PRECISION)
                .map(String::as_str),
            spatial,
        };
        contract.validate_values()?;
        Ok(contract)
    }

    /// Applica anche i requisiti obbligatori per un campo emesso nel protocollo
    /// corrente, dopo che lo schema è stato validato con
    /// [`validate_schema_contract`].
    ///
    /// # Errors
    ///
    /// `Schema` quando manca un metadato obbligatorio, `Crs` quando manca
    /// `crs_resolution` (REJ-001).
    pub fn validate_current(self) -> Result<()> {
        if !self.spatial {
            return Ok(());
        }
        for (value, name) in [
            (self.encoding, protocol::GEOMETRY_ENCODING),
            (self.dimensions, protocol::GEOMETRY_DIMENSIONS),
            (self.types_declaration, protocol::GEOMETRY_TYPES_DECLARATION),
            (self.crs_resolution, protocol::GEOMETRY_CRS_RESOLUTION),
        ] {
            if value.is_none() {
                // REJ-001: l'assenza di una chiave CRS e della classe CRS.
                let category = if name == protocol::GEOMETRY_CRS_RESOLUTION {
                    ErrorCategory::Crs
                } else {
                    ErrorCategory::Schema
                };
                return Err(contract_error(
                    category,
                    format!("campo geometrico privo del metadato obbligatorio {name}"),
                ));
            }
        }
        Ok(())
    }

    /// I requisiti di un campo che un componente **pubblica**
    /// (ARROW-VOCABULARY §4): tutte le chiavi di [`Self::validate_current`],
    /// piu field id, semantica spaziale e precisione.
    ///
    /// Sta separata da `validate_current`, che giudica anche gli ingressi:
    /// chi riceve resta tollerante verso un produttore che omette la
    /// precisione, chi pubblica no.
    ///
    /// # Errors
    ///
    /// `DataMapping` quando manca una chiave obbligatoria.
    pub fn validate_published(self) -> Result<()> {
        self.validate_current()?;
        if !self.spatial {
            return Ok(());
        }
        let metadata = self.field.metadata();
        let canonical = |key: &str| metadata.get(key).map(String::as_str);
        let missing = |name: &str| {
            contract_error(
                ErrorCategory::DataMapping,
                format!("campo geometrico pubblicato senza {name}"),
            )
        };
        // Solo le chiavi canoniche: una chiave legacy e tollerata in
        // ingresso, non pubblicata al posto della canonica.
        for legacy in [
            LEGACY_DIMENSIONS,
            LEGACY_SRID,
            LEGACY_SPATIAL_SEMANTICS,
            LEGACY_GEOMETRY_TYPE,
        ] {
            if metadata.contains_key(legacy) {
                return Err(contract_error(
                    ErrorCategory::DataMapping,
                    "campo geometrico pubblicato con chiavi legacy",
                ));
            }
        }
        if canonical(protocol::GEOARROW_EXTENSION_NAME) != Some(GEOARROW_WKB_EXTENSION_NAME) {
            return Err(missing(protocol::GEOARROW_EXTENSION_NAME));
        }
        for key in [
            protocol::FIELD_ID,
            protocol::GEOMETRY_ENCODING,
            protocol::GEOMETRY_DIMENSIONS,
            protocol::GEOMETRY_SPATIAL_SEMANTICS,
            protocol::GEOMETRY_PRECISION,
            protocol::GEOMETRY_TYPES_DECLARATION,
            protocol::GEOMETRY_CRS_RESOLUTION,
        ] {
            if canonical(key).is_none() {
                return Err(missing(key));
            }
        }
        // §3: il catalogo dei tipi e case-sensitive.
        if let Some(types) = canonical(protocol::GEOMETRY_TYPES) {
            if types.split(',').any(|item| !GEOMETRY_TYPES.contains(&item)) {
                return Err(contract_error(
                    ErrorCategory::DataMapping,
                    "tipo geometrico pubblicato fuori dalla forma canonica",
                ));
            }
        }
        // §4: un CRS risolto o dichiarato ha un'identita e un ordine degli
        // assi; un CRS mancante non ne ha.
        let has_crs = canonical(protocol::GEOMETRY_CRS_ID).is_some()
            || canonical(protocol::GEOMETRY_CRS_DEFINITION).is_some();
        let has_axis = canonical(protocol::GEOMETRY_AXIS_ORDER).is_some();
        match canonical(protocol::GEOMETRY_CRS_RESOLUTION) {
            Some("resolved" | "declared_unresolved") if !(has_crs && has_axis) => {
                Err(contract_error(
                    ErrorCategory::Crs,
                    "CRS pubblicato senza identita o senza ordine degli assi",
                ))
            }
            Some("missing") if has_crs || has_axis => Err(contract_error(
                ErrorCategory::Crs,
                "CRS mancante pubblicato con identita o ordine degli assi",
            )),
            _ => Ok(()),
        }
    }

    /// Lo SRID numerico con cui un bersaglio che ne vuole uno registra il CRS
    /// del campo (Arrow Geometry Semantics 1.0, GEO-008).
    ///
    /// E `plenora.geometry.srid` quando il campo lo dichiara. Senza, un CRS
    /// dichiarato (`resolved` o `declared_unresolved`) lo deriva da `crs_id`
    /// con una sola corrispondenza documentata: `EPSG:<n>`, con `n` intero
    /// decimale canonico nel dominio signed 32-bit, vale lo SRID `n`. Lo
    /// verifica poi il bersaglio contro il proprio catalogo (PostgreSQL:
    /// `spatial_ref_sys`). Un'autorita di catalogo locale non si deriva mai
    /// (GEO-020): lo stesso codice in un'altra installazione puo essere un
    /// altro CRS. Senza CRS dichiarato non c'e SRID da registrare.
    ///
    /// Non e lo SRID con cui si confronta un valore EWKB: quello e soltanto lo
    /// SRID dichiarato ([`Self::srid`], GEO-009).
    ///
    /// # Errors
    ///
    /// `Crs` per un CRS dichiarato senza SRID e senza un `crs_id` `EPSG`: il
    /// bersaglio lo scriverebbe con SRID 0, cioe senza CRS.
    pub fn target_srid(self) -> Result<Option<u32>> {
        if self.srid.is_some() || !self.spatial {
            return Ok(self.srid);
        }
        match self.crs_resolution {
            Some("resolved" | "declared_unresolved") => {
                self.crs_id.and_then(epsg_srid).map(Some).ok_or_else(|| {
                    contract_error(
                        ErrorCategory::Crs,
                        "CRS dichiarato senza plenora.geometry.srid ne un crs_id EPSG da cui \
                         derivarlo: il bersaglio lo registrerebbe senza CRS",
                    )
                })
            }
            _ => Ok(None),
        }
    }

    #[must_use]
    pub fn is_geometry(self) -> bool {
        self.spatial
            && self
                .spatial_semantics
                .is_none_or(|value| value == "geometry")
    }

    #[must_use]
    pub fn is_geography(self) -> bool {
        self.spatial && self.spatial_semantics == Some("geography")
    }

    fn validate_values(self) -> Result<()> {
        validate_enum(self.encoding, &["wkb", "ewkb"], "encoding geometrico")?;
        validate_enum(
            self.dimensions,
            &["xy", "xyz", "xym", "xyzm", "unknown"],
            "dimensioni geometriche",
        )?;
        validate_enum(
            self.types_declaration,
            &["exact", "mixed", "unresolved"],
            "dichiarazione tipi geometrici",
        )?;
        validate_enum(
            self.spatial_semantics,
            &["geometry", "geography"],
            "semantica spaziale",
        )?;
        validate_enum(
            self.precision,
            &["float64", "float32", "native"],
            "precisione geometrica",
        )?;
        validate_crs_enum(
            self.axis_order,
            &[
                "lon_lat",
                "lat_lon",
                "easting_northing",
                "northing_easting",
                "other",
                "unknown",
            ],
            "ordine assi CRS",
        )?;
        validate_crs_enum(
            self.crs_definition_format,
            &["wkt", "wkt2", "projjson"],
            "formato definizione CRS",
        )?;
        validate_crs_enum(
            self.crs_resolution,
            &["resolved", "declared_unresolved", "missing"],
            "stato di risoluzione CRS",
        )?;
        self.validate_geometry_types()?;
        self.validate_crs()
    }

    fn validate_geometry_types(self) -> Result<()> {
        let Some(value) = self.geometry_types else {
            if self.types_declaration == Some("exact") {
                return Err(contract_error(
                    ErrorCategory::Schema,
                    "types_declaration=exact richiede tipi geometrici",
                ));
            }
            return Ok(());
        };
        if value.is_empty() || value.contains(char::is_whitespace) {
            return Err(contract_error(
                ErrorCategory::Schema,
                "lista dei tipi geometrici vuota o contenente spazi",
            ));
        }
        if self.types_declaration == Some("unresolved") {
            return Err(contract_error(
                ErrorCategory::Schema,
                "types_declaration=unresolved non ammette tipi geometrici",
            ));
        }
        let mut previous = None;
        for item in value.split(',') {
            let Some(index) = GEOMETRY_TYPES
                .iter()
                .position(|candidate| item.eq_ignore_ascii_case(candidate))
            else {
                return Err(contract_error(
                    ErrorCategory::Schema,
                    "tipo geometrico fuori dal catalogo canonico",
                ));
            };
            if previous.is_some_and(|old| index <= old) {
                return Err(contract_error(
                    ErrorCategory::Schema,
                    "tipi geometrici duplicati o fuori dall'ordine canonico",
                ));
            }
            previous = Some(index);
        }
        Ok(())
    }

    fn validate_crs(self) -> Result<()> {
        // §3: una definizione presente non e vuota; vuota o di soli spazi
        // non identifica alcun CRS.
        if self
            .crs_definition
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(contract_error(ErrorCategory::Crs, "definizione CRS vuota"));
        }
        if self.crs_definition.is_some() != self.crs_definition_format.is_some() {
            return Err(contract_error(
                ErrorCategory::Crs,
                "definizione CRS e formato devono essere presenti insieme",
            ));
        }
        if (self.crs_id.is_some() || self.crs_definition.is_some()) && self.axis_order.is_none() {
            return Err(contract_error(
                ErrorCategory::Crs,
                "identificatore o definizione CRS richiede un ordine assi esplicito",
            ));
        }
        if self.crs_id.is_some_and(|value| {
            // Limite dichiarato (GEO-016, contracts/adoption-source.json): oltre 1024 caratteri o
            // con caratteri di controllo il crs_id si rifiuta, pur nella grammatica.
            value.is_empty() || value.len() > 1_024 || value.chars().any(char::is_control)
        }) {
            return Err(contract_error(
                ErrorCategory::Crs,
                "identificatore CRS non valido",
            ));
        }
        // ARROW-VOCABULARY §4: `missing` vieta identita, definizione, formato
        // e ordine degli assi, non lo SRID numerico.
        if self.crs_resolution == Some("missing")
            && (self.crs_id.is_some()
                || self.crs_definition.is_some()
                || self.crs_definition_format.is_some()
                || self.axis_order.is_some())
        {
            return Err(contract_error(
                ErrorCategory::Crs,
                "CRS missing non ammette rappresentazioni CRS",
            ));
        }
        if let (Some(crs_id), Some(srid)) = (self.crs_id, self.srid) {
            // Deviazione dichiarata (GEO-008, contracts/adoption-source.json): `EPSG:<n>` con uno
            // SRID diverso non si scrive con nessuna delle due identita.
            if authority_srid(crs_id).is_some_and(|authority| authority != srid) {
                return Err(contract_error(
                    ErrorCategory::Crs,
                    "identificatore CRS e SRID numerico divergenti",
                ));
            }
        }
        Ok(())
    }
}

/// Verifica la versione a livello schema e tutti i campi.
///
/// Uno schema con almeno una chiave canonica deve dichiarare esattamente la
/// versione supportata. Le versioni future non vengono interpretate
/// parzialmente.
///
/// # Classi e ordine (Arrow Geometry Semantics 1.0, REJ-001 e REJ-002)
///
/// Uno schema che porta il contratto (la versione, un `plenora.field_id` o
/// una chiave `plenora.geometry.`) si giudica nelle classi dell'oracolo e nel
/// loro ordine: versione; vocabolario di **tutti** i campi (`schema`); stato
/// CRS di ogni campo `geoarrow.wkb` (`crs`). Solo dopo valgono i controlli di
/// questo componente ([`FieldContract::parse`]): la coerenza con le chiavi
/// legacy e i limiti di supporto (`unsupported`). Uno schema che non porta il
/// contratto e Arrow estraneo: lo leggono soltanto i controlli legacy.
///
/// # Errors
///
/// Restituisce un errore categorizzato per versione assente/non supportata o
/// per un campo non conforme.
pub fn validate_schema_contract(schema: &Schema) -> Result<()> {
    let version = schema
        .metadata()
        .get(protocol::CONTRACT_VERSION_KEY)
        .map(String::as_str);
    let carries_canonical = version.is_some()
        || schema.fields().iter().any(|field| {
            field
                .metadata()
                .keys()
                .any(|key| key == protocol::FIELD_ID || key.starts_with(GEOMETRY_PREFIX))
        });
    // Arrow Geometry Semantics 1.0, REJ-001: una versione assente o malformata
    // e' un difetto dei metadati dello schema (`schema`); solo una versione ben
    // formata che il componente non supporta e' `unsupported`.
    if carries_canonical && version.is_none() {
        return Err(contract_error(
            ErrorCategory::Schema,
            "schema con metadati canonici privo di plenora.contract.version",
        ));
    }
    if version.is_some_and(|value| !versione_ben_formata(value)) {
        return Err(contract_error(
            ErrorCategory::Schema,
            "plenora.contract.version non e' un intero positivo",
        ));
    }
    if version.is_some_and(|value| value != protocol::CONTRACT_VERSION) {
        return Err(contract_error(
            ErrorCategory::Unsupported,
            "versione del contratto Arrow non supportata",
        ));
    }
    if carries_canonical {
        let mut ids = std::collections::HashSet::new();
        let mut repeated = false;
        for field in schema.fields() {
            vocabulary_defect(field)?;
            if let Some(id) = field.metadata().get(protocol::FIELD_ID) {
                repeated |= !ids.insert(id.as_str());
            }
        }
        // L'oracolo cerca i doppioni dopo aver letto ogni campo: un difetto
        // di un campo successivo e anch'esso di vocabolario, e vince lo stesso.
        if repeated {
            return Err(contract_error(
                ErrorCategory::Schema,
                "plenora.field_id ripetuto nello schema",
            ));
        }
        for field in schema.fields().iter().filter(|field| is_geoarrow(field)) {
            crs_defect(field)?;
        }
    }
    for field in schema.fields() {
        let contract = FieldContract::parse(field)?;
        if carries_canonical {
            contract.validate_current()?;
        }
    }
    Ok(())
}

/// `[1-9][0-9]*`, la grammatica della versione del contratto.
fn versione_ben_formata(valore: &str) -> bool {
    let mut cifre = valore.bytes();
    matches!(cifre.next(), Some(b'1'..=b'9')) && cifre.all(|b| b.is_ascii_digit())
}

/// `0|[1-9][0-9]*`, la grammatica di `plenora.field_id` (GEO-016: il valore
/// intero, senza zeri iniziali).
fn decimale_ben_formato(valore: &str) -> bool {
    valore == "0" || versione_ben_formata(valore)
}

/// `0|-?[1-9][0-9]*` dentro i 32 bit con segno, la grammatica dello SRID.
fn srid_ben_formato(valore: &str) -> bool {
    let assoluto = valore.strip_prefix('-').unwrap_or(valore);
    (valore == "0" || versione_ben_formata(assoluto)) && valore.parse::<i32>().is_ok()
}

const GEOMETRY_PREFIX: &str = "plenora.geometry.";
const NATIVE_PREFIX: &str = "plenora.geometry.native.";

/// Le chiavi obbligatorie di un campo `geoarrow.wkb` che non sono chiavi CRS:
/// la loro assenza e un difetto di vocabolario.
const REQUIRED_NON_CRS_KEYS: [&str; 6] = [
    protocol::FIELD_ID,
    protocol::GEOMETRY_ENCODING,
    protocol::GEOMETRY_DIMENSIONS,
    protocol::GEOMETRY_SPATIAL_SEMANTICS,
    protocol::GEOMETRY_PRECISION,
    protocol::GEOMETRY_TYPES_DECLARATION,
];

/// REJ-001: ogni difetto di queste chiavi, l'assenza compresa, e della classe
/// CRS.
const CRS_KEYS: [&str; 5] = [
    protocol::GEOMETRY_CRS_RESOLUTION,
    protocol::GEOMETRY_CRS_ID,
    protocol::GEOMETRY_CRS_DEFINITION,
    protocol::GEOMETRY_CRS_DEFINITION_FORMAT,
    protocol::GEOMETRY_AXIS_ORDER,
];

const CLOSED_VALUES: [(&str, &[&str]); 8] = [
    (protocol::GEOMETRY_ENCODING, &["wkb", "ewkb"]),
    (
        protocol::GEOMETRY_DIMENSIONS,
        &["xy", "xyz", "xym", "xyzm", "unknown"],
    ),
    (
        protocol::GEOMETRY_SPATIAL_SEMANTICS,
        &["geometry", "geography"],
    ),
    (
        protocol::GEOMETRY_PRECISION,
        &["float64", "float32", "native"],
    ),
    (
        protocol::GEOMETRY_TYPES_DECLARATION,
        &["exact", "mixed", "unresolved"],
    ),
    (
        protocol::GEOMETRY_CRS_RESOLUTION,
        &["resolved", "declared_unresolved", "missing"],
    ),
    (
        protocol::GEOMETRY_CRS_DEFINITION_FORMAT,
        &["wkt", "wkt2", "projjson"],
    ),
    (
        protocol::GEOMETRY_AXIS_ORDER,
        &[
            "lon_lat",
            "lat_lon",
            "easting_northing",
            "northing_easting",
            "other",
            "unknown",
        ],
    ),
];

fn is_geoarrow(field: &Field) -> bool {
    field
        .metadata()
        .get(protocol::GEOARROW_EXTENSION_NAME)
        .is_some_and(|value| value == GEOARROW_WKB_EXTENSION_NAME)
}

fn outside_closed_set(key: &str, value: &str) -> bool {
    CLOSED_VALUES
        .iter()
        .find(|(candidate, _)| *candidate == key)
        .is_some_and(|(_, admitted)| !admitted.contains(&value))
}

/// La classe vocabolario di REJ-001 per un campo: ogni difetto e `schema`.
fn vocabulary_defect(field: &Field) -> Result<()> {
    let schema_error = |message: &'static str| Err(contract_error(ErrorCategory::Schema, message));
    let metadata = field.metadata();
    if metadata
        .get(protocol::FIELD_ID)
        .is_some_and(|id| !decimale_ben_formato(id))
    {
        return schema_error("plenora.field_id non e un intero decimale canonico");
    }
    if !is_geoarrow(field) {
        if metadata.keys().any(|key| key.starts_with(GEOMETRY_PREFIX)) {
            return schema_error(
                "chiave plenora.geometry su un campo senza estensione geoarrow.wkb",
            );
        }
        return Ok(());
    }
    if !matches!(field.data_type(), DataType::Binary | DataType::LargeBinary) {
        return schema_error("campo geoarrow.wkb con tipo Arrow non binario");
    }
    if REQUIRED_NON_CRS_KEYS
        .iter()
        .any(|key| !metadata.contains_key(key))
    {
        return schema_error("campo geometrico senza una chiave obbligatoria del vocabolario");
    }
    if metadata.keys().any(|key| {
        key.starts_with(GEOMETRY_PREFIX)
            && !key.starts_with(NATIVE_PREFIX)
            && !CANONICAL_GEOMETRY_KEYS.contains(&key.as_str())
    }) {
        return schema_error("chiave plenora.geometry sconosciuta");
    }
    if metadata
        .iter()
        .any(|(key, value)| !CRS_KEYS.contains(&key.as_str()) && outside_closed_set(key, value))
    {
        return schema_error("valore fuori dal vocabolario chiuso");
    }
    if metadata
        .get(protocol::GEOMETRY_SRID)
        .is_some_and(|srid| !srid_ben_formato(srid))
    {
        return schema_error("SRID non e un intero decimale signed a 32 bit");
    }
    let declaration = metadata
        .get(protocol::GEOMETRY_TYPES_DECLARATION)
        .map(String::as_str);
    match metadata.get(protocol::GEOMETRY_TYPES).map(String::as_str) {
        Some("") => schema_error("lista dei tipi geometrici vuota"),
        None if declaration == Some("exact") => {
            schema_error("types_declaration=exact richiede tipi geometrici")
        }
        Some(_) if declaration == Some("unresolved") => {
            schema_error("types_declaration=unresolved non ammette tipi geometrici")
        }
        Some(types) => {
            // §3: il catalogo dei tipi e case-sensitive, unico e in ordine.
            let mut previous = None;
            for item in types.split(',') {
                let Some(index) = GEOMETRY_TYPES
                    .iter()
                    .position(|candidate| *candidate == item)
                else {
                    return schema_error("tipo geometrico fuori dal catalogo canonico");
                };
                if previous.is_some_and(|old| index <= old) {
                    return schema_error("tipi geometrici duplicati o fuori dall'ordine canonico");
                }
                previous = Some(index);
            }
            Ok(())
        }
        None => Ok(()),
    }
}

/// La classe CRS di REJ-001 per un campo `geoarrow.wkb`: ogni difetto e
/// `crs`, l'assenza di una chiave richiesta compresa (GEO-005, GEO-016,
/// GEO-020).
fn crs_defect(field: &Field) -> Result<()> {
    let crs_error = |message: &'static str| Err(contract_error(ErrorCategory::Crs, message));
    let metadata = field.metadata();
    let get = |key: &str| metadata.get(key).map(String::as_str);
    let Some(resolution) = get(protocol::GEOMETRY_CRS_RESOLUTION) else {
        return crs_error("campo geometrico senza plenora.geometry.crs_resolution");
    };
    if metadata
        .iter()
        .any(|(key, value)| CRS_KEYS.contains(&key.as_str()) && outside_closed_set(key, value))
    {
        return crs_error("metadato CRS fuori dal vocabolario chiuso");
    }
    let definition = get(protocol::GEOMETRY_CRS_DEFINITION);
    if definition == Some("") {
        return crs_error("definizione CRS vuota");
    }
    let crs_id = get(protocol::GEOMETRY_CRS_ID);
    if crs_id.is_some_and(|id| !crate::crs_definition::is_well_formed_crs_id(id)) {
        return crs_error("identificatore CRS fuori dalla grammatica AUTHORITY:CODE");
    }
    let format = get(protocol::GEOMETRY_CRS_DEFINITION_FORMAT);
    let axis = get(protocol::GEOMETRY_AXIS_ORDER);
    if definition.is_some() != format.is_some() {
        return crs_error("definizione CRS e formato devono essere presenti insieme");
    }
    if resolution == "missing" {
        if crs_id.is_some() || definition.is_some() || format.is_some() || axis.is_some() {
            return crs_error("CRS missing non ammette identita, definizione o ordine degli assi");
        }
        return Ok(());
    }
    if crs_id.is_none() && definition.is_none() {
        return crs_error("CRS dichiarato senza identificatore ne definizione");
    }
    if axis.is_none() {
        return crs_error("CRS dichiarato senza ordine degli assi");
    }
    if resolution == "resolved" && crs_id.is_some_and(crate::crs_definition::is_local_authority) {
        return crs_error("crs_id di un catalogo locale dichiarato risolto");
    }
    if let (Some(text), Some(format)) = (definition, format) {
        if !crate::crs_definition::within_size_limit(text) {
            return Err(DatabaseError::new(
                ErrorCategory::ResourceLimit,
                ErrorPhase::Validate,
                None,
                "definizione CRS oltre il tetto di 1 MiB",
            ));
        }
        let format = crate::crs_definition::DefinitionFormat::parse(format).ok_or_else(|| {
            contract_error(ErrorCategory::Crs, "formato definizione CRS non valido")
        })?;
        match crate::crs_definition::contradicts(crs_id, text, format) {
            Err(crate::crs_definition::Malformed) => {
                return crs_error("definizione CRS non ben formata nel formato dichiarato")
            }
            Ok(true) => return crs_error("definizione CRS che descrive un CRS diverso da crs_id"),
            Ok(false) => {}
        }
    }
    Ok(())
}

/// Verifica lo schema che un componente pubblica.
///
/// Versione del contratto presente, ogni campo conforme a
/// [`FieldContract::validate_published`], field id unici (ARROW-VOCABULARY §2
/// e §4). Si chiama prima di scrivere il primo byte di un artefatto.
///
/// # Errors
///
/// Come [`validate_schema_contract`], piu `DataMapping` per una versione
/// assente, una chiave obbligatoria mancante o field id ripetuti.
pub fn validate_published_schema(schema: &Schema) -> Result<()> {
    if !schema
        .metadata()
        .contains_key(protocol::CONTRACT_VERSION_KEY)
    {
        return Err(contract_error(
            ErrorCategory::DataMapping,
            "schema pubblicato senza plenora.contract.version",
        ));
    }
    validate_schema_contract(schema)?;
    let mut ids = std::collections::HashSet::new();
    for field in schema.fields() {
        let contract = FieldContract::parse(field)?;
        contract.validate_published()?;
        // Forma canonica, come in `contract_schema`: "7" e "007" sono lo
        // stesso id.
        if let Some(id) = contract.field_id.and_then(protocol::canonical_field_id) {
            if !ids.insert(id) {
                return Err(contract_error(
                    ErrorCategory::DataMapping,
                    "field_id ripetuto nello schema pubblicato",
                ));
            }
        }
    }
    Ok(())
}

/// `EPSG:<n>` con `n` intero decimale canonico in `1..=i32::MAX`.
fn epsg_srid(crs_id: &str) -> Option<u32> {
    let (authority, code) = crs_id.split_once(':')?;
    (authority.eq_ignore_ascii_case("EPSG") && versione_ben_formata(code))
        .then(|| code.parse::<i32>().ok())
        .flatten()
        .and_then(|code| u32::try_from(code).ok())
}

fn authority_srid(value: &str) -> Option<u32> {
    let (authority, code) = value.split_once(':')?;
    authority
        .eq_ignore_ascii_case("EPSG")
        .then(|| code.parse::<u32>().ok())
        .flatten()
}

/// Lo SRID secondo ARROW-VOCABULARY §3: un intero decimale signed 32-bit,
/// cifre con un eventuale `-` iniziale. Il controllo vale per l'ingresso e
/// per la pubblicazione, che passa da questo parser.
///
/// # Deviazione dichiarata
///
/// Un negativo e nel dominio del contratto, ma nessun provider lo
/// rappresenta (gli SRID dei cataloghi sono non negativi): si rifiuta come
/// `unsupported`, non come fuori formato. Rientra quando un provider
/// qualificato dichiara SRID negativi.
fn parse_srid(value: Option<impl AsRef<str>>) -> Result<Option<u32>> {
    value
        .map(|raw| {
            let raw = raw.as_ref();
            let digits = raw.strip_prefix('-').unwrap_or(raw);
            let parsed = (!digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()))
                .then(|| raw.parse::<i32>().ok())
                .flatten()
                .ok_or_else(|| {
                    contract_error(
                        ErrorCategory::Schema,
                        "SRID deve essere un intero decimale signed a 32 bit",
                    )
                })?;
            u32::try_from(parsed).map_err(|_| {
                contract_error(
                    ErrorCategory::Unsupported,
                    "SRID negativo: nessun provider lo rappresenta",
                )
            })
        })
        .transpose()
}

fn validate_enum(value: Option<&str>, allowed: &[&str], label: &str) -> Result<()> {
    validate_closed(value, allowed, label, ErrorCategory::Schema)
}

/// Come [`validate_enum`] per una chiave CRS: REJ-001 la mette nella classe CRS.
fn validate_crs_enum(value: Option<&str>, allowed: &[&str], label: &str) -> Result<()> {
    validate_closed(value, allowed, label, ErrorCategory::Crs)
}

fn validate_closed(
    value: Option<&str>,
    allowed: &[&str],
    label: &str,
    category: ErrorCategory,
) -> Result<()> {
    if value.is_some_and(|candidate| !allowed.contains(&candidate)) {
        Err(contract_error(category, format!("{label} non valido")))
    } else {
        Ok(())
    }
}

fn coherent_value<'a>(
    metadata: &'a Metadata,
    canonical: &str,
    legacy: &str,
    case_insensitive: bool,
) -> Result<Option<&'a str>> {
    let current = metadata.get(canonical).map(String::as_str);
    let previous = metadata.get(legacy).map(String::as_str);
    if let (Some(current), Some(previous)) = (current, previous) {
        let coherent = if case_insensitive {
            current.eq_ignore_ascii_case(previous)
        } else {
            current == previous
        };
        if !coherent {
            return Err(contract_error(
                ErrorCategory::Schema,
                "metadati canonici e legacy divergenti",
            ));
        }
    }
    Ok(current.or(previous))
}

fn contract_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(category, ErrorPhase::Validate, None, message)
}

#[cfg(test)]
#[path = "field_contract_tests.rs"]
mod tests;
