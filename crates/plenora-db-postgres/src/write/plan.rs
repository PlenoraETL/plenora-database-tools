use super::sql::{native_declaration_sql, pg_type};
use crate::field_contract::FieldContract;
use arrow_schema::{DataType, Field};
use plenora_database_core::geometry_value::ValueContract;
use plenora_database_core::{ErrorCategory, ErrorPhase, Result};

#[derive(Debug, Clone)]
pub(super) struct WriteColumnPlan {
    pub(super) data_type: DataType,
    pub(super) postgres_type: String,
    pub(super) native_type: Option<String>,
    pub(super) native_declaration_sql: Option<String>,
    /// La lista dei tipi come il campo la dichiara.
    pub(super) geometry_types: Option<String>,
    /// `wkb` o `ewkb`; assente sugli ingressi legacy.
    pub(super) encoding: Option<String>,
    pub(super) dimensions: Option<String>,
    /// Lo SRID dichiarato, contro cui si confronta lo SRID di un valore EWKB.
    pub(super) srid: Option<u32>,
    /// Lo SRID della colonna, che riceve un valore senza SRID proprio.
    pub(super) target_srid: Option<u32>,
    pub(super) semantics: ColumnSemantics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ColumnSemantics {
    Scalar,
    Geometry,
    Geography,
    Range,
    Composite,
}

impl WriteColumnPlan {
    pub(super) fn compile(field: &Field) -> Result<Self> {
        let contract = FieldContract::parse(field)?;
        ensure_spatial_support(&contract)?;
        Ok(Self {
            data_type: field.data_type().clone(),
            postgres_type: pg_type(&contract)?,
            native_type: contract.native_type.map(str::to_owned),
            native_declaration_sql: native_declaration_sql(&contract),
            geometry_types: contract.geometry_type.map(str::to_owned),
            encoding: contract.canonical.encoding.map(str::to_owned),
            dimensions: contract.dimensions.map(str::to_owned),
            srid: contract.srid,
            target_srid: contract.target_srid,
            semantics: if contract.is_geography() {
                ColumnSemantics::Geography
            } else if contract.is_geometry() {
                ColumnSemantics::Geometry
            } else if contract.is_range() {
                ColumnSemantics::Range
            } else if contract.is_composite() {
                ColumnSemantics::Composite
            } else {
                ColumnSemantics::Scalar
            },
        })
    }

    /// Un campo con SRID di colonna: i suoi valori senza SRID proprio si
    /// riscrivono in EWKB con quello prima di arrivare a `PostGIS`. Vale per
    /// `wkb`, che non ne porta mai, e per `ewkb`, dove un valore senza SRID ha
    /// lo SRID del campo (GEO-009).
    pub(super) fn needs_srid_stamp(&self) -> bool {
        self.is_spatial()
            && matches!(self.encoding.as_deref(), Some("wkb" | "ewkb"))
            && self.target_srid.is_some_and(|srid| srid != 0)
    }

    /// Cio che il campo dichiara dei propri valori, per la verifica comune.
    pub(super) fn value_contract(&self) -> ValueContract<'_> {
        ValueContract {
            ewkb: self.encoding.as_deref().map(|encoding| encoding == "ewkb"),
            srid: self.srid,
            types: self.geometry_types.as_deref(),
            dimensions: self.dimensions.as_deref(),
        }
    }

    pub(super) const fn is_spatial(&self) -> bool {
        matches!(
            self.semantics,
            ColumnSemantics::Geometry | ColumnSemantics::Geography
        )
    }
}

/// I limiti di supporto di un campo geometrico che si decidono dallo schema,
/// prima di ogni effetto (GEO-000, verdetto di supporto) e prima di contattare
/// il server: un dry run li vede come la scrittura.
fn ensure_spatial_support(contract: &FieldContract<'_>) -> Result<()> {
    let canonical = contract.canonical;
    if !canonical.spatial {
        return Ok(());
    }
    let refuse = |category: ErrorCategory, message: &str| {
        Err(crate::error::public_error(
            category,
            ErrorPhase::Validate,
            false,
            message,
        ))
    };
    // Deviazione dichiarata (GEO-000, contracts/adoption-source.json): i codec
    // di scrittura leggono `Binary`; `LargeBinary`, che il vocabolario ammette,
    // si rifiuta qui invece che al primo batch con `data_mapping`.
    if contract.field.data_type() != &DataType::Binary {
        return refuse(
            ErrorCategory::Unsupported,
            "write PostgreSQL di geoarrow.wkb richiede un campo Arrow Binary, non LargeBinary",
        );
    }
    canonical.ensure_east_first_axis("PostgreSQL")?;
    if canonical.is_geography() {
        // `geography(..., 0)` non e una colonna senza CRS: PostGIS le assegna
        // 4326. Scrivere cosi un campo senza SRID inventerebbe il CRS.
        if contract.target_srid.is_none_or(|srid| srid == 0) {
            return refuse(
                ErrorCategory::Crs,
                "geography PostgreSQL senza SRID: PostGIS assegnerebbe 4326 a un CRS non dichiarato",
            );
        }
        // Un ordine degli assi proiettato contraddice `geography`, che vuole
        // longitudine e latitudine: lo si sa dallo schema, senza il catalogo
        // (che `preflight` legge comunque per lo SRID).
        if matches!(canonical.axis_order, Some("easting_northing" | "other")) {
            return refuse(
                ErrorCategory::Unsupported,
                "geography PostgreSQL richiede un CRS geografico: l'ordine degli assi dichiarato e proiettato",
            );
        }
    }
    Ok(())
}
