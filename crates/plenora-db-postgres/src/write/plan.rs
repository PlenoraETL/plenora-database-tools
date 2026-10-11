use super::sql::{native_declaration_sql, pg_type};
use crate::field_contract::FieldContract;
use arrow_schema::{DataType, Field};
use plenora_database_core::Result;

#[derive(Debug, Clone)]
pub(super) struct WriteColumnPlan {
    pub(super) data_type: DataType,
    pub(super) postgres_type: String,
    pub(super) native_type: Option<String>,
    pub(super) native_declaration_sql: Option<String>,
    /// I tipi dichiarati, in minuscolo; vuota se il campo non ne dichiara.
    pub(super) geometry_types: Vec<String>,
    /// `wkb` o `ewkb`; assente sugli ingressi legacy.
    pub(super) encoding: Option<String>,
    pub(super) dimensions: Option<String>,
    pub(super) srid: Option<u32>,
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
        Ok(Self {
            data_type: field.data_type().clone(),
            postgres_type: pg_type(&contract)?,
            native_type: contract.native_type.map(str::to_owned),
            native_declaration_sql: native_declaration_sql(&contract),
            geometry_types: contract
                .geometry_type
                .map(|types| types.split(',').map(str::to_ascii_lowercase).collect())
                .unwrap_or_default(),
            encoding: contract.canonical.encoding.map(str::to_owned),
            dimensions: contract.dimensions.map(str::to_owned),
            srid: contract.srid,
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

    /// Un campo `wkb` con SRID dichiarato: i suoi valori si riscrivono in
    /// EWKB prima di arrivare a `PostGIS`.
    pub(super) fn needs_srid_stamp(&self) -> bool {
        self.is_spatial()
            && self.encoding.as_deref() == Some("wkb")
            && self.srid.is_some_and(|srid| srid != 0)
    }

    pub(super) const fn is_spatial(&self) -> bool {
        matches!(
            self.semantics,
            ColumnSemantics::Geometry | ColumnSemantics::Geography
        )
    }
}
