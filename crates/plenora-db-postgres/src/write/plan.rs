use super::sql::{native_declaration_sql, pg_type};
use crate::field_contract::FieldContract;
use arrow_schema::{DataType, Field};
use plenora_database_core::Result;

#[derive(Debug, Clone)]
pub(super) struct WriteColumnPlan {
    pub(super) target_type: Option<super::intermediate::TargetType>,
    pub(super) data_type: DataType,
    pub(super) postgres_type: String,
    pub(super) native_declaration_sql: Option<String>,
    geometry: crate::geometry_properties::GeometryPlan,
    pub(super) semantics: ColumnSemantics,
    /// Il prefisso dello schema di `PostGIS` (`"public".`) con cui lo SQL
    /// della scrittura nomina `geometry` e `geography` nel DDL
    /// (regola Q): vuoto finche la transazione della scrittura non lo legge
    /// da `pg_extension` ([`Self::qualify_postgis`]).
    pub(super) postgis: String,
    /// La regola V della colonna del target (`super::canonical`), letta dal
    /// catalogo nella transazione della scrittura o nel dry run con la DSN.
    pub(super) session_rule: Option<super::canonical::SessionRule>,
    /// L'uguaglianza con cui Update e `DeleteByKeys` confrontano la colonna,
    /// se e una chiave (regola K2, `super::visibility::key_equality`).
    pub(super) key_equality: Option<super::visibility::KeyEquality>,
    /// Le riscritture al salvataggio della colonna, misurate (regola A,
    /// `super::admission`), con la politica e se la colonna e una chiave.
    pub(super) store: StoreRule,
}

/// La regola S della colonna con le riscritture misurate della regola A.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct StoreRule {
    pub(super) rewrites: Vec<plenora_database_core::store_rewrite::StoreRewrite>,
    /// La politica e `Strict`: un valore riscritto si rifiuta.
    pub(super) strict: bool,
    /// La colonna e una chiave: un valore riscritto si rifiuta con ogni
    /// politica.
    pub(super) key: bool,
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
            target_type: None,
            data_type: field.data_type().clone(),
            postgres_type: pg_type(&contract)?,
            native_declaration_sql: native_declaration_sql(&contract),
            geometry: crate::geometry_properties::GeometryPlan::compile(&contract)?,
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
            postgis: String::new(),
            session_rule: None,
            key_equality: None,
            store: StoreRule::default(),
        })
    }

    /// Qualifica i nomi di `PostGIS` del piano con `qualifier` (regola Q):
    /// il tipo della colonna (`CREATE TABLE`, `ADD COLUMN`). I parametri
    /// ricevono direttamente il formato binario del tipo spatial.
    pub(super) fn qualify_postgis(&mut self, qualifier: &str) {
        if self.is_spatial() && self.postgis.is_empty() && !qualifier.is_empty() {
            self.postgres_type = format!("{qualifier}{}", self.postgres_type);
            qualifier.clone_into(&mut self.postgis);
        }
    }

    pub(super) const fn is_spatial(&self) -> bool {
        matches!(
            self.semantics,
            ColumnSemantics::Geometry | ColumnSemantics::Geography
        )
    }
}

impl std::ops::Deref for WriteColumnPlan {
    type Target = crate::geometry_properties::GeometryPlan;
    fn deref(&self) -> &Self::Target {
        &self.geometry
    }
}
#[cfg(test)]
impl std::ops::DerefMut for WriteColumnPlan {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.geometry
    }
}
