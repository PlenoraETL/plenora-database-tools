//! Descrittore di catalogo comune a DML, predicati e parameter-array.
//! Il writer non crea staging o tabelle di chiavi. La collation Db2 e
//! ereditata dal database; i testi richiedono UTF-8 e la collation provata del tipo built-in.
use crate::write_admission::ColumnFacts;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::{DatabaseError, ErrorPhase, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetType {
    declaration: String,
    facts: ColumnFacts,
}

pub(super) fn unsupported() -> DatabaseError {
    DatabaseError::unsupported(
        ProviderKind::Db2,
        ErrorPhase::Prepare,
        "tipo intermedio Db2 non identico o non qualificato",
    )
}

impl TargetType {
    pub(crate) fn from_catalog(facts: &ColumnFacts) -> Result<Self> {
        if facts.type_schema != "SYSIBM" {
            return Err(unsupported());
        }
        let native = facts.type_name.as_str();
        let declaration = match native {
            "BOOLEAN" | "SMALLINT" | "INTEGER" | "BIGINT" | "REAL" | "DOUBLE" | "DATE" => {
                native.to_owned()
            }
            "DECIMAL"
                if (1..=31).contains(&facts.length)
                    && facts.scale >= 0
                    && u64::try_from(facts.scale).is_ok_and(|s| s <= facts.length) =>
            {
                format!("DECIMAL({},{})", facts.length, facts.scale)
            }
            "TIMESTAMP" if (0..=12).contains(&facts.scale) => format!("TIMESTAMP({})", facts.scale),
            "CHARACTER" | "VARCHAR" | "CLOB"
                if facts.codepage == 1208
                    && facts.collation.strip_prefix("SYSIBM.").is_some_and(|name| {
                        !name.is_empty()
                            && name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
                    })
                    && facts.string_units_length > 0
                    && matches!(facts.string_units.as_str(), "OCTETS" | "CODEUNITS32") =>
            {
                format!(
                    "{native}({} {})",
                    facts.string_units_length, facts.string_units
                )
            }
            _ => return Err(unsupported()),
        };
        Ok(Self {
            declaration,
            facts: facts.clone(),
        })
    }

    pub(crate) fn operand(&self) -> String {
        format!("CAST(? AS {})", self.declaration)
    }
}
