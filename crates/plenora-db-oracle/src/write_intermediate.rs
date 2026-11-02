//! Il tipo dell'operando SQL viene dal catalogo, mai dal tipo Arrow del bind.
//! CLOB/BLOB usano locator tipizzati e verifica transazionale, senza CAST.
//! NCLOB, LONG e JSON restano chiusi. Oracle non usa staging/GTT nel writer.

use crate::write_admission::ColumnFacts;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::{DatabaseError, ErrorPhase, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetType {
    pub(super) declaration: String,
    pub(super) collation: Option<String>,
    text_limits: Option<(u64, u64, bool)>,
    raw_limit: Option<u64>,
}

pub fn unsupported() -> DatabaseError {
    DatabaseError::unsupported(
        ProviderKind::Oracle,
        ErrorPhase::Prepare,
        "tipo intermedio Oracle non identico o non qualificato",
    )
}

impl TargetType {
    pub(super) fn from_catalog(facts: &ColumnFacts) -> Result<Self> {
        let native = facts.data_type.to_ascii_uppercase();
        let mut collation = None;
        let declaration = match native.as_str() {
            "NUMBER" => match (facts.precision, facts.scale) {
                (None, None) => native,
                (precision, Some(scale))
                    if (-84..=127).contains(&scale)
                        && precision.is_none_or(|p| (1..=38).contains(&p)) =>
                {
                    let precision = precision.map_or_else(|| "*".to_owned(), |p| p.to_string());
                    format!("NUMBER({precision},{scale})")
                }
                _ => return Err(unsupported()),
            },
            "FLOAT" => match facts.precision {
                Some(p @ 1..=126) => format!("FLOAT({p})"),
                _ => return Err(unsupported()),
            },
            "CHAR" | "VARCHAR2" | "NCHAR" | "NVARCHAR2" => {
                let declared = facts.collation.as_deref().ok_or_else(unsupported)?;
                if !matches!(declared, "BINARY" | "USING_NLS_COMP") {
                    return Err(unsupported());
                }
                collation = Some(declared.to_owned());
                let national = native.starts_with('N');
                let (length, unit) = match (national, facts.char_used.as_deref()) {
                    (true, Some("C")) => (facts.char_length, ""),
                    (false, Some("C")) => (facts.char_length, " CHAR"),
                    (false, Some("B")) => (facts.data_length, " BYTE"),
                    _ => return Err(unsupported()),
                };
                if length == 0 {
                    return Err(unsupported());
                }
                format!("{native}({length}{unit})")
            }
            "RAW" if facts.data_length > 0 => format!("RAW({})", facts.data_length),
            "BOOLEAN" | "BINARY_FLOAT" | "BINARY_DOUBLE" | "DATE" | "CLOB" | "BLOB" => native,
            _ if (0..=9).any(|p| {
                [
                    format!("TIMESTAMP({p})"),
                    format!("TIMESTAMP({p}) WITH TIME ZONE"),
                ]
                .contains(&native)
            }) =>
            {
                native
            }
            _ => return Err(unsupported()),
        };
        let text_limits = collation.as_ref().map(|_| {
            (
                facts.char_length,
                facts.data_length,
                declaration.starts_with('N'),
            )
        });
        Ok(Self {
            raw_limit: (facts.data_type.eq_ignore_ascii_case("RAW")).then_some(facts.data_length),
            declaration,
            collation,
            text_limits,
        })
    }

    // CAST puo troncare dove l'assegnazione diretta darebbe ORA-12899.
    // Il limite in byte vale anche per CHAR semantics; AL16UTF16 conta
    // coppie surrogate, non scalari Unicode. Non dipende dalle regole A/S.
    pub(super) fn check_value(
        &self,
        value: &plenora_database_core::provider::ParameterValue,
    ) -> Result<()> {
        use plenora_database_core::provider::ParameterValue;
        match value {
            ParameterValue::String(text) => self.check_text(text)?,
            ParameterValue::Bytes(bytes) => self.check_bytes(bytes)?,
            _ => {}
        }
        Ok(())
    }

    pub(super) fn check_bytes(&self, bytes: &[u8]) -> Result<()> {
        if self
            .raw_limit
            .is_some_and(|limit| u64::try_from(bytes.len()).unwrap_or(u64::MAX) > limit)
        {
            return Err(DatabaseError::new(
                plenora_database_core::ErrorCategory::DataMapping,
                ErrorPhase::Write,
                Some(ProviderKind::Oracle),
                "binario oltre la lunghezza del target Oracle",
            ));
        }
        Ok(())
    }

    pub(super) fn check_text(&self, text: &str) -> Result<()> {
        if let Some((chars, bytes, national)) = self.text_limits {
            let (characters, encoded_bytes) = if national {
                let units = text.encode_utf16().count();
                (units, units.saturating_mul(2))
            } else {
                (text.chars().count(), text.len())
            };
            if u64::try_from(characters).unwrap_or(u64::MAX) > chars
                || u64::try_from(encoded_bytes).unwrap_or(u64::MAX) > bytes
            {
                return Err(DatabaseError::new(
                    plenora_database_core::ErrorCategory::DataMapping,
                    ErrorPhase::Write,
                    Some(ProviderKind::Oracle),
                    "testo oltre la lunghezza del target Oracle",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn lob_type(&self) -> Option<oracle_rs::OracleType> {
        match self.declaration.as_str() {
            "CLOB" => Some(oracle_rs::OracleType::Clob),
            "BLOB" => Some(oracle_rs::OracleType::Blob),
            _ => None,
        }
    }

    pub(super) fn operand(&self, input: &str) -> String {
        if self.lob_type().is_some() {
            return input.to_owned();
        }
        let cast = format!("CAST({input} AS {})", self.declaration);
        self.collation.as_ref().map_or_else(
            || cast.clone(),
            |collation| format!("({cast} COLLATE {collation})"),
        )
    }
}
