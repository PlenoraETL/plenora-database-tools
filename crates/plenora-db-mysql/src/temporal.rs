//! Dominio dei campi temporali MySQL, comune a query e Arrow.
use mysql_async::Value;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};

pub(crate) fn validate(value: &Value) -> Result<()> {
    let valid = match value {
        Value::Date(y, mo, d, h, mi, s, us) => {
            (1..=9999).contains(y)
                && *us < 1_000_000
                && *s < 60
                && chrono::NaiveDate::from_ymd_opt(i32::from(*y), u32::from(*mo), u32::from(*d))
                    .and_then(|d| {
                        d.and_hms_micro_opt(u32::from(*h), u32::from(*mi), u32::from(*s), *us)
                    })
                    .is_some()
        }
        Value::Time(_, days, h, mi, s, us) => {
            *h < 24
                && *mi < 60
                && *s < 60
                && *us < 1_000_000
                && u64::from(*days) * 24 + u64::from(*h) <= 838
                && !(u64::from(*days) * 24 + u64::from(*h) == 838
                    && *mi == 59
                    && *s == 59
                    && *us != 0)
        }
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(DatabaseError::new(
            ErrorCategory::DataMapping,
            ErrorPhase::Read,
            None,
            "valore temporale MySQL fuori dominio",
        ))
    }
}

#[cfg(test)]
#[path = "temporal_tests.rs"]
mod tests;
