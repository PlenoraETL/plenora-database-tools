//! Validazione temporale prima del decoder chrono: TIME non deve avvolgere.
use tokio_postgres::types::{FromSql, Type};

pub fn interval_nanoseconds(micros: i64) -> plenora_database_core::Result<i64> {
    micros.checked_mul(1000).ok_or_else(|| {
        crate::arrow::read_mapping_error("interval PostgreSQL oltre il range Arrow nanosecond")
    })
}

#[derive(Debug)]
pub struct CheckedTime(pub chrono::NaiveTime);

impl<'a> FromSql<'a> for CheckedTime {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let bytes = raw
            .try_into()
            .map_err(|_| "lunghezza TIME PostgreSQL non valida")?;
        let micros = i64::from_be_bytes(bytes);
        if !(0..86_400_000_000).contains(&micros) {
            return Err("TIME PostgreSQL fuori dal dominio giornaliero".into());
        }
        chrono::NaiveTime::from_sql(ty, raw).map(Self)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::TIME
    }
}

#[cfg(test)]
#[path = "temporal_tests.rs"]
mod tests;
