//! Controllo dei campi TDS prima delle conversioni chrono del driver.
use tiberius::{ColumnData, FromSql};

pub struct Checked<T>(pub T);

fn invalid() -> tiberius::error::Error {
    tiberius::error::Error::Conversion("valore temporale TDS fuori dominio".into())
}

const fn date(days: u32) -> bool {
    days <= 3_652_058
}

fn time(value: tiberius::time::Time) -> bool {
    value.scale() <= 7 && value.increments() < 86_400 * 10_u64.pow(u32::from(value.scale()))
}

fn datetime(value: tiberius::time::DateTime2) -> bool {
    date(value.date().days()) && time(value.time())
}

fn validate(value: &ColumnData<'_>) -> Result<(), tiberius::error::Error> {
    let valid = match value {
        ColumnData::Date(Some(v)) => date(v.days()),
        ColumnData::Time(Some(v)) => time(*v),
        ColumnData::DateTime2(Some(v)) => datetime(*v),
        ColumnData::DateTime(Some(v)) => {
            (-53_690..=2_958_463).contains(&v.days()) && v.seconds_fragments() < 25_920_000
        }
        ColumnData::SmallDateTime(Some(v)) => v.seconds_fragments() < 1_440,
        ColumnData::DateTimeOffset(Some(v)) => {
            if !datetime(v.datetime2()) || !(-840..=840).contains(&v.offset()) {
                return Err(invalid());
            }
            // Il wire porta UTC; anche il calendario locale deve restare 0001..9999.
            let dt = v.datetime2();
            let ticks = i128::from(dt.date().days()) * 864_000_000_000
                + i128::from(dt.time().increments())
                    * 10_i128.pow(7 - u32::from(dt.time().scale()));
            let local = ticks + i128::from(v.offset()) * 600_000_000;
            (0..3_652_059_i128 * 864_000_000_000).contains(&local)
        }
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(invalid())
    }
}

impl<'a, T: FromSql<'a>> FromSql<'a> for Checked<T> {
    fn from_sql(value: &'a ColumnData<'static>) -> Result<Option<Self>, tiberius::error::Error> {
        validate(value)?;
        T::from_sql(value).map(|value| value.map(Self))
    }
}

#[cfg(test)]
#[path = "temporal_tests.rs"]
mod tests;
