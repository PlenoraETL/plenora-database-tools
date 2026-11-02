//! Profilo catturato solo dopo il bootstrap, immutabile per lo scope acquisito.
use mysql_async::Row;

const VARIABLES: &[(&str, &str)] = &[
    ("sql_notes", "1"),
    ("sql_mode", "NO_AUTO_VALUE_ON_ZERO,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"),
    ("sql_select_limit", "18446744073709551615"),
    ("foreign_key_checks", "1"),
    ("unique_checks", "1"),
    ("time_zone", "+00:00"),
    ("autocommit", "1"),
    ("character_set_client", "utf8mb4"),
    ("character_set_connection", "utf8mb4"),
    ("character_set_results", "utf8mb4"),
    ("collation_connection", "utf8mb4_general_ci"),
    ("max_error_count", "65535"),
];

pub fn select_sql() -> String {
    let columns = VARIABLES
        .iter()
        .map(|(name, _)| format!("CAST(@@session.{name} AS CHAR)"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT DATABASE(), {columns}")
}

pub fn normalized(name: &str, value: &str) -> String {
    match name {
        "sql_mode" => value
            .split(',')
            .map(|mode| mode.trim().to_ascii_uppercase())
            .filter(|mode| !mode.is_empty())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(","),
        "autocommit" | "sql_notes" | "foreign_key_checks" | "unique_checks" => {
            match value.to_ascii_uppercase().as_str() {
                "ON" | "1" => "1".to_owned(),
                "OFF" | "0" => "0".to_owned(),
                _ => value.to_owned(),
            }
        }
        _ => value.to_owned(),
    }
}

pub struct SessionProfile(Vec<Option<String>>);

impl SessionProfile {
    pub fn capture(rows: &[Row], database: &str) -> Result<Self, &'static str> {
        if rows.len() != 1 || rows[0].len() != VARIABLES.len() + 1 {
            return Err("profile_shape");
        }
        let mut values = Vec::with_capacity(VARIABLES.len() + 1);
        for (index, name) in std::iter::once("database")
            .chain(VARIABLES.iter().map(|(name, _)| *name))
            .enumerate()
        {
            let value = rows[0]
                .get_opt::<Option<String>, _>(index)
                .and_then(Result::ok)
                .ok_or(name)?;
            if (value.is_none() && name != "character_set_results")
                || (index == 0 && value.as_deref() != Some(database))
            {
                return Err(name);
            }
            values.push(value.map(|value| normalized(name, &value)));
        }
        Ok(Self(values))
    }

    #[cfg(test)]
    pub fn log_bootstrap(rows: &[Row]) {
        if rows.len() != 1 || rows[0].len() != VARIABLES.len() + 1 {
            return;
        }
        for (index, (name, expected)) in VARIABLES.iter().enumerate() {
            if rows[0]
                .get_opt::<String, _>(index + 1)
                .and_then(Result::ok)
                .as_deref()
                != Some(expected)
            {
                // Solo nomi statici: nessun valore della sessione nei log live.
                println!("session bootstrap representation variable={name}");
            }
        }
    }

    pub fn divergence(&self, rows: &[Row], database: &str) -> Option<&'static str> {
        let actual = match Self::capture(rows, database) {
            Ok(actual) => actual,
            Err(name) => return Some(name),
        };
        std::iter::once("database")
            .chain(VARIABLES.iter().map(|(name, _)| *name))
            .zip(self.0.iter().zip(&actual.0))
            .find_map(|(name, (expected, actual))| (expected != actual).then_some(name))
    }
}

#[cfg(test)]
#[path = "session_profile_tests.rs"]
mod tests;
