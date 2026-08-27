//! Vincoli **strutturali** della PRIMARY KEY che `WriteMode::Create` costruisce.
//!
//! Tre regole non dipendono dal motore: una chiave deve esistere nello schema
//! dichiarato, non puo ripetersi, e non puo essere nullable. Valgono ovunque
//! perche derivano da cosa significa una chiave primaria, non da come un
//! server la implementa. La validazione condivisa impedisce che un provider
//! trasformi implicitamente uno schema nullable in `NOT NULL`.
//!
//! Restano ai provider i vincoli che il **loro** motore impone: quali tipi
//! possono stare in una chiave, quante colonne, con quali limiti di
//! dimensione. Quelli non sono condivisibili, perche non sono gli stessi.

use arrow_schema::Schema;

/// Il motivo per cui una chiave dichiarata non puo diventare PRIMARY KEY.
///
/// Il tipo e pubblico e nominato: il provider decide categoria, fase ed
/// eventuale redazione dell'errore che ne deriva, senza dover interpretare
/// una stringa.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrimaryKeyViolation {
    /// La chiave non compare fra i campi dello schema in ingresso.
    Missing(String),
    /// Il campo esiste ma ammette NULL.
    Nullable(String),
    /// La stessa colonna compare piu volte nell'elenco delle chiavi.
    Repeated(String),
}

impl PrimaryKeyViolation {
    /// La colonna che ha causato il rifiuto.
    #[must_use]
    pub fn key(&self) -> &str {
        match self {
            Self::Missing(key) | Self::Nullable(key) | Self::Repeated(key) => key,
        }
    }

    /// Messaggio pubblico, con il nome del provider che lo emette.
    ///
    /// Il nome resta un parametro invece di derivare da [`ProviderKind`]
    /// perche i messaggi usano la forma leggibile — `PostgreSQL`, `MySQL` —
    /// e non il nome della variante.
    ///
    /// [`ProviderKind`]: crate::plan::ProviderKind
    #[must_use]
    pub fn message(&self, provider: &str) -> String {
        match self {
            Self::Missing(key) => {
                format!("chiave primaria {provider} '{key}' assente dallo schema Arrow")
            }
            Self::Nullable(key) => format!(
                "chiave primaria {provider} '{key}' e nullable nello schema Arrow: \
                 una PRIMARY KEY non ammette NULL"
            ),
            Self::Repeated(key) => format!("chiave primaria {provider} '{key}' ripetuta"),
        }
    }
}

/// Verifica le tre regole strutturali sulle chiavi di un `Create`.
///
/// L'ordine dei controlli e osservabile: per una stessa chiave la presenza
/// precede la nullability, e la ripetizione si valuta per ultima. Chi legge
/// l'errore vede la causa piu vicina alla sua dichiarazione.
///
/// # Errors
///
/// La prima violazione incontrata, scorrendo le chiavi nell'ordine dichiarato.
pub fn validate_create_primary_key(
    schema: &Schema,
    keys: &[String],
) -> Result<(), PrimaryKeyViolation> {
    let mut seen = std::collections::BTreeSet::new();
    for key in keys {
        let Ok(field) = schema.field_with_name(key) else {
            return Err(PrimaryKeyViolation::Missing(key.clone()));
        };
        if field.is_nullable() {
            return Err(PrimaryKeyViolation::Nullable(key.clone()));
        }
        if !seen.insert(key.as_str()) {
            return Err(PrimaryKeyViolation::Repeated(key.clone()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{validate_create_primary_key, PrimaryKeyViolation};
    use arrow_schema::{DataType, Field, Schema};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("tenant", DataType::Int64, false),
            Field::new("nota", DataType::Utf8, true),
        ])
    }

    #[test]
    fn a_composite_key_of_non_nullable_columns_is_accepted() {
        let keys = vec!["id".to_owned(), "tenant".to_owned()];
        assert_eq!(validate_create_primary_key(&schema(), &keys), Ok(()));
    }

    #[test]
    fn no_keys_is_not_a_violation() {
        assert_eq!(validate_create_primary_key(&schema(), &[]), Ok(()));
    }

    #[test]
    fn a_key_outside_the_schema_is_refused() {
        let keys = vec!["assente".to_owned()];
        assert_eq!(
            validate_create_primary_key(&schema(), &keys),
            Err(PrimaryKeyViolation::Missing("assente".to_owned()))
        );
    }

    #[test]
    fn a_nullable_key_is_refused_before_any_provider_coerces_it() {
        let keys = vec!["nota".to_owned()];
        assert_eq!(
            validate_create_primary_key(&schema(), &keys),
            Err(PrimaryKeyViolation::Nullable("nota".to_owned()))
        );
    }

    #[test]
    fn a_repeated_key_is_refused() {
        let keys = vec!["id".to_owned(), "id".to_owned()];
        assert_eq!(
            validate_create_primary_key(&schema(), &keys),
            Err(PrimaryKeyViolation::Repeated("id".to_owned()))
        );
    }

    #[test]
    fn the_message_names_the_provider_and_the_column() {
        let violation = PrimaryKeyViolation::Nullable("nota".to_owned());
        let message = violation.message("MySQL");
        assert!(message.contains("MySQL"), "{message}");
        assert!(message.contains("'nota'"), "{message}");
        assert_eq!(violation.key(), "nota");
    }
}
