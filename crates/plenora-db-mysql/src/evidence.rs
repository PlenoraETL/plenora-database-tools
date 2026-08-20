//! Il verdetto condiviso delle misure di evidenza.
//!
//! Due misure distinte lo producono — quella su `MariaDB` di ADR 0014 e
//! quella sulla semantica di sessione — e i loro runner leggono lo stesso
//! documento. Tenere una sola forma non e simmetria: un secondo `Recorder`
//! con gli stessi campi diverge alla prima aggiunta, e i due runner
//! comincerebbero a interpretare JSON diversi credendoli uguali.
//!
//! Esiste solo nei test: nessuna misura entra nel binario pubblico.

#![allow(clippy::redundant_pub_crate)]

use crate::MysqlConfig;
use plenora_database_core::provider::SecretString;

use serde_json::json;

pub(crate) fn environment(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

pub(crate) fn secret() -> SecretString {
    SecretString::new(environment("PLENORA_MYSQL_PASSWORD", "DataFlow_Test_2026!"))
}

pub(crate) fn config() -> MysqlConfig {
    let ca = std::env::var("PLENORA_MYSQL_CA")
        .expect("PLENORA_MYSQL_CA obbligatoria: la misura non accetta TLS non verificata");
    MysqlConfig::new(
        environment("PLENORA_MYSQL_HOST", "127.0.0.1"),
        environment("PLENORA_MYSQL_DATABASE", "dataflow_test"),
        environment("PLENORA_MYSQL_USER", "dataflow"),
        secret(),
    )
    .with_port(
        environment("PLENORA_MYSQL_PORT", "3306")
            .parse()
            .expect("porta MySQL della misura"),
    )
    .with_private_ca_certificate(ca)
}

/// Una riga del verdetto.
pub(crate) struct Observation {
    pub(crate) probe: &'static str,
    pub(crate) family: &'static str,
    pub(crate) surface: &'static str,
    pub(crate) question: &'static str,
    pub(crate) outcome: String,
    pub(crate) detail: String,
    pub(crate) server_code: Option<u16>,
}

impl Observation {
    pub(crate) fn to_json(&self) -> serde_json::Value {
        json!({
            "probe": self.probe,
            "family": self.family,
            "surface": self.surface,
            "question": self.question,
            "outcome": self.outcome,
            "detail": self.detail,
            "server_code": self.server_code,
        })
    }
}

pub(crate) struct Recorder(pub(crate) Vec<Observation>);

/// Se una sonda gia registrata e stata accettata.
///
/// Serve alle sonde che dipendono da un'altra: quando la dipendenza fallisce,
/// il loro errore e la stessa cosa vista due volte. Registrarlo come rifiuto
/// autonomo gonfierebbe il conto delle divergenze con una sola causa, e
/// nasconderebbe che la superficie non e mai stata raggiunta.
impl Recorder {
    pub(crate) fn accepted_probe(&self, probe: &str) -> bool {
        self.0
            .iter()
            .any(|entry| entry.probe == probe && entry.outcome == "accepted")
    }
}

impl Recorder {
    pub(crate) fn accepted(
        &mut self,
        probe: &'static str,
        family: &'static str,
        surface: &'static str,
        question: &'static str,
        detail: String,
    ) {
        self.0.push(Observation {
            probe,
            family,
            surface,
            question,
            outcome: "accepted".to_owned(),
            detail,
            server_code: None,
        });
    }

    pub(crate) fn rejected(
        &mut self,
        probe: &'static str,
        family: &'static str,
        surface: &'static str,
        question: &'static str,
        detail: String,
        server_code: Option<u16>,
    ) {
        self.0.push(Observation {
            probe,
            family,
            surface,
            question,
            outcome: "rejected".to_owned(),
            detail,
            server_code,
        });
    }

    /// Una superficie che questa tranche non misura, e perche.
    ///
    /// Dichiararlo e piu onesto che dedurlo: un esito assente non e un esito
    /// negativo, e un verdetto che li confondesse porterebbe a decidere su
    /// una prova che non e stata fatta.
    pub(crate) fn not_measured(
        &mut self,
        probe: &'static str,
        family: &'static str,
        surface: &'static str,
        question: &'static str,
        reason: &str,
    ) {
        self.0.push(Observation {
            probe,
            family,
            surface,
            question,
            outcome: "not_measured".to_owned(),
            detail: reason.to_owned(),
            server_code: None,
        });
    }
}

/// Il codice d'errore che il server ha mandato, se l'errore viene da li.
pub(crate) fn server_code(error: &mysql_async::Error) -> Option<u16> {
    match error {
        mysql_async::Error::Server(server) => Some(server.code),
        _ => None,
    }
}

pub(crate) fn truncate(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        text.to_owned()
    } else {
        text.chars().take(limit).collect::<String>() + "…"
    }
}

pub(crate) fn condense(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Le assegnazioni di uno `SET ... a = 1, b = 'x'`, come coppie nome/valore.
///
/// Le virgole dentro un valore quotato non separano: `sql_mode` ne contiene
/// due, e uno split ingenuo produrrebbe assegnazioni inventate.
///
/// # Panics
///
/// Se lo statement non comincia con `SET SESSION ` o se un pezzo non contiene
/// `=`: chi lo chiama sta descrivendo una costante del crate, non input.
pub(crate) fn sql_assignments(statement: &str) -> Vec<(String, String)> {
    let body = statement
        .strip_prefix("SET SESSION ")
        .expect("lo statement deve cominciare con SET SESSION");
    let mut pieces = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for character in body.chars() {
        match character {
            '\'' => quoted = !quoted,
            ',' if !quoted => pieces.push(std::mem::take(&mut current)),
            other => current.push(other),
        }
    }
    pieces.push(current);
    pieces
        .into_iter()
        .map(|piece| {
            let (name, value) = piece.split_once('=').expect("assegnazione senza '='");
            (name.trim().to_owned(), value.trim().to_owned())
        })
        .collect()
}

/// Il codice del server dentro il messaggio di un `DatabaseError`.
///
/// Il contratto non porta un campo per il codice: lo porta il messaggio, che
/// la classificazione compone come `(codice N)`. Leggerlo di li e una
/// dipendenza dal testo, e va detta — se quella forma cambiasse, la sonda che
/// lo usa fallirebbe invece di tacere, che e il verso giusto in cui rompersi.
pub(crate) fn server_code_in_message(message: &str) -> Option<u16> {
    let at = message.find("codice ")? + "codice ".len();
    let digits: String = message[at..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::{server_code_in_message, sql_assignments};

    #[test]
    fn assignments_survive_a_comma_inside_a_quoted_value() {
        let parsed =
            sql_assignments("SET SESSION autocommit = 1, time_zone = '+00:00', sql_mode = 'A,B,C'");
        assert_eq!(
            parsed,
            vec![
                ("autocommit".to_owned(), "1".to_owned()),
                ("time_zone".to_owned(), "+00:00".to_owned()),
                ("sql_mode".to_owned(), "A,B,C".to_owned()),
            ],
            "le virgole dentro gli apici non separano assegnazioni"
        );
    }

    #[test]
    fn the_real_bootstrap_parses_into_three_assignments() {
        let parsed = sql_assignments(crate::SESSION_BOOTSTRAP_SQL);
        let names: Vec<&str> = parsed.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, vec!["autocommit", "time_zone", "sql_mode"]);
    }

    #[test]
    fn the_server_code_is_read_from_the_message_or_absent() {
        assert_eq!(
            server_code_in_message("errore server MySQL redatto (codice 1792)"),
            Some(1_792)
        );
        assert_eq!(
            server_code_in_message("colonna MySQL non valida (codice 1054)"),
            Some(1_054)
        );
        // Nessun codice: un errore che nasce prima del server non ne ha uno,
        // e dedurne zero sarebbe peggio che dire "assente".
        assert_eq!(
            server_code_in_message("schema Arrow vuoto per append"),
            None
        );
        assert_eq!(server_code_in_message("codice non numerico"), None);
    }
}
