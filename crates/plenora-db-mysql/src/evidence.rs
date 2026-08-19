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
