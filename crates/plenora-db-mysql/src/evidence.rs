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

/// Cosa una lettura ha prodotto, per intero.
///
/// Il conteggio dei batch e il digest esistono per la stessa ragione: una
/// sonda che si fermasse al primo batch non distingue una lettura che streamma
/// da una che materializza tutto e la consegna in un colpo solo, e un
/// confronto sul solo primo batch direbbe "identici" di due stream che
/// divergono alla riga successiva.
#[derive(Debug, Default)]
pub(crate) struct ReadOutcome {
    pub(crate) batches: usize,
    pub(crate) rows: usize,
    /// I nomi dei campi, nell'ordine in cui la lettura li pubblica.
    pub(crate) names: Vec<String>,
    /// Lo schema leggibile, metadata compresi.
    pub(crate) schema: String,
    /// Il **primo** batch, per chi legge il verdetto. Il confronto fra server
    /// non passa di qui ma dal digest, che copre tutti i batch.
    pub(crate) first_batch: String,
    /// L'impronta di **tutto** cio che e stato decodificato.
    pub(crate) digest: String,
    /// Campi annotati con la chiave del proprio prodotto, e con quella
    /// dell'altro.
    pub(crate) own_namespace: usize,
    pub(crate) foreign_namespace: usize,
    /// Il primo valore intero della prima colonna, quando c'e: e cio che
    /// distingue un ordinamento ascendente da uno discendente.
    pub(crate) first_integer: Option<i64>,
}

/// Cosa una sonda di lettura pretende di osservare.
///
/// Esiste perche `accepted` non diventi "la chiamata non ha dato errore". Una
/// projection ignorata, un ordinamento che non ordina, uno stream che
/// consegna un batch solo: tutte e tre restituiscono `Ok`, e senza attese
/// esatte finirebbero verdi — su tutti e tre i server, il che le farebbe
/// sembrare pure una convergenza.
#[derive(Debug)]
pub(crate) struct ReadContract {
    /// I nomi attesi, nell'ordine. Vuoto significa "non e questa la domanda".
    pub(crate) columns: &'static [&'static str],
    pub(crate) rows: usize,
    /// I batch attesi. `None` quando la sonda non parla di streaming.
    pub(crate) batches: Option<usize>,
    /// Il primo intero atteso, per le sonde su ordinamento e filtro.
    pub(crate) first_integer: Option<i64>,
}

/// Cosa manca perche l'osservazione soddisfi il contratto, o `None`.
///
/// Restituisce la **prima** differenza e non tutte: il verdetto deve dire
/// cosa e successo, e un elenco di sei righe su una sonda che ne ha sbagliata
/// una sola si legge peggio.
pub(crate) fn read_mismatch(contract: &ReadContract, outcome: &ReadOutcome) -> Option<String> {
    if !contract.columns.is_empty() && outcome.names != contract.columns {
        return Some(format!(
            "colonne attese {:?}, osservate {:?}",
            contract.columns, outcome.names
        ));
    }
    if outcome.rows != contract.rows {
        return Some(format!(
            "righe attese {}, osservate {}",
            contract.rows, outcome.rows
        ));
    }
    if let Some(batches) = contract.batches {
        if outcome.batches != batches {
            return Some(format!(
                "batch attesi {batches}, osservati {}",
                outcome.batches
            ));
        }
    }
    if contract.first_integer.is_some() && outcome.first_integer != contract.first_integer {
        return Some(format!(
            "primo valore atteso {:?}, osservato {:?}",
            contract.first_integer, outcome.first_integer
        ));
    }
    // Il namespace non e un parametro del contratto: e sempre lo stesso
    // requisito, e vale per ogni lettura. Ogni campo pubblicato porta la
    // chiave del proprio prodotto, e nessuno porta quella dell'altro.
    if outcome.foreign_namespace != 0 {
        return Some(format!(
            "{} campi annotati con il namespace dell'altro prodotto",
            outcome.foreign_namespace
        ));
    }
    if !outcome.names.is_empty() && outcome.own_namespace != outcome.names.len() {
        return Some(format!(
            "campi annotati {} su {}",
            outcome.own_namespace,
            outcome.names.len()
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{
        read_mismatch, server_code_in_message, sql_assignments, ReadContract, ReadOutcome,
    };

    fn observed() -> ReadOutcome {
        ReadOutcome {
            batches: 2,
            rows: 8_193,
            names: vec!["id".to_owned(), "payload".to_owned()],
            schema: String::new(),
            first_batch: String::new(),
            digest: String::new(),
            own_namespace: 2,
            foreign_namespace: 0,
            first_integer: Some(1),
        }
    }

    fn contract() -> ReadContract {
        ReadContract {
            columns: &["id", "payload"],
            rows: 8_193,
            batches: Some(2),
            first_integer: Some(1),
        }
    }

    #[test]
    fn a_read_that_meets_the_contract_has_nothing_to_report() {
        assert_eq!(read_mismatch(&contract(), &observed()), None);
    }

    #[test]
    fn every_clause_of_the_read_contract_can_fail_on_its_own() {
        // Una per difetto, e sono i difetti che le sonde live non potrebbero
        // distinguere da un successo: la projection ignorata, il filtro che
        // non filtra, l'ordinamento che non ordina, lo stream che consegna
        // tutto in un colpo, il namespace dell'altro prodotto.
        //
        // Sono tutti casi che restituiscono `Ok` al chiamante, ed e la
        // ragione per cui esiste questo validatore invece di un `is_ok()`.
        let perturbations: Vec<(&str, ReadOutcome, &str)> = vec![
            (
                "projection ignorata",
                ReadOutcome {
                    names: vec!["id".to_owned(), "payload".to_owned(), "label".to_owned()],
                    own_namespace: 3,
                    ..observed()
                },
                "colonne attese",
            ),
            (
                "filtro che non filtra",
                ReadOutcome {
                    rows: 8_192,
                    ..observed()
                },
                "righe attese",
            ),
            (
                "stream consegnato in un colpo solo",
                ReadOutcome {
                    batches: 1,
                    ..observed()
                },
                "batch attesi",
            ),
            (
                "ordinamento che non ordina",
                ReadOutcome {
                    first_integer: Some(8_193),
                    ..observed()
                },
                "primo valore atteso",
            ),
            (
                "namespace dell'altro prodotto",
                ReadOutcome {
                    foreign_namespace: 1,
                    ..observed()
                },
                "namespace dell'altro prodotto",
            ),
            (
                "campo senza annotazione",
                ReadOutcome {
                    own_namespace: 1,
                    ..observed()
                },
                "campi annotati",
            ),
        ];
        for (what, outcome, expected) in perturbations {
            let reported = read_mismatch(&contract(), &outcome)
                .unwrap_or_else(|| panic!("{what}: il validatore non se n'e accorto"));
            assert!(
                reported.contains(expected),
                "{what}: il verdetto non dice cosa manca — {reported}"
            );
        }
    }

    #[test]
    fn a_contract_without_a_question_does_not_invent_one() {
        // `columns` vuoto e `batches`/`first_integer` assenti significano "non
        // e questa la domanda", non "va bene qualunque cosa": cio che resta
        // dichiarato continua a essere verificato.
        let loose = ReadContract {
            columns: &[],
            rows: 8_193,
            batches: None,
            first_integer: None,
        };
        let different = ReadOutcome {
            names: vec!["altro".to_owned()],
            own_namespace: 1,
            batches: 9,
            first_integer: Some(42),
            ..observed()
        };
        assert_eq!(read_mismatch(&loose, &different), None);
        assert!(read_mismatch(
            &loose,
            &ReadOutcome {
                rows: 1,
                ..observed()
            }
        )
        .is_some());
    }

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
