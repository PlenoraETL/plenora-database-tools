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
use plenora_database_core::plan::FilterExpression;
use plenora_database_core::provider::{ParameterBag, ParameterValue, SecretString};
use plenora_database_core::{ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition};

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

    /// Una superficie non misurata e il motivo dell'esclusione.
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

/// Quante righe mette la tabella su cui si misurano lettura e filtri.
///
/// `DEFAULT_BATCH_ROWS + 1`, cioe la tabella piu piccola che **non** puo stare
/// in un batch solo: e il taglio del lettore, quindi una lettura che ne
/// consegna due sta streammando e una che ne consegna uno ha ignorato il
/// proprio limite.
pub(crate) const STREAMING_ROWS: usize = crate::DEFAULT_BATCH_ROWS + 1;

/// Lo stesso numero con segno, per i parametri legati e per le attese sul
/// primo valore.
#[allow(clippy::cast_possible_wrap)]
pub(crate) const STREAMING_ROWS_I64: i64 = STREAMING_ROWS as i64;

/// Ogni terza riga ha `label` nulla: e cio che rende `IS NULL` e `IS NOT NULL`
/// due domande con due risposte diverse invece di due modi di dire "tutte".
pub(crate) const UNLABELLED_ROWS: usize = STREAMING_ROWS / 3;
pub(crate) const LABELLED_ROWS: usize = STREAMING_ROWS - UNLABELLED_ROWS;

/// I nomi delle forme di filtro qualificate, nell'ordine in cui la tabella le
/// dichiara.
///
/// L'elenco vive separato dalla tabella perche una sonda aggregata non si
/// accorge di una voce mancante: toglierne una cambierebbe soltanto il
/// dettaglio testuale, e la sonda resterebbe verde su tutti e tre i server.
/// Qui invece un test puro confronta i due, e la differenza si vede prima di
/// accendere un server.
pub(crate) const QUALIFIED_FILTER_FORMS: &[&str] = &[
    "eq",
    "ne",
    "lt",
    "lte",
    "gt",
    "gte",
    "is_null",
    "is_not_null",
    "in",
    "between",
    "like",
    "and",
    "or",
];

/// Una forma di filtro qualificata, con cosa deve rendere.
pub(crate) struct FilterCase {
    pub(crate) name: &'static str,
    pub(crate) expression: FilterExpression,
    pub(crate) rows: usize,
    pub(crate) first: i64,
    pub(crate) parameters: ParameterBag,
}

/// Le forme di filtro che il renderer qualifica, con cosa devono rendere.
///
/// I numeri non sono calcolati dall'harness: sono scritti qui, derivati dalla
/// definizione della fixture. Calcolarli con lo stesso codice che li misura
/// farebbe passare per verificata una formula sbagliata due volte allo stesso
/// modo.
///
/// Ogni riga porta anche il **primo** valore atteso, perche il conteggio da
/// solo non distingue `id < 100` da `id > 8093`: sono novantanove righe
/// entrambi.
///
/// E ogni riga porta i **propri** parametri. Non e una comodita: il provider
/// rifiuta un `ParameterBag` che contenga voci che il piano non lega, e ha
/// ragione: un parametro non usato e quasi sempre un filtro scritto male.
#[allow(clippy::too_many_lines)]
pub(crate) fn qualified_filter_forms() -> Vec<FilterCase> {
    let bag = |pairs: Vec<(&str, ParameterValue)>| {
        ParameterBag::new(
            pairs
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value))
                .collect(),
        )
    };
    let integer = |name: &'static str, value: i64| (name, ParameterValue::I64(value));
    let field = |name: &str| name.to_owned();
    let penultimate = STREAMING_ROWS_I64 - 3;
    vec![
        FilterCase {
            name: "eq",
            expression: FilterExpression::Eq {
                field: field("id"),
                parameter: field("sette"),
            },
            rows: 1,
            first: 7,
            parameters: bag(vec![integer("sette", 7)]),
        },
        FilterCase {
            name: "ne",
            expression: FilterExpression::Ne {
                field: field("id"),
                parameter: field("sette"),
            },
            rows: STREAMING_ROWS - 1,
            first: 1,
            parameters: bag(vec![integer("sette", 7)]),
        },
        FilterCase {
            name: "lt",
            expression: FilterExpression::Lt {
                field: field("id"),
                parameter: field("cento"),
            },
            rows: 99,
            first: 1,
            parameters: bag(vec![integer("cento", 100)]),
        },
        FilterCase {
            name: "lte",
            expression: FilterExpression::Lte {
                field: field("id"),
                parameter: field("cento"),
            },
            rows: 100,
            first: 1,
            parameters: bag(vec![integer("cento", 100)]),
        },
        FilterCase {
            name: "gt",
            expression: FilterExpression::Gt {
                field: field("id"),
                parameter: field("penultime"),
            },
            rows: 3,
            first: penultimate + 1,
            parameters: bag(vec![integer("penultime", penultimate)]),
        },
        FilterCase {
            name: "gte",
            expression: FilterExpression::Gte {
                field: field("id"),
                parameter: field("penultime"),
            },
            rows: 4,
            first: penultimate,
            parameters: bag(vec![integer("penultime", penultimate)]),
        },
        FilterCase {
            name: "is_null",
            expression: FilterExpression::IsNull {
                field: field("label"),
            },
            rows: UNLABELLED_ROWS,
            first: 3,
            parameters: ParameterBag::default(),
        },
        FilterCase {
            name: "is_not_null",
            expression: FilterExpression::IsNotNull {
                field: field("label"),
            },
            rows: LABELLED_ROWS,
            first: 1,
            parameters: ParameterBag::default(),
        },
        FilterCase {
            name: "in",
            expression: FilterExpression::In {
                field: field("id"),
                parameters: vec![field("uno"), field("due"), field("tre")],
            },
            rows: 3,
            first: 1,
            parameters: bag(vec![
                integer("uno", 1),
                integer("due", 2),
                integer("tre", 3),
            ]),
        },
        FilterCase {
            name: "between",
            expression: FilterExpression::Between {
                field: field("id"),
                lower_parameter: field("dieci"),
                upper_parameter: field("venti"),
            },
            rows: 11,
            first: 10,
            parameters: bag(vec![integer("dieci", 10), integer("venti", 20)]),
        },
        FilterCase {
            name: "like",
            expression: FilterExpression::Like {
                field: field("payload"),
                parameter: field("coda_sette"),
                case_insensitive: false,
            },
            rows: 1,
            first: 7,
            parameters: bag(vec![(
                "coda_sette",
                ParameterValue::String("%0007".to_owned()),
            )]),
        },
        FilterCase {
            name: "and",
            expression: FilterExpression::And {
                args: vec![
                    FilterExpression::Gt {
                        field: field("id"),
                        parameter: field("penultime"),
                    },
                    FilterExpression::Lt {
                        field: field("id"),
                        parameter: field("ultima"),
                    },
                ],
            },
            rows: 2,
            first: penultimate + 1,
            parameters: bag(vec![
                integer("penultime", penultimate),
                integer("ultima", STREAMING_ROWS_I64),
            ]),
        },
        FilterCase {
            name: "or",
            expression: FilterExpression::Or {
                args: vec![
                    FilterExpression::Eq {
                        field: field("id"),
                        parameter: field("uno"),
                    },
                    FilterExpression::Eq {
                        field: field("id"),
                        parameter: field("due"),
                    },
                ],
            },
            rows: 2,
            first: 1,
            parameters: bag(vec![integer("uno", 1), integer("due", 2)]),
        },
    ]
}

/// L'esito che la DDL dell'indice su espressione **deve** avere, per prodotto.
///
/// La prova live stabilisce che `MySQL` la accetta e `MariaDB` la rifiuta con
/// 1064 per sintassi non supportata. Fissarne l'esito
/// serve perche l'esito della DDL decide cosa il catalogo debba mostrare
/// dopo: senza, un errore qualunque — un privilegio mancante, un timeout —
/// diventerebbe "l'indice non c'e", e un catalogo senza indice passerebbe per
/// la conferma di un rifiuto che non e mai avvenuto.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExpressionIndexDdl {
    /// Creato: il catalogo deve mostrarlo, e deve mostrarlo non confrontabile.
    Accepted,
    /// Rifiutato con questo codice: il catalogo non deve mostrarlo affatto.
    Refused(u16),
}

impl ExpressionIndexDdl {
    /// Cosa ci si aspetta da questo prodotto.
    pub(crate) fn of(profile: &dyn crate::profile::ProductProfile) -> Self {
        if profile.kind() == plenora_database_core::plan::ProviderKind::Mariadb {
            Self::Refused(1_064)
        } else {
            Self::Accepted
        }
    }

    /// Cosa non torna fra l'esito atteso e quello osservato, o `None`.
    ///
    /// `observed` porta il **codice** e non l'errore del driver: cosi il
    /// giudizio si prova offline, senza costruire un errore di `mysql_async`.
    /// Il testo completo di cio che il server ha detto resta nella sonda
    /// `raw` che lo registra, dove serve a leggere il verdetto.
    pub(crate) fn mismatch(self, observed: Result<(), Option<u16>>) -> Option<String> {
        match (self, observed) {
            (Self::Accepted, Ok(())) => None,
            (Self::Accepted, Err(code)) => Some(format!(
                "la DDL doveva essere accettata, e stata rifiutata ({})",
                code.map_or_else(
                    || "senza codice del server".to_owned(),
                    |code| format!("codice {code}")
                )
            )),
            (Self::Refused(expected), Err(Some(observed))) if observed == expected => None,
            (Self::Refused(expected), Err(Some(observed))) => Some(format!(
                "la DDL doveva essere rifiutata con {expected}, osservato {observed}"
            )),
            (Self::Refused(expected), Err(None)) => Some(format!(
                "la DDL doveva essere rifiutata con {expected}, ma l'errore non porta \
                 un codice del server"
            )),
            (Self::Refused(expected), Ok(())) => Some(format!(
                "la DDL doveva essere rifiutata con {expected}, ed e passata"
            )),
        }
    }
}

/// La forma che una tabella con un indice unico su colonna generata deve
/// avere, vista dal catalogo.
///
/// Registrare cio che si vede e diverso dal verificare che sia cio che serve:
/// da questa forma dipendono due decisioni — se la colonna sia scrivibile e se
/// l'indice sia confrontabile con le keys di un Upsert — e una descrizione che
/// perdesse la colonna, o rendesse l'indice non unico, le cambierebbe entrambe
/// senza che nulla fallisse.
pub(crate) fn generated_index_mismatch(
    description: &crate::MysqlObjectDescription,
    column_name: &str,
    index_name: &str,
) -> Option<String> {
    let generated = description
        .columns
        .iter()
        .find(|column| column.name == column_name);
    let index = description
        .indexes
        .iter()
        .find(|index| index.name == index_name);
    match (generated, index) {
        (None, _) => Some(format!("la colonna generata {column_name} non compare")),
        (_, None) => Some(format!("l'indice {index_name} non compare")),
        (Some(column), _) if column.generation_expression.is_empty() => Some(format!(
            "la colonna {column_name} risulta non generata: sarebbe scrivibile"
        )),
        (_, Some(index)) if index.columns != [column_name] => Some(format!(
            "l'indice {index_name} non e sulla sola colonna generata: {:?}",
            index.columns
        )),
        (_, Some(index)) if !index.unique => {
            Some(format!("l'indice {index_name} non risulta unico"))
        }
        (_, Some(index)) if !index.column_backed => Some(format!(
            "l'indice {index_name} non risulta confrontabile per colonne"
        )),
        _ => None,
    }
}

/// Il rifiuto che una sonda si aspetta, per intero.
///
/// Esiste perche "ha dato errore" non e una misura. Una forma di filtro che
/// il renderer rifiuta **per scelta** e indistinguibile, dal solo `Err`, da
/// una rifiutata perche la colonna non esiste o il parametro e del tipo
/// sbagliato: il giorno in cui quella scelta cambiasse, la sonda resterebbe
/// verde per la ragione sbagliata e il fail-close sembrerebbe ancora
/// verificato.
///
/// La quaterna categoria/fase/effetto/retry e cio che il chiamante usa per
/// decidere cosa fare dopo; il frammento di messaggio e cio che identifica
/// **quale** rifiuto deliberato sia.
#[derive(Debug)]
pub(crate) struct RefusalContract {
    pub(crate) category: ErrorCategory,
    pub(crate) phase: ErrorPhase,
    pub(crate) remote_effect: RemoteEffect,
    pub(crate) retry: RetryDisposition,
    pub(crate) message_contains: &'static str,
}

/// Cosa non torna fra il rifiuto atteso e quello osservato, o `None`.
pub(crate) fn refusal_mismatch(
    contract: &RefusalContract,
    error: &plenora_database_core::DatabaseError,
) -> Option<String> {
    if error.category != contract.category {
        return Some(format!(
            "categoria attesa {:?}, osservata {:?}",
            contract.category, error.category
        ));
    }
    if error.phase != contract.phase {
        return Some(format!(
            "fase attesa {:?}, osservata {:?}",
            contract.phase, error.phase
        ));
    }
    if error.remote_effect != contract.remote_effect {
        return Some(format!(
            "effetto remoto atteso {:?}, osservato {:?}",
            contract.remote_effect, error.remote_effect
        ));
    }
    if error.retry != contract.retry {
        return Some(format!(
            "retry atteso {:?}, osservato {:?}",
            contract.retry, error.retry
        ));
    }
    if !error.message.contains(contract.message_contains) {
        return Some(format!(
            "il messaggio non porta {:?}: {}",
            contract.message_contains, error.message
        ));
    }
    None
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
        generated_index_mismatch, qualified_filter_forms, read_mismatch, refusal_mismatch,
        server_code_in_message, sql_assignments, ExpressionIndexDdl, ReadContract, ReadOutcome,
        RefusalContract, QUALIFIED_FILTER_FORMS,
    };
    #[test]
    fn the_expected_ddl_outcome_is_the_measured_one() {
        // I due prodotti partono da due punti diversi, e ciascuno ha il suo:
        // MySQL accetta l'indice su espressione, MariaDB lo rifiuta con 1064.
        assert_eq!(
            ExpressionIndexDdl::of(&crate::profile::MYSQL_PROFILE),
            ExpressionIndexDdl::Accepted
        );
        assert_eq!(
            ExpressionIndexDdl::of(&crate::profile::MARIADB_PROFILE),
            ExpressionIndexDdl::Refused(1_064)
        );

        assert_eq!(ExpressionIndexDdl::Accepted.mismatch(Ok(())), None);
        assert_eq!(
            ExpressionIndexDdl::Refused(1_064).mismatch(Err(Some(1_064))),
            None
        );

        // E ogni altro esito e una premessa che manca. Il caso che conta e il
        // terzo: un errore diverso da quello misurato — un privilegio, un
        // timeout — rendeva la sonda verde, perche "l'indice non c'e" era
        // indistinguibile da "il server lo ha rifiutato come sappiamo".
        for (what, expectation, observed, expected) in [
            (
                "accettata ma rifiutata",
                ExpressionIndexDdl::Accepted,
                Err(Some(1_142)),
                "doveva essere accettata",
            ),
            (
                "accettata ma rifiutata senza codice",
                ExpressionIndexDdl::Accepted,
                Err(None),
                "senza codice del server",
            ),
            (
                "rifiutata con un altro codice",
                ExpressionIndexDdl::Refused(1_064),
                Err(Some(1_142)),
                "osservato 1142",
            ),
            (
                "rifiutata senza codice",
                ExpressionIndexDdl::Refused(1_064),
                Err(None),
                "non porta un codice del server",
            ),
            (
                "rifiutata ma passata",
                ExpressionIndexDdl::Refused(1_064),
                Ok(()),
                "ed e passata",
            ),
        ] {
            let reported = expectation
                .mismatch(observed)
                .unwrap_or_else(|| panic!("{what}: scambiato per l'esito atteso"));
            assert!(
                reported.contains(expected),
                "{what}: il verdetto non dice cosa non torna — {reported}"
            );
        }
    }

    fn generated_description() -> crate::MysqlObjectDescription {
        let column = |name: &str, generation: &str| crate::MysqlColumn {
            name: name.to_owned(),
            ordinal: 1,
            data_type: "varchar".to_owned(),
            native_declaration: "varchar(32)".to_owned(),
            nullable: true,
            default_expression: None,
            character_set: None,
            collation: None,
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
            spatial_srid: None,
            extra: String::new(),
            generation_expression: generation.to_owned(),
        };
        crate::MysqlObjectDescription {
            schema: "dataflow_test".to_owned(),
            name: "generata".to_owned(),
            kind: "BASE TABLE".to_owned(),
            engine: Some("InnoDB".to_owned()),
            columns: vec![column("name", ""), column("lname", "lower(`name`)")],
            indexes: vec![crate::MysqlIndex {
                name: "uq_lname".to_owned(),
                unique: true,
                column_backed: true,
                columns: vec!["lname".to_owned()],
            }],
            token: crate::MysqlSchemaToken(String::new()),
        }
    }

    #[test]
    fn the_generated_index_contract_is_verified_in_full() {
        assert_eq!(
            generated_index_mismatch(&generated_description(), "lname", "uq_lname"),
            None
        );

        // Cinque modi di perdere la forma, e ciascuno cambia una delle due
        // decisioni che da quella forma dipendono: se la colonna sia
        // scrivibile, e se l'indice sia confrontabile con le keys.
        let without = |mutate: fn(&mut crate::MysqlObjectDescription)| {
            let mut description = generated_description();
            mutate(&mut description);
            description
        };
        for (what, description, expected) in [
            (
                "colonna assente",
                without(|description| description.columns.retain(|column| column.name != "lname")),
                "non compare",
            ),
            (
                "indice assente",
                without(|description| description.indexes.clear()),
                "non compare",
            ),
            (
                "colonna non piu generata",
                without(|description| {
                    for column in &mut description.columns {
                        column.generation_expression.clear();
                    }
                }),
                "sarebbe scrivibile",
            ),
            (
                "indice su piu colonne",
                without(|description| {
                    description.indexes[0].columns.push("name".to_owned());
                }),
                "non e sulla sola colonna generata",
            ),
            (
                "indice non unico",
                without(|description| description.indexes[0].unique = false),
                "non risulta unico",
            ),
            (
                "indice non confrontabile",
                without(|description| description.indexes[0].column_backed = false),
                "non risulta confrontabile",
            ),
        ] {
            let reported = generated_index_mismatch(&description, "lname", "uq_lname")
                .unwrap_or_else(|| panic!("{what}: la forma perduta e passata per buona"));
            assert!(
                reported.contains(expected),
                "{what}: il verdetto non dice cosa manca — {reported}"
            );
        }
    }

    use plenora_database_core::plan::FilterExpression;
    use plenora_database_core::{ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition};

    #[test]
    fn the_qualified_filter_forms_are_the_thirteen_declared() {
        // L'elenco e la tabella devono coincidere nome per nome e in ordine.
        // Senza, togliere una voce dalla tabella lascerebbe la sonda aggregata
        // verde: cambierebbe solo la stringa di dettaglio, e nessuno dei tre
        // server avrebbe niente da dire.
        let observed: Vec<&str> = qualified_filter_forms()
            .iter()
            .map(|case| case.name)
            .collect();
        assert_eq!(observed, QUALIFIED_FILTER_FORMS);
        assert_eq!(observed.len(), 13, "le forme qualificate sono tredici");

        // E nessuna delle due forme che il renderer rifiuta compare qui: se
        // ci finissero, `filter` si aprirebbe su una superficie che il flag
        // non sostiene.
        for closed in ["like_case_insensitive", "spatial"] {
            assert!(!observed.contains(&closed), "{closed} non e qualificata");
        }

        // Ogni forma porta i parametri che lega, e nessun altro: il provider
        // rifiuta un bag con voci che il piano non usa.
        for case in qualified_filter_forms() {
            let bound = bound_parameters(&case.expression);
            let provided: Vec<String> = case
                .parameters
                .iter()
                .map(|(name, _)| name.clone())
                .collect();
            let mut expected = bound;
            expected.sort();
            expected.dedup();
            let mut observed = provided;
            observed.sort();
            assert_eq!(observed, expected, "parametri della forma {}", case.name);
        }
    }

    /// I nomi dei parametri che un'espressione lega, in profondita.
    fn bound_parameters(expression: &FilterExpression) -> Vec<String> {
        match expression {
            FilterExpression::And { args } | FilterExpression::Or { args } => {
                args.iter().flat_map(bound_parameters).collect()
            }
            FilterExpression::Eq { parameter, .. }
            | FilterExpression::Ne { parameter, .. }
            | FilterExpression::Lt { parameter, .. }
            | FilterExpression::Lte { parameter, .. }
            | FilterExpression::Gt { parameter, .. }
            | FilterExpression::Gte { parameter, .. }
            | FilterExpression::Like { parameter, .. } => vec![parameter.clone()],
            FilterExpression::In { parameters, .. } => parameters.clone(),
            FilterExpression::Between {
                lower_parameter,
                upper_parameter,
                ..
            } => vec![lower_parameter.clone(), upper_parameter.clone()],
            FilterExpression::IsNull { .. } | FilterExpression::IsNotNull { .. } => Vec::new(),
            FilterExpression::Spatial {
                geometry_parameter,
                distance_parameter,
                ..
            } => geometry_parameter
                .iter()
                .chain(distance_parameter)
                .cloned()
                .collect(),
        }
    }

    fn deliberate() -> RefusalContract {
        RefusalContract {
            category: ErrorCategory::Unsupported,
            phase: ErrorPhase::Prepare,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            message_contains: "filtro spatial richiede",
        }
    }

    fn refused() -> plenora_database_core::DatabaseError {
        plenora_database_core::DatabaseError {
            category: ErrorCategory::Unsupported,
            phase: ErrorPhase::Prepare,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider: None,
            execution_id: None,
            message: "filtro spatial richiede validazione WKB e SRID".to_owned(),
            diagnostics: None,
        }
    }

    #[test]
    fn the_deliberate_refusal_is_recognised() {
        assert_eq!(refusal_mismatch(&deliberate(), &refused()), None);
    }

    #[test]
    fn a_refusal_for_another_reason_is_not_the_one_expected() {
        // E il caso che conta: la sonda sul fail-close riceve un `Err` anche
        // quando la colonna non esiste o il parametro e del tipo sbagliato, e
        // senza questo confronto lo scambierebbe per la prova che cercava.
        for (what, error, expected) in [
            (
                "colonna inesistente",
                plenora_database_core::DatabaseError {
                    category: ErrorCategory::Schema,
                    message: "colonna non trovata".to_owned(),
                    ..refused()
                },
                "categoria attesa",
            ),
            (
                "rifiuto in lettura invece che in prepare",
                plenora_database_core::DatabaseError {
                    phase: ErrorPhase::Read,
                    ..refused()
                },
                "fase attesa",
            ),
            (
                "effetto remoto ignoto",
                plenora_database_core::DatabaseError {
                    remote_effect: RemoteEffect::Unknown,
                    ..refused()
                },
                "effetto remoto atteso",
            ),
            (
                "rifiuto dichiarato ritentabile",
                plenora_database_core::DatabaseError {
                    retry: RetryDisposition::Safe,
                    ..refused()
                },
                "retry atteso",
            ),
            (
                "altro rifiuto della stessa famiglia",
                plenora_database_core::DatabaseError {
                    message: "parametro spatial non valido".to_owned(),
                    ..refused()
                },
                "il messaggio non porta",
            ),
        ] {
            let reported = refusal_mismatch(&deliberate(), &error)
                .unwrap_or_else(|| panic!("{what}: scambiato per il rifiuto atteso"));
            assert!(
                reported.contains(expected),
                "{what}: il verdetto non dice cosa non torna — {reported}"
            );
        }
    }

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
