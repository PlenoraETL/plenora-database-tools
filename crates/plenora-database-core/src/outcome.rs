//! Esiti portabili delle scritture e informazioni necessarie al recupero.

use crate::plan::ProviderKind;
use crate::RemoteEffect;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteStatus {
    Committed,
    RolledBack,
    PartiallyCommitted,
    OutcomeUnknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowCounts {
    pub received: u64,
    pub confirmed: u64,
    pub inserted: Option<u64>,
    pub updated: Option<u64>,
    pub deleted: Option<u64>,
    pub failed: u64,
    pub skipped: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertainPhase {
    SessionReady,
    TransactionBegun,
    StagingPrepared,
    Writing,
    Finalizing,
    CommitRequested,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Recovery {
    pub last_certain_phase: CertainPhase,
    pub automatic_retry_allowed: bool,
    pub idempotency_key: Option<String>,
    pub staging_object: Option<String>,
    pub verification_action: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteOutcome {
    pub schema_version: u32,
    pub status: WriteStatus,
    pub execution_id: String,
    pub provider: ProviderKind,
    pub rows: RowCounts,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery: Option<Recovery>,
}

/// Le lunghezze massime che `contracts/v2/write-outcome.schema.json`
/// dichiara, in code point come `maxLength` di JSON Schema.
const MAX_EXECUTION_ID_CHARS: usize = 128;
const MAX_IDEMPOTENCY_KEY_CHARS: usize = 256;
const MAX_STAGING_OBJECT_CHARS: usize = 512;
const MAX_VERIFICATION_ACTION_CHARS: usize = 1024;

impl WriteOutcome {
    /// Verifica che l'esito stia dentro il contratto, e che sia coerente.
    ///
    /// Guardava soltanto recovery e contabilità: la major, la lunghezza di
    /// `execution_id` e quelle dei campi di recovery — tutte scritte nello
    /// schema — non le controllava nessuno. Un esito con `execution_id` vuoto
    /// o con un `verification_action` di diecimila caratteri passava di qui e
    /// arrivava al consumatore, che e l'unico posto dove si sarebbe scoperto.
    ///
    /// # Errors
    ///
    /// `InvalidPlan` per major non supportata, `execution_id` fuori dalle
    /// lunghezze del contratto, campi di recovery troppo lunghi, combinazioni
    /// di stato/recovery incoerenti o conteggi superiori alle righe ricevute.
    pub fn validate(&self) -> crate::Result<()> {
        if self.schema_version != 2 {
            return Err(crate::DatabaseError::invalid_plan(
                "esito di scrittura con schema_version non supportata",
            ));
        }
        // `minLength: 1` e `maxLength: 128` contano code point.
        let execution_id_chars = self.execution_id.chars().count();
        if execution_id_chars == 0 {
            return Err(crate::DatabaseError::invalid_plan(
                "esito di scrittura senza execution_id",
            ));
        }
        if execution_id_chars > MAX_EXECUTION_ID_CHARS {
            return Err(crate::DatabaseError::invalid_plan(
                "execution_id oltre la lunghezza del contratto",
            ));
        }
        if let Some(recovery) = &self.recovery {
            // I messaggi non riportano il campo: `staging_object` e un nome di
            // oggetto e `verification_action` una frase costruita su di esso.
            let too_long = [
                (&recovery.idempotency_key, MAX_IDEMPOTENCY_KEY_CHARS),
                (&recovery.staging_object, MAX_STAGING_OBJECT_CHARS),
                (&recovery.verification_action, MAX_VERIFICATION_ACTION_CHARS),
            ]
            .into_iter()
            .any(|(field, limit)| {
                field
                    .as_ref()
                    .is_some_and(|value| value.chars().count() > limit)
            });
            if too_long {
                return Err(crate::DatabaseError::invalid_plan(
                    "campo di recovery oltre la lunghezza del contratto",
                ));
            }
        }
        let uncertain = matches!(
            self.status,
            WriteStatus::PartiallyCommitted | WriteStatus::OutcomeUnknown
        );
        if uncertain != self.recovery.is_some() {
            return Err(crate::DatabaseError::invalid_plan(
                "recovery deve essere presente soltanto per outcome parziale o incerto",
            ));
        }
        if matches!(self.status, WriteStatus::OutcomeUnknown)
            && self
                .recovery
                .as_ref()
                .is_some_and(|recovery| recovery.automatic_retry_allowed)
        {
            return Err(crate::DatabaseError::invalid_plan(
                "un outcome ignoto richiede recovery prima del retry",
            ));
        }
        if matches!(
            self.status,
            WriteStatus::RolledBack | WriteStatus::OutcomeUnknown
        ) && self.rows.confirmed != 0
        {
            return Err(crate::DatabaseError::invalid_plan(
                "un outcome rolled_back o unknown non può confermare righe",
            ));
        }
        let accounted = self
            .rows
            .confirmed
            .checked_add(self.rows.failed)
            .and_then(|value| value.checked_add(self.rows.skipped))
            .ok_or_else(|| {
                crate::DatabaseError::invalid_plan("overflow nella contabilità delle righe")
            })?;
        if accounted > self.rows.received {
            return Err(crate::DatabaseError::invalid_plan(
                "i conteggi di write superano le righe ricevute",
            ));
        }
        let mutations = [self.rows.inserted, self.rows.updated, self.rows.deleted]
            .into_iter()
            .flatten()
            .try_fold(0_u64, u64::checked_add)
            .ok_or_else(|| {
                crate::DatabaseError::invalid_plan("overflow nei conteggi delle mutazioni")
            })?;
        if mutations > self.rows.confirmed {
            return Err(crate::DatabaseError::invalid_plan(
                "le mutazioni confermate superano le righe confermate",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub const fn remote_effect(&self) -> RemoteEffect {
        match self.status {
            WriteStatus::Committed => RemoteEffect::Committed,
            WriteStatus::RolledBack => RemoteEffect::RolledBack,
            WriteStatus::PartiallyCommitted => RemoteEffect::Partial,
            WriteStatus::OutcomeUnknown => RemoteEffect::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_unknown_outcome_example() {
        let input = include_str!("../../../contracts/v2/examples/outcome-unknown.json");
        let outcome: WriteOutcome = serde_json::from_str(input).expect("outcome example");
        outcome.validate().expect("valid outcome");
        assert_eq!(outcome.schema_version, 2);
        assert_eq!(outcome.status, WriteStatus::OutcomeUnknown);
        assert_eq!(
            outcome
                .recovery
                .as_ref()
                .expect("recovery")
                .last_certain_phase,
            CertainPhase::CommitRequested
        );
        assert_eq!(outcome.remote_effect(), RemoteEffect::Unknown);
    }

    #[test]
    fn row_accounting_overflow_is_rejected_without_panicking() {
        let outcome = WriteOutcome {
            schema_version: 2,
            status: WriteStatus::Committed,
            execution_id: "overflow".to_owned(),
            provider: ProviderKind::Postgres,
            rows: RowCounts {
                received: u64::MAX,
                confirmed: u64::MAX,
                inserted: None,
                updated: None,
                deleted: None,
                failed: 1,
                skipped: 0,
            },
            recovery: None,
        };
        assert!(outcome.validate().is_err());
    }

    /// Un esito valido, da deformare un campo alla volta.
    fn committed_outcome_for_boundaries() -> WriteOutcome {
        WriteOutcome {
            schema_version: 2,
            status: WriteStatus::Committed,
            execution_id: "01J3BOUNDARY".to_owned(),
            provider: ProviderKind::Postgres,
            rows: RowCounts {
                received: 10,
                confirmed: 10,
                inserted: Some(10),
                updated: Some(0),
                deleted: Some(0),
                failed: 0,
                skipped: 0,
            },
            recovery: None,
        }
    }

    #[test]
    fn the_execution_id_is_measured_in_code_points_at_its_boundaries() {
        let mut outcome = committed_outcome_for_boundaries();

        outcome.execution_id = String::new();
        outcome
            .validate()
            .expect_err("`minLength: 1` esclude il vuoto");

        outcome.execution_id = "x".to_owned();
        outcome.validate().expect("un carattere e dentro");

        // Il caso che distingue davvero i due conteggi: 128 `e` accentate sono
        // 128 code point e 256 byte, quindi ammesse dal contratto e sul filo
        // di un controllo in byte. Il caso che c'era prima — 128 coppie
        // base + combinante — non distingueva nulla: 256 code point sono fuori
        // in entrambi i modi, e una regressione da `chars().count()` a `len()`
        // sarebbe passata inosservata.
        outcome.execution_id = "\u{e9}".repeat(128);
        assert_eq!(outcome.execution_id.chars().count(), 128);
        assert_eq!(outcome.execution_id.len(), 256);
        outcome
            .validate()
            .expect("128 code point sono dentro, per quanti byte pesino");

        outcome.execution_id = "\u{e9}".repeat(129);
        assert_eq!(outcome.execution_id.chars().count(), 129);
        outcome
            .validate()
            .expect_err("129 code point sono fuori, accentati o no");

        outcome.execution_id = "a".repeat(128);
        outcome.validate().expect("128 e il massimo dichiarato");

        outcome.execution_id = "a".repeat(129);
        outcome.validate().expect_err("129 e oltre");
    }

    #[test]
    fn the_recovery_fields_are_measured_at_their_boundaries() {
        let mut outcome = committed_outcome_for_boundaries();
        outcome.status = WriteStatus::OutcomeUnknown;
        outcome.rows.confirmed = 0;
        outcome.rows.inserted = None;
        outcome.rows.updated = None;
        outcome.rows.deleted = None;

        for (limit, set) in [(256_usize, 0_usize), (512, 1), (1024, 2)] {
            let build = |length: usize| {
                let filler = "a".repeat(length);
                Recovery {
                    last_certain_phase: CertainPhase::CommitRequested,
                    automatic_retry_allowed: false,
                    idempotency_key: (set == 0).then(|| filler.clone()),
                    staging_object: (set == 1).then(|| filler.clone()),
                    verification_action: (set == 2).then_some(filler),
                }
            };

            outcome.recovery = Some(build(limit));
            outcome
                .validate()
                .unwrap_or_else(|_| panic!("il campo {set} accetta {limit} caratteri"));

            outcome.recovery = Some(build(limit + 1));
            assert!(
                outcome.validate().is_err(),
                "il campo {set} deve rifiutare {} caratteri",
                limit + 1
            );
        }
    }

    #[test]
    fn only_the_declared_major_is_consumed() {
        let mut outcome = committed_outcome_for_boundaries();
        outcome.schema_version = 3;
        outcome
            .validate()
            .expect_err("il contratto fissa la major con un const");
    }

    /// Un conteggio oltre `u64` sta nel contratto e non in questo lettore.
    ///
    /// Lo schema dichiara `minimum: 0` e nessun massimo: il dominio JSON degli
    /// interi non e limitato a `u64`, e `RowCounts` lo e. Il rifiuto arriva
    /// dalla deserializzazione, prima di qualunque validatore, ed e lo stesso
    /// confine gia fissato per il documento capability e per il piano.
    #[test]
    fn row_counts_beyond_u64_are_within_the_contract_and_outside_this_reader() {
        let input =
            include_str!("../../../contracts/v2/examples/unconsumable-outcome-rows-over-u64.json");
        serde_json::from_str::<WriteOutcome>(input)
            .expect_err("un conteggio oltre u64 non e rappresentabile qui");
    }

    /// I documenti `unconsumable-outcome-*.json` sono **validi per lo schema
    /// v2** e rifiutati da questo consumatore.
    ///
    /// La divergenza e voluta. Lo schema v2 e pubblicato: descrive la forma
    /// dei documenti, non la loro coerenza contabile, e restringerlo dopo la
    /// pubblicazione romperebbe i produttori che oggi lo rispettano — una
    /// modifica incompatibile chiede una major, non una patch. Le relazioni
    /// che il contratto non enuncia vivono percio qui, dove il documento
    /// viene consumato, e questi tre file sono la prova che continuano a
    /// mordere. Il giorno in cui esistera una v3, sono i suoi candidati.
    #[test]
    fn documents_the_contract_accepts_can_still_be_unconsumable() {
        for input in [
            include_str!(
                "../../../contracts/v2/examples/unconsumable-outcome-unknown-auto-retry.json"
            ),
            include_str!(
                "../../../contracts/v2/examples/unconsumable-outcome-rolled-back-confirmed.json"
            ),
            include_str!(
                "../../../contracts/v2/examples/unconsumable-outcome-unknown-mutations.json"
            ),
        ] {
            let outcome: WriteOutcome =
                serde_json::from_str(input).expect("documento conforme allo schema v2");
            assert!(
                outcome.validate().is_err(),
                "esito incoerente accettato: {}",
                outcome.execution_id
            );
        }
    }

    #[test]
    fn unknown_outcome_cannot_authorize_automatic_retry() {
        let mut outcome: WriteOutcome = serde_json::from_str(include_str!(
            "../../../contracts/v2/examples/outcome-unknown.json"
        ))
        .expect("outcome example");
        outcome
            .recovery
            .as_mut()
            .expect("unknown recovery")
            .automatic_retry_allowed = true;
        assert!(outcome.validate().is_err());
    }
}
