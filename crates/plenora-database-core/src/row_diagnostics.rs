//! Diagnostica row-scoped `plenora-row-diagnostics-v1`.
//!
//! Il contratto pubblica quali righe sorgente sono state rifiutate, con quale
//! causa e con quale effetto remoto certo. L'indice è sempre quello della riga
//! nella sorgente, zero-based, anche quando la scrittura attraversa più batch.
//!
//! Il documento non trasporta valori di riga: la chiave configurata viene
//! pubblicata redatta e i messaggi restano privi di payload. Le quantità sono
//! sempre partizionate fra ciò che è certo e ciò che resta ignoto, così che un
//! rollback non confermato non venga letto come un annullamento.

use crate::plan::ProviderKind;
use crate::{DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;

/// Identificatore del contratto pubblicato in ogni documento.
pub const CONTRACT: &str = "plenora-row-diagnostics-v1";

/// Unica base di indicizzazione ammessa dal contratto.
pub const INDEX_BASIS: &str = "source_row_zero_based";

/// Numero di esempi pubblicati quando la configurazione non ne impone altri.
pub const DEFAULT_EXAMPLES_LIMIT: u32 = 10;

/// Causa di rifiuto per una violazione di vincolo dichiarata dal database.
pub const CAUSE_CONSTRAINT_VIOLATION: &str = "database.constraint_violation";

/// Causa di rifiuto per un valore che non entra nel contratto Arrow dichiarato.
///
/// È la sola classificazione che il percorso di lettura può provare: il codice
/// di categoria distingue un difetto di conversione da un limite di risorsa o
/// da un errore di protocollo, mentre la ragione precisa vive solo nel testo
/// vendor e non è quindi una fonte ammessa.
pub const CAUSE_VALUE_NOT_REPRESENTABLE: &str = "conversion.value_not_representable";

/// Limite di conoscenza: la scansione si è fermata al primo difetto osservato.
pub const LIMIT_SCAN_STOPPED_AT_FIRST_DEFECT: &str = "read.scan_stopped_at_first_defect";

/// Limite di conoscenza: i batch già consegnati al consumatore non sono più
/// ispezionabili e non possono essere dichiarati privi di difetti.
pub const LIMIT_BATCHES_ALREADY_PUBLISHED: &str = "read.batches_already_published";

/// Limite di conoscenza: la riga sorgente del difetto non è attribuibile.
pub const LIMIT_ROW_ATTRIBUTION_UNAVAILABLE: &str = "read.row_attribution_unavailable";

/// Calcola il primo indice successivo a un batch senza permettere wraparound.
///
/// I provider mantengono il proprio envelope d'errore, ma condividono qui
/// l'aritmetica che definisce la base assoluta delle righe sorgente.
#[must_use]
pub const fn checked_source_row_end(batch_start: u64, rows: u64) -> Option<u64> {
    batch_start.checked_add(rows)
}

/// Valore massimo intero rappresentabile senza perdita dal contratto JSON.
const MAX_EXACT_INTEGER: i64 = 9_007_199_254_740_991;

const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_FIELD_CHARS: usize = 256;
const MAX_KEY_TEXT_CHARS: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticScope {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Completeness {
    Complete,
    Partial,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyState {
    Value,
    Redacted,
    Unavailable,
}

/// Valore di chiave pubblicabile in chiaro solo da una policy esplicita.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KeyValue {
    Text(String),
    Integer(i64),
    Boolean(bool),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowKey {
    pub field: String,
    pub state: KeyState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<KeyValue>,
}

impl RowKey {
    /// Chiave pubblicata senza valore: il campo resta identificabile, il
    /// contenuto della riga no.
    #[must_use]
    pub fn redacted(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            state: KeyState::Redacted,
            value: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteState {
    CertainlyRejected,
    CertainlyNotAttempted,
    CertainlyRolledBack,
    EffectUnknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowExample {
    pub source_index: u64,
    pub cause: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<RowKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_state: Option<WriteState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticStateCounts {
    pub certainly_rejected: u64,
    pub certainly_not_attempted: u64,
    pub certainly_rolled_back: u64,
    pub effect_unknown: u64,
}

impl DiagnosticStateCounts {
    fn checked_sum(&self) -> Option<u64> {
        self.certainly_rejected
            .checked_add(self.certainly_not_attempted)?
            .checked_add(self.certainly_rolled_back)?
            .checked_add(self.effect_unknown)
    }

    const fn state(&self, state: WriteState) -> u64 {
        match state {
            WriteState::CertainlyRejected => self.certainly_rejected,
            WriteState::CertainlyNotAttempted => self.certainly_not_attempted,
            WriteState::CertainlyRolledBack => self.certainly_rolled_back,
            WriteState::EffectUnknown => self.effect_unknown,
        }
    }
}

/// Conteggio che dichiara esplicitamente quando la quantità non è nota.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum PartitionCount {
    Known { value: u64 },
    Unknown,
}

impl PartitionCount {
    #[must_use]
    pub const fn known(&self) -> Option<u64> {
        match self {
            Self::Known { value } => Some(*value),
            Self::Unknown => None,
        }
    }
}

/// Partizione dell'input fra esiti certi e ignoti.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteOutcomePartition {
    pub certainly_rejected: PartitionCount,
    pub certainly_not_attempted: PartitionCount,
    pub certainly_rolled_back: PartitionCount,
    pub effect_unknown: PartitionCount,
}

impl WriteOutcomePartition {
    const fn bucket(&self, state: WriteState) -> PartitionCount {
        match state {
            WriteState::CertainlyRejected => self.certainly_rejected,
            WriteState::CertainlyNotAttempted => self.certainly_not_attempted,
            WriteState::CertainlyRolledBack => self.certainly_rolled_back,
            WriteState::EffectUnknown => self.effect_unknown,
        }
    }
}

const WRITE_STATES: [WriteState; 4] = [
    WriteState::CertainlyRejected,
    WriteState::CertainlyNotAttempted,
    WriteState::CertainlyRolledBack,
    WriteState::EffectUnknown,
];

/// Documento `plenora-row-diagnostics-v1`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowDiagnostics {
    pub contract: String,
    pub scope: DiagnosticScope,
    pub index_basis: String,
    pub completeness: Completeness,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub knowledge_limits: Vec<String>,
    pub observed_total: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_total: Option<u64>,
    pub counts: BTreeMap<String, u64>,
    pub examples_limit: u32,
    pub examples_truncated: bool,
    pub examples: Vec<RowExample>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic_state_counts: Option<DiagnosticStateCounts>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_outcome: Option<WriteOutcomePartition>,
}

impl Serialize for RowDiagnostics {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.validate().map_err(|_| {
            serde::ser::Error::custom("diagnostica row-scoped non valida per la pubblicazione")
        })?;
        let optional_fields = usize::from(!self.knowledge_limits.is_empty())
            + usize::from(self.total.is_some())
            + usize::from(self.input_total.is_some())
            + usize::from(self.diagnostic_state_counts.is_some())
            + usize::from(self.write_outcome.is_some());
        let mut state = serializer.serialize_struct("RowDiagnostics", 9 + optional_fields)?;
        state.serialize_field("contract", &self.contract)?;
        state.serialize_field("scope", &self.scope)?;
        state.serialize_field("index_basis", &self.index_basis)?;
        state.serialize_field("completeness", &self.completeness)?;
        if !self.knowledge_limits.is_empty() {
            state.serialize_field("knowledge_limits", &self.knowledge_limits)?;
        }
        state.serialize_field("observed_total", &self.observed_total)?;
        if let Some(total) = self.total {
            state.serialize_field("total", &total)?;
        }
        if let Some(input_total) = self.input_total {
            state.serialize_field("input_total", &input_total)?;
        }
        state.serialize_field("counts", &self.counts)?;
        state.serialize_field("examples_limit", &self.examples_limit)?;
        state.serialize_field("examples_truncated", &self.examples_truncated)?;
        state.serialize_field("examples", &self.examples)?;
        if let Some(counts) = self.diagnostic_state_counts {
            state.serialize_field("diagnostic_state_counts", &counts)?;
        }
        if let Some(outcome) = self.write_outcome {
            state.serialize_field("write_outcome", &outcome)?;
        }
        state.end()
    }
}

impl RowDiagnostics {
    /// Verifica forma e aritmetica del documento.
    ///
    /// Le regole sono quelle dello schema `row-diagnostics-v1` più le
    /// invarianti aritmetiche fra conteggi, esempi e partizione degli esiti:
    /// un documento che non le supera non è pubblicabile.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` alla prima invariante violata.
    pub fn validate(&self) -> Result<()> {
        if self.contract != CONTRACT {
            return Err(invalid("contratto row diagnostics diverso da v1"));
        }
        if self.index_basis != INDEX_BASIS {
            return Err(invalid("base di indicizzazione row diagnostics non valida"));
        }
        self.validate_completeness()?;
        self.validate_counts()?;
        self.validate_examples()?;
        self.validate_write_partition()
    }

    /// Serializza il documento soltanto dopo averlo validato.
    ///
    /// La pubblicazione è fail-closed: un documento che non supera le
    /// invarianti non raggiunge mai un sink, e un fallimento di codifica
    /// diventa un errore invece di un JSON parziale.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` se le invarianti non tornano e `Internal` se
    /// la codifica JSON fallisce.
    pub fn to_json(&self) -> Result<String> {
        self.validate()?;
        serde_json::to_string(self).map_err(|_| {
            DatabaseError::new(
                ErrorCategory::Internal,
                ErrorPhase::Finalize,
                None,
                "codifica JSON della diagnostica row-scoped fallita",
            )
        })
    }

    fn validate_completeness(&self) -> Result<()> {
        match self.completeness {
            Completeness::Complete => {
                if !self.knowledge_limits.is_empty() {
                    return Err(invalid(
                        "un documento completo non dichiara limiti di conoscenza",
                    ));
                }
                if self.total != Some(self.observed_total) {
                    return Err(invalid(
                        "un documento completo deve dichiarare total pari a observed_total",
                    ));
                }
            }
            Completeness::Partial | Completeness::Unknown => {
                if self.knowledge_limits.is_empty() {
                    return Err(invalid(
                        "un documento non completo deve dichiarare i limiti di conoscenza",
                    ));
                }
                let mut seen = BTreeSet::new();
                for limit in &self.knowledge_limits {
                    if !is_contract_identifier(limit) || !seen.insert(limit.as_str()) {
                        return Err(invalid("limite di conoscenza non valido o ripetuto"));
                    }
                }
                if self.completeness == Completeness::Unknown && self.total.is_some() {
                    return Err(invalid(
                        "un totale non è dichiarabile quando la completezza è ignota",
                    ));
                }
            }
        }
        if let Some(total) = self.total {
            if total == 0 || total < self.observed_total {
                return Err(invalid(
                    "total row diagnostics incoerente con observed_total",
                ));
            }
        }
        Ok(())
    }

    fn validate_counts(&self) -> Result<()> {
        let mut sum = 0_u64;
        for (cause, count) in &self.counts {
            if !is_contract_identifier(cause) {
                return Err(invalid("causa row diagnostics non valida"));
            }
            if *count == 0 {
                return Err(invalid("una causa osservata non può contare zero righe"));
            }
            sum = sum
                .checked_add(*count)
                .ok_or_else(|| invalid("overflow nella somma delle cause osservate"))?;
        }
        if sum != self.observed_total {
            return Err(invalid(
                "la somma delle cause non corrisponde a observed_total",
            ));
        }
        Ok(())
    }

    fn validate_examples(&self) -> Result<()> {
        if self.examples_limit == 0 {
            return Err(invalid("il limite di esempi deve essere almeno uno"));
        }
        let limit = u64::from(self.examples_limit);
        let published = u64::try_from(self.examples.len())
            .map_err(|_| invalid("numero di esempi non rappresentabile"))?;
        if published > limit || published > self.observed_total {
            return Err(invalid(
                "gli esempi superano il limite o le righe osservate",
            ));
        }
        if self.completeness == Completeness::Complete
            && published != self.observed_total.min(limit)
        {
            return Err(invalid(
                "un documento completo deve pubblicare tutti gli esempi ammessi dal limite",
            ));
        }
        if self.examples_truncated != (self.observed_total > published && published == limit) {
            return Err(invalid(
                "examples_truncated è vero soltanto quando il limite omette righe osservate",
            ));
        }

        let mut indices = BTreeSet::new();
        let mut per_cause: BTreeMap<&str, u64> = BTreeMap::new();
        for example in &self.examples {
            if !indices.insert(example.source_index) {
                return Err(invalid("indice sorgente ripetuto fra gli esempi"));
            }
            if !self.counts.contains_key(&example.cause) {
                return Err(invalid("causa di un esempio assente dai conteggi"));
            }
            validate_optional_field(example.column.as_deref(), "colonna")?;
            validate_key(example.key.as_ref())?;
            match (self.scope, example.write_state) {
                (DiagnosticScope::Write, Some(_)) | (DiagnosticScope::Read, None) => {}
                (DiagnosticScope::Write, None) => {
                    return Err(invalid(
                        "un esempio di scrittura deve dichiarare write_state",
                    ));
                }
                (DiagnosticScope::Read, Some(_)) => {
                    return Err(invalid("un esempio di lettura non dichiara write_state"));
                }
            }
            let counter = per_cause.entry(example.cause.as_str()).or_default();
            *counter = counter
                .checked_add(1)
                .ok_or_else(|| invalid("overflow nel conteggio degli esempi per causa"))?;
        }
        for (cause, published) in per_cause {
            if self
                .counts
                .get(cause)
                .is_none_or(|total| published > *total)
            {
                return Err(invalid("gli esempi superano il conteggio della causa"));
            }
        }
        Ok(())
    }

    fn validate_write_partition(&self) -> Result<()> {
        let (input_total, states, partition) = match self.scope {
            DiagnosticScope::Read => {
                if self.input_total.is_some()
                    || self.diagnostic_state_counts.is_some()
                    || self.write_outcome.is_some()
                {
                    return Err(invalid(
                        "un documento di lettura non dichiara campi di scrittura",
                    ));
                }
                return Ok(());
            }
            DiagnosticScope::Write => {
                let input_total = self
                    .input_total
                    .ok_or_else(|| invalid("un documento di scrittura dichiara input_total"))?;
                let states = self.diagnostic_state_counts.ok_or_else(|| {
                    invalid("un documento di scrittura dichiara diagnostic_state_counts")
                })?;
                let partition = self
                    .write_outcome
                    .ok_or_else(|| invalid("un documento di scrittura dichiara write_outcome"))?;
                (input_total, states, partition)
            }
        };
        if input_total == 0 || self.observed_total > input_total {
            return Err(invalid("input_total incoerente con le righe osservate"));
        }
        if states.checked_sum() != Some(self.observed_total) {
            return Err(invalid(
                "la somma degli stati diagnostici non corrisponde a observed_total",
            ));
        }

        let mut example_states = [0_u64; WRITE_STATES.len()];
        for example in &self.examples {
            let state = example
                .write_state
                .ok_or_else(|| invalid("un esempio di scrittura deve dichiarare write_state"))?;
            let slot = WRITE_STATES
                .iter()
                .position(|candidate| *candidate == state)
                .ok_or_else(|| invalid("stato di scrittura sconosciuto"))?;
            example_states[slot] = example_states[slot]
                .checked_add(1)
                .ok_or_else(|| invalid("overflow nel conteggio degli esempi per stato"))?;
        }

        let mut known_sum = 0_u64;
        let mut diagnosed_unknown = 0_u64;
        for (slot, state) in WRITE_STATES.iter().enumerate() {
            let diagnosed = states.state(*state);
            match partition.bucket(*state).known() {
                Some(value) => {
                    if diagnosed > value {
                        return Err(invalid(
                            "uno stato diagnostico supera il proprio esito certo",
                        ));
                    }
                    known_sum = known_sum
                        .checked_add(value)
                        .ok_or_else(|| invalid("overflow nella partizione degli esiti certi"))?;
                }
                None => {
                    diagnosed_unknown = diagnosed_unknown
                        .checked_add(diagnosed)
                        .ok_or_else(|| invalid("overflow nella partizione degli esiti ignoti"))?;
                }
            }
            if example_states[slot] > diagnosed {
                return Err(invalid("gli esempi superano il proprio stato diagnostico"));
            }
        }
        if known_sum > input_total {
            return Err(invalid("gli esiti certi superano input_total"));
        }
        if known_sum
            .checked_add(diagnosed_unknown)
            .is_none_or(|total| total > input_total)
        {
            return Err(invalid(
                "esiti certi e righe diagnosticate ignote superano input_total",
            ));
        }
        if WRITE_STATES
            .iter()
            .all(|state| partition.bucket(*state).known().is_some())
            && known_sum != input_total
        {
            return Err(invalid(
                "una partizione interamente certa deve coprire input_total",
            ));
        }
        Ok(())
    }
}

fn validate_optional_field(value: Option<&str>, what: &str) -> Result<()> {
    match value {
        None => Ok(()),
        Some(value) if !value.is_empty() && value.chars().count() <= MAX_FIELD_CHARS => Ok(()),
        Some(_) => Err(invalid(match what {
            "colonna" => "colonna di un esempio fuori dai limiti del contratto",
            _ => "campo chiave fuori dai limiti del contratto",
        })),
    }
}

fn validate_key(key: Option<&RowKey>) -> Result<()> {
    let Some(key) = key else {
        return Ok(());
    };
    validate_optional_field(Some(key.field.as_str()), "chiave")?;
    match (key.state, key.value.as_ref()) {
        (KeyState::Value, Some(value)) => validate_key_value(value),
        (KeyState::Value, None) => Err(invalid("chiave dichiarata in chiaro senza valore")),
        (KeyState::Redacted | KeyState::Unavailable, None) => Ok(()),
        (KeyState::Redacted | KeyState::Unavailable, Some(_)) => Err(invalid(
            "una chiave redatta o non disponibile non trasporta valore",
        )),
    }
}

fn validate_key_value(value: &KeyValue) -> Result<()> {
    let representable = match value {
        KeyValue::Text(text) => text.chars().count() <= MAX_KEY_TEXT_CHARS,
        // `i64::MIN.abs()` andrebbe in overflow: il confronto resta su un
        // intervallo chiuso, così nessun valore di chiave può far panicare la
        // validazione.
        KeyValue::Integer(number) => (-MAX_EXACT_INTEGER..=MAX_EXACT_INTEGER).contains(number),
        KeyValue::Boolean(_) => true,
    };
    if representable {
        Ok(())
    } else {
        Err(invalid("valore di chiave fuori dai limiti del contratto"))
    }
}

/// Verifica il pattern degli identificatori del contratto.
fn is_contract_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_IDENTIFIER_BYTES || !bytes[0].is_ascii_lowercase() {
        return false;
    }
    let mut after_separator = false;
    for byte in &bytes[1..] {
        match byte {
            b'a'..=b'z' | b'0'..=b'9' => after_separator = false,
            b'.' | b'_' | b'-' if !after_separator => after_separator = true,
            _ => return false,
        }
    }
    !after_separator
}

/// Politica di pubblicazione degli esempi dichiarata dalla sorgente.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDiagnosticsPolicy {
    /// Campo chiave della sorgente, pubblicato sempre redatto in scrittura.
    pub key_field: Option<String>,
    /// Colonna associata al vincolo nel contratto dichiarato dalla sorgente.
    ///
    /// Il provider deve verificarne almeno l'esistenza nello schema preparato
    /// prima della transazione. Il messaggio vendor non è una fonte ammessa.
    pub constraint_column: Option<String>,
    /// Numero massimo di esempi pubblicati.
    pub examples_limit: u32,
}

impl Default for RowDiagnosticsPolicy {
    fn default() -> Self {
        Self {
            key_field: None,
            constraint_column: None,
            examples_limit: DEFAULT_EXAMPLES_LIMIT,
        }
    }
}

/// Evidenza raccolta sull'annullamento della transazione.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollbackEvidence {
    /// Il server ha confermato il ROLLBACK.
    Confirmed,
    /// L'acknowledgement del ROLLBACK non è stato osservato.
    Lost,
}

/// Riga sorgente rifiutata e identificata in modo esatto.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedRow {
    pub source_index: u64,
    pub cause: String,
    pub column: Option<String>,
}

/// Contabilità row-scoped di una scrittura a transazione singola.
///
/// Il tracker conosce soltanto quante righe sorgente sono state messe in scena
/// nella transazione aperta: l'indice della riga rifiutata deve arrivare da una
/// prova per riga, mai da un messaggio vendor o da una chiave.
#[derive(Debug, Clone)]
pub struct WriteDiagnosticsTracker {
    input_total: u64,
    policy: RowDiagnosticsPolicy,
    staged_rows: u64,
}

impl WriteDiagnosticsTracker {
    /// Apre la contabilità su un input di dimensione dichiarata.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` per un input vuoto o per un limite di esempi
    /// nullo, che il contratto non ammette.
    pub fn new(input_total: u64, policy: RowDiagnosticsPolicy) -> Result<Self> {
        if input_total == 0 {
            return Err(invalid(
                "la diagnostica di scrittura richiede un input dichiarato non vuoto",
            ));
        }
        if policy.examples_limit == 0 {
            return Err(invalid("il limite di esempi deve essere almeno uno"));
        }
        validate_optional_field(policy.key_field.as_deref(), "chiave")?;
        validate_optional_field(policy.constraint_column.as_deref(), "colonna")?;
        Ok(Self {
            input_total,
            policy,
            staged_rows: 0,
        })
    }

    #[must_use]
    pub const fn input_total(&self) -> u64 {
        self.input_total
    }

    /// Indice sorgente della prossima riga non ancora messa in scena.
    ///
    /// È l'offset che attraversa i batch: nessun indice viene ricostruito a
    /// posteriori dalla posizione dentro un batch.
    #[must_use]
    pub const fn staged_rows(&self) -> u64 {
        self.staged_rows
    }

    /// Registra righe applicate nella transazione aperta.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` se il conteggio esce dall'input dichiarato.
    pub fn stage_rows(&mut self, rows: u64) -> Result<()> {
        let staged = self
            .staged_rows
            .checked_add(rows)
            .ok_or_else(|| invalid("overflow nel conteggio delle righe messe in scena"))?;
        if staged > self.input_total {
            return Err(invalid(
                "le righe messe in scena superano l'input dichiarato",
            ));
        }
        self.staged_rows = staged;
        Ok(())
    }

    /// Chiude la diagnosi sulla prima riga sorgente rifiutata.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` se l'indice non è quello della prima riga non
    /// ancora messa in scena: un indice diverso non è stato provato per riga.
    pub fn reject_row(
        &self,
        rejected: &RejectedRow,
        rollback: RollbackEvidence,
    ) -> Result<RowDiagnostics> {
        if rejected.source_index != self.staged_rows {
            return Err(invalid(
                "l'indice rifiutato non è la prima riga non ancora applicata",
            ));
        }
        if !is_contract_identifier(&rejected.cause) {
            return Err(invalid("causa row diagnostics non valida"));
        }
        let not_attempted = self
            .input_total
            .checked_sub(self.staged_rows)
            .and_then(|remaining| remaining.checked_sub(1))
            .ok_or_else(|| invalid("la riga rifiutata è fuori dall'input dichiarato"))?;

        let (rolled_back, effect_unknown) = match rollback {
            RollbackEvidence::Confirmed => (
                PartitionCount::Known {
                    value: self.staged_rows,
                },
                PartitionCount::Known { value: 0 },
            ),
            RollbackEvidence::Lost => (PartitionCount::Unknown, PartitionCount::Unknown),
        };

        let report = RowDiagnostics {
            contract: CONTRACT.to_owned(),
            scope: DiagnosticScope::Write,
            index_basis: INDEX_BASIS.to_owned(),
            completeness: Completeness::Complete,
            knowledge_limits: Vec::new(),
            observed_total: 1,
            total: Some(1),
            input_total: Some(self.input_total),
            counts: BTreeMap::from([(rejected.cause.clone(), 1)]),
            examples_limit: self.policy.examples_limit,
            examples_truncated: false,
            examples: vec![RowExample {
                source_index: rejected.source_index,
                cause: rejected.cause.clone(),
                column: rejected.column.clone(),
                key: self.policy.key_field.as_ref().map(RowKey::redacted),
                write_state: Some(WriteState::CertainlyRejected),
            }],
            diagnostic_state_counts: Some(DiagnosticStateCounts {
                certainly_rejected: 1,
                certainly_not_attempted: 0,
                certainly_rolled_back: 0,
                effect_unknown: 0,
            }),
            write_outcome: Some(WriteOutcomePartition {
                certainly_rejected: PartitionCount::Known { value: 1 },
                certainly_not_attempted: PartitionCount::Known {
                    value: not_attempted,
                },
                certainly_rolled_back: rolled_back,
                effect_unknown,
            }),
        };
        report.validate()?;
        Ok(report)
    }
}

/// Futuro restituito dal seam row-scoped.
pub type RowWriteFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Rifiuto provato per una singola riga sorgente.
///
/// La causa arriva da una classificazione strutturata dell'errore remoto — un
/// codice, non un messaggio — e la colonna solo da ciò che il piano dichiara.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowRejection {
    pub cause: String,
    pub column: Option<String>,
}

/// Esito certo dell'applicazione di una singola riga sorgente.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowApplication {
    /// Lo statement della riga è andato a buon fine.
    Applied,
    /// Lo statement della riga è stato rifiutato dal server.
    Rejected(RowRejection),
}

/// Seam minimo del percorso di scrittura diagnostico.
///
/// L'implementazione deve applicare **una riga sorgente per statement**: è la
/// sola prova che lega un rifiuto a un indice sorgente. Un batch multi-riga
/// non permette l'attribuzione e non appartiene a questo percorso.
pub trait RowScopedWriter: Send {
    /// Applica la riga sorgente all'indice assoluto indicato.
    fn apply_row(&mut self, source_index: u64) -> RowWriteFuture<'_, Result<RowApplication>>;

    /// Verifica che non esistano righe oltre il totale dichiarato.
    ///
    /// La conferma è obbligatoria prima del commit: ignorare righe residue
    /// trasformerebbe una dichiarazione sorgente errata in un drop silenzioso.
    fn finish_declared_input(&mut self) -> RowWriteFuture<'_, Result<()>>;

    /// Annulla la transazione e riporta l'evidenza raccolta.
    ///
    /// Un annullamento non confermato deve restare `Lost`: dedurre un
    /// `Confirmed` è esattamente ciò che il contratto vieta.
    fn rollback(&mut self) -> RowWriteFuture<'_, RollbackEvidence>;
}

/// Diagnosi chiusa di una scrittura row-scoped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowRejectionOutcome {
    diagnostics: RowDiagnostics,
    rollback: RollbackEvidence,
}

impl RowRejectionOutcome {
    #[must_use]
    pub const fn diagnostics(&self) -> &RowDiagnostics {
        &self.diagnostics
    }

    #[must_use]
    pub const fn rollback(&self) -> RollbackEvidence {
        self.rollback
    }

    /// Assi d'errore certi corrispondenti all'evidenza raccolta.
    ///
    /// Un annullamento confermato è un effetto remoto annullato in fase di
    /// scrittura e non va ritentato; un acknowledgement perso lascia l'effetto
    /// ignoto in fase di annullamento e impone la quarantena della sessione.
    #[must_use]
    pub const fn axes(&self) -> (ErrorPhase, RemoteEffect, RetryDisposition) {
        match self.rollback {
            RollbackEvidence::Confirmed => (
                ErrorPhase::Write,
                RemoteEffect::RolledBack,
                RetryDisposition::Never,
            ),
            RollbackEvidence::Lost => (
                ErrorPhase::Rollback,
                RemoteEffect::Unknown,
                RetryDisposition::Quarantine,
            ),
        }
    }

    /// Costruisce l'errore che trasporta la diagnostica.
    ///
    /// Il messaggio non contiene valori di riga: l'indice sorgente e la causa
    /// vivono nel documento, non nel testo.
    ///
    /// # Errors
    ///
    /// Propaga `InvalidPlan` se il documento non supera la validazione.
    pub fn into_error(
        self,
        provider: Option<ProviderKind>,
        execution_id: Option<String>,
    ) -> Result<DatabaseError> {
        let (phase, remote_effect, retry) = self.axes();
        DatabaseError {
            category: ErrorCategory::DataMapping,
            phase,
            remote_effect,
            retry,
            provider,
            execution_id,
            message: "riga sorgente rifiutata dal database".to_owned(),
            diagnostics: None,
        }
        .with_row_diagnostics(self.diagnostics)
    }
}

/// Esegue la scrittura riga per riga e chiude la diagnosi al primo rifiuto.
///
/// Il tracker porta l'offset assoluto: l'indice provato è sempre quello della
/// riga nella sorgente, anche quando la scrittura ha già attraversato più
/// batch. Restituisce `None` quando l'intero input dichiarato è stato
/// applicato senza rifiuti.
///
/// # Errors
///
/// Propaga l'errore del seam e `InvalidPlan` se la contabilità non torna.
pub async fn diagnose_row_scoped_write<W>(
    writer: &mut W,
    tracker: &mut WriteDiagnosticsTracker,
) -> Result<Option<RowRejectionOutcome>>
where
    W: RowScopedWriter + ?Sized,
{
    while tracker.staged_rows() < tracker.input_total() {
        let source_index = tracker.staged_rows();
        match writer.apply_row(source_index).await? {
            RowApplication::Applied => tracker.stage_rows(1)?,
            RowApplication::Rejected(rejection) => {
                let rollback = writer.rollback().await;
                let rejected = RejectedRow {
                    source_index,
                    cause: rejection.cause,
                    column: rejection.column,
                };
                return tracker.reject_row(&rejected, rollback).map(|diagnostics| {
                    Some(RowRejectionOutcome {
                        diagnostics,
                        rollback,
                    })
                });
            }
        }
    }
    writer.finish_declared_input().await?;
    Ok(None)
}

/// Politica di pubblicazione della diagnostica row-scoped in lettura.
///
/// La lettura non conosce una cardinalità dichiarata dalla sorgente: la sola
/// configurazione che il chiamante può imporre è quale colonna identifica la
/// riga e quanti esempi pubblicare.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadDiagnosticsPolicy {
    /// Campo chiave del result set, pubblicato sempre redatto.
    pub key_field: Option<String>,
    /// Numero massimo di esempi pubblicati.
    pub examples_limit: u32,
}

impl Default for ReadDiagnosticsPolicy {
    fn default() -> Self {
        Self {
            key_field: None,
            examples_limit: DEFAULT_EXAMPLES_LIMIT,
        }
    }
}

/// Contabilità row-scoped di una lettura a batch.
///
/// Il tracker conosce soltanto quante righe sono già state consegnate al
/// consumatore. L'indice pubblicato è la somma fra quel cursore e la posizione
/// della riga difettosa dentro il batch in costruzione: nessun indice viene
/// ricostruito dopo la pubblicazione di un batch, e nessuna completezza viene
/// dichiarata su righe che non sono più ispezionabili.
#[derive(Debug, Clone)]
pub struct ReadDiagnosticsTracker {
    policy: ReadDiagnosticsPolicy,
    published_rows: u64,
}

impl Default for ReadDiagnosticsTracker {
    /// Contabilità con la politica di default, che il contratto ammette
    /// sempre: nessuna chiave configurata e il limite di esempi predefinito.
    fn default() -> Self {
        Self {
            policy: ReadDiagnosticsPolicy::default(),
            published_rows: 0,
        }
    }
}

impl ReadDiagnosticsTracker {
    /// Apre la contabilità di lettura.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` per un limite di esempi nullo o per un campo
    /// chiave fuori dai limiti del contratto.
    pub fn new(policy: ReadDiagnosticsPolicy) -> Result<Self> {
        if policy.examples_limit == 0 {
            return Err(invalid("il limite di esempi deve essere almeno uno"));
        }
        validate_optional_field(policy.key_field.as_deref(), "chiave")?;
        Ok(Self {
            policy,
            published_rows: 0,
        })
    }

    /// Righe già consegnate al consumatore.
    #[must_use]
    pub const fn published_rows(&self) -> u64 {
        self.published_rows
    }

    /// Avanza il cursore dopo aver consegnato un batch.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` se il cursore uscirebbe dall'intervallo
    /// rappresentabile: un indice avvolto sarebbe un indice inventato.
    pub fn publish_batch(&mut self, rows: u64) -> Result<()> {
        self.published_rows = self
            .published_rows
            .checked_add(rows)
            .ok_or_else(|| invalid("overflow nel cursore delle righe lette"))?;
        Ok(())
    }

    /// Indice sorgente assoluto della riga in costruzione.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` in caso di overflow del cursore.
    pub fn source_index(&self, offset_in_batch: u64) -> Result<u64> {
        self.published_rows
            .checked_add(offset_in_batch)
            .ok_or_else(|| invalid("overflow nell'indice sorgente di lettura"))
    }

    /// Documento di un difetto attribuito con certezza a una riga sorgente.
    ///
    /// Il report resta `partial`: la scansione si è fermata al primo difetto e
    /// i batch già pubblicati non sono più ispezionabili, quindi né il totale
    /// dei difetti né la loro identità sono osservabili.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` per una causa fuori dal pattern del
    /// contratto, per una colonna fuori dai limiti o per un cursore in
    /// overflow.
    pub fn attributed_defect(
        &self,
        offset_in_batch: u64,
        cause: &str,
        column: Option<&str>,
    ) -> Result<RowDiagnostics> {
        if !is_contract_identifier(cause) {
            return Err(invalid("causa row diagnostics non valida"));
        }
        validate_optional_field(column, "colonna")?;
        let source_index = self.source_index(offset_in_batch)?;
        let mut knowledge_limits = Vec::with_capacity(2);
        if self.published_rows > 0 {
            knowledge_limits.push(LIMIT_BATCHES_ALREADY_PUBLISHED.to_owned());
        }
        knowledge_limits.push(LIMIT_SCAN_STOPPED_AT_FIRST_DEFECT.to_owned());
        let report = RowDiagnostics {
            contract: CONTRACT.to_owned(),
            scope: DiagnosticScope::Read,
            index_basis: INDEX_BASIS.to_owned(),
            completeness: Completeness::Partial,
            knowledge_limits,
            observed_total: 1,
            total: None,
            input_total: None,
            counts: BTreeMap::from([(cause.to_owned(), 1)]),
            examples_limit: self.policy.examples_limit,
            examples_truncated: false,
            examples: vec![RowExample {
                source_index,
                cause: cause.to_owned(),
                column: column.map(ToOwned::to_owned),
                key: self.policy.key_field.as_ref().map(RowKey::redacted),
                write_state: None,
            }],
            diagnostic_state_counts: None,
            write_outcome: None,
        };
        report.validate()?;
        Ok(report)
    }

    /// Chiude un difetto di conversione osservato mentre il batch è in
    /// costruzione, cioè prima che quelle righe raggiungano il consumatore.
    ///
    /// `offset_in_batch` è `None` quando il percorso non sa a quale riga
    /// appartenga il difetto: il documento resta allora esplicitamente ignoto
    /// invece di ricevere una posizione plausibile.
    ///
    /// La funzione è totale per costruzione: qualunque cosa impedisca
    /// l'attribuzione — un offset non rappresentabile, una colonna fuori dai
    /// limiti del contratto — degrada la conoscenza dichiarata, mai l'errore.
    /// Un errore che non è un difetto di conversione attraversa immutato,
    /// perché una riga sorgente non gli appartiene.
    #[must_use]
    pub fn reject_conversion_defect(
        &self,
        error: DatabaseError,
        offset_in_batch: Option<u64>,
        column: Option<&str>,
    ) -> DatabaseError {
        if error.category != ErrorCategory::DataMapping {
            return error;
        }
        let column = column.filter(|value| validate_optional_field(Some(value), "colonna").is_ok());
        let diagnostics = offset_in_batch
            .ok_or_else(|| invalid("riga sorgente non osservabile"))
            .and_then(|offset| {
                self.attributed_defect(offset, CAUSE_VALUE_NOT_REPRESENTABLE, column)
            })
            .or_else(|_| self.unattributable_defect());
        match diagnostics {
            Ok(diagnostics) => into_read_rejection(error.clone(), diagnostics).unwrap_or(error),
            Err(_) => error,
        }
    }

    /// Documento di un difetto che il percorso non sa attribuire.
    ///
    /// Cardinalità, causa e identità restano dichiaratamente ignote: il
    /// contratto ammette il vuoto, non un indice plausibile.
    ///
    /// # Errors
    ///
    /// Propaga `InvalidPlan` se il documento non supera la validazione.
    pub fn unattributable_defect(&self) -> Result<RowDiagnostics> {
        let report = RowDiagnostics {
            contract: CONTRACT.to_owned(),
            scope: DiagnosticScope::Read,
            index_basis: INDEX_BASIS.to_owned(),
            completeness: Completeness::Unknown,
            knowledge_limits: vec![LIMIT_ROW_ATTRIBUTION_UNAVAILABLE.to_owned()],
            observed_total: 0,
            total: None,
            input_total: None,
            counts: BTreeMap::new(),
            examples_limit: self.policy.examples_limit,
            examples_truncated: false,
            examples: Vec::new(),
            diagnostic_state_counts: None,
            write_outcome: None,
        };
        report.validate()?;
        Ok(report)
    }
}

/// Trasforma un errore di conversione nel rifiuto row-scoped di lettura.
///
/// Gli assi certi di un difetto di lettura sono quelli della campagna: fase
/// `Read`, nessun effetto remoto, nessun retry. Un errore che non è un difetto
/// di conversione — un limite di risorsa, un errore di protocollo — non viene
/// riclassificato e non porta diagnostica: attribuirgli una riga sarebbe
/// esattamente l'invenzione che il contratto vieta.
///
/// # Errors
///
/// Restituisce `InvalidPlan` se il documento non è di lettura o non supera la
/// validazione del contratto.
pub fn into_read_rejection(
    error: DatabaseError,
    diagnostics: RowDiagnostics,
) -> Result<DatabaseError> {
    diagnostics.validate()?;
    if diagnostics.scope != DiagnosticScope::Read {
        return Err(invalid(
            "un rifiuto di lettura richiede una diagnostica di lettura",
        ));
    }
    if error.category != ErrorCategory::DataMapping {
        return Ok(error);
    }
    DatabaseError {
        phase: ErrorPhase::Read,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        ..error
    }
    .with_row_diagnostics(diagnostics)
}

fn invalid(message: &str) -> DatabaseError {
    DatabaseError::invalid_plan(message)
}

#[cfg(test)]
#[path = "row_diagnostics_tests.rs"]
mod tests;
