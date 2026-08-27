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
mod tests {
    use super::*;

    #[test]
    fn absolute_source_offsets_are_checked_once_for_every_provider() {
        assert_eq!(checked_source_row_end(1_000, 25), Some(1_025));
        assert_eq!(checked_source_row_end(u64::MAX, 1), None);
    }

    fn policy() -> RowDiagnosticsPolicy {
        RowDiagnosticsPolicy {
            key_field: Some("parcel_id".to_owned()),
            constraint_column: Some("area_m2".to_owned()),
            examples_limit: DEFAULT_EXAMPLES_LIMIT,
        }
    }

    fn tracker_at(staged: u64) -> WriteDiagnosticsTracker {
        let mut tracker = WriteDiagnosticsTracker::new(5_200, policy()).expect("tracker");
        tracker.stage_rows(staged).expect("righe messe in scena");
        tracker
    }

    fn rejected() -> RejectedRow {
        RejectedRow {
            source_index: 4_999,
            cause: CAUSE_CONSTRAINT_VIOLATION.to_owned(),
            column: None,
        }
    }

    /// Un rollback confermato partiziona l'intero input in quantità certe: la
    /// somma deve tornare esattamente a `input_total`.
    #[test]
    fn a_confirmed_rollback_partitions_the_whole_input_into_certain_counts() {
        let report = tracker_at(4_999)
            .reject_row(&rejected(), RollbackEvidence::Confirmed)
            .expect("diagnostica pubblicabile");
        report.validate().expect("documento valido");

        let partition = report.write_outcome.expect("partizione");
        assert_eq!(partition.certainly_rejected.known(), Some(1));
        assert_eq!(partition.certainly_not_attempted.known(), Some(200));
        assert_eq!(partition.certainly_rolled_back.known(), Some(4_999));
        assert_eq!(partition.effect_unknown.known(), Some(0));
        assert_eq!(report.input_total, Some(5_200));
        assert_eq!(report.observed_total, 1);
        assert_eq!(report.completeness, Completeness::Complete);
        assert_eq!(
            report.counts,
            BTreeMap::from([(CAUSE_CONSTRAINT_VIOLATION.to_owned(), 1)])
        );

        let example = &report.examples[0];
        assert_eq!(example.source_index, 4_999);
        assert_eq!(example.write_state, Some(WriteState::CertainlyRejected));
        let key = example.key.as_ref().expect("chiave configurata");
        assert_eq!(key.field, "parcel_id");
        assert_eq!(key.state, KeyState::Redacted);
        assert!(key.value.is_none(), "nessun valore di riga nel documento");
    }

    /// Un acknowledgement di rollback perso non autorizza a dichiarare righe
    /// annullate: le quantità remote restano esplicitamente ignote.
    #[test]
    fn a_lost_rollback_acknowledgement_keeps_the_remote_counts_unknown() {
        let report = tracker_at(4_999)
            .reject_row(&rejected(), RollbackEvidence::Lost)
            .expect("diagnostica pubblicabile");
        report.validate().expect("documento valido");

        let partition = report.write_outcome.expect("partizione");
        assert_eq!(partition.certainly_rejected.known(), Some(1));
        assert_eq!(partition.certainly_not_attempted.known(), Some(200));
        assert_eq!(partition.certainly_rolled_back, PartitionCount::Unknown);
        assert_eq!(partition.effect_unknown, PartitionCount::Unknown);
        assert_eq!(
            report
                .diagnostic_state_counts
                .expect("stati")
                .certainly_rolled_back,
            0,
            "nessuna riga è diagnosticata come annullata senza conferma"
        );
    }

    /// L'indice pubblicato è quello della sorgente: attraversa i batch e non
    /// viene ricostruito dalla posizione dentro l'ultimo batch.
    #[test]
    fn the_published_index_is_the_source_offset_across_batches() {
        let mut tracker = WriteDiagnosticsTracker::new(5_200, policy()).expect("tracker");
        for _ in 0..3 {
            tracker.stage_rows(1_300).expect("batch messo in scena");
        }
        assert_eq!(tracker.staged_rows(), 3_900);
        tracker.stage_rows(1_099).expect("chunk parziale");
        assert_eq!(tracker.staged_rows(), 4_999);

        let report = tracker
            .reject_row(&rejected(), RollbackEvidence::Confirmed)
            .expect("diagnostica pubblicabile");
        assert_eq!(report.examples[0].source_index, 4_999);
        assert_eq!(report.index_basis, INDEX_BASIS);
    }

    /// L'indice non può essere dedotto: solo la prima riga non ancora
    /// applicata è stata davvero provata da uno statement per riga.
    #[test]
    fn an_index_that_was_not_proven_row_by_row_is_refused() {
        let tracker = tracker_at(4_000);
        assert!(tracker
            .reject_row(&rejected(), RollbackEvidence::Confirmed)
            .is_err());

        let mut beyond = WriteDiagnosticsTracker::new(5_200, policy()).expect("tracker");
        beyond.stage_rows(5_200).expect("input completo");
        assert!(beyond
            .reject_row(
                &RejectedRow {
                    source_index: 5_200,
                    cause: CAUSE_CONSTRAINT_VIOLATION.to_owned(),
                    column: None,
                },
                RollbackEvidence::Confirmed,
            )
            .is_err());
    }

    /// La contabilità è controllata: nessun conteggio può uscire dall'input
    /// dichiarato né andare in overflow.
    #[test]
    fn staging_counts_are_checked_against_the_declared_input() {
        assert!(WriteDiagnosticsTracker::new(0, policy()).is_err());
        assert!(WriteDiagnosticsTracker::new(
            1,
            RowDiagnosticsPolicy {
                key_field: None,
                constraint_column: None,
                examples_limit: 0,
            }
        )
        .is_err());

        let mut tracker = WriteDiagnosticsTracker::new(10, policy()).expect("tracker");
        assert!(tracker.stage_rows(11).is_err());
        assert!(tracker.stage_rows(u64::MAX).is_err());
        tracker.stage_rows(10).expect("input completo");
        assert_eq!(tracker.staged_rows(), 10);
    }

    /// Il documento serializzato non contiene i campi che il contratto vieta e
    /// pubblica la chiave senza valore.
    #[test]
    fn the_serialized_document_matches_the_write_contract_shape() {
        let report = tracker_at(4_999)
            .reject_row(&rejected(), RollbackEvidence::Lost)
            .expect("diagnostica pubblicabile");
        let value = serde_json::to_value(&report).expect("documento serializzabile");

        assert_eq!(value["contract"], CONTRACT);
        assert_eq!(value["scope"], "write");
        assert_eq!(value["index_basis"], INDEX_BASIS);
        assert_eq!(value["completeness"], "complete");
        assert!(value.get("knowledge_limits").is_none());
        assert_eq!(value["examples"][0]["key"]["state"], "redacted");
        assert!(value["examples"][0]["key"].get("value").is_none());
        assert!(value["examples"][0].get("column").is_none());
        assert_eq!(
            value["write_outcome"]["certainly_rolled_back"]["state"],
            "unknown"
        );
        assert!(value["write_outcome"]["certainly_rolled_back"]
            .get("value")
            .is_none());

        let restored: RowDiagnostics =
            serde_json::from_value(value).expect("documento deserializzabile");
        assert_eq!(restored, report);
    }

    fn valid_write_report() -> RowDiagnostics {
        tracker_at(4_999)
            .reject_row(&rejected(), RollbackEvidence::Confirmed)
            .expect("diagnostica pubblicabile")
    }

    /// Ogni mutazione dell'aritmetica del contratto deve essere rifiutata:
    /// un documento che non torna non è pubblicabile.
    #[test]
    fn arithmetic_mutations_are_rejected_before_publication() {
        let mut counts_drift = valid_write_report();
        counts_drift.counts.insert("database.other".to_owned(), 1);
        assert!(counts_drift.validate().is_err());

        let mut fabricated_unknown = valid_write_report();
        fabricated_unknown.write_outcome = Some(WriteOutcomePartition {
            certainly_rejected: PartitionCount::Known { value: 1 },
            certainly_not_attempted: PartitionCount::Known { value: 200 },
            certainly_rolled_back: PartitionCount::Known { value: 5_000 },
            effect_unknown: PartitionCount::Known { value: 0 },
        });
        assert!(fabricated_unknown.validate().is_err());

        let mut duplicated = valid_write_report();
        let example = duplicated.examples[0].clone();
        duplicated.examples.push(example);
        assert!(duplicated.validate().is_err());

        let mut read_shaped = valid_write_report();
        read_shaped.scope = DiagnosticScope::Read;
        assert!(read_shaped.validate().is_err());

        let mut foreign_contract = valid_write_report();
        foreign_contract.contract = "plenora-row-diagnostics-v2".to_owned();
        assert!(foreign_contract.validate().is_err());

        let mut foreign_basis = valid_write_report();
        foreign_basis.index_basis = "batch_row_zero_based".to_owned();
        assert!(foreign_basis.validate().is_err());

        let mut truncation_drift = valid_write_report();
        truncation_drift.examples_truncated = true;
        assert!(truncation_drift.validate().is_err());

        let mut leaked_key = valid_write_report();
        leaked_key.examples[0].key = Some(RowKey {
            field: "parcel_id".to_owned(),
            state: KeyState::Redacted,
            value: Some(KeyValue::Integer(4_999)),
        });
        assert!(leaked_key.validate().is_err());

        let mut states_drift = valid_write_report();
        states_drift.diagnostic_state_counts = Some(DiagnosticStateCounts {
            certainly_rejected: 1,
            certainly_not_attempted: 0,
            certainly_rolled_back: 4_999,
            effect_unknown: 0,
        });
        assert!(states_drift.validate().is_err());
    }

    /// Un documento di lettura non dichiara campi di scrittura e non può
    /// dichiarare uno stato per riga.
    #[test]
    fn read_documents_stay_outside_the_write_partition() {
        let read = RowDiagnostics {
            contract: CONTRACT.to_owned(),
            scope: DiagnosticScope::Read,
            index_basis: INDEX_BASIS.to_owned(),
            completeness: Completeness::Complete,
            knowledge_limits: Vec::new(),
            observed_total: 2,
            total: Some(2),
            input_total: None,
            counts: BTreeMap::from([("conversion.invalid_date".to_owned(), 2)]),
            examples_limit: 10,
            examples_truncated: false,
            examples: vec![
                RowExample {
                    source_index: 4,
                    cause: "conversion.invalid_date".to_owned(),
                    column: Some("effective_date".to_owned()),
                    key: None,
                    write_state: None,
                },
                RowExample {
                    source_index: 1_004,
                    cause: "conversion.invalid_date".to_owned(),
                    column: Some("effective_date".to_owned()),
                    key: None,
                    write_state: None,
                },
            ],
            diagnostic_state_counts: None,
            write_outcome: None,
        };
        read.validate().expect("documento di lettura valido");

        let mut with_write_state = read.clone();
        with_write_state.examples[0].write_state = Some(WriteState::CertainlyRejected);
        assert!(with_write_state.validate().is_err());

        let mut with_input_total = read;
        with_input_total.input_total = Some(1_025);
        assert!(with_input_total.validate().is_err());
    }

    /// Un documento non completo deve dichiarare limiti di conoscenza validi e
    /// non può dichiarare un totale quando la completezza è ignota.
    #[test]
    fn incomplete_documents_must_declare_valid_knowledge_limits() {
        let mut partial = valid_write_report();
        partial.completeness = Completeness::Partial;
        assert!(partial.validate().is_err());

        partial.knowledge_limits = vec!["server.truncated_diagnostics".to_owned()];
        partial.validate().expect("documento parziale valido");

        let mut repeated = partial.clone();
        repeated
            .knowledge_limits
            .push("server.truncated_diagnostics".to_owned());
        assert!(repeated.validate().is_err());

        let mut malformed = partial.clone();
        malformed.knowledge_limits = vec!["Server..Truncated".to_owned()];
        assert!(malformed.validate().is_err());

        let mut unknown = partial;
        unknown.completeness = Completeness::Unknown;
        assert!(unknown.validate().is_err(), "total resta dichiarato");
        unknown.total = None;
        unknown.validate().expect("documento ignoto valido");
    }

    /// Un valore di chiave al limite dell'intero i64 non deve far panicare la
    /// validazione: il confronto è su un intervallo chiuso, non su `abs()`.
    #[test]
    fn extreme_key_integers_are_refused_without_panicking() {
        for refused in [
            i64::MIN,
            i64::MAX,
            -MAX_EXACT_INTEGER - 1,
            MAX_EXACT_INTEGER + 1,
        ] {
            let key = RowKey {
                field: "parcel_id".to_owned(),
                state: KeyState::Value,
                value: Some(KeyValue::Integer(refused)),
            };
            assert!(validate_key(Some(&key)).is_err(), "{refused}");
        }
        for accepted in [-MAX_EXACT_INTEGER, 0, MAX_EXACT_INTEGER] {
            let key = RowKey {
                field: "parcel_id".to_owned(),
                state: KeyState::Value,
                value: Some(KeyValue::Integer(accepted)),
            };
            validate_key(Some(&key)).expect("valore di chiave rappresentabile");
        }
    }

    #[test]
    fn unicode_limits_are_counted_in_characters_not_utf8_bytes() {
        let field_at_limit = "é".repeat(MAX_FIELD_CHARS);
        validate_optional_field(Some(&field_at_limit), "colonna")
            .expect("256 caratteri Unicode sono ammessi");
        assert!(
            validate_optional_field(Some(&"é".repeat(MAX_FIELD_CHARS + 1)), "colonna").is_err()
        );

        validate_key_value(&KeyValue::Text("界".repeat(MAX_KEY_TEXT_CHARS)))
            .expect("1024 caratteri Unicode sono ammessi");
        assert!(validate_key_value(&KeyValue::Text("界".repeat(MAX_KEY_TEXT_CHARS + 1))).is_err());
    }

    /// La pubblicazione è fail-closed: un documento incoerente non produce
    /// JSON, uno valido produce esattamente il documento validato.
    #[test]
    fn serialization_refuses_to_publish_an_invalid_document() {
        let report = valid_write_report();
        let encoded = report.to_json().expect("documento pubblicabile");
        let decoded: RowDiagnostics =
            serde_json::from_str(&encoded).expect("documento rileggibile");
        assert_eq!(decoded, report);

        let mut broken = report;
        broken.observed_total = 9;
        assert!(
            broken.to_json().is_err(),
            "nessun JSON da un documento rotto"
        );
        assert!(
            serde_json::to_value(&broken).is_err(),
            "il trait Serialize pubblico non deve aggirare la validazione"
        );
    }

    /// Esecutore di prova del seam: applica le righe una per volta e rifiuta
    /// esattamente l'indice sorgente programmato.
    struct ScriptedWriter {
        reject_at: u64,
        rollback: RollbackEvidence,
        applied: Vec<u64>,
        rolled_back: bool,
        finish_error: bool,
    }

    impl RowScopedWriter for ScriptedWriter {
        fn apply_row(&mut self, source_index: u64) -> RowWriteFuture<'_, Result<RowApplication>> {
            let rejected = source_index == self.reject_at;
            if !rejected {
                self.applied.push(source_index);
            }
            Box::pin(async move {
                Ok(if rejected {
                    RowApplication::Rejected(RowRejection {
                        cause: CAUSE_CONSTRAINT_VIOLATION.to_owned(),
                        column: Some("area_m2".to_owned()),
                    })
                } else {
                    RowApplication::Applied
                })
            })
        }

        fn finish_declared_input(&mut self) -> RowWriteFuture<'_, Result<()>> {
            let failed = self.finish_error;
            Box::pin(async move {
                if failed {
                    Err(invalid("input oltre il totale dichiarato"))
                } else {
                    Ok(())
                }
            })
        }

        fn rollback(&mut self) -> RowWriteFuture<'_, RollbackEvidence> {
            self.rolled_back = true;
            let evidence = self.rollback;
            Box::pin(async move { evidence })
        }
    }

    /// Esecutore minimo senza runtime: il seam resta agnostico e i test
    /// offline non introducono dipendenze.
    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        use std::sync::Arc;
        use std::task::{Context, Poll, Wake, Waker};

        struct Idle;
        impl Wake for Idle {
            fn wake(self: Arc<Self>) {}
        }

        let mut future = std::pin::pin!(future);
        let waker = Waker::from(Arc::new(Idle));
        let mut context = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
        }
    }

    fn drive(reject_at: u64, rollback: RollbackEvidence) -> (ScriptedWriter, RowRejectionOutcome) {
        let mut writer = ScriptedWriter {
            reject_at,
            rollback,
            applied: Vec::new(),
            rolled_back: false,
            finish_error: false,
        };
        let mut tracker = WriteDiagnosticsTracker::new(5_200, policy()).expect("tracker");
        let outcome = block_on(diagnose_row_scoped_write(&mut writer, &mut tracker))
            .expect("diagnosi eseguibile")
            .expect("riga rifiutata");
        (writer, outcome)
    }

    /// Caso `write-constraint-confirmed-rollback`: 5200 righe, vincolo certo a
    /// 4999, annullamento confermato e partizione interamente certa.
    #[test]
    fn the_confirmed_rollback_case_is_driven_row_by_row() {
        let (writer, outcome) = drive(4_999, RollbackEvidence::Confirmed);
        assert_eq!(
            writer.applied.len(),
            4_999,
            "una riga per statement fino al rifiuto"
        );
        assert_eq!(writer.applied.first(), Some(&0));
        assert_eq!(writer.applied.last(), Some(&4_998));
        assert!(writer.rolled_back);

        assert_eq!(
            outcome.axes(),
            (
                ErrorPhase::Write,
                RemoteEffect::RolledBack,
                RetryDisposition::Never
            )
        );
        let report = outcome.diagnostics().clone();
        report.validate().expect("documento valido");
        assert_eq!(report.input_total, Some(5_200));
        assert_eq!(report.examples[0].source_index, 4_999);
        assert_eq!(report.examples[0].column.as_deref(), Some("area_m2"));
        let partition = report.write_outcome.expect("partizione");
        assert_eq!(partition.certainly_rejected.known(), Some(1));
        assert_eq!(partition.certainly_not_attempted.known(), Some(200));
        assert_eq!(partition.certainly_rolled_back.known(), Some(4_999));
        assert_eq!(partition.effect_unknown.known(), Some(0));

        let error = outcome
            .into_error(Some(ProviderKind::Mysql), Some("execution-3".to_owned()))
            .expect("errore con diagnostica");
        assert_eq!(error.category, ErrorCategory::DataMapping);
        assert!(!error.is_retryable());
        assert!(
            !error.message.contains("4999"),
            "l'indice vive nel documento, non nel messaggio"
        );
    }

    /// Caso `write-constraint-rollback-outcome-unknown`: stesso rifiuto, ma
    /// l'acknowledgement dell'annullamento è perso.
    #[test]
    fn the_lost_rollback_case_keeps_the_remote_effect_unknown() {
        let (_, outcome) = drive(4_999, RollbackEvidence::Lost);
        assert_eq!(
            outcome.axes(),
            (
                ErrorPhase::Rollback,
                RemoteEffect::Unknown,
                RetryDisposition::Quarantine
            )
        );
        let report = outcome.diagnostics().clone();
        report.validate().expect("documento valido");
        let partition = report.write_outcome.expect("partizione");
        assert_eq!(partition.certainly_rejected.known(), Some(1));
        assert_eq!(partition.certainly_not_attempted.known(), Some(200));
        assert_eq!(partition.certainly_rolled_back, PartitionCount::Unknown);
        assert_eq!(partition.effect_unknown, PartitionCount::Unknown);

        let error = outcome
            .into_error(Some(ProviderKind::Mysql), None)
            .expect("errore con diagnostica");
        assert!(!error.is_retryable(), "la quarantena non è un retry");
    }

    /// Un input applicato per intero non produce diagnostica: il documento
    /// esiste solo quando c'è un rifiuto provato.
    #[test]
    fn a_fully_applied_input_produces_no_document() {
        let mut writer = ScriptedWriter {
            reject_at: u64::MAX,
            rollback: RollbackEvidence::Confirmed,
            applied: Vec::new(),
            rolled_back: false,
            finish_error: false,
        };
        let mut tracker = WriteDiagnosticsTracker::new(16, policy()).expect("tracker");
        let outcome =
            block_on(diagnose_row_scoped_write(&mut writer, &mut tracker)).expect("diagnosi");
        assert!(outcome.is_none());
        assert_eq!(writer.applied.len(), 16);
        assert!(!writer.rolled_back, "nessun annullamento senza rifiuto");
        assert_eq!(tracker.staged_rows(), 16);
    }

    #[test]
    fn rows_beyond_the_declared_total_fail_closed_before_success() {
        let mut writer = ScriptedWriter {
            reject_at: u64::MAX,
            rollback: RollbackEvidence::Confirmed,
            applied: Vec::new(),
            rolled_back: false,
            finish_error: true,
        };
        let mut tracker = WriteDiagnosticsTracker::new(16, policy()).expect("tracker");
        assert!(
            block_on(diagnose_row_scoped_write(&mut writer, &mut tracker)).is_err(),
            "righe extra non devono essere ignorate"
        );
        assert_eq!(writer.applied.len(), 16);
    }

    fn read_policy() -> ReadDiagnosticsPolicy {
        ReadDiagnosticsPolicy {
            key_field: Some("parcel_id".to_owned()),
            examples_limit: DEFAULT_EXAMPLES_LIMIT,
        }
    }

    /// L'indice pubblicato in lettura è l'offset assoluto nel result set:
    /// batch già pubblicati più la posizione dentro il batch corrente.
    #[test]
    fn a_read_defect_publishes_the_absolute_source_index() {
        let mut tracker = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
        tracker
            .publish_batch(4_096)
            .expect("primo batch pubblicato");
        assert_eq!(tracker.published_rows(), 4_096);

        let report = tracker
            .attributed_defect(903, CAUSE_VALUE_NOT_REPRESENTABLE, Some("area_m2"))
            .expect("diagnostica pubblicabile");
        report.validate().expect("documento valido");

        assert_eq!(report.scope, DiagnosticScope::Read);
        assert_eq!(report.index_basis, INDEX_BASIS);
        assert_eq!(report.observed_total, 1);
        assert_eq!(report.examples[0].source_index, 4_999);
        assert_eq!(report.examples[0].column.as_deref(), Some("area_m2"));
        assert_eq!(
            report.examples[0].key.as_ref().map(|key| key.state),
            Some(KeyState::Redacted)
        );
        assert!(report.examples[0].write_state.is_none());
        assert!(report.input_total.is_none());
        assert!(report.write_outcome.is_none());
        assert!(report.diagnostic_state_counts.is_none());
    }

    /// La conoscenza non si estende oltre ciò che è stato osservato: la
    /// scansione si ferma al primo difetto e i batch già pubblicati non sono
    /// più ispezionabili.
    #[test]
    fn a_read_report_never_claims_completeness_it_cannot_observe() {
        let first = ReadDiagnosticsTracker::new(read_policy())
            .expect("tracker")
            .attributed_defect(4, CAUSE_VALUE_NOT_REPRESENTABLE, None)
            .expect("diagnostica pubblicabile");
        assert_eq!(first.completeness, Completeness::Partial);
        assert_eq!(
            first.knowledge_limits,
            vec![LIMIT_SCAN_STOPPED_AT_FIRST_DEFECT.to_owned()]
        );
        assert!(first.total.is_none(), "nessun totale osservato in lettura");

        let mut later = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
        later.publish_batch(4_096).expect("batch pubblicato");
        let report = later
            .attributed_defect(0, CAUSE_VALUE_NOT_REPRESENTABLE, None)
            .expect("diagnostica pubblicabile");
        assert_eq!(
            report.knowledge_limits,
            vec![
                LIMIT_BATCHES_ALREADY_PUBLISHED.to_owned(),
                LIMIT_SCAN_STOPPED_AT_FIRST_DEFECT.to_owned(),
            ]
        );
        report.validate().expect("documento valido");
    }

    /// Un difetto non attribuibile non inventa né indice né causa: resta un
    /// documento ignoto con conteggi ed esempi vuoti.
    #[test]
    fn an_unattributable_read_defect_publishes_an_unknown_report() {
        let report = ReadDiagnosticsTracker::new(read_policy())
            .expect("tracker")
            .unattributable_defect()
            .expect("diagnostica pubblicabile");
        report.validate().expect("documento valido");

        assert_eq!(report.completeness, Completeness::Unknown);
        assert_eq!(report.observed_total, 0);
        assert!(report.total.is_none());
        assert!(report.counts.is_empty());
        assert!(report.examples.is_empty());
        assert!(!report.examples_truncated);
        assert_eq!(
            report.knowledge_limits,
            vec![LIMIT_ROW_ATTRIBUTION_UNAVAILABLE.to_owned()]
        );
    }

    /// L'offset assoluto è aritmetica controllata: un cursore che uscirebbe
    /// dall'intervallo rappresentabile fallisce invece di avvolgersi.
    #[test]
    fn read_source_indexes_are_checked_arithmetic() {
        let mut tracker = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
        tracker.publish_batch(u64::MAX).expect("cursore al limite");
        assert!(tracker.publish_batch(1).is_err());
        assert!(tracker.source_index(1).is_err());
        assert!(tracker
            .attributed_defect(1, CAUSE_VALUE_NOT_REPRESENTABLE, None)
            .is_err());

        let mut small = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
        small.publish_batch(7).expect("batch pubblicato");
        assert_eq!(small.source_index(3).expect("offset assoluto"), 10);
    }

    /// La contabilità di default è quella dichiarata dalla politica di
    /// default: i percorsi che non possono propagare un errore in costruzione
    /// non hanno bisogno di aggirare la validazione.
    #[test]
    fn the_default_tracker_matches_the_default_policy() {
        let built = ReadDiagnosticsTracker::new(ReadDiagnosticsPolicy::default()).expect("tracker");
        let default = ReadDiagnosticsTracker::default();
        assert_eq!(default.published_rows(), built.published_rows());
        assert_eq!(
            default
                .attributed_defect(0, CAUSE_VALUE_NOT_REPRESENTABLE, None)
                .expect("documento"),
            built
                .attributed_defect(0, CAUSE_VALUE_NOT_REPRESENTABLE, None)
                .expect("documento")
        );
        assert_eq!(
            default
                .attributed_defect(0, CAUSE_VALUE_NOT_REPRESENTABLE, None)
                .expect("documento")
                .examples_limit,
            DEFAULT_EXAMPLES_LIMIT
        );
    }

    /// La causa deve restare un identificatore di contratto: nessun testo
    /// vendor può diventare una causa pubblicata.
    #[test]
    fn a_read_cause_outside_the_contract_pattern_is_refused() {
        let tracker = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
        for refused in [
            "",
            "Conversion.Invalid",
            "conversion..invalid",
            "conversion.",
        ] {
            assert!(
                tracker.attributed_defect(0, refused, None).is_err(),
                "{refused}"
            );
        }
        assert!(tracker
            .attributed_defect(0, CAUSE_VALUE_NOT_REPRESENTABLE, Some(""))
            .is_err());
    }

    fn conversion_error() -> DatabaseError {
        DatabaseError {
            category: ErrorCategory::DataMapping,
            phase: ErrorPhase::Read,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider: Some(ProviderKind::Mysql),
            execution_id: None,
            message: "valore non rappresentabile".to_owned(),
            diagnostics: None,
        }
    }

    /// Il seam condiviso dai provider chiude il difetto osservato durante la
    /// costruzione del batch senza che ciascuno riscriva la contabilità.
    #[test]
    fn a_conversion_defect_becomes_a_read_rejection() {
        let mut tracker = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
        tracker.publish_batch(8_192).expect("batch pubblicato");

        let error =
            tracker.reject_conversion_defect(conversion_error(), Some(12), Some("effective_date"));
        let report = error.row_diagnostics().expect("diagnostica allegata");
        report.validate().expect("documento valido");
        assert_eq!(error.phase, ErrorPhase::Read);
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert_eq!(error.retry, RetryDisposition::Never);
        assert_eq!(report.examples[0].source_index, 8_204);
        assert_eq!(report.examples[0].cause, CAUSE_VALUE_NOT_REPRESENTABLE);
        assert_eq!(report.examples[0].column.as_deref(), Some("effective_date"));
    }

    /// Un errore che non è un difetto di conversione non viene riclassificato
    /// e non riceve una riga che non gli appartiene.
    #[test]
    fn a_non_conversion_error_never_receives_a_source_row() {
        let tracker = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
        for error in [
            DatabaseError::resource_limit("budget esaurito"),
            DatabaseError::cancelled(Some(ProviderKind::Mysql), ErrorPhase::Read, "cancellata"),
            DatabaseError::invalid_plan("piano non valido"),
        ] {
            let category = error.category;
            let closed = tracker.reject_conversion_defect(error, Some(3), Some("area_m2"));
            assert_eq!(closed.category, category);
            assert!(closed.row_diagnostics().is_none());
        }
    }

    /// Una riga che il percorso non sa individuare non riceve un indice
    /// plausibile: il documento dichiara che l'identità non è osservabile.
    #[test]
    fn an_unobservable_row_position_publishes_an_unknown_report() {
        let tracker = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
        let error = tracker.reject_conversion_defect(conversion_error(), None, Some("area_m2"));
        let report = error.row_diagnostics().expect("diagnostica allegata");
        report.validate().expect("documento valido");
        assert_eq!(report.completeness, Completeness::Unknown);
        assert_eq!(report.observed_total, 0);
        assert!(report.examples.is_empty());
        assert_eq!(
            report.knowledge_limits,
            vec![LIMIT_ROW_ATTRIBUTION_UNAVAILABLE.to_owned()]
        );
    }

    /// Una posizione non rappresentabile o una colonna fuori contratto non
    /// producono un indice inventato: l'indice provato resta, il resto no.
    #[test]
    fn an_unattributable_position_degrades_to_an_unknown_report() {
        let mut tracker = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
        tracker.publish_batch(u64::MAX).expect("cursore al limite");
        let error = tracker.reject_conversion_defect(conversion_error(), Some(1), Some("area_m2"));
        let report = error.row_diagnostics().expect("diagnostica allegata");
        assert_eq!(report.completeness, Completeness::Unknown);
        assert!(report.examples.is_empty());

        let bounded = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
        let error = bounded.reject_conversion_defect(conversion_error(), Some(5), Some(""));
        let report = error.row_diagnostics().expect("diagnostica allegata");
        assert_eq!(report.examples[0].source_index, 5, "l'indice provato resta");
        assert!(
            report.examples[0].column.is_none(),
            "una colonna fuori contratto non viene pubblicata"
        );
    }

    /// Il rifiuto di lettura porta gli assi dichiarati dalla campagna e non
    /// riclassifica errori che non sono difetti di conversione.
    #[test]
    fn a_read_rejection_carries_the_declared_axes() {
        let report = ReadDiagnosticsTracker::new(read_policy())
            .expect("tracker")
            .attributed_defect(4, CAUSE_VALUE_NOT_REPRESENTABLE, Some("effective_date"))
            .expect("diagnostica pubblicabile");

        let error = into_read_rejection(
            DatabaseError {
                category: ErrorCategory::DataMapping,
                phase: ErrorPhase::Prepare,
                remote_effect: RemoteEffect::Unknown,
                retry: RetryDisposition::RequiresRecovery,
                provider: Some(ProviderKind::Mysql),
                execution_id: None,
                message: "valore non rappresentabile".to_owned(),
                diagnostics: None,
            },
            report.clone(),
        )
        .expect("rifiuto di lettura");
        assert_eq!(error.category, ErrorCategory::DataMapping);
        assert_eq!(error.phase, ErrorPhase::Read);
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert_eq!(error.retry, RetryDisposition::Never);
        assert_eq!(error.row_diagnostics(), Some(&report));

        let untouched = into_read_rejection(
            DatabaseError::resource_limit("budget esaurito"),
            report.clone(),
        )
        .expect("errore non riclassificato");
        assert_eq!(untouched.category, ErrorCategory::ResourceLimit);
        assert!(
            untouched.row_diagnostics().is_none(),
            "un errore che non è un difetto di riga non porta diagnostica"
        );

        let mut write_shaped = report;
        write_shaped.scope = DiagnosticScope::Write;
        assert!(into_read_rejection(
            DatabaseError {
                category: ErrorCategory::DataMapping,
                phase: ErrorPhase::Read,
                remote_effect: RemoteEffect::None,
                retry: RetryDisposition::Never,
                provider: None,
                execution_id: None,
                message: "documento di scrittura".to_owned(),
                diagnostics: None,
            },
            write_shaped,
        )
        .is_err());
    }

    #[test]
    fn contract_identifiers_follow_the_declared_pattern() {
        for accepted in [
            "database.constraint_violation",
            "a",
            "shapefile.inner_ring_without_outer",
            "conversion.invalid-date",
        ] {
            assert!(is_contract_identifier(accepted), "{accepted}");
        }
        for refused in [
            "",
            "1database",
            "Database.violation",
            "database..violation",
            "database.",
            "database violation",
            &"a".repeat(129),
        ] {
            assert!(!is_contract_identifier(refused), "{refused}");
        }
    }
}
