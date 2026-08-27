//! Policy di mapping e rapporto strutturato delle conversioni con perdita.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MappingPolicy {
    Strict,
    Compatible,
    Lossy,
    Native,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LossSeverity {
    Information,
    Warning,
    DataLoss,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LossCategory {
    Precision,
    Scale,
    Range,
    Timezone,
    Encoding,
    Collation,
    NativeType,
    GeometryType,
    Dimensions,
    Srid,
    Crs,
    Nullability,
    Default,
    Generated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MappingLoss {
    pub field_id: u32,
    pub category: LossCategory,
    pub severity: LossSeverity,
    pub reason: String,
    // Lo schema dichiara `source_type` e `target_type` come `{"type":
    // "string"}` e non li mette fra i `required`: sono ammessi come stringa o
    // come campo **assente**, mai come `null`. Senza questo attributo un
    // `None` usciva come `null`, e il producer emetteva documenti che il
    // proprio contratto rifiuta — PostgreSQL li produce davvero, per le
    // perdite di nullability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LossReport {
    pub schema_version: u32,
    pub policy: MappingPolicy,
    pub losses: Vec<MappingLoss>,
}

/// Numero massimo di perdite trasportabili da un report.
///
/// Viene da `contracts/v2/loss-report.schema.json` (`maxItems`). Il tipo Rust
/// non lo conosceva: un target molto largo produceva un report che il proprio
/// contratto rifiuta, e il rifiuto arrivava a valle — al consumatore che prova
/// a validarlo — invece che qui.
pub const MAX_LOSSES: usize = 4096;

/// Lunghezza massima di `reason`, in **caratteri**.
///
/// `maxLength` di JSON Schema conta caratteri Unicode, non byte UTF-8:
/// misurarla con `String::len()` avrebbe rifiutato documenti schema-validi
/// non appena il testo usciva dall'ASCII — cioe in italiano, che e la lingua
/// in cui questi messaggi sono scritti.
pub const MAX_REASON_CHARS: usize = 1024;

/// Lunghezza massima di `source_type` e `target_type`, in caratteri.
pub const MAX_TYPE_CHARS: usize = 512;

impl LossReport {
    #[must_use]
    pub fn permits_execution(&self) -> bool {
        self.policy != MappingPolicy::Strict
            || !self
                .losses
                .iter()
                .any(|loss| loss.severity == LossSeverity::DataLoss)
    }

    /// Verifica che il report stia dentro il proprio contratto.
    ///
    /// Da chiamare prima di restituirlo: un report che eccede i limiti non e
    /// un report piu ricco, e un documento che nessun consumatore conforme
    /// puo accettare. Meglio fallire dove il report si costruisce, dove si sa
    /// ancora quale operazione lo stava producendo.
    ///
    /// Non tronca: le perdite sono l'informazione, e scartarne una parte in
    /// silenzio produrrebbe un documento valido che dichiara meno danno di
    /// quello reale — il modo peggiore di rispettare un limite.
    ///
    /// # Errors
    ///
    /// `InvalidPlan` per una major diversa da quella del contratto;
    /// `ResourceLimit` quando il numero di perdite o la lunghezza di un campo
    /// testuale eccede il contratto. I messaggi riportano soglie e conteggi,
    /// mai il contenuto: `reason` e i nomi di tipo derivano dallo schema
    /// sorgente, e il contratto vieta di rimetterli in un errore pubblico.
    pub fn validate(&self) -> crate::Result<()> {
        // Il contratto fissa `schema_version` a 2 con un `const`, non con un
        // minimo: un report che dichiara un'altra major non e un report piu
        // recente da tollerare, e un documento di un contratto diverso. Il
        // controllo mancava qui e mancava in `WriteOutcome`, che ha lo stesso
        // vincolo; `ProviderCapabilities` era l'unico ad averlo.
        if self.schema_version != 2 {
            return Err(crate::DatabaseError::invalid_plan(
                "loss report con schema_version non supportata",
            ));
        }
        if self.losses.len() > MAX_LOSSES {
            return Err(crate::DatabaseError::resource_limit(format!(
                "loss report con {} perdite, il contratto ne ammette {MAX_LOSSES}",
                self.losses.len()
            )));
        }
        for (index, loss) in self.losses.iter().enumerate() {
            if loss.reason.is_empty() {
                return Err(crate::DatabaseError::resource_limit(format!(
                    "loss report: la perdita {index} non ha motivo"
                )));
            }
            let reason_chars = loss.reason.chars().count();
            if reason_chars > MAX_REASON_CHARS {
                return Err(crate::DatabaseError::resource_limit(format!(
                    "loss report: motivo della perdita {index} di {reason_chars} caratteri, \
                     il contratto ne ammette {MAX_REASON_CHARS}"
                )));
            }
            for (field, value) in [
                ("source_type", loss.source_type.as_ref()),
                ("target_type", loss.target_type.as_ref()),
            ] {
                if let Some(value) = value {
                    let characters = value.chars().count();
                    if characters > MAX_TYPE_CHARS {
                        return Err(crate::DatabaseError::resource_limit(format!(
                            "loss report: {field} della perdita {index} di {characters} \
                             caratteri, il contratto ne ammette {MAX_TYPE_CHARS}"
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "loss_tests.rs"]
mod tests;
