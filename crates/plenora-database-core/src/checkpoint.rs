//! Checkpoint persistenti per letture keyset riprendibili.
//!
//! Il token non identifica una risorsa lato server: porta invece l'identita
//! del provider, della sorgente, dell'ordinamento e i valori dell'ultima riga
//! consegnata. Applicarlo costruisce un predicato lessicografico strettamente
//! successivo, combinato con l'eventuale filtro originale.

use crate::plan::{
    FilterExpression, ObjectRef, OrderBy, ProviderKind, ReadOperation, SortDirection,
};
use crate::provider::{ParameterBag, ParameterValue};
use crate::{DatabaseError, Result, Row};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const READ_CHECKPOINT_SCHEMA_VERSION: u16 = 2;
const PARAMETER_PREFIX: &str = "__plenora_resume_";

/// Token provider-qualified che consente di riaprire una lettura keyset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadCheckpoint {
    pub schema_version: u16,
    pub provider: ProviderKind,
    pub source: ObjectRef,
    pub order_by: Vec<OrderBy>,
    /// Fingerprint dell'intero scope logico della lettura, esclusi soltanto
    /// limite e offset. Impedisce di riusare il token con filtro, bind,
    /// proiezione o dichiarazioni CRS differenti.
    pub scope_fingerprint: String,
    pub values: Vec<ParameterValue>,
}

impl ReadCheckpoint {
    /// Cattura il checkpoint dai valori espliciti delle colonne ordinate.
    ///
    /// # Errors
    ///
    /// Rifiuta ordinamenti vuoti o duplicati, arita diversa e valori che non
    /// hanno un ordinamento SQL portabile (NULL, JSON e geometrie).
    pub fn new(
        provider: ProviderKind,
        operation: &ReadOperation,
        parameters: &ParameterBag,
        values: Vec<ParameterValue>,
    ) -> Result<Self> {
        let checkpoint = Self {
            schema_version: READ_CHECKPOINT_SCHEMA_VERSION,
            provider,
            source: operation.source.clone(),
            order_by: operation.order_by.clone(),
            scope_fingerprint: scope_fingerprint(provider, operation, parameters)?,
            values,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    /// Cattura il checkpoint dall'ultima riga consegnata.
    ///
    /// Le colonne usate dall'ordinamento devono essere presenti nella riga;
    /// il messaggio di errore non ne pubblica nomi o valori.
    ///
    /// # Errors
    ///
    /// Rifiuta righe prive di una colonna ordinata o checkpoint non validi.
    pub fn from_row(
        provider: ProviderKind,
        operation: &ReadOperation,
        parameters: &ParameterBag,
        row: &Row,
    ) -> Result<Self> {
        let values = operation
            .order_by
            .iter()
            .map(|ordering| {
                row.get(&ordering.field).cloned().ok_or_else(|| {
                    DatabaseError::invalid_plan(
                        "la riga del checkpoint non contiene tutte le colonne ordinate",
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Self::new(provider, operation, parameters, values)
    }

    /// Verifica forma, versione e portabilita del token.
    ///
    /// # Errors
    ///
    /// Rifiuta versioni, fingerprint, ordinamenti o valori non portabili.
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != READ_CHECKPOINT_SCHEMA_VERSION {
            return Err(DatabaseError::invalid_plan(
                "versione del checkpoint di lettura non supportata",
            ));
        }
        if self.order_by.is_empty() || self.order_by.len() > 256 {
            return Err(DatabaseError::invalid_plan(
                "un checkpoint richiede da una a 256 colonne ordinate",
            ));
        }
        if self.values.len() != self.order_by.len() {
            return Err(DatabaseError::invalid_plan(
                "numero di valori del checkpoint diverso dall'ordinamento",
            ));
        }
        if self.scope_fingerprint.len() != 71
            || !self.scope_fingerprint.starts_with("sha256:")
            || !self.scope_fingerprint[7..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(DatabaseError::invalid_plan(
                "fingerprint dello scope checkpoint non valido",
            ));
        }
        let mut fields = BTreeSet::new();
        if self
            .order_by
            .iter()
            .any(|ordering| ordering.field.is_empty() || !fields.insert(&ordering.field))
        {
            return Err(DatabaseError::invalid_plan(
                "ordinamento del checkpoint vuoto o duplicato",
            ));
        }
        if self.values.iter().any(|value| {
            matches!(
                value,
                ParameterValue::Null { .. } | ParameterValue::Json(_) | ParameterValue::Wkb { .. }
            ) || matches!(value, ParameterValue::F64(number) if !number.is_finite())
        }) {
            return Err(DatabaseError::invalid_plan(
                "checkpoint con valore privo di ordinamento SQL portabile",
            ));
        }
        Ok(())
    }

    /// Applica il checkpoint a una nuova lettura e lega i suoi parametri.
    ///
    /// Il provider, la sorgente e l'ordinamento devono coincidere esattamente.
    /// Un offset non puo essere composto con una ripresa keyset.
    ///
    /// # Errors
    ///
    /// Rifiuta token non qualificati per lo scope o collisioni nei bind.
    pub fn resume(
        &self,
        provider: ProviderKind,
        operation: &ReadOperation,
        parameters: &ParameterBag,
    ) -> Result<(ReadOperation, ParameterBag)> {
        self.validate()?;
        if self.provider != provider
            || self.source != operation.source
            || self.order_by != operation.order_by
            || self.scope_fingerprint != scope_fingerprint(provider, operation, parameters)?
        {
            return Err(DatabaseError::invalid_plan(
                "checkpoint non qualificato per provider, sorgente o ordinamento richiesti",
            ));
        }
        if operation.row_offset.is_some() {
            return Err(DatabaseError::invalid_plan(
                "checkpoint keyset e row_offset non sono componibili",
            ));
        }

        let mut values = parameters
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        for (index, value) in self.values.iter().enumerate() {
            let name = parameter_name(index);
            if values.insert(name, value.clone()).is_some() {
                return Err(DatabaseError::invalid_plan(
                    "collisione con un parametro riservato al checkpoint",
                ));
            }
        }

        let keyset = lexicographic_filter(&self.order_by);
        let mut resumed = operation.clone();
        resumed.filter = Some(match resumed.filter.take() {
            Some(original) => FilterExpression::And {
                args: vec![original, keyset],
            },
            None => keyset,
        });
        Ok((resumed, ParameterBag::new(values)))
    }

    /// JSON persistibile, emesso solo dopo la validazione completa.
    ///
    /// # Errors
    ///
    /// Rifiuta token non validi o non serializzabili.
    pub fn to_json(&self) -> Result<String> {
        self.validate()?;
        serde_json::to_string(self)
            .map_err(|_| DatabaseError::invalid_plan("codifica del checkpoint fallita"))
    }

    /// Decodifica fail-closed di un checkpoint persistito.
    ///
    /// # Errors
    ///
    /// Rifiuta JSON malformato, campi ignoti o token non validi.
    pub fn from_json(document: &str) -> Result<Self> {
        let checkpoint: Self = serde_json::from_str(document)
            .map_err(|_| DatabaseError::invalid_plan("checkpoint di lettura non valido"))?;
        checkpoint.validate()?;
        Ok(checkpoint)
    }
}

fn scope_fingerprint(
    provider: ProviderKind,
    operation: &ReadOperation,
    parameters: &ParameterBag,
) -> Result<String> {
    let canonical = serde_json::to_vec(&(
        provider,
        &operation.source,
        &operation.projection,
        &operation.order_by,
        &operation.filter,
        &operation.declared_crs,
        parameters,
    ))
    .map_err(|_| DatabaseError::invalid_plan("scope checkpoint non serializzabile"))?;
    let digest = Sha256::digest(canonical);
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("scrittura su String infallibile");
    }
    Ok(encoded)
}

fn parameter_name(index: usize) -> String {
    format!("{PARAMETER_PREFIX}{index}")
}

fn lexicographic_filter(order_by: &[OrderBy]) -> FilterExpression {
    let alternatives = order_by
        .iter()
        .enumerate()
        .map(|(index, ordering)| {
            let mut terms = order_by[..index]
                .iter()
                .enumerate()
                .map(|(prefix, prior)| FilterExpression::Eq {
                    field: prior.field.clone(),
                    parameter: parameter_name(prefix),
                })
                .collect::<Vec<_>>();
            terms.push(match ordering.direction {
                SortDirection::Asc => FilterExpression::Gt {
                    field: ordering.field.clone(),
                    parameter: parameter_name(index),
                },
                SortDirection::Desc => FilterExpression::Lt {
                    field: ordering.field.clone(),
                    parameter: parameter_name(index),
                },
            });
            if terms.len() == 1 {
                terms.pop().expect("un termine costruito")
            } else {
                FilterExpression::And { args: terms }
            }
        })
        .collect::<Vec<_>>();
    if alternatives.len() == 1 {
        alternatives.into_iter().next().expect("un'alternativa")
    } else {
        FilterExpression::Or { args: alternatives }
    }
}

#[cfg(test)]
#[path = "checkpoint_tests.rs"]
mod tests;
