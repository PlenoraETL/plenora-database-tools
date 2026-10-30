//! Oracolo del server separato dalle guardie di ammissione del prodotto.
use super::{
    ddl, trace, verdict, AdmissionKey, Arc, ArrayRef, BTreeMap, Battery, BinaryArray, MeasuredKey,
    ProbeHooks, ProbeSpec, StringArray, TargetDimensions,
};
use plenora_database_core::provider::ParameterValue;

/// Esito osservato dopo DML diretto e rollback confermato.
pub enum DirectObservation {
    Refused,
    Unreadable,
    Read(ArrayRef),
}

/// Converte il valore scalare riletto senza cambiarne il dominio.
///
/// # Panics
/// Se l'oracolo restituisce un tipo non previsto dalle sonde dirette.
#[must_use]
pub fn direct_value(value: &ParameterValue) -> ArrayRef {
    match value {
        ParameterValue::Null { .. } => Arc::new(StringArray::from(vec![None::<&str>])),
        ParameterValue::String(value)
        | ParameterValue::Timestamp(value)
        | ParameterValue::TimestampTz(value) => Arc::new(StringArray::from(vec![value.as_str()])),
        ParameterValue::Json(value) => Arc::new(StringArray::from(vec![value.to_string()])),
        ParameterValue::Bytes(value) => Arc::new(BinaryArray::from(vec![value.as_slice()])),
        _ => panic!("tipo scalare non previsto dall'oracolo diretto"),
    }
}

/// Un errore di trasporto non e una misura negativa.
///
/// # Panics
/// Se l'errore impedisce di misurare il server.
#[must_use]
pub fn direct_error(
    error: &plenora_database_core::DatabaseError,
    reading: bool,
) -> DirectObservation {
    trace("oracolo diretto", error);
    if reading {
        DirectObservation::Unreadable
    } else {
        DirectObservation::Refused
    }
}

pub(super) async fn measure(
    hooks: &dyn ProbeHooks,
    spec: &ProbeSpec,
    battery: &Battery,
    key: AdmissionKey,
    dimensions: TargetDimensions,
) -> MeasuredKey {
    let mut refused = vec![false; battery.names.len()];
    let mut positions = BTreeMap::new();
    let mut batches = Vec::new();
    let mut readable = true;
    for (row, refusal) in refused.iter_mut().enumerate() {
        let observation = hooks
            .direct_probe(spec, battery.values.as_ref(), row)
            .expect("oracolo selezionato")
            .await
            .expect("misura diretta");
        match observation {
            DirectObservation::Refused => *refusal = true,
            DirectObservation::Unreadable => readable = false,
            DirectObservation::Read(value) => {
                positions.insert(i64::try_from(row).expect("indice"), (batches.len(), 0));
                batches.push(vec![value]);
            }
        }
    }
    let read = readable.then_some((positions, batches));
    let measured = verdict(spec, battery, key, dimensions, &refused, read.as_ref(), 0);
    assert!(ddl(hooks, &hooks.drop_sql()).await, "pulizia oracolo");
    measured
}
