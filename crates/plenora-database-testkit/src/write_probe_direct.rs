//! Oracolo del server separato dalle guardie di ammissione del prodotto.
use super::{
    ddl, trace, verdict, AdmissionKey, Arc, ArrayRef, BTreeMap, Battery, BinaryArray, MeasuredKey,
    ProbeHooks, ProbeSpec, StringArray, TargetDimensions,
};
use plenora_database_core::arrow::array::{
    BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
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
        ParameterValue::Bool(value) => Arc::new(BooleanArray::from(vec![*value])),
        ParameterValue::I32(value) => Arc::new(Int64Array::from(vec![i64::from(*value)])),
        ParameterValue::I64(value) => Arc::new(Int64Array::from(vec![*value])),
        ParameterValue::F64(value) => Arc::new(Float64Array::from(vec![*value])),
        ParameterValue::Decimal(value) => {
            let scale = value
                .split_once('.')
                .map_or(0, |(_, fraction)| fraction.len());
            let mantissa = value
                .replace('.', "")
                .parse::<i128>()
                .expect("decimale posizionale riletto");
            Arc::new(
                Decimal128Array::from(vec![mantissa])
                    .with_precision_and_scale(38, i8::try_from(scale).expect("scala riletta"))
                    .expect("decimale riletto"),
            )
        }
        ParameterValue::Date(value) => {
            let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").expect("data riletta");
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoca");
            Arc::new(Date32Array::from(vec![i32::try_from(
                (date - epoch).num_days(),
            )
            .expect("giorni riletti")]))
        }
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

/// I float della batteria arrivano al driver anche se non finiti.
/// La validazione del codec applicativo non e una risposta del server.
#[must_use]
pub fn direct_float(
    array: &dyn plenora_database_core::arrow::array::Array,
    row: usize,
) -> Option<ParameterValue> {
    use plenora_database_core::arrow::array::cast::AsArray;
    use plenora_database_core::arrow::array::types::{Float32Type, Float64Type};
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        plenora_database_core::arrow::schema::DataType::Float32 => Some(ParameterValue::F64(
            f64::from(array.as_primitive::<Float32Type>().value(row)),
        )),
        plenora_database_core::arrow::schema::DataType::Float64 => Some(ParameterValue::F64(
            array.as_primitive::<Float64Type>().value(row),
        )),
        _ => None,
    }
}
