//! Conversioni Python ↔ `ParameterValue`.
//!
//! Tabella di supporto (Python → ParameterValue):
//!
//! | Python                | ParameterValue        | Note                            |
//! |-----------------------|-----------------------|---------------------------------|
//! | `None`                | `Null { type_name }`  | type_name = "unknown"           |
//! | `bool`                | `Bool`                |                                 |
//! | `int` (i32 range)     | `I32`                 |                                 |
//! | `int` (i64 range)     | `I64`                 |                                 |
//! | `float`               | `F64`                 |                                 |
//! | `str`                 | `String`              |                                 |
//! | `bytes`, `bytearray`  | `Bytes`               |                                 |
//! | `dict` / `list`       | `Json(Value)`         | via traversal (no `json.dumps`) |
//!
//! In direzione opposta (server → Python):
//!
//! | ParameterValue                                                        | Python  |
//! |-----------------------------------------------------------------------|---------|
//! | `Bool`                                                                | `bool`  |
//! | `I32`, `I64`                                                          | `int`   |
//! | `F64`                                                                 | `float` |
//! | `String`, `Date`, `Timestamp`, `TimestampTz`, `Decimal`, `Uuid`       | `str`   |
//! | `Bytes`                                                               | `bytes` |
//! | `Json`                                                                | `dict` / `list` / scalar |
//! | `Null`                                                                | `None`  |
//!
//! Il mapping ricco di date/timestamp/uuid/Decimal a tipi Python nativi
//! (`datetime.date`, `uuid.UUID`, `decimal.Decimal`) è deferito a una
//! milestone successiva (probabilmente F3-6 insieme all'error mapping).

#![allow(clippy::doc_markdown)]

use crate::errors::to_py_err;
use plenora_database_core::facade::scalar_opt;
use plenora_database_core::portable::PortableStatement;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::transaction::Statement;
use plenora_database_core::{DatabaseError, Row};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyFloat, PyList, PyString};
use pyo3::IntoPyObjectExt;

/// Converte una lista Python di parametri in `Vec<ParameterValue>`.
///
/// # Errors
///
/// Ritorna `PyTypeError` se un elemento ha tipo non supportato.
pub fn params_from_python(params: Option<&Bound<'_, PyList>>) -> PyResult<Vec<ParameterValue>> {
    let Some(list) = params else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(list.len());
    for (i, item) in list.iter().enumerate() {
        out.push(
            python_to_param(&item)
                .map_err(|_| PyTypeError::new_err(format!("parametro #{i} non convertibile")))?,
        );
    }
    Ok(out)
}

/// Converte un singolo valore Python in `ParameterValue`.
///
/// # Errors
///
/// Ritorna `PyTypeError` se il tipo non è supportato.
pub fn python_to_param(value: &Bound<'_, PyAny>) -> PyResult<ParameterValue> {
    // Priorità 1: TypedValue (helper `plenora_database.uuid/date/...`)
    // bypassano l'auto-inference.
    if let Ok(kind) = value.getattr("_plenora_typed_kind") {
        let kind_str: String = kind.extract()?;
        let payload = value.getattr("_plenora_typed_value")?;
        return typed_to_param(&kind_str, &payload);
    }
    // Ordine importante: `bool` è sottoclasse di `int` in Python; senza
    // controllo esplicito, `True`/`False` verrebbero estratti come 1/0.
    if value.is_none() {
        return Ok(ParameterValue::Null {
            type_name: "unknown".to_owned(),
        });
    }
    if let Ok(b) = value.extract::<bool>() {
        return Ok(ParameterValue::Bool(b));
    }
    if value.is_instance_of::<PyFloat>() {
        let f: f64 = value.extract()?;
        return Ok(ParameterValue::F64(f));
    }
    if let Ok(i) = value.extract::<i64>() {
        return i32::try_from(i).map_or_else(
            |_| Ok(ParameterValue::I64(i)),
            |i32v| Ok(ParameterValue::I32(i32v)),
        );
    }
    if value.is_instance_of::<PyString>() {
        let s: String = value.extract()?;
        return Ok(ParameterValue::String(s));
    }
    if let Ok(bytes) = value.downcast::<PyBytes>() {
        return Ok(ParameterValue::Bytes(bytes.as_bytes().to_vec()));
    }
    if value.is_instance_of::<PyDict>() || value.is_instance_of::<PyList>() {
        let json = python_to_json(value)?;
        return Ok(ParameterValue::Json(json));
    }
    Err(PyTypeError::new_err(format!(
        "tipo Python non supportato come parametro: {}",
        type_name_of(value)
    )))
}

/// Costruisce un `ParameterValue` dal tag esplicito di un `TypedValue`
/// Python. I tag validi sono i variant snake_case di `ParameterValue`
/// (`uuid`, `date`, `timestamp`, `timestamp_tz`, `decimal`, `null`).
fn typed_to_param(kind: &str, value: &Bound<'_, PyAny>) -> PyResult<ParameterValue> {
    match kind {
        "uuid" => Ok(ParameterValue::Uuid(value.extract::<String>()?)),
        "date" => Ok(ParameterValue::Date(value.extract::<String>()?)),
        "timestamp" => Ok(ParameterValue::Timestamp(value.extract::<String>()?)),
        "timestamp_tz" => Ok(ParameterValue::TimestampTz(value.extract::<String>()?)),
        "decimal" => Ok(ParameterValue::Decimal(value.extract::<String>()?)),
        "null" => {
            let type_name: String = value
                .downcast::<PyDict>()
                .ok()
                .and_then(|d| d.get_item("type_name").ok().flatten())
                .and_then(|v| v.extract::<String>().ok())
                .ok_or_else(|| {
                    PyTypeError::new_err(
                        "typed null richiede dict {'type_name': '<pg_type>'}",
                    )
                })?;
            Ok(ParameterValue::Null { type_name })
        }
        _ => Err(PyTypeError::new_err(
            "TypedValue kind sconosciuto (attesi: uuid, date, timestamp, timestamp_tz, decimal, null)",
        )),
    }
}

fn type_name_of(value: &Bound<'_, PyAny>) -> String {
    value
        .get_type()
        .name()
        .map_or_else(|_| "<sconosciuto>".to_owned(), |cow| cow.to_string())
}

/// Converte un `ParameterValue` in un oggetto Python (owned reference).
///
/// # Errors
///
/// Ritorna errore solo se una conversione JSON annidata fallisce.
pub fn param_to_python<'py>(
    py: Python<'py>,
    value: &ParameterValue,
) -> PyResult<Bound<'py, PyAny>> {
    match value {
        ParameterValue::Bool(b) => b.into_bound_py_any(py),
        ParameterValue::I32(i) => i.into_bound_py_any(py),
        ParameterValue::I64(i) => i.into_bound_py_any(py),
        ParameterValue::F64(f) => f.into_bound_py_any(py),
        ParameterValue::String(s)
        | ParameterValue::Date(s)
        | ParameterValue::Timestamp(s)
        | ParameterValue::TimestampTz(s)
        | ParameterValue::Decimal(s)
        | ParameterValue::Uuid(s) => s.into_bound_py_any(py),
        ParameterValue::Bytes(b) => Ok(PyBytes::new(py, b).into_any()),
        ParameterValue::Wkb { bytes, .. } => Ok(PyBytes::new(py, bytes).into_any()),
        ParameterValue::Enum { label, .. } => label.into_bound_py_any(py),
        ParameterValue::Json(v) => json_to_python(py, v),
        ParameterValue::Null { .. } => Ok(py.None().into_bound(py)),
    }
}

fn python_to_json(value: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if value.is_none() {
        return Ok(serde_json::Value::Null);
    }
    if let Ok(b) = value.extract::<bool>() {
        return Ok(serde_json::Value::Bool(b));
    }
    if value.is_instance_of::<PyFloat>() {
        let f: f64 = value.extract()?;
        // JSON non ammette NaN o infinito: convertirli in `null` perderebbe
        // informazione, quindi il binding fallisce in modo esplicito.
        // esplicito che dice al consumer di gestire il caso.
        if !f.is_finite() {
            // Il valore *e* il dato: NaN o infinito arrivano da una colonna
            // dell'applicazione, e il messaggio non lo ricopia.
            return Err(PyValueError::new_err(
                "float non-finito non serializzabile a JSON: JSON \
                 standard ammette solo numeri finiti. Sanitizza il valore \
                 lato Python (None, math.isfinite check) prima di passarlo.",
            ));
        }
        return Ok(serde_json::Value::Number(
            serde_json::Number::from_f64(f).expect("valore f64 finito"),
        ));
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(serde_json::Value::Number(serde_json::Number::from(i)));
    }
    if value.is_instance_of::<PyString>() {
        let s: String = value.extract()?;
        return Ok(serde_json::Value::String(s));
    }
    if let Ok(list) = value.downcast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            out.push(python_to_json(&item)?);
        }
        return Ok(serde_json::Value::Array(out));
    }
    if let Ok(dict) = value.downcast::<PyDict>() {
        let mut out = serde_json::Map::with_capacity(dict.len());
        for (key, val) in dict.iter() {
            let key_str: String = key
                .extract()
                .map_err(|_| PyTypeError::new_err("chiavi JSON devono essere stringhe"))?;
            out.insert(key_str, python_to_json(&val)?);
        }
        return Ok(serde_json::Value::Object(out));
    }
    Err(PyTypeError::new_err(format!(
        "tipo Python non serializzabile a JSON: {}",
        type_name_of(value)
    )))
}

/// La forma numerica scelta per un numero JSON.
///
/// Sta fuori da [`json_to_python`] perche la scelta si possa provare senza un
/// interprete Python: la decisione e tutta qui, la conversione e meccanica.
#[derive(Debug, PartialEq)]
pub enum NumberKind {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    /// Un numero che nessuna delle tre forme rappresenta: resta una stringa,
    /// che e meglio di un valore sbagliato.
    Opaque,
}

/// `as_u64()` va provato **prima** di `as_f64()`: un conteggio oltre
/// `i64::MAX` — e i conteggi di `RowCounts` sono tutti `u64` — passava per
/// `as_f64()` ma perderebbe precisione. Python ha interi di precisione
/// arbitraria, quindi il ramo unsigned deve precedere quello float.
pub fn classify_number(n: &serde_json::Number) -> NumberKind {
    n.as_i64().map_or_else(
        || {
            n.as_u64().map_or_else(
                || n.as_f64().map_or(NumberKind::Opaque, NumberKind::Float),
                NumberKind::Unsigned,
            )
        },
        NumberKind::Signed,
    )
}

pub fn json_to_python<'py>(
    py: Python<'py>,
    value: &serde_json::Value,
) -> PyResult<Bound<'py, PyAny>> {
    match value {
        serde_json::Value::Null => Ok(py.None().into_bound(py)),
        serde_json::Value::Bool(b) => b.into_bound_py_any(py),
        serde_json::Value::Number(n) => match classify_number(n) {
            NumberKind::Signed(i) => i.into_bound_py_any(py),
            NumberKind::Unsigned(u) => u.into_bound_py_any(py),
            NumberKind::Float(f) => f.into_bound_py_any(py),
            NumberKind::Opaque => n.to_string().into_bound_py_any(py),
        },
        serde_json::Value::String(s) => s.into_bound_py_any(py),
        serde_json::Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                list.append(json_to_python(py, item)?)?;
            }
            Ok(list.into_any())
        }
        serde_json::Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (k, v) in obj {
                dict.set_item(k, json_to_python(py, v)?)?;
            }
            Ok(dict.into_any())
        }
    }
}

/// Costruisce uno statement canonico da SQL e parametri Python.
pub fn statement_from_python(sql: &str, params: Option<&Bound<'_, PyList>>) -> PyResult<Statement> {
    Ok(Statement::new(sql.to_owned()).with_params(params_from_python(params)?))
}

/// Deserializza l'AST portabile senza esporre nel messaggio il payload JSON.
pub fn portable_from_json(value: &str) -> PyResult<PortableStatement> {
    serde_json::from_str(value).map_err(|error| {
        to_py_err(DatabaseError::invalid_plan(format!(
            "AST portable non valida a riga {}, colonna {}",
            error.line(),
            error.column()
        )))
    })
}

/// Applica la cardinalita scalare comune e converte il valore in Python.
pub fn scalar_to_python(py: Python<'_>, rows: Vec<Row>) -> PyResult<Bound<'_, PyAny>> {
    scalar_opt(rows).map_err(to_py_err)?.as_ref().map_or_else(
        || Ok(py.None().into_bound(py)),
        |value| param_to_python(py, value),
    )
}

/// Converte righe canoniche in `list[dict]` senza indicizzazione posizionale.
pub fn rows_to_pylist(py: Python<'_>, rows: Vec<Row>) -> PyResult<Bound<'_, PyList>> {
    let out = PyList::empty(py);
    for row in rows {
        let dict = PyDict::new(py);
        for (column, value) in row.columns().iter().zip(row.values()) {
            dict.set_item(column.as_str(), param_to_python(py, value)?)?;
        }
        out.append(dict)?;
    }
    Ok(out)
}

#[cfg(test)]
#[path = "py_convert_number_tests.rs"]
mod number_tests;
