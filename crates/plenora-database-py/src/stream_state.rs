//! Stato posseduto dal reader, indipendente dalla durata della capsule C.
use crate::errors::to_py_err;
use plenora_database_core::DatabaseError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct StreamState(Arc<Mutex<Terminal>>);

#[derive(Clone, Default)]
struct Terminal {
    complete: bool,
    closed: bool,
    error: Option<DatabaseError>,
    read_loss: Option<plenora_database_core::read_loss::ReadLossReport>,
}

impl StreamState {
    pub fn close(&self) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        if let Some(report) = &mut state.read_loss {
            if report.status == plenora_database_core::read_loss::ReadStatus::Open {
                report.status = plenora_database_core::read_loss::ReadStatus::Interrupted;
            }
        }
    }

    pub fn report(&self, report: Option<plenora_database_core::read_loss::ReadLossReport>) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.complete && !state.closed && state.error.is_none() {
            state.read_loss = report;
        }
    }

    pub fn observe<T>(&self, result: &Result<Option<T>, DatabaseError>) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.error.is_none() && !state.closed && !state.complete {
            match result {
                Err(error) => {
                    state.error = Some(error.clone());
                    if let Some(report) = &mut state.read_loss {
                        if report.status == plenora_database_core::read_loss::ReadStatus::Open {
                            report.status = plenora_database_core::read_loss::ReadStatus::Failed;
                        }
                    }
                }
                Ok(None) => state.complete = true,
                Ok(Some(_)) => (),
            }
        }
    }

    pub fn failure(&self, error: DatabaseError) -> PyErr {
        self.observe::<()>(&Err(error.clone()));
        to_py_err(error)
    }

    pub fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let dict = PyDict::new(py);
        dict.set_item("schema_version", 1)?;
        let report = serde_json::to_string(&state.read_loss).map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err("rapporto read_loss non serializzabile")
        })?;
        dict.set_item(
            "read_loss",
            py.import("json")?.call_method1("loads", (report,))?,
        )?;
        dict.set_item(
            "status",
            if state.error.is_some() {
                "failed"
            } else if state.complete {
                "complete"
            } else if state.closed {
                "interrupted"
            } else {
                "open"
            },
        )?;
        if let Some(error) = &state.error {
            dict.set_item("error", to_py_err(error.clone()).value(py))?;
        } else {
            dict.set_item("error", py.None())?;
        }
        Ok(dict)
    }
}

#[cfg(test)]
#[path = "stream_state_tests.rs"]
mod tests;
