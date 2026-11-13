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
    status: Status,
    error: Option<DatabaseError>,
    read_loss: Option<plenora_database_core::read_loss::ReadLossReport>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Status {
    #[default]
    Open,
    Complete,
    Failed,
    Interrupted,
}

impl StreamState {
    pub fn close(&self) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.status == Status::Open {
            state.status = Status::Interrupted;
            if let Some(report) = &mut state.read_loss {
                report.status = plenora_database_core::read_loss::ReadStatus::Interrupted;
            }
            let mut error = Self::interrupted();
            error.read_loss = state.read_loss.clone().map(Box::new);
            state.error = Some(error);
        }
    }

    pub fn interrupted() -> DatabaseError {
        DatabaseError::new(
            plenora_database_core::ErrorCategory::Cancelled,
            plenora_database_core::ErrorPhase::Read,
            None,
            "lettura Arrow interrotta",
        )
    }

    pub fn error(&self) -> Option<DatabaseError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .error
            .clone()
    }

    pub fn report(&self, report: Option<plenora_database_core::read_loss::ReadLossReport>) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.status == Status::Open {
            state.read_loss = report;
        }
    }

    #[cfg(test)]
    pub fn observe<T: Clone>(&self, result: &Result<Option<T>, DatabaseError>) {
        let _ = self.publish(result.clone());
    }

    pub fn publish<T>(
        &self,
        mut result: Result<Option<T>, DatabaseError>,
    ) -> Result<Option<T>, DatabaseError> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(error) = &state.error {
            return Err(error.clone());
        }
        if state.status == Status::Open {
            match &mut result {
                Err(error) => {
                    if state.read_loss.is_none() {
                        state.read_loss = error.read_loss.as_deref().cloned();
                    }
                    state.status = Status::Failed;
                    if let Some(report) = &mut state.read_loss {
                        if report.status == plenora_database_core::read_loss::ReadStatus::Open {
                            report.status = plenora_database_core::read_loss::ReadStatus::Failed;
                        }
                    }
                    if error.read_loss.is_none() {
                        error.read_loss = state.read_loss.clone().map(Box::new);
                    }
                    state.error = Some(error.clone());
                }
                Ok(None) => {
                    state.status = Status::Complete;
                    if let Some(report) = &mut state.read_loss {
                        report.status = plenora_database_core::read_loss::ReadStatus::Complete;
                    }
                }
                Ok(Some(_)) => (),
            }
        }
        result
    }

    pub fn record_failure(&self, mut error: DatabaseError) -> DatabaseError {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(original) = &state.error {
            return original.clone();
        }
        if state.read_loss.is_none() {
            state.read_loss = error.read_loss.as_deref().cloned();
        }
        let interrupted = error.category == plenora_database_core::ErrorCategory::Cancelled;
        state.status = if interrupted {
            Status::Interrupted
        } else {
            Status::Failed
        };
        if let Some(report) = &mut state.read_loss {
            report.status = if interrupted {
                plenora_database_core::read_loss::ReadStatus::Interrupted
            } else {
                plenora_database_core::read_loss::ReadStatus::Failed
            };
        }
        if error.read_loss.is_none() {
            error.read_loss = state.read_loss.clone().map(Box::new);
        }
        state.error = Some(error.clone());
        drop(state);
        error
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
            match state.status {
                Status::Open => "open",
                Status::Complete => "complete",
                Status::Failed => "failed",
                Status::Interrupted => "interrupted",
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
