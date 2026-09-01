//! Binding Python del checkpoint persistente per letture keyset.

use crate::arrow_reader::make_qualified_read_operation;
use crate::errors::to_py_err;
use crate::py_convert::params_from_python;
use plenora_database_core::checkpoint::ReadCheckpoint;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::ParameterBag;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

#[pyclass(
    name = "ReadCheckpoint",
    module = "plenora_database._native",
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyReadCheckpoint {
    inner: ReadCheckpoint,
}

impl PyReadCheckpoint {
    pub(crate) const fn inner(&self) -> &ReadCheckpoint {
        &self.inner
    }
}

#[pymethods]
impl PyReadCheckpoint {
    /// Cattura un token dai valori dell'ultima riga consegnata, nello stesso
    /// ordine di `order_by`. I valori non compaiono mai nel `repr` o negli
    /// errori pubblici.
    #[new]
    #[pyo3(signature = (
        provider,
        schema,
        object,
        order_by,
        values,
        projection=None,
        *,
        catalog=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        provider: &str,
        schema: &str,
        object: &str,
        order_by: Vec<(String, String)>,
        values: &Bound<'_, PyList>,
        projection: Option<Vec<String>>,
        catalog: Option<&str>,
    ) -> PyResult<Self> {
        let provider = parse_provider(provider)?;
        let operation = make_qualified_read_operation(
            catalog,
            schema,
            object,
            projection.unwrap_or_default(),
            order_by,
            None,
        )
        .map_err(to_py_err)?;
        let values = params_from_python(Some(values))?;
        let inner = ReadCheckpoint::new(provider, &operation, &ParameterBag::default(), values)
            .map_err(to_py_err)?;
        Ok(Self { inner })
    }

    #[staticmethod]
    fn from_json(document: &str) -> PyResult<Self> {
        ReadCheckpoint::from_json(document)
            .map(|inner| Self { inner })
            .map_err(to_py_err)
    }

    fn to_json(&self) -> PyResult<String> {
        self.inner.to_json().map_err(to_py_err)
    }

    #[getter]
    const fn provider(&self) -> &'static str {
        provider_name(self.inner.provider)
    }

    #[getter]
    fn catalog(&self) -> Option<&str> {
        self.inner.source.catalog.as_deref()
    }

    #[getter]
    fn schema(&self) -> Option<&str> {
        self.inner.source.schema.as_deref()
    }

    #[getter]
    fn object(&self) -> &str {
        &self.inner.source.object
    }

    #[getter]
    fn order_by(&self) -> Vec<(String, String)> {
        self.inner
            .order_by
            .iter()
            .map(|ordering| {
                (
                    ordering.field.clone(),
                    match ordering.direction {
                        plenora_database_core::plan::SortDirection::Asc => "asc".to_owned(),
                        plenora_database_core::plan::SortDirection::Desc => "desc".to_owned(),
                    },
                )
            })
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "<ReadCheckpoint provider='{}' columns={}>",
            provider_name(self.inner.provider),
            self.inner.order_by.len()
        )
    }
}

fn parse_provider(provider: &str) -> PyResult<ProviderKind> {
    match provider {
        "postgres" => Ok(ProviderKind::Postgres),
        "mysql" => Ok(ProviderKind::Mysql),
        "mariadb" => Ok(ProviderKind::Mariadb),
        "sqlserver" => Ok(ProviderKind::Sqlserver),
        "db2" => Ok(ProviderKind::Db2),
        _ => Err(PyValueError::new_err(
            "provider checkpoint non supportato: attesi postgres, mysql, mariadb, sqlserver o db2",
        )),
    }
}

const fn provider_name(provider: ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Postgres => "postgres",
        ProviderKind::Mysql => "mysql",
        ProviderKind::Mariadb => "mariadb",
        ProviderKind::Sqlserver => "sqlserver",
        ProviderKind::Db2 => "db2",
        ProviderKind::Oracle => "oracle",
        ProviderKind::Sqlite => "sqlite",
        ProviderKind::Duckdb => "duckdb",
    }
}
