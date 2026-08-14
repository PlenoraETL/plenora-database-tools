//! Session Postgres esposta a Python.
//!
//! Il consumer Python fa:
//!
//! ```python
//! import plenora_database
//! with plenora_database.connect(dsn="host=localhost user=me dbname=app") as s:
//!     print(s.server_version, s.postgis_version)
//!     affected = s.execute("INSERT INTO t(x) VALUES ($1)", [42])
//!     value = s.execute_scalar("SELECT COUNT(*)::BIGINT FROM t")
//!     rows = s.execute_returning_rows("SELECT id, name FROM t WHERE id = $1", [1])
//! ```
//!
//! Runtime tokio globale (`OnceLock`) condiviso da tutte le Session: evita
//! di ricreare un runtime per ogni chiamata e permette di riusare il pool
//! di worker thread di tokio. Non è mai droppato durante la vita del
//! processo Python.
//!
//! Ogni chiamata `execute*` apre una transazione dedicata, esegue lo
//! statement e committa; questo dà semantica auto-commit stile psycopg
//! `autocommit=True`. Le transazioni esplicite gestite dall'utente
//! (`with s.begin() as tx:`) sono milestone F3-5.

// Suppressioni per idiomi PyO3:
// - doc_markdown: firma dei pymethod cita nomi Python (close, __enter__)
//   che non sono item Rust e non vogliamo backtick-are ovunque.
// - missing_const_for_fn: i #[pymethods] non possono essere const per via
//   dei macro attributi di pyo3.
// - needless_pass_by_value: __exit__ deve avere firma esatta (tre PyObject).
#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
)]

use crate::arrow_reader::{open_reader, BatchReader};
use crate::errors::to_py_err;
use crate::py_convert::{param_to_python, params_from_python};
use crate::runtime;
use crate::transaction::{parse_isolation, Transaction};
use plenora_database_core::facade::{execute_portable, execute_portable_returning};
use plenora_database_core::plan::{ObjectRef, Operation};
use plenora_database_core::portable::PortableStatement;
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{AccessMode, Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, DatabaseError, Row};
use plenora_db_postgres::PostgresProvider;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::sync::Arc;

fn default_budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("default budget")
}

/// Sessione Postgres. Wrapper thin sopra `PostgresProvider` + DSN + metadata
/// scoperti in probe. È un context manager: `with connect(...) as s: ...`.
#[pyclass(module = "plenora_database._native")]
pub struct Session {
    provider: Arc<PostgresProvider>,
    secret: SecretString,
    server_version: String,
    postgis_version: Option<String>,
    closed: bool,
}

impl Session {
    /// Chiama `Provider::inspect(op)` sul runtime tokio globale e ritorna
    /// il documento JSON come `serde_json::Value`.
    fn run_inspect(
        &self,
        py: Python<'_>,
        op: Operation,
    ) -> PyResult<serde_json::Value> {
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        let cancel = CancellationToken::new();
        let inspection = py
            .allow_threads(|| {
                runtime().block_on(async move {
                    provider.inspect(&secret, &op, &cancel).await
                })
            })
            .map_err(to_py_err)?;
        Ok(inspection.document)
    }

    fn ensure_open(&self) -> PyResult<()> {
        if self.closed {
            return Err(PyRuntimeError::new_err(
                "sessione chiusa: aprine una nuova con plenora_database.connect(...)",
            ));
        }
        Ok(())
    }

    /// Esegue uno statement in una transazione dedicata e committa.
    fn run_tx<F, R>(&self, py: Python<'_>, work: F) -> PyResult<R>
    where
        F: for<'a> FnOnce(
                &'a mut dyn plenora_database_core::transaction::TransactionScope,
                &'a CancellationToken,
            ) -> plenora_database_core::provider::ProviderFuture<'a, R>
            + Send
            + 'static,
        R: Send + 'static,
    {
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        py.allow_threads(|| {
            runtime().block_on(async move {
                let cancel = CancellationToken::new();
                let mut tx = provider
                    .begin_transaction(&secret, &TransactionOptions::default(), &default_budget(), &cancel)
                    .await?;
                let result = work(tx.as_mut(), &cancel).await;
                match result {
                    Ok(value) => {
                        let outcome = Box::new(tx).commit(&cancel).await?;
                        if !outcome.is_committed() {
                            return Err(DatabaseError {
                                category: plenora_database_core::ErrorCategory::Internal,
                                phase: plenora_database_core::ErrorPhase::Write,
                                remote_effect: plenora_database_core::RemoteEffect::None,
                                retry: plenora_database_core::RetryDisposition::Never,
                                provider: None,
                                execution_id: None,
                                message: "commit outcome unknown: verificare stato del target".to_owned(),
                                diagnostics: None,
                            });
                        }
                        Ok(value)
                    }
                    Err(e) => {
                        let _ = Box::new(tx).rollback(&cancel).await;
                        Err(e)
                    }
                }
            })
        })
        .map_err(to_py_err)
    }
}

#[pymethods]
impl Session {
    /// Versione del server Postgres (stringa piena come da `server_version`).
    #[getter]
    fn server_version(&self) -> &str {
        &self.server_version
    }

    /// Versione dell'estensione PostGIS se installata sul target, altrimenti None.
    #[getter]
    fn postgis_version(&self) -> Option<&str> {
        self.postgis_version.as_deref()
    }

    /// True se la sessione è stata chiusa (via `close()` o uscendo dal
    /// context manager).
    #[getter]
    fn is_closed(&self) -> bool {
        self.closed
    }

    /// Marca la sessione come chiusa. Idempotente. Le risorse di connessione
    /// vengono rilasciate quando l'oggetto Python viene garbage-collected.
    fn close(&mut self) {
        self.closed = true;
    }

    /// Esegue un statement DML/DDL e ritorna il numero di righe modificate.
    ///
    /// I placeholder sono positional-style Postgres: `$1`, `$2`, ...
    /// I parametri sono una `list` Python (o None) con valori serializzabili
    /// per il type mapping (`py_convert`).
    #[pyo3(signature = (sql, params=None))]
    fn execute(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<u64> {
        self.ensure_open()?;
        let param_values = params_from_python(params.as_ref())?;
        let statement = Statement::new(sql.to_owned()).with_params(param_values);
        self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { tx.execute(&statement, cancel).await })
        })
    }

    /// Esegue una query e ritorna il primo valore (prima riga, prima colonna).
    /// `None` se la query non ritorna righe.
    #[pyo3(signature = (sql, params=None))]
    fn execute_scalar<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let param_values = params_from_python(params.as_ref())?;
        let statement = Statement::new(sql.to_owned()).with_params(param_values);
        let rows: Vec<Row> = self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { tx.query(&statement, cancel).await })
        })?;
        rows.first().and_then(|r| r.get_index(0)).map_or_else(
            || Ok(py.None().into_bound(py)),
            |v| param_to_python(py, v),
        )
    }

    /// Esegue una query e ritorna tutte le righe come lista di dict
    /// (`colonna` → `valore`).
    #[pyo3(signature = (sql, params=None))]
    fn execute_returning_rows<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<Bound<'py, PyList>> {
        self.ensure_open()?;
        let param_values = params_from_python(params.as_ref())?;
        let statement = Statement::new(sql.to_owned()).with_params(param_values);
        let rows: Vec<Row> = self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { tx.query(&statement, cancel).await })
        })?;
        let out = PyList::empty(py);
        for row in rows {
            let dict = PyDict::new(py);
            for (col, val) in row.columns().iter().zip(row.values().iter()) {
                dict.set_item(col.as_str(), param_to_python(py, val)?)?;
            }
            out.append(dict)?;
        }
        Ok(out)
    }

    /// Esegue un `PortableStatement` (serializzato come JSON) e ritorna
    /// le righe come `list[dict]`. Usato dal layer di builder Python
    /// (`plenora_database.query`) per Select o statement con RETURNING.
    ///
    /// # Errors
    ///
    /// - `PyRuntimeError` se il JSON non è un AST valido
    /// - `PyRuntimeError` mappato da `DatabaseError` in caso di errore SQL
    fn execute_portable_rows<'py>(
        &self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyList>> {
        self.ensure_open()?;
        let ast: PortableStatement = serde_json::from_str(ast_json).map_err(|e| {
            to_py_err(DatabaseError::invalid_plan(format!("AST portable non valida: {e}")))
        })?;
        let rows: Vec<Row> = self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { execute_portable_returning(tx, &ast, cancel).await })
        })?;
        let out = PyList::empty(py);
        for row in rows {
            let dict = PyDict::new(py);
            for (col, val) in row.columns().iter().zip(row.values().iter()) {
                dict.set_item(col.as_str(), param_to_python(py, val)?)?;
            }
            out.append(dict)?;
        }
        Ok(out)
    }

    /// Esegue un `PortableStatement` (serializzato come JSON) senza
    /// RETURNING e ritorna il numero di righe modificate. Solo per
    /// Insert/Update/Delete/Upsert privi di RETURNING.
    ///
    /// # Errors
    ///
    /// Come `execute_portable_rows`.
    fn execute_portable_count(&self, py: Python<'_>, ast_json: &str) -> PyResult<u64> {
        self.ensure_open()?;
        let ast: PortableStatement = serde_json::from_str(ast_json).map_err(|e| {
            to_py_err(DatabaseError::invalid_plan(format!("AST portable non valida: {e}")))
        })?;
        self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { execute_portable(tx, &ast, cancel).await })
        })
    }

    /// Snapshot dei contatori interni del `PostgresProvider`
    /// (pool_checkouts, schema_cache_hits/misses, catalog_introspections,
    /// read_rows/bytes, writes_committed, ecc.). Utile per osservabilità
    /// oncall.
    ///
    /// Ritorna un dict con ~25 chiavi u64.
    fn metrics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let snap = self.provider.metrics_snapshot();
        let value = serde_json::to_value(snap).map_err(|e| {
            PyRuntimeError::new_err(format!("metrics serialize: {e}"))
        })?;
        let json_str = value.to_string();
        let json_mod = py.import("json")?;
        let obj = json_mod.getattr("loads")?.call1((json_str,))?;
        Ok(obj.downcast_into::<PyDict>()?)
    }

    /// Ritorna l'elenco dei catalog (database) accessibili.
    fn inspect_catalogs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        self.ensure_open()?;
        let doc = self.run_inspect(py, Operation::DatabaseListCatalogs)?;
        json_to_pylist_of_strings(py, &doc, "catalogs")
    }

    /// Ritorna l'elenco degli schemas (filtrati dai system schemas).
    fn inspect_schemas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        self.ensure_open()?;
        let doc = self.run_inspect(py, Operation::DatabaseListSchemas { source: None })?;
        json_to_pylist_of_strings(py, &doc, "schemas")
    }

    /// Ritorna la lista degli oggetti (tabelle, viste, materialized views,
    /// foreign tables, partition parents) nello schema indicato. Ogni
    /// entry ha `{name, kind, is_partition}`.
    fn inspect_tables<'py>(
        &self,
        py: Python<'py>,
        schema: &str,
    ) -> PyResult<Bound<'py, PyList>> {
        self.ensure_open()?;
        let source = Some(ObjectRef {
            catalog: None,
            schema: Some(schema.to_owned()),
            object: String::new(),
            layer_id: None,
        });
        let doc = self.run_inspect(py, Operation::DatabaseListObjects { source })?;
        json_to_pylist_of_dicts(py, &doc, "objects")
    }

    /// Descrive una tabella/vista: ritorna dict con schema, columns,
    /// schema_token (fingerprint strutturale).
    fn inspect_describe<'py>(
        &self,
        py: Python<'py>,
        schema: &str,
        object: &str,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.ensure_open()?;
        let source = ObjectRef {
            catalog: None,
            schema: Some(schema.to_owned()),
            object: object.to_owned(),
            layer_id: None,
        };
        let doc = self.run_inspect(py, Operation::DatabaseDescribeObject { source })?;
        json_value_to_pydict(py, &doc)
    }

    /// Apre uno stream di record batch Arrow su una tabella/vista.
    ///
    /// Ritorna un `BatchReader` che implementa il Python iterator
    /// protocol: ogni `next(reader)` produce `bytes` Arrow IPC stream
    /// self-contained (schema + 1 record batch + EOS).
    ///
    /// Uso:
    ///
    ///     import io, pyarrow.ipc as ipc
    ///     for chunk in s.read("public", "large_table"):
    ///         batch = ipc.open_stream(io.BytesIO(chunk)).read_all()
    ///
    /// Non carica tutto il dataset in memoria: legge batch-by-batch
    /// dal cursor server-side. Sblocca la migrazione dal CLI
    /// `postgres-read-ipc` per query >1M righe.
    #[pyo3(signature = (schema, object))]
    fn read(
        &self,
        py: Python<'_>,
        schema: &str,
        object: &str,
    ) -> PyResult<BatchReader> {
        self.ensure_open()?;
        py.allow_threads(|| {
            open_reader(&self.provider, &self.secret, schema, object)
        })
        .map_err(to_py_err)
    }

    /// Bulk write via `prepare_write` + `write` del provider Postgres
    /// (usa COPY internamente per mode `append`). Il consumer Python
    /// passa un buffer Arrow IPC stream contenente schema + N record
    /// batches + EOS.
    ///
    /// Ritorna un dict con struttura `WriteOutcome`:
    ///
    ///     {
    ///       "status": "committed",
    ///       "execution_id": "...",
    ///       "provider": "postgres",
    ///       "rows": {"received": N, "confirmed": N, "inserted": N, ...},
    ///       "layer_outcomes": [...],
    ///       "recovery": None,
    ///     }
    #[pyo3(signature = (
        schema,
        table,
        ipc_bytes,
        mode="append",
        transaction_profile="single_transaction",
        mapping_policy="compatible",
    ))]
    #[allow(clippy::too_many_arguments)]
    fn copy_from<'py>(
        &self,
        py: Python<'py>,
        schema: &str,
        table: &str,
        ipc_bytes: &[u8],
        mode: &str,
        transaction_profile: &str,
        mapping_policy: &str,
    ) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        self.ensure_open()?;
        let result = py.allow_threads(|| {
            crate::write::copy_from_sync(
                &self.provider,
                &self.secret,
                schema,
                table,
                ipc_bytes,
                mode,
                transaction_profile,
                mapping_policy,
            )
        });
        crate::write::wrap_outcome(py, result)
    }

    /// Apre una nuova transazione user-managed. Usa `with s.begin() as tx:`
    /// per commit/rollback automatico (rollback su eccezione, commit su
    /// uscita normale).
    ///
    /// Opzioni:
    /// - `isolation`: "read_uncommitted" / "read_committed" /
    ///   "repeatable_read" / "serializable" (None = default sessione)
    /// - `read_only`: True/False (None = default)
    /// - `deferrable`: True/False (solo effettivo con Serializable+ReadOnly)
    /// - `statement_timeout_ms`: timeout per singolo statement
    #[pyo3(signature = (
        isolation=None,
        read_only=None,
        deferrable=None,
        statement_timeout_ms=None,
    ))]
    fn begin(
        &self,
        py: Python<'_>,
        isolation: Option<&str>,
        read_only: Option<bool>,
        deferrable: Option<bool>,
        statement_timeout_ms: Option<u64>,
    ) -> PyResult<Transaction> {
        self.ensure_open()?;
        let mut opts = TransactionOptions::default();
        if let Some(iso) = isolation {
            opts.isolation = Some(parse_isolation(iso)?);
        }
        if let Some(ro) = read_only {
            opts.access_mode = Some(if ro {
                AccessMode::ReadOnly
            } else {
                AccessMode::ReadWrite
            });
        }
        opts.deferrable = deferrable;
        opts.statement_timeout_ms = statement_timeout_ms;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        let tx = py
            .allow_threads(|| {
                runtime().block_on(async move {
                    let cancel = CancellationToken::new();
                    provider
                        .begin_transaction(&secret, &opts, &default_budget(), &cancel)
                        .await
                })
            })
            .map_err(to_py_err)?;
        Ok(Transaction::new(tx))
    }

    /// Context manager: entrata restituisce self.
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Context manager: uscita chiama close(). Non sopprime eccezioni
    /// (ritorna False, secondo il protocollo Python).
    fn __exit__(
        &mut self,
        _exc_type: PyObject,
        _exc_value: PyObject,
        _traceback: PyObject,
    ) -> bool {
        self.closed = true;
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<Session server_version={:?} postgis_version={:?} closed={}>",
            self.server_version, self.postgis_version, self.closed
        )
    }
}

/// Helper: estrae `doc[key]` come `Vec<Value::String>` e la trasforma
/// in `PyList<str>`.
fn json_to_pylist_of_strings<'py>(
    py: Python<'py>,
    doc: &serde_json::Value,
    key: &str,
) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    let Some(arr) = doc.get(key).and_then(|v| v.as_array()) else {
        return Ok(out);
    };
    for item in arr {
        if let Some(s) = item.as_str() {
            out.append(s)?;
        } else if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
            out.append(name)?;
        }
    }
    Ok(out)
}

/// Helper: estrae `doc[key]` come `Vec<dict>` (ogni entry è un JSON
/// object con `{name, kind, is_partition}`).
fn json_to_pylist_of_dicts<'py>(
    py: Python<'py>,
    doc: &serde_json::Value,
    key: &str,
) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    let Some(arr) = doc.get(key).and_then(|v| v.as_array()) else {
        return Ok(out);
    };
    for item in arr {
        let dict = json_value_to_pydict(py, item)?;
        out.append(dict)?;
    }
    Ok(out)
}

/// Converte un `serde_json::Value::Object` in `PyDict`. Se il Value non
/// è un object, ritorna dict vuoto (il caller ha già filtrato).
fn json_value_to_pydict<'py>(
    py: Python<'py>,
    value: &serde_json::Value,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    let Some(obj) = value.as_object() else {
        return Ok(dict);
    };
    let json_mod = py.import("json")?;
    let loads = json_mod.getattr("loads")?;
    for (k, v) in obj {
        let serialized = v.to_string();
        let py_v = loads.call1((serialized,))?;
        dict.set_item(k, py_v)?;
    }
    Ok(dict)
}

/// Apre una nuova sessione Postgres. La DSN è nel formato libpq
/// (`host=... user=... password=... dbname=...`).
///
/// # Errors
///
/// Restituisce `PyRuntimeError` con messaggio "<category>: <message>"
/// se il probe iniziale fallisce.
#[pyfunction]
pub fn connect(py: Python<'_>, dsn: &str) -> PyResult<Session> {
    let provider = Arc::new(PostgresProvider::default());
    let secret = SecretString::new(dsn.to_owned());
    let cancel = CancellationToken::new();
    let provider_for_probe = Arc::clone(&provider);
    let secret_for_probe = SecretString::new(dsn.to_owned());
    let caps_result = py.allow_threads(|| {
        runtime().block_on(async move {
            provider_for_probe
                .probe_capabilities(&secret_for_probe, &cancel)
                .await
        })
    });
    let caps = caps_result.map_err(to_py_err)?;
    Ok(Session {
        provider,
        secret,
        server_version: caps.provider_version,
        postgis_version: caps.extension_versions.get("postgis").cloned(),
        closed: false,
    })
}
