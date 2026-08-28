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
// - needless_pass_by_value: __exit__ deve avere firma esatta (tre Py<PyAny>).
#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value
)]

use crate::arrow_reader::{make_read_operation, open_reader, BatchReader};
use crate::errors::to_py_err;
use crate::py_convert::{
    portable_from_json, rows_to_pylist, scalar_to_python, statement_from_python,
};
use crate::runtime;
use crate::transaction::Transaction;
use plenora_database_core::facade::{execute_portable, execute_portable_returning};
use plenora_database_core::plan::{ObjectRef, Operation};
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::CancellationToken;
use plenora_database_engine::{Engine, Session as EngineSession};
use plenora_db_postgres::PostgresProvider;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::sync::{Arc, Mutex, PoisonError};

use crate::budget::session_budget as default_budget;

/// Mappa `tls_mode` (parametro Python) a `PostgresProvider` configurato.
/// Solo due varianti supportate: `"require"` (default sicuro, WebPKI) e
/// `"insecure_local"` (Disabled, per dev/test locali).
///
/// ADR-011: nomi espliciti, no default booleano che nasconde il rischio.
#[allow(clippy::redundant_pub_crate)]
pub(crate) fn build_provider(tls_mode: &str) -> PyResult<PostgresProvider> {
    match tls_mode {
        "require" => Ok(PostgresProvider::default()),
        "insecure_local" => Ok(PostgresProvider::insecure_local()),
        // Il valore non entra nel messaggio: e un argomento del chiamante, e
        // questo testo diventa un'eccezione Python e finisce nei log.
        _ => Err(PyRuntimeError::new_err(
            "tls_mode non riconosciuto: attesi 'require' o 'insecure_local'",
        )),
    }
}

/// Sessione Postgres. Wrapper thin sopra `PostgresProvider` + DSN + metadata
/// scoperti in probe. È un context manager: `with connect(...) as s: ...`.
#[pyclass(module = "plenora_database._native")]
pub struct Session {
    provider: Arc<PostgresProvider>,
    secret: SecretString,
    capabilities: plenora_database_core::capabilities::ProviderCapabilities,
    server_version: String,
    postgis_version: Option<String>,
    engine_handle: Mutex<Option<EngineSession>>,
    operation_cancellation: CancellationToken,
    closed: bool,
}

impl Session {
    pub(crate) fn from_engine(
        engine: &Engine,
        provider: Arc<PostgresProvider>,
        secret: SecretString,
        capabilities: plenora_database_core::capabilities::ProviderCapabilities,
    ) -> plenora_database_core::Result<Self> {
        let server_version = capabilities.provider_version.clone();
        let postgis_version = capabilities.extension_versions.get("postgis").cloned();
        let session = engine.session()?;
        let operation_cancellation = session.cancellation_token();
        Ok(Self {
            provider,
            secret,
            capabilities,
            server_version,
            postgis_version,
            engine_handle: Mutex::new(Some(session)),
            operation_cancellation,
            closed: false,
        })
    }

    /// Chiama `Provider::inspect(op)` sul runtime tokio globale e ritorna
    /// il documento JSON come `serde_json::Value`.
    fn run_inspect(&self, py: Python<'_>, op: Operation) -> PyResult<serde_json::Value> {
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        let cancel = self.cancellation();
        let inspection = py
            .detach(|| {
                runtime().block_on(async move { provider.inspect(&secret, &op, &cancel).await })
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

    fn cancellation(&self) -> CancellationToken {
        self.operation_cancellation.clone()
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
        let mut guard = self
            .engine_handle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let session = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("sessione consumata da una transazione"))?;
        let cancellation = self.cancellation();
        let result = py.detach(|| {
            runtime().block_on(crate::session_tx::run_engine_transaction(
                session,
                &cancellation,
                work,
            ))
        });
        drop(guard);
        result.map_err(to_py_err)
    }
}

#[pymethods]
impl Session {
    /// Versione del server Postgres (stringa piena come da `server_version`).
    #[getter]
    fn server_version(&self) -> &str {
        &self.server_version
    }

    /// Documento capability effettivamente sondato all'apertura.
    #[getter]
    fn capabilities<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let value = serde_json::to_value(&self.capabilities)
            .map_err(|_| PyRuntimeError::new_err("capability non serializzabili"))?;
        json_value_to_pydict(py, &value)
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
        self.operation_cancellation.cancel();
        self.engine_handle
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
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
        let statement = statement_from_python(sql, params.as_ref())?;
        self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { tx.execute(&statement, cancel).await })
        })
    }

    /// SELECT scalare: **al piu una riga, esattamente una colonna**.
    ///
    /// `None` quando la query non restituisce righe. Piu di una riga, o piu di
    /// una colonna, sono un errore: selezionare un valore arbitrario
    /// scarterebbe in silenzio il resto del result set.
    ///
    /// E' la stessa cardinalita dei costruttori scalar del core. Chi vuole la
    /// prima riga di un result set piu ampio usa `execute_returning_rows`.
    #[pyo3(signature = (sql, params=None))]
    fn execute_scalar<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'_, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let statement = statement_from_python(sql, params.as_ref())?;
        let rows = self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { tx.query(&statement, cancel).await })
        })?;
        // Cardinalita imposta, non dedotta: `scalar_opt` rifiuta piu di una
        // riga o piu di una colonna invece di prendere la prima e buttare via
        // il resto. È la stessa regola dei costruttori scalar tipizzati del
        // core e della firma pubblica.
        scalar_to_python(py, rows)
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
        let statement = statement_from_python(sql, params.as_ref())?;
        let rows = self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { tx.query(&statement, cancel).await })
        })?;
        rows_to_pylist(py, rows)
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
        let ast = portable_from_json(ast_json)?;
        let rows = self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { execute_portable_returning(tx, &ast, cancel).await })
        })?;
        rows_to_pylist(py, rows)
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
        let ast = portable_from_json(ast_json)?;
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
        postgres_metrics_to_pydict(py, &self.provider)
    }

    /// Esegue DDL **fuori transazione**, in autocommit.
    ///
    /// Escape hatch per gli statement che il motore non ammette dentro una
    /// transazione. Su PostgreSQL la DDL e transazionale, quindi questa
    /// superficie serve di rado ma resta coerente con le sessioni di famiglia.
    fn execute_ddl(&self, py: Python<'_>, sql: &str) -> PyResult<()> {
        self.ensure_open()?;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        let sql = sql.to_owned();
        let cancel = self.cancellation();
        py.detach(|| {
            runtime().block_on(async move { provider.execute_ddl(&secret, &sql, &cancel).await })
        })
        .map_err(to_py_err)
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
    fn inspect_tables<'py>(&self, py: Python<'py>, schema: &str) -> PyResult<Bound<'py, PyList>> {
        self.ensure_open()?;
        let source = Some(ObjectRef {
            catalog: None,
            schema: Some(schema.to_owned()),
            object: String::new(),
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
    #[pyo3(signature = (schema, object, projection=None, order_by=None, limit=None))]
    fn read(
        &self,
        py: Python<'_>,
        schema: &str,
        object: &str,
        projection: Option<Vec<String>>,
        order_by: Option<Vec<(String, String)>>,
        limit: Option<u64>,
    ) -> PyResult<BatchReader> {
        self.ensure_open()?;
        let projection = projection.unwrap_or_default();
        let order_by = order_by.unwrap_or_default();
        let operation =
            make_read_operation(schema, object, projection, order_by, limit).map_err(to_py_err)?;
        py.detach(|| open_reader(&self.provider, &self.secret, operation, self.cancellation()))
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
    ///       "recovery": None,
    ///     }
    #[pyo3(signature = (
        schema,
        table,
        ipc_bytes,
        mode="append",
        transaction_profile="single_transaction",
        mapping_policy="compatible",
        keys=None,
        update_columns=None,
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
        keys: Option<Vec<String>>,
        update_columns: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        self.ensure_open()?;
        let keys = keys.unwrap_or_default();
        let update_columns = update_columns.unwrap_or_default();
        let result = py.detach(|| {
            crate::write::copy_from_sync(
                &self.provider,
                &self.secret,
                schema,
                table,
                ipc_bytes,
                mode,
                transaction_profile,
                mapping_policy,
                keys,
                update_columns,
                self.cancellation(),
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
        context=None,
        native_query_policy=None,
    ))]
    #[allow(clippy::too_many_arguments)] // API PyO3 keyword — non fattibile compressione
    fn begin(
        &mut self,
        py: Python<'_>,
        isolation: Option<&str>,
        read_only: Option<bool>,
        deferrable: Option<bool>,
        statement_timeout_ms: Option<u64>,
        context: Option<crate::session_context_py::PySessionContext>,
        native_query_policy: Option<&str>,
    ) -> PyResult<Transaction> {
        self.ensure_open()?;
        let opts = crate::session_tx::transaction_options(
            isolation,
            read_only,
            deferrable,
            statement_timeout_ms,
            context,
            native_query_policy,
        )?;
        let operation_cancellation = self.cancellation();
        let session = self
            .engine_handle
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("sessione consumata da una transazione"))?;
        let transaction = py
            .detach(|| {
                runtime().block_on(async move {
                    session
                        .begin_owned_transaction(&opts, &default_budget(), &operation_cancellation)
                        .await
                })
            })
            .map_err(to_py_err)?;
        self.closed = true;
        Ok(Transaction::new(Box::new(transaction)))
    }

    /// Context manager: entrata restituisce self.
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Context manager: uscita chiama close(). Non sopprime eccezioni
    /// (ritorna False, secondo il protocollo Python).
    fn __exit__(
        &mut self,
        _exc_type: Py<PyAny>,
        _exc_value: Py<PyAny>,
        _traceback: Py<PyAny>,
    ) -> bool {
        self.close();
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
pub fn json_to_pylist_of_strings<'py>(
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
pub fn json_to_pylist_of_dicts<'py>(
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
pub fn json_value_to_pydict<'py>(
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

/// Converte lo snapshot locale nella forma condivisa dai bordi sync e async.
#[allow(clippy::redundant_pub_crate)]
pub(crate) fn postgres_metrics_to_pydict<'py>(
    py: Python<'py>,
    provider: &PostgresProvider,
) -> PyResult<Bound<'py, PyDict>> {
    let serialized = serde_json::to_string(&provider.metrics_snapshot())
        .map_err(|_| PyRuntimeError::new_err("metriche non serializzabili"))?;
    let value = py.import("json")?.getattr("loads")?.call1((serialized,))?;
    Ok(value.cast_into::<PyDict>()?)
}

/// Apre una nuova sessione Postgres. La DSN è nel formato libpq
/// (`host=... user=... password=... dbname=...`).
///
/// # TLS
///
/// Default: `tls_mode="require"` + WebPKI trust store pubblico.
/// Per test/dev locali senza TLS passare `tls_mode="insecure_local"`.
/// Per CA private / mTLS costruire il provider Rust in-process (non
/// esposto via `connect(dsn)`).
///
/// # Errors
///
/// Restituisce `PyRuntimeError` con messaggio "<category>: <message>"
/// se il probe iniziale fallisce (tipicamente handshake TLS o auth).
#[pyfunction]
#[pyo3(signature = (dsn, tls_mode="require"))]
pub fn connect(py: Python<'_>, dsn: &str, tls_mode: &str) -> PyResult<Session> {
    let provider = Arc::new(build_provider(tls_mode)?);
    let secret = SecretString::new(dsn.to_owned());
    let provider_for_core: Arc<dyn Provider> = Arc::clone(&provider) as Arc<dyn Provider>;
    let engine = Engine::new(provider_for_core, secret.clone());
    let cancel = CancellationToken::new();
    let engine_for_probe = engine.clone();
    let caps_result = py.detach(|| {
        runtime().block_on(async move { engine_for_probe.capabilities(false, &cancel).await })
    });
    let caps = caps_result.map_err(to_py_err)?;
    Session::from_engine(&engine, provider, secret, caps).map_err(to_py_err)
}
