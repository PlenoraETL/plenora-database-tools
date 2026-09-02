//! `DatabaseSession` — sessione `MySQL` sincrona del SDK Python.
//!
//! Non e piu uno scaffold: la superficie e quella di `Session` su Postgres,
//! meno spatial.
//!
//! * `connect_mysql(host, database, user, password, port=None,
//!   tls_ca_pem=None, tls_mode="require")`
//! * `execute(sql, params)` -> `affected_rows`
//! * `execute_scalar(sql, params)` -> valore
//! * `execute_returning_rows(sql, params)` -> `list[dict]`
//! * `execute_ddl(sql)` -> `None`
//! * `begin(isolation, read_only, statement_timeout_ms, context,
//!   native_query_policy)` -> `Transaction` con savepoint. `context` accetta
//!   un `SessionContext`; `native_query_policy` vale `allow` o `deny`
//! * `read(schema, object, projection, order_by, limit)` -> `BatchReader`,
//!   streaming Arrow IPC bounded
//! * `copy_from(...)` -> bulk write, sei `WriteMode` su sette:
//!   `TruncateInsert` resta fail-closed perche `TRUNCATE` e DDL con commit
//!   implicito
//! * `execute_portable_rows` / `execute_portable_count`, su cui girano i
//!   builder AST del wrapper Python
//! * `close()`, `__enter__`/`__exit__`, `__repr__`
//!
//! Non esposto: spatial predicates e `SpatialReference`.
//!
//! L'equivalente async e [`crate::async_session_family`].
//!
//! Placeholder `MySQL`: `?` (non `$1` come Postgres). Il consumer deve
//! fornire SQL provider-compatibile.

#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
    clippy::future_not_send,
    clippy::significant_drop_tightening,
    clippy::redundant_pub_crate,
    clippy::unused_self
)]

use crate::arrow_reader::BatchReader;
use crate::checkpoint::PyReadCheckpoint;
use crate::errors::to_py_err;
use crate::family_arrow_reader::open_family_reader;
use crate::py_convert::{
    portable_from_json, rows_to_pylist, scalar_to_python, statement_from_python,
};
use crate::runtime;
use crate::transaction::Transaction;
use plenora_database_core::facade::{execute_portable, execute_portable_returning};
#[cfg(not(feature = "db2"))]
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::plan::{ObjectRef, Operation};
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::transaction::TransactionScope;
use plenora_database_core::CancellationToken;
#[cfg(not(feature = "db2"))]
use plenora_database_core::DatabaseError;
#[cfg(not(feature = "db2"))]
use plenora_database_core::ErrorPhase;
use plenora_database_engine::{Engine, Session as EngineSession};
#[cfg(feature = "db2")]
use plenora_db_db2::{Db2Config, Db2Provider, Db2TlsMode};
use plenora_db_mysql::{MariadbProvider, MysqlCertificatePolicy, MysqlConfig, MysqlProvider};
use plenora_db_sqlserver::{
    CertificatePolicy as SqlServerCertificatePolicy, SqlServerConfig, SqlServerProvider,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::budget::session_budget as default_budget;

/// Sessione della famiglia `MySQL`: `MySQL` o `MariaDB`.
///
/// Il provider e dietro `dyn Provider` perche i due prodotti hanno due tipi
/// distinti — ADR 0014 vieta che sia il server a decidere quale — e questa
/// sessione non ne conosce nessuno dei due: usa il contratto. Cio che sa e
/// **quale** ha in mano, e lo dice ovunque nomini un prodotto: il `repr`, il
/// messaggio della sessione chiusa. Una sessione `MariaDB` che si dichiarasse
/// `MySQL` mentirebbe proprio a chi sta cercando di capire cosa ha aperto.
///
/// Prodotta da `plenora_database.connect_mysql(...)`. Context-manager
/// friendly (`with connect_mysql(...) as s:`).
#[pyclass(module = "plenora_database._native")]
pub struct DatabaseSession {
    provider: Arc<dyn Provider>,
    secret: SecretString,
    capabilities: plenora_database_core::capabilities::ProviderCapabilities,
    /// Il prodotto che questa sessione serve, per le superfici che lo
    /// nominano. Non si deduce da `server_version`: quella e una stringa del
    /// server, e leggerla per decidere sarebbe la selezione automatica che
    /// ADR 0014 esclude.
    product: &'static str,
    /// Il nome della factory da citare quando la sessione e chiusa.
    factory: &'static str,
    server_version: String,
    engine: Engine,
    engine_handle: Mutex<Option<EngineSession>>,
    operation_cancellation: CancellationToken,
    transaction_active: Arc<AtomicBool>,
    closed: bool,
}

impl DatabaseSession {
    pub(crate) fn from_engine(
        engine: &Engine,
        provider: Arc<dyn Provider>,
        secret: SecretString,
        capabilities: plenora_database_core::capabilities::ProviderCapabilities,
        product: &'static str,
        factory: &'static str,
    ) -> plenora_database_core::Result<Self> {
        let session = engine.session()?;
        let operation_cancellation = session.cancellation_token();
        Ok(Self {
            provider,
            secret,
            server_version: capabilities.provider_version.clone(),
            capabilities,
            product,
            factory,
            engine: engine.clone(),
            engine_handle: Mutex::new(Some(session)),
            operation_cancellation,
            transaction_active: Arc::new(AtomicBool::new(false)),
            closed: false,
        })
    }

    fn cancellation(&self) -> CancellationToken {
        self.operation_cancellation.clone()
    }

    /// Esegue una `Operation` di ispezione e rende il documento JSON.
    ///
    /// Passa da `Provider::inspect`, cosi tutti i prodotti seguono lo stesso
    /// contratto e la stessa forma del documento.
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
            return Err(PyRuntimeError::new_err(format!(
                "sessione {} chiusa: aprine una nuova con plenora_database.{}(...)",
                self.product, self.factory
            )));
        }
        if self.transaction_active.load(Ordering::Acquire) {
            return Err(PyRuntimeError::new_err(
                "sessione occupata da una transazione esplicita",
            ));
        }
        Ok(())
    }

    /// Esegue uno statement in una transazione dedicata (auto-commit stile).
    fn run_tx<F, R>(&self, py: Python<'_>, work: F) -> PyResult<R>
    where
        F: for<'a> FnOnce(
                &'a mut dyn TransactionScope,
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
impl DatabaseSession {
    #[getter]
    fn server_version(&self) -> &str {
        &self.server_version
    }

    #[getter]
    fn capabilities<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let value = serde_json::to_value(&self.capabilities)
            .map_err(|_| PyRuntimeError::new_err("capability non serializzabili"))?;
        crate::session::json_value_to_pydict(py, &value)
    }

    #[getter]
    fn is_closed(&self) -> bool {
        self.closed
    }

    fn close(&mut self) {
        self.closed = true;
        self.operation_cancellation.cancel();
        self.engine_handle
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

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
            "<DatabaseSession product='{}' server='{}' closed={}>",
            self.product, self.server_version, self.closed
        )
    }

    /// Esegue DML (INSERT/UPDATE/DELETE) senza rows. Ritorna affected_rows.
    ///
    /// SQL usa placeholder `?` (convenzione MySQL). Params in ordine posizionale.
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
        params: Option<Bound<'py, PyList>>,
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

    /// SELECT con rows → list[dict] (nome colonna → valore Python).
    #[pyo3(signature = (sql, params=None))]
    fn execute_returning_rows<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'py, PyList>>,
    ) -> PyResult<Bound<'py, PyList>> {
        self.ensure_open()?;
        let statement = statement_from_python(sql, params.as_ref())?;
        let rows = self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { tx.query(&statement, cancel).await })
        })?;
        rows_to_pylist(py, rows)
    }

    /// Apre una nuova transazione user-managed su MySQL.
    ///
    /// Uso: `with s.begin() as tx: tx.execute(...); tx.commit()`.
    /// `Transaction` è provider-agnostic (wrapper sopra `dyn TransactionScope`)
    /// e supporta savepoints, conditional_update, execute_returning_rows.
    ///
    /// Opzioni:
    /// - `isolation`: "read_uncommitted" / "read_committed" /
    ///   "repeatable_read" / "serializable" (None = default MySQL)
    /// - `read_only`: True/False (default: False)
    /// - `statement_timeout_ms`: MAX_EXECUTION_TIME session-scoped
    ///
    /// Nota: MySQL non ha `deferrable` — parametro non esposto qui.
    #[pyo3(signature = (
        isolation=None,
        read_only=None,
        statement_timeout_ms=None,
        context=None,
        native_query_policy=None,
    ))]
    #[allow(clippy::too_many_arguments)] // API PyO3 keyword — parity con Postgres
    fn begin(
        &mut self,
        py: Python<'_>,
        isolation: Option<&str>,
        read_only: Option<bool>,
        statement_timeout_ms: Option<u64>,
        context: Option<crate::session_context_py::PySessionContext>,
        native_query_policy: Option<&str>,
    ) -> PyResult<Transaction> {
        self.ensure_open()?;
        let opts = crate::session_tx::transaction_options(
            isolation,
            read_only,
            None,
            statement_timeout_ms,
            context,
            native_query_policy,
        )?;
        let cancellation = self.cancellation();
        if self
            .transaction_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(PyRuntimeError::new_err(
                "sessione occupata da una transazione esplicita",
            ));
        }
        let session = match self.engine.session() {
            Ok(session) => session,
            Err(error) => {
                self.transaction_active.store(false, Ordering::Release);
                return Err(to_py_err(error));
            }
        };
        let transaction = py
            .detach(|| {
                runtime().block_on(async move {
                    session
                        .begin_owned_transaction(&opts, &default_budget(), &cancellation)
                        .await
                })
            })
            .map_err(to_py_err);
        match transaction {
            Ok(transaction) => Ok(Transaction::new(
                Box::new(transaction),
                Arc::clone(&self.transaction_active),
            )),
            Err(error) => {
                self.transaction_active.store(false, Ordering::Release);
                Err(error)
            }
        }
    }

    /// Esegue un PortableStatement (JSON) e ritorna rows come list[dict].
    /// Usato dai builder Python (`s.select(t).where_eq(...).all()`).
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

    /// Esegue un PortableStatement (JSON) senza RETURNING e ritorna
    /// affected_rows. Per Insert/Update/Delete/Upsert MySQL (no RETURNING).
    fn execute_portable_count(&self, py: Python<'_>, ast_json: &str) -> PyResult<u64> {
        self.ensure_open()?;
        let ast = portable_from_json(ast_json)?;
        self.run_tx(py, move |tx, cancel| {
            Box::pin(async move { execute_portable(tx, &ast, cancel).await })
        })
    }

    /// Apre uno stream Arrow IPC su una tabella/vista MySQL.
    ///
    /// Ritorna un `BatchReader` che implementa il Python iterator protocol;
    /// ogni `next(reader)` produce `bytes` Arrow IPC stream self-contained
    /// (schema + 1 record batch + EOS marker).
    ///
    /// Parametri opzionali:
    /// - `projection`: lista di colonne (default: tutte)
    /// - `order_by`: lista di `(colonna, "asc"|"desc")` per ORDER BY
    /// - `limit`: numero massimo di righe (default: nessun limite)
    ///
    /// Uso tipico (richiede pyarrow):
    ///
    /// ```python
    /// import io, pyarrow.ipc as ipc
    /// for chunk in s.read("mydb", "events", limit=10000):
    ///     batch = ipc.open_stream(io.BytesIO(chunk)).read_all()
    /// ```
    ///
    /// La size dei batch è decisa dal provider (MySQL: bounded dal
    /// buffer del cursor `mysql_async`).
    #[pyo3(signature = (
        schema,
        object,
        projection=None,
        order_by=None,
        limit=None,
        *,
        catalog=None,
        checkpoint=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn read(
        &self,
        py: Python<'_>,
        schema: &str,
        object: &str,
        projection: Option<Vec<String>>,
        order_by: Option<Vec<(String, String)>>,
        limit: Option<u64>,
        catalog: Option<&str>,
        checkpoint: Option<PyRef<'_, PyReadCheckpoint>>,
    ) -> PyResult<BatchReader> {
        self.ensure_open()?;
        let projection = projection.unwrap_or_default();
        let order_by = order_by.unwrap_or_default();
        let catalog = catalog.map(str::to_owned);
        let checkpoint = checkpoint.map(|value| value.inner().clone());
        py.detach(|| {
            open_family_reader(
                &self.provider,
                &self.secret,
                schema,
                object,
                projection,
                order_by,
                limit,
                catalog.as_deref(),
                self.capabilities.reads.resumable,
                checkpoint.as_ref(),
                self.cancellation(),
            )
        })
        .map_err(to_py_err)
    }

    /// Bulk write MySQL via `prepare_write` + `write` del provider.
    /// Il consumer Python passa un buffer Arrow IPC stream (schema + N
    /// record batches + EOS).
    ///
    /// **WriteMode supportati** (6 su 7):
    /// - `append` (default)
    /// - `create` (CREATE TABLE + INSERT). `keys` e opzionale e diventa la
    ///   PRIMARY KEY della tabella creata: le colonne indicate devono
    ///   esistere nello schema Arrow, essere **non-nullable** e non
    ///   ripetersi, altrimenti il piano viene rifiutato prima di toccare il
    ///   server
    /// - `replace` (DELETE FROM + INSERT nella stessa transazione: il
    ///   target deve gia esistere e non viene ricreato, quindi schema,
    ///   indici, FK, trigger, check, default, grant e `AUTO_INCREMENT`
    ///   restano quelli di prima)
    /// - `upsert` (INSERT ... ON DUPLICATE KEY UPDATE)
    /// - `update` (UPDATE JOIN staging)
    /// - `delete_by_keys` (DELETE WHERE keys IN staging)
    ///
    /// **Fail-closed** (`PlenoraUnsupportedError`):
    /// - `truncate_insert` — TRUNCATE e DDL con commit implicito, quindi
    ///   non rollback-safe, e non viene emulato con DELETE perche avrebbe
    ///   semantica diversa. Usare `replace`.
    ///
    /// `mapping_policy` deve essere `"strict"` (il provider rifiuta
    /// `"compatible"` con `Unsupported` finché loss preflight non è
    /// qualificato).
    ///
    /// Ritorna dict con struttura `WriteOutcome`:
    /// `{ "status": "committed", "rows": {"received": N, "confirmed": N, ...}, ...}`
    #[pyo3(signature = (
        schema,
        table,
        ipc_bytes,
        mode="append",
        transaction_profile="single_transaction",
        mapping_policy="strict",
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
            crate::family_write::copy_from_sync_family(
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

    /// DDL raw (CREATE/DROP/ALTER). MySQL fa autocommit implicito.
    /// Ritorna l'elenco dei catalog (database) accessibili.
    fn inspect_catalogs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        self.ensure_open()?;
        let doc = self.run_inspect(py, Operation::DatabaseListCatalogs)?;
        crate::session::json_to_pylist_of_strings(py, &doc, "catalogs")
    }

    /// Ritorna l'elenco degli schemas.
    fn inspect_schemas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        self.ensure_open()?;
        let doc = self.run_inspect(py, Operation::DatabaseListSchemas { source: None })?;
        crate::session::json_to_pylist_of_strings(py, &doc, "schemas")
    }

    /// Ritorna la lista degli oggetti nello schema indicato.
    fn inspect_tables<'py>(&self, py: Python<'py>, schema: &str) -> PyResult<Bound<'py, PyList>> {
        self.ensure_open()?;
        let source = Some(ObjectRef {
            catalog: None,
            schema: Some(schema.to_owned()),
            object: String::new(),
        });
        let doc = self.run_inspect(py, Operation::DatabaseListObjects { source })?;
        crate::session::json_to_pylist_of_dicts(py, &doc, "objects")
    }

    /// Descrive una tabella o vista: schema, colonne, `schema_token`.
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
        crate::session::json_value_to_pydict(py, &doc)
    }

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
}

/// I parametri di connessione, uguali per i tre prodotti che questa sessione
/// serve.
///
/// Esiste perche il **costruttore del provider** sia un argomento invece di
/// una decisione presa qui dentro. Il percorso async sceglieva confrontando
/// una stringa — `if product == "MariaDB"` — e aggiungere un terzo prodotto
/// avrebbe allungato quella catena: un refuso nel nome avrebbe costruito in
/// silenzio il provider sbagliato, e il rifiuto sarebbe arrivato dalla probe
/// con la faccia di un problema di configurazione del server.
pub(crate) struct Endpoint {
    pub host: String,
    pub database: String,
    pub user: String,
    pub secret: SecretString,
    pub port: Option<u16>,
    pub tls_ca_pem: Option<Vec<u8>>,
    // I wheel standard conservano la forma comune dell'endpoint ma non
    // costruiscono il provider DB2 che consuma questo campo.
    #[cfg_attr(not(feature = "db2"), allow(dead_code))]
    pub tls_ca_path: Option<PathBuf>,
    pub tls_mode: String,
    pub max_connections: usize,
    pub acquire_timeout_ms: u64,
}

/// Come si costruisce il provider di un prodotto dai suoi parametri.
///
/// Una funzione e non un enum: l'insieme dei prodotti non e chiuso, e ogni
/// riga qui sotto e l'unico punto in cui quel prodotto viene scelto.
pub(crate) type ProviderBuilder = fn(Endpoint) -> PyResult<Arc<dyn Provider>>;

/// Il provider `MySQL`.
pub(crate) fn mysql_provider(endpoint: Endpoint) -> PyResult<Arc<dyn Provider>> {
    let config = family_config(
        &endpoint.host,
        &endpoint.database,
        &endpoint.user,
        &endpoint.secret,
        endpoint.port,
        endpoint.tls_ca_pem,
        &endpoint.tls_mode,
    )?
    .with_timeouts(
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_millis(endpoint.acquire_timeout_ms),
    );
    Ok(Arc::new(
        MysqlProvider::new(config, endpoint.max_connections).map_err(to_py_err)?,
    ))
}

/// Il provider `MariaDB`.
pub(crate) fn mariadb_provider(endpoint: Endpoint) -> PyResult<Arc<dyn Provider>> {
    let config = family_config(
        &endpoint.host,
        &endpoint.database,
        &endpoint.user,
        &endpoint.secret,
        endpoint.port,
        endpoint.tls_ca_pem,
        &endpoint.tls_mode,
    )?
    .with_timeouts(
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_millis(endpoint.acquire_timeout_ms),
    );
    Ok(Arc::new(
        MariadbProvider::new(config, endpoint.max_connections).map_err(to_py_err)?,
    ))
}

/// Il provider `SQL Server`.
///
/// La configurazione e un tipo diverso — `SqlServerConfig` invece di
/// `MysqlConfig` — perche il protocollo e un altro: TDS, porta 1433, e un
/// nome applicativo che il server registra. Cio che **non** cambia e la
/// sessione: tiene `Arc<dyn Provider>` e non sa quale prodotto le sia stato
/// dato, quindi non c'e una terza copia della sua superficie.
///
/// Il fail-close TLS e lo stesso dei due prodotti della famiglia, e per la
/// stessa ragione: `require` verifica catena e nome host,
/// `insecure_trust_server` e un opt-out esplicito. Il default verifica.
pub(crate) fn sqlserver_provider(endpoint: Endpoint) -> PyResult<Arc<dyn Provider>> {
    let mut config = SqlServerConfig::new(
        &endpoint.host,
        &endpoint.database,
        &endpoint.user,
        endpoint.secret.clone(),
    );
    if let Some(port) = endpoint.port {
        config = config.with_port(port);
    }
    if let Some(pem) = endpoint.tls_ca_pem {
        if pem.len() > 1024 * 1024 {
            return Err(PyRuntimeError::new_err("CA PEM oltre 1 MiB"));
        }
        config = config
            .with_private_ca_certificate_pem(&pem)
            .map_err(to_py_err)?;
    }
    config =
        match endpoint.tls_mode.as_str() {
            "require" => config,
            "insecure_trust_server" => {
                config.with_certificate_policy(SqlServerCertificatePolicy::TrustServerCertificate)
            }
            _ => return Err(PyRuntimeError::new_err(
                "tls_mode non riconosciuto. Valori: 'require' (default) | 'insecure_trust_server'",
            )),
        };
    config = config.with_timeouts(
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_millis(endpoint.acquire_timeout_ms),
    );
    // Batch e pool: gli stessi valori del percorso MySQL, per la stessa
    // ragione — sono il profilo del SDK, non una caratteristica del prodotto.
    Ok(Arc::new(
        SqlServerProvider::new(config, 1024, endpoint.max_connections).map_err(to_py_err)?,
    ))
}

/// Il provider `IBM Db2 LUW` attraverso il client ODBC ufficiale.
///
/// A differenza dei provider RustLS, il client IBM riceve il percorso della
/// CA e puo rileggerlo a ogni nuova connessione; per questo la factory accetta
/// un path persistente invece di copiare bytes PEM nella configurazione.
#[cfg(feature = "db2")]
pub(crate) fn db2_provider(endpoint: Endpoint) -> PyResult<Arc<dyn Provider>> {
    if endpoint.tls_ca_pem.is_some() {
        return Err(PyRuntimeError::new_err(
            "Db2 richiede tls_ca_path, non bytes PEM",
        ));
    }
    let mut config = Db2Config::new(&endpoint.host, &endpoint.database, &endpoint.user);
    if let Some(port) = endpoint.port {
        config = config.with_port(port);
    }
    if let Some(path) = endpoint.tls_ca_path {
        let absolute = std::fs::canonicalize(path)
            .map_err(|_| PyRuntimeError::new_err("CA Db2 non leggibile"))?;
        config = config.with_private_ca_certificate(absolute);
    }
    config = match endpoint.tls_mode.as_str() {
        "require" => config,
        "disable" => config.with_tls_mode(Db2TlsMode::Disable),
        _ => {
            return Err(PyRuntimeError::new_err(
                "tls_mode Db2 non riconosciuto. Valori: 'require' (default) | 'disable'",
            ))
        }
    };
    Ok(Arc::new(Db2Provider::new(config).map_err(to_py_err)?))
}

/// Apre una connessione MySQL e produce una `DatabaseSession`.
///
/// Parametri:
/// - `host`, `database`, `user`, `password`
/// - `port`: opzionale, default 3306
/// - `tls_ca_pem`: opzionale, bytes del certificato CA privato PEM. Se
///   `None`, la verifica usa il trust store pubblico `WebPKI`
/// - `tls_mode`: `require` (default) verifica il certificato del server;
///   `insecure_trust_server` la disattiva ed e opt-in esplicito per
///   test e sviluppo locale
///
/// Il default verifica il certificato; la disattivazione richiede un valore
/// esplicito di `tls_mode`.
///
/// # Errors
///
/// `PlenoraError` se la configurazione è invalida, la connessione fallisce,
/// o il probe capabilities restituisce errore.
#[pyfunction]
#[pyo3(signature = (host, database, user, password, port=None, tls_ca_pem=None, tls_mode="require"))]
pub fn connect_mysql(
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_pem: Option<Vec<u8>>,
    tls_mode: &str,
) -> PyResult<DatabaseSession> {
    let secret = SecretString::new(password.to_owned());
    let provider = mysql_provider(Endpoint {
        host: host.to_owned(),
        database: database.to_owned(),
        user: user.to_owned(),
        secret: secret.clone(),
        port,
        tls_ca_pem,
        tls_ca_path: None,
        tls_mode: tls_mode.to_owned(),
        max_connections: 4,
        acquire_timeout_ms: 10_000,
    })?;
    open_family_session(provider, secret, "MySQL", "connect_mysql")
}

/// La configurazione comune ai due prodotti, TLS compreso.
///
/// Sta in un posto solo perche il fail-close TLS non puo permettersi due
/// copie. Il default non accetta `TrustServerCertificate` quando la CA manca:
/// TLS deve verificare il certificato del server. Il default
/// **verifica**, e `insecure_trust_server` e un opt-in esplicito per test e
/// sviluppo locale. Duplicarlo per `MariaDB` avrebbe rimesso in gioco quel
/// difetto sul secondo prodotto.
///
/// # Errors
///
/// `PlenoraError` per una CA oltre 1 MiB o un `tls_mode` non riconosciuto.
pub(crate) fn family_config(
    host: &str,
    database: &str,
    user: &str,
    secret: &SecretString,
    port: Option<u16>,
    tls_ca_pem: Option<Vec<u8>>,
    tls_mode: &str,
) -> PyResult<MysqlConfig> {
    let mut config = MysqlConfig::new(host, database, user, secret.clone());
    if let Some(p) = port {
        config = config.with_port(p);
    }
    if let Some(pem) = tls_ca_pem {
        if pem.len() > 1024 * 1024 {
            return Err(PyRuntimeError::new_err("CA PEM oltre 1 MiB"));
        }
        config = config.with_private_ca_certificate_pem(pem);
    }
    match tls_mode {
        "require" => Ok(config),
        "insecure_trust_server" => {
            Ok(config.with_certificate_policy(MysqlCertificatePolicy::TrustServerCertificate))
        }
        _ => Err(PyRuntimeError::new_err(
            "tls_mode non riconosciuto. Valori: 'require' (default) | 'insecure_trust_server'",
        )),
    }
}

/// Apre una connessione `MariaDB` e produce una `DatabaseSession`.
///
/// Una factory sua, non un parametro di [`connect_mysql`], ed e la meta di
/// ADR 0014 che riguarda il SDK: «nessuna selezione automatica». Il
/// consumatore dichiara il prodotto, e la probe verifica quella scelta invece
/// di compierla — `connect_mysql` puntata su `MariaDB` viene rifiutata, e
/// questa puntata su `MySQL` pure.
///
/// Gli argomenti sono gli stessi perche i due prodotti parlano lo stesso
/// protocollo. Cio che cambia e il provider costruito, e con lui il profilo
/// che decide le query di catalogo, l'istruzione di timeout, i metadata
/// pubblicati e la classificazione dei codici server.
///
/// # Errors
///
/// Come [`connect_mysql`], piu il rifiuto della probe se il server non e
/// `MariaDB`.
#[pyfunction]
#[pyo3(signature = (host, database, user, password, port=None, tls_ca_pem=None, tls_mode="require"))]
#[allow(clippy::too_many_arguments)] // API PyO3 keyword — parity con connect_mysql
pub fn connect_mariadb(
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_pem: Option<Vec<u8>>,
    tls_mode: &str,
) -> PyResult<DatabaseSession> {
    let secret = SecretString::new(password.to_owned());
    let provider = mariadb_provider(Endpoint {
        host: host.to_owned(),
        database: database.to_owned(),
        user: user.to_owned(),
        secret: secret.clone(),
        port,
        tls_ca_pem,
        tls_ca_path: None,
        tls_mode: tls_mode.to_owned(),
        max_connections: 4,
        acquire_timeout_ms: 10_000,
    })?;
    open_family_session(provider, secret, "MariaDB", "connect_mariadb")
}

/// Sonda il server e costruisce la sessione, per entrambi i prodotti.
///
/// La probe non e una formalita: e il punto in cui il provider verifica di
/// avere davanti il motore che il consumatore ha dichiarato, e una `connect_*`
/// che la saltasse restituirebbe una sessione utilizzabile contro il prodotto
/// sbagliato.
fn open_family_session(
    provider: Arc<dyn Provider>,
    secret: SecretString,
    product: &'static str,
    factory: &'static str,
) -> PyResult<DatabaseSession> {
    let engine = Engine::new(Arc::clone(&provider), secret.clone());
    let engine_for_probe = engine.clone();
    let capabilities = runtime()
        .block_on(async move {
            engine_for_probe
                .capabilities(false, &CancellationToken::new())
                .await
        })
        .map_err(to_py_err)?;
    DatabaseSession::from_engine(&engine, provider, secret, capabilities, product, factory)
        .map_err(to_py_err)
}

/// Apre una connessione `SQL Server` e produce una sessione della famiglia.
///
/// Una factory sua, come per `MariaDB`, e per la stessa meta di ADR 0014 che
/// riguarda il SDK: nessuna selezione automatica. Il consumatore dichiara il
/// prodotto, e la probe verifica quella scelta invece di compierla.
///
/// Parametri:
/// - `host`, `database`, `user`, `password`
/// - `port`: opzionale, default 1433
/// - `tls_ca_pem`: opzionale, bytes del certificato CA privato PEM. Se
///   `None`, la verifica usa il trust store pubblico
/// - `tls_mode`: `require` (default) verifica catena e nome host;
///   `insecure_trust_server` la disattiva ed e opt-in esplicito
///
/// Il default **verifica**, come sugli altri tre motori del SDK.
///
/// # Errors
///
/// `PlenoraError` se la configurazione e invalida, la connessione fallisce, o
/// la probe delle capability restituisce errore.
#[pyfunction]
#[pyo3(signature = (host, database, user, password, port=None, tls_ca_pem=None, tls_mode="require"))]
#[allow(clippy::too_many_arguments)] // API PyO3 keyword — parity con connect_mysql
pub fn connect_sqlserver(
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_pem: Option<Vec<u8>>,
    tls_mode: &str,
) -> PyResult<DatabaseSession> {
    let secret = SecretString::new(password.to_owned());
    let provider = sqlserver_provider(Endpoint {
        host: host.to_owned(),
        database: database.to_owned(),
        user: user.to_owned(),
        secret: secret.clone(),
        port,
        tls_ca_pem,
        tls_ca_path: None,
        tls_mode: tls_mode.to_owned(),
        max_connections: 4,
        acquire_timeout_ms: 10_000,
    })?;
    open_family_session(provider, secret, "SQL Server", "connect_sqlserver")
}

/// Apre una sessione `IBM Db2 LUW` sulla superficie comune del SDK.
///
/// `tls_mode="require"` e il default sicuro. `disable` disabilita TLS e deve
/// essere richiesto esplicitamente per fixture o ambienti locali. La CA
/// privata e un percorso persistente, non un payload copiato nel binding.
///
/// # Errors
///
/// `PlenoraError` se configurazione, driver ODBC, connessione o probe falliscono.
#[pyfunction]
#[pyo3(signature = (host, database, user, password, port=None, tls_ca_path=None, tls_mode="require"))]
#[allow(clippy::too_many_arguments)]
#[cfg(feature = "db2")]
pub fn connect_db2(
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_path: Option<PathBuf>,
    tls_mode: &str,
) -> PyResult<DatabaseSession> {
    let secret = SecretString::new(password.to_owned());
    let provider = db2_provider(Endpoint {
        host: host.to_owned(),
        database: database.to_owned(),
        user: user.to_owned(),
        secret: secret.clone(),
        port,
        tls_ca_pem: None,
        tls_ca_path,
        tls_mode: tls_mode.to_owned(),
        max_connections: 1,
        acquire_timeout_ms: 10_000,
    })?;
    open_family_session(provider, secret, "IBM Db2 LUW", "connect_db2")
}

/// Stub fail-closed dei wheel standard, che non incorporano il runtime ODBC.
///
/// Conserva la stessa API del wheel DB2: il codice applicativo riceve un
/// errore tipizzato e azionabile invece di un `ImportError` dipendente dalla
/// piattaforma.
#[pyfunction]
#[pyo3(signature = (host, database, user, password, port=None, tls_ca_path=None, tls_mode="require"))]
#[allow(clippy::too_many_arguments)]
#[cfg(not(feature = "db2"))]
pub fn connect_db2(
    host: &str,
    database: &str,
    user: &str,
    password: &str,
    port: Option<u16>,
    tls_ca_path: Option<PathBuf>,
    tls_mode: &str,
) -> PyResult<DatabaseSession> {
    let _ = (host, database, user, password, port, tls_ca_path, tls_mode);
    Err(to_py_err(DatabaseError::unsupported(
        ProviderKind::Db2,
        ErrorPhase::Prepare,
        "supporto Db2 non incluso in questo wheel; usa un artefatto costruito con la feature 'db2'",
    )))
}
