//! `MysqlSession` — sessione `MySQL` sincrona del SDK Python.
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
//! L'equivalente async e [`crate::async_mysql_session`].
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
use crate::errors::to_py_err;
use crate::mysql_arrow_reader::open_mysql_reader;
use crate::py_convert::{param_to_python, params_from_python};
use crate::runtime;
use crate::transaction::{parse_isolation, Transaction};
use plenora_database_core::facade::{execute_portable, execute_portable_returning};
use plenora_database_core::portable::PortableStatement;
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::Row;
// Fase E: ResourceBudget/ResourceLimits ora consumati solo via `budget` module
use plenora_database_core::transaction::{
    AccessMode, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{CancellationToken, DatabaseError};
use plenora_db_mysql::{MysqlCertificatePolicy, MysqlConfig, MysqlProvider};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::sync::Arc;

// Fase E: consolidato in `crate::budget::session_budget`.
use crate::budget::session_budget as default_budget;

/// Sessione MySQL. Wrapper thin sopra `MysqlProvider`.
///
/// Prodotta da `plenora_database.connect_mysql(...)`. Context-manager
/// friendly (`with connect_mysql(...) as s:`).
#[pyclass(module = "plenora_database._native")]
pub struct MysqlSession {
    provider: Arc<MysqlProvider>,
    secret: SecretString,
    server_version: String,
    closed: bool,
}

impl MysqlSession {
    fn ensure_open(&self) -> PyResult<()> {
        if self.closed {
            return Err(PyRuntimeError::new_err(
                "sessione MySQL chiusa: aprine una nuova con plenora_database.connect_mysql(...)",
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
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        py.allow_threads(|| {
            runtime().block_on(async move {
                let cancel = CancellationToken::new();
                let mut tx = provider
                    .begin_transaction(
                        &secret,
                        &TransactionOptions::default(),
                        &default_budget(),
                        &cancel,
                    )
                    .await?;
                let result = work(tx.as_mut(), &cancel).await;
                match result {
                    Ok(value) => {
                        let outcome = Box::new(tx).commit(&cancel).await?;
                        if !outcome.is_committed() {
                            // Fix review #9: helper unico.
                            return Err(crate::errors_commit::commit_outcome_unknown(
                                plenora_database_core::plan::ProviderKind::Mysql,
                            ));
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
impl MysqlSession {
    #[getter]
    fn server_version(&self) -> &str {
        &self.server_version
    }

    #[getter]
    fn is_closed(&self) -> bool {
        self.closed
    }

    fn close(&mut self) {
        self.closed = true;
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: PyObject,
        _exc_value: PyObject,
        _traceback: PyObject,
    ) -> bool {
        self.close();
        false
    }

    fn __repr__(&self) -> String {
        format!(
            "<MysqlSession server='{}' closed={}>",
            self.server_version, self.closed
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
        let params = params_from_python(params.as_ref())?;
        let sql = sql.to_owned();
        self.run_tx(py, move |tx, cancel| {
            Box::pin(async move {
                let stmt = Statement { sql, params };
                tx.execute(&stmt, cancel).await
            })
        })
    }

    /// SELECT scalare — 1 riga × 1 colonna.
    #[pyo3(signature = (sql, params=None))]
    fn execute_scalar<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Bound<'py, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.ensure_open()?;
        let params = params_from_python(params.as_ref())?;
        let sql = sql.to_owned();
        let rows = self.run_tx(py, move |tx, cancel| {
            Box::pin(async move {
                let stmt = Statement { sql, params };
                tx.query(&stmt, cancel).await
            })
        })?;
        let value = rows
            .first()
            .and_then(|row| row.values().first())
            .cloned()
            .unwrap_or_else(|| plenora_database_core::provider::ParameterValue::Null {
                type_name: "unknown".to_owned(),
            });
        param_to_python(py, &value)
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
        let params = params_from_python(params.as_ref())?;
        let sql = sql.to_owned();
        let rows = self.run_tx(py, move |tx, cancel| {
            Box::pin(async move {
                let stmt = Statement { sql, params };
                tx.query(&stmt, cancel).await
            })
        })?;
        let out = PyList::empty(py);
        for row in rows {
            let dict = PyDict::new(py);
            let columns: Vec<String> = row.columns().to_vec();
            for (idx, name) in columns.iter().enumerate() {
                let value = row.values().get(idx).cloned().unwrap_or_else(|| {
                    plenora_database_core::provider::ParameterValue::Null {
                        type_name: "unknown".to_owned(),
                    }
                });
                let py_val = param_to_python(py, &value)?;
                dict.set_item(name.as_str(), py_val)?;
            }
            out.append(dict)?;
        }
        Ok(out)
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
        &self,
        py: Python<'_>,
        isolation: Option<&str>,
        read_only: Option<bool>,
        statement_timeout_ms: Option<u64>,
        context: Option<crate::session_context_py::PySessionContext>,
        native_query_policy: Option<&str>,
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
        if let Some(ms) = statement_timeout_ms {
            opts.statement_timeout_ms = Some(ms);
        }
        // Fix P1 review MySQL 2026-08-15 — parity con Session (Postgres):
        // - `context`: SessionContext applicato via `SET
        //   @plenora_ctx_*` (session-scoped MySQL).
        // - `native_query_policy`: "allow" (default) | "deny".
        if let Some(ctx) = context {
            opts.context = ctx.inner;
        }
        if let Some(policy) = native_query_policy {
            opts.native_query_policy = crate::transaction::parse_native_query_policy(policy)?;
        }
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        let scope = py
            .allow_threads(|| {
                runtime().block_on(async move {
                    let cancel = CancellationToken::new();
                    provider
                        .begin_transaction(&secret, &opts, &default_budget(), &cancel)
                        .await
                })
            })
            .map_err(to_py_err)?;
        Ok(Transaction::new(scope))
    }

    /// Esegue un PortableStatement (JSON) e ritorna rows come list[dict].
    /// Usato dai builder Python (`s.select(t).where_eq(...).all()`).
    fn execute_portable_rows<'py>(
        &self,
        py: Python<'py>,
        ast_json: &str,
    ) -> PyResult<Bound<'py, PyList>> {
        self.ensure_open()?;
        let ast: PortableStatement = serde_json::from_str(ast_json).map_err(|e| {
            to_py_err(DatabaseError::invalid_plan(format!(
                "AST portable non valida: {e}"
            )))
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

    /// Esegue un PortableStatement (JSON) senza RETURNING e ritorna
    /// affected_rows. Per Insert/Update/Delete/Upsert MySQL (no RETURNING).
    fn execute_portable_count(&self, py: Python<'_>, ast_json: &str) -> PyResult<u64> {
        self.ensure_open()?;
        let ast: PortableStatement = serde_json::from_str(ast_json).map_err(|e| {
            to_py_err(DatabaseError::invalid_plan(format!(
                "AST portable non valida: {e}"
            )))
        })?;
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
        py.allow_threads(|| {
            open_mysql_reader(
                &self.provider,
                &self.secret,
                schema,
                object,
                projection,
                order_by,
                limit,
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
        let result = py.allow_threads(|| {
            crate::mysql_write::copy_from_sync_mysql(
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
            )
        });
        crate::write::wrap_outcome(py, result)
    }

    /// DDL raw (CREATE/DROP/ALTER). MySQL fa autocommit implicito.
    fn execute_ddl(&self, py: Python<'_>, sql: &str) -> PyResult<()> {
        self.ensure_open()?;
        let provider = Arc::clone(&self.provider);
        let secret = self.secret.clone();
        let sql = sql.to_owned();
        py.allow_threads(|| {
            runtime().block_on(async move {
                let cancel = CancellationToken::new();
                provider.execute_ddl(&secret, &sql, &cancel).await
            })
        })
        .map_err(to_py_err)
    }
}

/// Apre una connessione MySQL e produce una `MysqlSession`.
///
/// Parametri:
/// - `host`, `database`, `user`, `password`
/// - `port`: opzionale, default 3306
/// - `tls_ca_pem`: opzionale, bytes del certificato CA privato PEM.
///   Se `None`, usa `TrustServerCertificate` (solo per sviluppo).
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
) -> PyResult<MysqlSession> {
    // Fix P1 review MySQL 2026-08-15 (parity con Postgres SDK 0.9.0):
    // prima il default settava `TrustServerCertificate` quando
    // `tls_ca_pem` era None — quindi ogni consumer che non forniva
    // una CA privata usava TLS senza verifica del certificato
    // server (vulnerabile a MITM). Ora il default è
    // `MysqlCertificatePolicy::Verify` (WebPKI trust store);
    // `tls_mode="insecure_trust_server"` è opt-in esplicito per
    // test/dev locali.
    let secret = SecretString::new(password.to_owned());
    let mut config = MysqlConfig::new(host, database, user, secret.clone());
    if let Some(p) = port {
        config = config.with_port(p);
    }
    if let Some(pem) = tls_ca_pem {
        if pem.len() > 1024 * 1024 {
            return Err(PyRuntimeError::new_err("CA PEM MySQL oltre 1 MiB"));
        }
        config = config.with_private_ca_certificate_pem(pem);
    }
    match tls_mode {
        "require" => {
            // Default `MysqlCertificatePolicy::Verify` — nessun
            // override necessario.
        }
        "insecure_trust_server" => {
            config = config.with_certificate_policy(MysqlCertificatePolicy::TrustServerCertificate);
        }
        other => {
            return Err(PyRuntimeError::new_err(format!(
                "tls_mode non riconosciuto: {other:?}. Valori: \
                 'require' (default) | 'insecure_trust_server'"
            )));
        }
    }
    let provider = Arc::new(MysqlProvider::new(config, 4).map_err(to_py_err)?);
    let provider_probe = Arc::clone(&provider);
    let secret_probe = secret.clone();
    let (connection, _capabilities) = runtime()
        .block_on(async move {
            let cancel = CancellationToken::new();
            let conn = provider_probe
                .test_connection(&secret_probe, &cancel)
                .await?;
            let caps = provider_probe
                .probe_capabilities(&secret_probe, &cancel)
                .await?;
            Ok::<_, DatabaseError>((conn, caps))
        })
        .map_err(to_py_err)?;
    Ok(MysqlSession {
        provider,
        secret,
        server_version: connection.server_version,
        closed: false,
    })
}
