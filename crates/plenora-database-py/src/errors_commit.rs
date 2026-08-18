//! Costruttore unico dell'errore "commit outcome unknown".
//!
//! Prima di questo modulo, la stessa `DatabaseError` era ricostruita in
//! 7 punti (`session`, `async_session`, `transaction`,
//! `async_transaction`, `mysql_session`, `async_mysql_session` —
//! quest'ultimo 2×). Rischio: metadati divergenti (`ErrorPhase`,
//! provider, message) → consumer riceveva codici incoerenti a seconda
//! del path che aveva chiamato.
//!
//! Fix review #9:
//! - `ErrorPhase::Commit` (non `Write` come alcuni path facevano).
//! - `RemoteEffect::Unknown` (già consolidato prima, ma qui garantito).
//! - `provider` sempre valorizzato quando noto (`Postgres`/`Mysql`).
//! - `retry: Never` — l'operatore deve verificare out-of-band, no
//!   automatic retry.

use plenora_database_core::plan::ProviderKind;
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition,
};

/// Costruisce l'errore standard per commit con esito ignoto (canale
/// compromesso, timeout dopo COMMIT SQL, ecc.).
///
/// Il `provider` è obbligatorio per non perdere attribution: consumer
/// che vuole differenziare `Postgres` da `MySQL` nel logging deve
/// poterlo leggere.
#[must_use]
#[allow(clippy::redundant_pub_crate)]
pub(crate) fn commit_outcome_unknown(provider: ProviderKind) -> DatabaseError {
    let provider_label = match provider {
        ProviderKind::Postgres => "PostgreSQL",
        ProviderKind::Mysql => "MySQL",
        ProviderKind::Mariadb => "MariaDB",
        ProviderKind::Sqlserver => "SQL Server",
        ProviderKind::Oracle => "Oracle",
        ProviderKind::Db2 => "Db2",
        ProviderKind::Sqlite => "SQLite",
        ProviderKind::Duckdb => "DuckDB",
        ProviderKind::Arcgis => "ArcGIS",
    };
    DatabaseError {
        category: ErrorCategory::Internal,
        // Fix review #9: `ErrorPhase::Commit`, non `Write` — la fase
        // "Write" è per lo statement DML; l'ambiguità è sul COMMIT.
        phase: ErrorPhase::Commit,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::Never,
        provider: Some(provider),
        execution_id: None,
        message: format!(
            "commit {provider_label} outcome unknown: verificare stato del target out-of-band"
        ),
        diagnostics: None,
    }
}
