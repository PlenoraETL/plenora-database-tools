//! Costruttore unico dell'errore "commit outcome unknown".
//!
//! Il fallimento appartiene a `ErrorPhase::Commit` e conserva
//! `RemoteEffect::Unknown`, perche l'esito remoto non e osservabile.
//! - `provider` sempre valorizzato quando noto (`Postgres`/`Mysql`).
//!
//! La disposizione e `RequiresRecovery`, non `Never`. Le due dicono cose
//! diverse: `Never` significa che l'operazione non e riprendibile, mentre qui
//! l'operazione **puo** essere ripresa dopo una verifica fuori banda — che e
//! esattamente cio che il messaggio chiede all'operatore. Con `Never` le due
//! `requires_manual_recovery()` deve quindi restituire vero.

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
    };
    DatabaseError {
        category: ErrorCategory::Internal,
        // `Write` appartiene allo statement DML; qui l'ambiguita e sul COMMIT.
        phase: ErrorPhase::Commit,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::RequiresRecovery,
        provider: Some(provider),
        execution_id: None,
        message: format!(
            "commit {provider_label} outcome unknown: verificare stato del target out-of-band"
        ),
        diagnostics: None,
    }
}
