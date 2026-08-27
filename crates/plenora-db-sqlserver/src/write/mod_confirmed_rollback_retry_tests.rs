use super::*;

/// Sul percorso a batch un annullamento confermato cambia l'effetto
/// remoto, non la disposizione di retry.
///
/// Un deadlock 1205 classificato `Safe` che il server ha annullato resta
/// ritentabile: forzarlo a `Never` trasformerebbe un errore transitorio
/// recuperabile in un fallimento definitivo. La conversione a `Never`
/// appartiene al solo percorso row-scoped, dove il rifiuto è del dato e
/// ritentare la stessa riga produrrebbe lo stesso rifiuto.
#[test]
fn a_confirmed_batch_rollback_preserves_every_retry_disposition() {
    for disposition in [
        RetryDisposition::Never,
        RetryDisposition::Quarantine,
        RetryDisposition::Safe,
        RetryDisposition::RequiresIdempotencyKey,
        RetryDisposition::RequiresRecovery,
        RetryDisposition::After(1),
    ] {
        let original = DatabaseError {
            category: ErrorCategory::Execution,
            phase: ErrorPhase::Write,
            remote_effect: RemoteEffect::Unknown,
            retry: disposition,
            provider: Some(ProviderKind::Sqlserver),
            execution_id: Some("sqlserver-batch-rollback".to_owned()),
            message: "errore pre-commit SQL Server".to_owned(),
            diagnostics: None,
        };
        let rolled_back = confirmed_rollback_axes(original);
        assert_eq!(
            rolled_back.retry, disposition,
            "la disposizione {disposition:?} non appartiene all'annullamento"
        );
        assert_eq!(rolled_back.remote_effect, RemoteEffect::RolledBack);
    }
}
