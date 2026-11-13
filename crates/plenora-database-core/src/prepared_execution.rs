//! Confine unico fra il piano Arrow, il consenso e l'esito pubblicato.

use crate::loss::{LossConsent, LossReport, LossSeverity};
use crate::outcome::WriteOutcome;
use crate::provider::PreparedWrite;
use crate::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::future::Future;

/// Verifica il rapporto prima di invocare il codice che puo scrivere.
///
/// # Errors
/// Rifiuta report invalidi e rischi incompatibili con Strict. I writer migrati
/// applicano il consenso ai valori; gli altri conservano il gate sul rischio.
pub async fn execute<F, Fut>(
    prepared: PreparedWrite,
    accept_loss: bool,
    run: F,
) -> Result<WriteOutcome>
where
    F: FnOnce(PreparedWrite, crate::loss::LossConsent) -> Fut,
    Fut: Future<Output = Result<WriteOutcome>>,
{
    execute_with_consent(prepared, LossConsent::from_accept_loss(accept_loss), run).await
}

/// Ingresso tipizzato degli adapter: non ricostruisce il consenso dal piano.
/// # Errors
/// Il rapporto esecutivo identifica i writer migrati: solo questi valutano
/// il consenso sui valori. Gli altri mantengono il gate legacy.
pub async fn execute_with_consent<F, Fut>(
    prepared: PreparedWrite,
    consent: LossConsent,
    run: F,
) -> Result<WriteOutcome>
where
    F: FnOnce(PreparedWrite, LossConsent) -> Fut,
    Fut: Future<Output = Result<WriteOutcome>>,
{
    let report = prepared.loss_report.clone();
    let execution = prepared.execution_loss.clone();
    let result = async {
        if report.policy != prepared.operation.mapping_policy {
            return Err(DatabaseError::invalid_plan(
                "policy del rapporto diversa dal piano",
            ));
        }
        if execution.is_some() {
            authorize(&report)?;
        } else {
            authorize_legacy(&report, consent)?;
        }
        run(prepared, consent).await
    }
    .await;
    match execution {
        Some(plan) => attach_execution(plan, result),
        None => attach(report, result),
    }
}

/// Ammissione condivisa anche dal percorso Rust SQL Server diretto.
///
/// # Errors
/// Rifiuta un rapporto invalido o una perdita non autorizzata.
pub fn authorize_legacy(report: &LossReport, consent: LossConsent) -> Result<()> {
    authorize(report)?;
    if !consent.accepted()
        && report
            .losses
            .iter()
            .any(|loss| loss.severity == LossSeverity::DataLoss)
    {
        return Err(DatabaseError::new(
            ErrorCategory::DataMapping,
            ErrorPhase::Prepare,
            None,
            "il piano richiede consenso esplicito alle perdite di dati",
        ));
    }
    Ok(())
}

/// Associa il rapporto al risultato anche in caso di commit parziale o incerto.
/// Anche gli errori senza outcome conservano il rapporto del piano.
///
/// # Errors
/// Preserva l'errore dell'esecutore senza modificarne gli assi.
pub fn attach(report: LossReport, result: Result<WriteOutcome>) -> Result<WriteOutcome> {
    match result {
        Ok(mut outcome) => {
            outcome.schema_version = 3;
            outcome.loss_report.get_or_insert_with(|| report.into());
            Ok(outcome)
        }
        Err(mut error) => {
            error
                .loss_report
                .get_or_insert_with(|| Box::new(report.into()));
            Err(error)
        }
    }
}

/// Valida il piano e la policy; non autorizza valori non ancora letti.
/// # Errors
/// Strict resta un rifiuto strutturale per i rischi `DataLoss`.
pub fn authorize(report: &LossReport) -> Result<()> {
    report.validate()?;
    if !report.permits_execution() {
        return Err(DatabaseError::new(
            ErrorCategory::DataMapping,
            ErrorPhase::Prepare,
            None,
            "piano non ammesso dalla policy Strict",
        ));
    }
    Ok(())
}

/// Confine v3 per i writer migrati. Non crea una misura per sanare un successo.
/// # Errors
/// Un successo senza rapporto esecutivo e una violazione interna; un errore prima
/// del writer conserva il piano con valutazione incompleta.
pub fn attach_execution(
    planned: crate::execution_loss::ExecutionLossReport,
    result: Result<WriteOutcome>,
) -> Result<WriteOutcome> {
    match result {
        Ok(outcome) => {
            if outcome.validate().is_err()
                || !matches!(
                    outcome.loss_report,
                    Some(crate::execution_loss::VersionedLossReport::Execution(_))
                )
            {
                let mut error = DatabaseError::new(
                    ErrorCategory::Internal,
                    ErrorPhase::Commit,
                    Some(outcome.provider),
                    "writer migrato con rapporto esecutivo assente o non valido",
                );
                error.remote_effect = outcome.remote_effect();
                error.retry = crate::RetryDisposition::RequiresRecovery;
                return Err(error);
            }
            Ok(outcome)
        }
        Err(mut error) => {
            if error.loss_report.is_none() {
                let fallback = crate::execution_loss::ExecutionLossReport::planned(
                    planned.policy,
                    planned.risk,
                )?;
                error.loss_report = Some(Box::new(fallback.into()));
            }
            Err(error)
        }
    }
}
