use super::*;
use plenora_database_core::{
    read_loss::{ReadLossReport, ReadStatus},
    ErrorCategory, ErrorPhase,
};
#[test]
fn terminal_error_is_structured_isolated_and_survives_close() {
    let state = StreamState::default();
    let healthy = StreamState::default();
    state.report(Some(ReadLossReport::new(Vec::new())));
    state.observe(&Ok(Some(())));
    let error = DatabaseError::new(
        ErrorCategory::Crs,
        ErrorPhase::Read,
        None,
        "geometria non conforme",
    );
    state.observe::<()>(&Err(error.clone()));
    state.close();
    state.close();
    state.observe::<()>(&Ok(None));
    state.report(Some(ReadLossReport::new(Vec::new())));
    let terminal = state.0.lock().unwrap();
    assert_eq!(terminal.error, Some(error));
    assert_eq!(
        terminal.read_loss.as_ref().unwrap().status,
        ReadStatus::Failed
    );
    assert!(!terminal.complete);
    drop(terminal);
    assert!(healthy.0.lock().unwrap().error.is_none());
}
#[test]
fn close_before_eof_is_interruption_and_eof_remains_complete() {
    let state = StreamState::default();
    state.report(Some(ReadLossReport::new(Vec::new())));
    state.close();
    assert_eq!(
        state.0.lock().unwrap().read_loss.as_ref().unwrap().status,
        ReadStatus::Interrupted
    );
    let state = StreamState::default();
    state.observe::<()>(&Ok(None));
    state.close();
    assert!(state.0.lock().unwrap().complete);
}
