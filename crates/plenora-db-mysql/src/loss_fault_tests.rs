//! Interruttore interno del runner seriale: nessun fault nel binario pubblico.
use std::sync::atomic::{AtomicU8, Ordering};
static FAULT: AtomicU8 = AtomicU8::new(0);
pub struct Guard;
impl Guard {
    pub fn engage(code: u8) -> Self {
        assert_eq!(FAULT.swap(code, Ordering::SeqCst), 0);
        Self
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        FAULT.store(0, Ordering::SeqCst);
    }
}
pub async fn inject(
    session: &mut crate::MysqlSession,
    point: u8,
    cancellation: &plenora_database_core::CancellationToken,
) -> plenora_database_core::Result<()> {
    let code = FAULT.load(Ordering::SeqCst);
    if code % 10 != point || code == 0 {
        return Ok(());
    }
    if code >= 20 {
        cancellation.cancel();
    }
    if code >= 10 {
        session.discard().await;
    }
    Err(crate::write::mapping_error(
        "fault esecutore MySQL richiesto dal test",
    ))
}
