use super::*;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

fn target(name: &str) -> ObjectRef {
    ObjectRef {
        catalog: None,
        schema: Some("public".to_owned()),
        object: name.to_owned(),
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

/// Un'operazione che arriva a un esito, anche un errore, non lascia nulla
/// nel registro: il residuo lo dichiara l'esito.
#[test]
fn an_operation_with_an_outcome_is_not_recorded() {
    let ledger = Arc::new(ResidueLedger::default());
    let mut ok = Box::pin(with_residue_guard(&ledger, target("t"), async {
        Ok::<_, crate::DatabaseError>(())
    }));
    assert!(matches!(poll_once(ok.as_mut()), Poll::Ready(Ok(()))));
    let mut failed = Box::pin(with_residue_guard(&ledger, target("t"), async {
        Err::<(), _>(crate::DatabaseError::invalid_plan("rifiutata"))
    }));
    assert!(matches!(poll_once(failed.as_mut()), Poll::Ready(Err(_))));
    assert_eq!(ledger.snapshot(), ResidueSnapshot::default());
}

/// La future lasciata cadere prima dell'esito (il chiamante abbandona la
/// scrittura) registra il target.
#[test]
fn an_abandoned_operation_is_recorded() {
    let ledger = Arc::new(ResidueLedger::default());
    let mut pending = Box::pin(with_residue_guard(
        &ledger,
        target("abbandonata"),
        std::future::pending::<crate::Result<()>>(),
    ));
    assert!(poll_once(pending.as_mut()).is_pending());
    drop(pending);
    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.total, 1);
    assert_eq!(snapshot.recent, vec![target("abbandonata")]);
}

/// Un panic del lavoro (per esempio del produttore dei batch) registra il
/// target durante l'unwinding.
#[test]
fn a_panicking_operation_is_recorded() {
    let ledger = Arc::new(ResidueLedger::default());
    let mut panicking = Box::pin(with_residue_guard(&ledger, target("panic"), async {
        panic!("produttore");
        #[allow(unreachable_code)]
        Ok::<(), crate::DatabaseError>(())
    }));
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = poll_once(panicking.as_mut());
    }));
    assert!(unwound.is_err());
    drop(panicking);
    assert_eq!(ledger.snapshot().total, 1);
}

/// Il registro e limitato: il totale conta tutto, i target recenti sono gli
/// ultimi [`RECENT_RESIDUES`].
#[test]
fn the_ledger_keeps_the_total_and_a_bounded_tail() {
    let ledger = Arc::new(ResidueLedger::default());
    for index in 0..(RECENT_RESIDUES + 5) {
        drop(ledger.arm(target(&format!("t{index}"))));
    }
    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.total, (RECENT_RESIDUES + 5) as u64);
    assert_eq!(snapshot.recent.len(), RECENT_RESIDUES);
    assert_eq!(snapshot.recent[0], target("t5"));
}
