use super::*;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::task::Wake;

#[derive(Debug)]
struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, AtomicOrdering::Release);
    }
}

#[test]
fn cancellation_is_idempotent_and_propagates_to_children() {
    let parent = CancellationToken::new();
    let child = parent.child_token();
    let independent_child = parent.child_token();
    independent_child.cancel();
    assert!(independent_child.is_cancelled());
    assert!(!parent.is_cancelled());
    assert!(!child.is_cancelled());

    parent.cancel();
    parent.cancel();
    assert_eq!(parent.reason(), Some(CancellationReason::Requested));
    assert_eq!(child.reason(), Some(CancellationReason::Parent));
}

#[test]
fn cancelled_future_is_woken_without_polling() {
    let token = CancellationToken::new();
    let flag = Arc::new(WakeFlag(AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&flag));
    let mut context = Context::from_waker(&waker);
    let mut future = token.cancelled();
    assert!(Pin::new(&mut future).poll(&mut context).is_pending());

    token.cancel();
    assert!(flag.0.load(AtomicOrdering::Acquire));
    assert_eq!(
        Pin::new(&mut future).poll(&mut context),
        Poll::Ready(CancellationReason::Requested)
    );
}

#[test]
fn dropped_future_removes_its_waiter() {
    let token = CancellationToken::new();
    let flag = Arc::new(WakeFlag(AtomicBool::new(false)));
    let waker = Waker::from(flag);
    let mut context = Context::from_waker(&waker);
    let mut future = token.cancelled();
    assert!(Pin::new(&mut future).poll(&mut context).is_pending());
    drop(future);
    assert!(lock_recover(&token.inner.waiters).is_empty());
}

#[test]
fn deadline_is_declarative_and_uses_monotonic_time() {
    let token = CancellationToken::with_deadline(Instant::now());
    assert!(token.is_cancelled());
    assert_eq!(token.reason(), Some(CancellationReason::Deadline));
}

/// Una deadline futura deve risvegliare un waiter già sospeso.
#[test]
fn a_future_deadline_wakes_a_pending_waiter() {
    let token =
        CancellationToken::with_deadline(Instant::now() + std::time::Duration::from_millis(50));
    let flag = Arc::new(WakeFlag(AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&flag));
    let mut context = Context::from_waker(&waker);
    let mut future = token.cancelled();

    assert!(
        Pin::new(&mut future).poll(&mut context).is_pending(),
        "la deadline non e ancora scaduta"
    );
    assert!(!flag.0.load(AtomicOrdering::Acquire));

    // Attesa generosa: la guardia riguarda "viene svegliato", non "entro
    // quanti millisecondi".
    let limit = Instant::now() + std::time::Duration::from_secs(5);
    while !flag.0.load(AtomicOrdering::Acquire) && Instant::now() < limit {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    assert!(
        flag.0.load(AtomicOrdering::Acquire),
        "nessuno ha risvegliato il waiter allo scadere della deadline"
    );
    assert_eq!(
        Pin::new(&mut future).poll(&mut context),
        Poll::Ready(CancellationReason::Deadline)
    );
}

/// Un waker che va in panic non porta con se lo scheduler.
///
/// `cancel_tree` chiama codice del chiamante. Senza contenimento, il primo
/// waker difettoso avrebbe terminato l'unico worker e disabilitato tutte
/// le deadline del processo: un difetto di un consumatore sarebbe
/// diventato un guasto globale. Il test misura proprio quello — dopo il
/// panic, una deadline nuova deve ancora scattare.
#[derive(Debug)]
struct PanickingWake;

impl Wake for PanickingWake {
    fn wake(self: Arc<Self>) {
        panic!("waker difettoso del chiamante");
    }
}

/// Un waker difettoso costa il proprio risveglio, non la cancellazione.
///
/// Il panico di un waker deve essere isolato: i waiter successivi vengono
/// comunque svegliati e la cancellazione continua a propagarsi ai figli.
#[test]
fn a_panicking_waker_does_not_swallow_the_rest_of_the_cancellation() {
    let token = CancellationToken::new();
    let child = token.child_token();

    let doomed = Waker::from(Arc::new(PanickingWake));
    let mut doomed_context = Context::from_waker(&doomed);
    let mut doomed_future = token.cancelled();
    assert!(Pin::new(&mut doomed_future)
        .poll(&mut doomed_context)
        .is_pending());

    let healthy = Arc::new(WakeFlag(AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&healthy));
    let mut context = Context::from_waker(&waker);
    let mut future = token.cancelled();
    assert!(Pin::new(&mut future).poll(&mut context).is_pending());

    token.cancel();

    assert!(
        healthy.0.load(AtomicOrdering::Acquire),
        "il waiter sano non e stato svegliato: il panic ha interrotto il ciclo"
    );
    assert_eq!(
        child.reason(),
        Some(CancellationReason::Parent),
        "il figlio non e stato raggiunto: il panic ha saltato la propagazione"
    );
}

#[test]
fn a_panicking_waker_does_not_stop_the_scheduler() {
    let doomed =
        CancellationToken::with_deadline(Instant::now() + std::time::Duration::from_millis(30));
    let waker = Waker::from(Arc::new(PanickingWake));
    let mut context = Context::from_waker(&waker);
    let mut future = doomed.cancelled();
    assert!(Pin::new(&mut future).poll(&mut context).is_pending());

    // Registrata dopo: deve scattare comunque.
    let survivor =
        CancellationToken::with_deadline(Instant::now() + std::time::Duration::from_millis(120));

    let limit = Instant::now() + std::time::Duration::from_secs(5);
    while !survivor.is_cancelled() && Instant::now() < limit {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(
        survivor.is_cancelled(),
        "il worker e morto insieme al waker difettoso"
    );
}

/// La coda non cresce con i token gia conclusi.
///
/// Le voci non si rimuovono alla cancellazione — costerebbe una scansione
/// a ogni evento — quindi con deadline lontane si accumulerebbero fino
/// alla loro scadenza. La compattazione le elimina in blocco.
#[test]
fn the_queue_drops_entries_whose_token_is_gone_or_cancelled() {
    let scheduler: &'static DeadlineScheduler = Box::leak(Box::new(DeadlineScheduler::default()));
    let far = Instant::now() + std::time::Duration::from_secs(3_600);

    // Oltre la soglia, tutti abbandonati subito dopo la registrazione.
    for _ in 0..=COMPACTION_THRESHOLD {
        let token = CancellationToken::new();
        scheduler.schedule(far, &token.inner);
        drop(token);
    }

    let live = CancellationToken::new();
    scheduler.schedule(far, &live.inner);

    // La lunghezza si copia e il guard si rilascia subito: tenerlo vivo
    // fino alla fine dell'assert bloccherebbe il worker mentre il test
    // formatta il messaggio.
    let held = lock_recover(&scheduler.pending).len();
    assert!(
        held <= 2,
        "la coda trattiene {held} voci di token che non esistono piu"
    );
}

/// La deadline scattata si propaga ai figli, come ogni cancellazione.
#[test]
fn a_future_deadline_reaches_the_children() {
    let parent =
        CancellationToken::with_deadline(Instant::now() + std::time::Duration::from_millis(50));
    let child = parent.child_token();
    let limit = Instant::now() + std::time::Duration::from_secs(5);
    while !child.is_cancelled() && Instant::now() < limit {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(child.is_cancelled());
    assert_eq!(parent.reason(), Some(CancellationReason::Deadline));
}

#[test]
fn child_deadline_cannot_weaken_parent_and_has_distinct_reason() {
    let parent_deadline = Instant::now();
    let parent = CancellationToken::with_deadline(parent_deadline);
    let child = parent
        .child_token_with_deadline(parent_deadline.checked_add(std::time::Duration::from_secs(1)));
    assert_eq!(child.deadline(), Some(parent_deadline));
    assert_eq!(child.reason(), Some(CancellationReason::Deadline));

    let explicit = CancellationToken::new();
    explicit.cancel_due_to_deadline();
    assert_eq!(explicit.reason(), Some(CancellationReason::Deadline));
}

/// La prima causa **registrata** vince, anche se la deadline era gia
/// trascorsa.
///
/// `reason()` legge l'atomico prima dell'orologio, quindi una `cancel()`
/// successiva a una scadenza gia passata la sovrascrive: il token resta
/// cancellato, ma la ragione diventa `Requested` e un provider che
/// distingue `Timeout` da `Cancelled` scegliera il secondo. Non e una
/// svista da correggere in silenzio — e il comportamento che la nota di
/// modulo dichiara, e questo test lo tiene fermo.
#[test]
fn a_recorded_cause_wins_over_an_already_elapsed_deadline() {
    let token = CancellationToken::with_deadline(Instant::now());
    assert_eq!(token.reason(), Some(CancellationReason::Deadline));

    let requested = CancellationToken::with_deadline(Instant::now());
    requested.cancel();
    assert_eq!(requested.reason(), Some(CancellationReason::Requested));

    let parent = CancellationToken::new();
    let child = parent.child_token_with_deadline(Some(Instant::now()));
    parent.cancel();
    assert_eq!(child.reason(), Some(CancellationReason::Parent));
}

/// Una scadenza chiude l'albero **come scadenza**, a ogni profondita.
///
/// Il figlio eredita la deadline del padre: se la propagazione lo marcasse
/// `Parent`, la ragione dipenderebbe da chi arriva prima fra la
/// propagazione e la registrazione della propria scadenza, e la stessa
/// scadenza diventerebbe `Timeout` o `Cancelled` a caso.
#[test]
fn a_deadline_propagates_to_the_whole_tree_as_a_deadline() {
    let parent = CancellationToken::new();
    let child = parent.child_token();
    let grandchild = child.child_token();
    parent.cancel_due_to_deadline();
    assert_eq!(parent.reason(), Some(CancellationReason::Deadline));
    assert_eq!(child.reason(), Some(CancellationReason::Deadline));
    assert_eq!(grandchild.reason(), Some(CancellationReason::Deadline));

    // Una cancellazione richiesta resta invece una decisione del padre.
    let requested = CancellationToken::new();
    let below = requested.child_token();
    requested.cancel();
    assert_eq!(below.reason(), Some(CancellationReason::Parent));
}
