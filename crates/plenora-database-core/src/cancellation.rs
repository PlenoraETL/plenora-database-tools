//! Cancellazione cooperativa con deadline.
//!
//! # La deadline sveglia, non aspetta di essere guardata
//!
//! Uno scheduler interno risveglia i task registrati su `cancelled()` quando
//! scade la deadline, senza attendere una successiva chiamata a `reason()`.
//! Lo scheduler e costruito
//! sulla sola `std`. Il crate non dipende da un runtime async — i suoi future
//! sono `Pin<Box<dyn Future>>` e girano su qualunque executor — e legarlo a
//! Tokio per un timer avrebbe deciso il runtime di tutti i consumatori. Un
//! thread solo, condiviso, avviato alla prima deadline registrata: chi non usa
//! deadline non paga niente.
//!
//! # La deadline e best-effort, e lo e per costruzione
//!
//! Il risveglio dipende da un thread, e creare un thread puo fallire: limiti
//! del processo, memoria esaurita. Il modulo riprova — a ogni registrazione e
//! a ogni poll di [`Cancelled`] — ma non all'infinito (`MAX_ARM_ATTEMPTS`),
//! perche un retry illimitato su un executor a thread singolo diventa
//! un'attesa attiva che affama proprio i task che libererebbero le risorse
//! mancanti.
//!
//! Quindi il contratto e: **la deadline garantisce che chi la osserva veda il
//! token cancellato, non che venga risvegliato entro un tempo massimo.**
//! [`CancellationToken::reason`] riporta `Deadline` al primo istante in cui
//! viene chiamata dopo la scadenza *se nessun'altra causa e gia stata
//! registrata*: la prima causa registrata vince, e una `cancel()` esplicita
//! sovrascrive l'attribuzione anche quando la deadline era gia trascorsa. Il
//! token resta cancellato in ogni caso; a cambiare e solo la ragione, e chi la
//! usa deve saperlo: i provider scelgono `Timeout` o `Cancelled` proprio a
//! partire da li.
//!
//! La propagazione ai figli conserva la deadline: un albero chiuso da una
//! scadenza riporta `Deadline` ovunque, non `Parent`. Altrimenti la ragione
//! sarebbe dipesa dall'ordine di scheduling, perche il figlio eredita la
//! deadline del padre e puo registrarla da se prima che la propagazione lo
//! raggiunga — e la stessa scadenza sarebbe diventata `Timeout` o `Cancelled`
//! a seconda di chi arrivava primo.
//!
//! Chi attende su [`CancellationToken::cancelled`] viene svegliato
//! puntualmente finche il worker esiste, e con un ritardo illimitato se il
//! processo non riesce a crearlo. Chi ha bisogno di un limite temporale
//! *garantito* — un timeout di protocollo, non una cortesia verso il server —
//! deve comporre la propria attesa con il timer del proprio runtime, non
//! affidarsi a questo.

use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationReason {
    Requested,
    Deadline,
    Parent,
}

impl CancellationReason {
    const fn code(self) -> u8 {
        match self {
            Self::Requested => 1,
            Self::Deadline => 2,
            Self::Parent => 3,
        }
    }

    const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Requested),
            2 => Some(Self::Deadline),
            3 => Some(Self::Parent),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct WaiterState;

#[derive(Debug)]
struct WaiterEntry {
    owner: Weak<WaiterState>,
    waker: Waker,
}

#[derive(Debug)]
struct Inner {
    reason: AtomicU8,
    deadline: Option<Instant>,
    waiters: Mutex<Vec<WaiterEntry>>,
    children: Mutex<Vec<Weak<Self>>>,
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
//  Scheduler delle deadline
// ---------------------------------------------------------------------------

/// Una deadline in attesa di scattare.
///
/// Il token e tenuto **debole**: un token abbandonato non deve restare vivo
/// perche una coda lo aspetta, e alla scadenza l'upgrade fallisce e la voce
/// viene semplicemente scartata.
struct ScheduledDeadline {
    at: Instant,
    token: Weak<Inner>,
}

impl PartialEq for ScheduledDeadline {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at
    }
}

impl Eq for ScheduledDeadline {}

impl Ord for ScheduledDeadline {
    /// Ordine invertito: `BinaryHeap` e un max-heap, e qui serve la scadenza
    /// piu vicina.
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other.at.cmp(&self.at)
    }
}

impl PartialOrd for ScheduledDeadline {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

/// Oltre questa soglia la coda viene compattata prima di crescere ancora.
///
/// Le voci non si tolgono alla cancellazione o al drop del token: costerebbe
/// una scansione lineare a ogni evento. Restano finche la loro scadenza
/// arriva, e con deadline lontane questo vuol dire trattenere l'allocazione di
/// token gia conclusi. La compattazione le elimina in blocco quando sono
/// abbastanza da valere il costo.
const COMPACTION_THRESHOLD: usize = 1024;

#[derive(Default)]
struct DeadlineScheduler {
    pending: Mutex<BinaryHeap<ScheduledDeadline>>,
    wakeup: Condvar,
    /// Il worker e vivo. Se l'avvio fallisce resta `false`, e il prossimo
    /// `schedule` riprova invece di lasciare le deadline senza nessuno che le
    /// faccia scattare.
    worker: Mutex<bool>,
}

impl DeadlineScheduler {
    fn schedule(&'static self, at: Instant, token: &Arc<Inner>) {
        {
            let mut pending = lock_recover(&self.pending);
            if pending.len() >= COMPACTION_THRESHOLD {
                // Una voce e morta se il token non esiste piu, oppure se e
                // gia stato cancellato per un'altra ragione: in entrambi i
                // casi farla scattare non cambierebbe niente.
                pending.retain(|entry| {
                    entry
                        .token
                        .upgrade()
                        .is_some_and(|inner| inner.reason.load(Ordering::Acquire) == 0)
                });
            }
            pending.push(ScheduledDeadline {
                at,
                token: Arc::downgrade(token),
            });
        }
        let _armed = self.ensure_worker();
        // La scadenza appena inserita puo precedere quella che il thread sta
        // aspettando: va risvegliato perche ricalcoli l'attesa.
        self.wakeup.notify_one();
    }

    /// Avvia il worker se non c'e.
    ///
    /// Idempotente e riprovabile: `Builder::spawn` fa I/O e puo fallire per
    /// limiti di thread del processo. Un fallimento non fissa lo scheduler,
    /// cosi ogni nuova registrazione puo tentare nuovamente l'avvio.
    /// Restituisce `true` se il worker e vivo dopo la chiamata.
    ///
    /// L'esito **e osservabile** perche il chiamante possa reagire: chi si sta
    /// sospendendo su una deadline deve sapere se esiste qualcuno che lo
    /// svegliera, e in caso contrario procurarsi un'altra sorgente di wake.
    /// Restituire `()` lasciava quella decisione a nessuno.
    fn ensure_worker(&'static self) -> bool {
        let mut worker = lock_recover(&self.worker);
        if *worker {
            return true;
        }
        if std::thread::Builder::new()
            .name("plenora-deadline".to_owned())
            .spawn(|| self.run())
            .is_ok()
        {
            *worker = true;
        }
        *worker
    }

    /// Ciclo del thread: dorme fino alla prossima scadenza, la fa scattare,
    /// ricomincia.
    ///
    /// Non termina mai per colpa di un token. `cancel_tree` risveglia i waker
    /// registrati, che sono codice di chiamanti arbitrari: se uno va in panic,
    /// senza protezione porterebbe con se l'unico worker e disabiliterebbe
    /// **tutte** le deadline del processo — un difetto in un consumatore
    /// diventerebbe un guasto globale. Il panic viene quindi contenuto alla
    /// singola scadenza.
    fn run(&self) {
        loop {
            let mut pending = lock_recover(&self.pending);
            let expired = loop {
                // L'istante si copia fuori dal prestito: `wait_timeout`
                // consuma il guard, e `peek()` lo terrebbe in prestito.
                let next_at = match pending.peek() {
                    None => {
                        // Niente in coda: attesa indefinita, a costo zero.
                        pending = self
                            .wakeup
                            .wait(pending)
                            .unwrap_or_else(PoisonError::into_inner);
                        continue;
                    }
                    Some(next) => next.at,
                };
                let now = Instant::now();
                if next_at <= now {
                    break pending.pop();
                }
                let (guard, _) = self
                    .wakeup
                    .wait_timeout(pending, next_at - now)
                    .unwrap_or_else(PoisonError::into_inner);
                pending = guard;
            };
            // Il lock si rilascia **prima** di cancellare: `cancel_tree`
            // risveglia i waiter, e uno di quelli potrebbe voler registrare
            // una nuova deadline.
            drop(pending);
            if let Some(token) = expired.and_then(|entry| entry.token.upgrade()) {
                let contained = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    cancel_tree(token, CancellationReason::Deadline);
                }));
                // `AssertUnwindSafe`: gli stati condivisi che questo blocco
                // tocca sono un `AtomicU8` e due `Mutex`, e i mutex di questo
                // modulo si leggono con `lock_recover`, che tratta il poison
                // come recuperabile. Non c'e invariante che un unwind possa
                // lasciare a meta.
                drop(contained);
            }
        }
    }
}

fn scheduler() -> &'static DeadlineScheduler {
    static SCHEDULER: OnceLock<&'static DeadlineScheduler> = OnceLock::new();
    // `Box::leak`: lo scheduler vive quanto il processo, e un riferimento
    // `'static` evita di clonare un `Arc` a ogni token con deadline. Il worker
    // non parte qui: lo avvia `schedule`, che puo riprovare.
    SCHEDULER.get_or_init(|| Box::leak(Box::new(DeadlineScheduler::default())))
}

#[derive(Clone, Debug)]
pub struct CancellationToken {
    inner: Arc<Inner>,
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationToken {
    #[must_use]
    pub fn new() -> Self {
        Self::new_inner(None)
    }

    /// Un token che scade a `deadline`.
    ///
    /// La scadenza e **best-effort**: e garantito che [`Self::reason`] riporti
    /// il token cancellato, non che chi attende su [`Self::cancelled`] venga
    /// risvegliato entro un ritardo massimo, ne che la ragione sia `Deadline`
    /// se un'altra causa e stata registrata prima della lettura. Vedi la nota
    /// di modulo.
    #[must_use]
    pub fn with_deadline(deadline: Instant) -> Self {
        Self::new_inner(Some(deadline))
    }

    fn new_inner(deadline: Option<Instant>) -> Self {
        let inner = Arc::new(Inner {
            reason: AtomicU8::new(0),
            deadline,
            waiters: Mutex::new(Vec::new()),
            children: Mutex::new(Vec::new()),
        });
        // Una deadline gia scaduta non ha bisogno dello scheduler: `reason()`
        // la vede subito, e registrarla farebbe partire il thread per niente.
        if let Some(at) = deadline {
            if at > Instant::now() {
                scheduler().schedule(at, &inner);
            }
        }
        Self { inner }
    }

    pub fn cancel(&self) {
        cancel_tree(Arc::clone(&self.inner), CancellationReason::Requested);
    }

    pub fn cancel_due_to_deadline(&self) {
        cancel_tree(Arc::clone(&self.inner), CancellationReason::Deadline);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.reason().is_some()
    }

    #[must_use]
    pub fn reason(&self) -> Option<CancellationReason> {
        CancellationReason::from_code(self.inner.reason.load(Ordering::Acquire)).or_else(|| {
            self.inner
                .deadline
                .filter(|deadline| Instant::now() >= *deadline)
                .map(|_| CancellationReason::Deadline)
        })
    }

    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        self.inner.deadline
    }

    #[must_use]
    pub fn child_token(&self) -> Self {
        self.child_token_with_deadline(self.inner.deadline)
    }

    #[must_use]
    pub fn child_token_with_deadline(&self, deadline: Option<Instant>) -> Self {
        let deadline = match (self.inner.deadline, deadline) {
            (Some(parent), Some(child)) => Some(parent.min(child)),
            (Some(parent), None) => Some(parent),
            (None, child) => child,
        };
        let child = Self::new_inner(deadline);
        {
            let mut children = lock_recover(&self.inner.children);
            children.retain(|existing| existing.strong_count() > 0);
            children.push(Arc::downgrade(&child.inner));
        }
        if let Some(reason) = self.reason() {
            cancel_tree(
                Arc::clone(&child.inner),
                if reason == CancellationReason::Deadline {
                    CancellationReason::Deadline
                } else {
                    CancellationReason::Parent
                },
            );
        }
        child
    }

    #[must_use]
    pub fn cancelled(&self) -> Cancelled<'_> {
        Cancelled {
            token: self,
            state: Arc::new(WaiterState),
            registered: false,
            arm_attempts: 0,
        }
    }

    fn remove_waiter(&self, state: &Arc<WaiterState>) {
        let mut waiters = lock_recover(&self.inner.waiters);
        waiters.retain(|entry| {
            entry
                .owner
                .upgrade()
                .is_some_and(|owner| !Arc::ptr_eq(&owner, state))
        });
    }
}

/// Cancella un token e tutto il sottoalbero.
///
/// # Perche ogni `wake()` e contenuto singolarmente
///
/// Il primo passo e un `compare_exchange` che marca il token: da quel momento
/// la cancellazione **e avvenuta**, e nessun secondo tentativo puo rifarla —
/// la CAS trova il token gia cancellato e salta. Tutto cio che segue va quindi
/// portato a termine.
///
/// I waker sono codice di chiamanti arbitrari. Con un solo contenimento attorno
/// all'intera funzione, un waker che panica interrompeva il ciclo: i waiter
/// successivi non venivano svegliati e i figli non venivano nemmeno raccolti.
/// Il token risultava cancellato, i suoi waiter restavano sospesi per sempre e
/// i figli continuavano a vivere — uno stato peggiore di quello che il panic
/// avrebbe prodotto lasciando morire il thread.
///
/// Contenendo ogni singola chiamata, un waker difettoso costa esattamente il
/// proprio risveglio: gli altri waiter e l'intera propagazione ai figli
/// avvengono comunque.
fn cancel_tree(root: Arc<Inner>, root_reason: CancellationReason) {
    let mut pending = vec![(root, root_reason)];
    while let Some((inner, reason)) = pending.pop() {
        if inner
            .reason
            .compare_exchange(0, reason.code(), Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            continue;
        }

        let waiters = {
            let mut registered = lock_recover(&inner.waiters);
            std::mem::take(&mut *registered)
        };
        for waiter in waiters {
            if waiter.owner.strong_count() > 0 {
                // `AssertUnwindSafe`: il waker e consumato da questa chiamata e
                // non e osservabile dopo, quindi non c'e stato condiviso che un
                // unwind possa lasciare a meta.
                let woken = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    waiter.waker.wake();
                }));
                drop(woken);
            }
        }

        let children = {
            let mut registered = lock_recover(&inner.children);
            let live = registered
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            registered.retain(|child| child.strong_count() > 0);
            live
        };
        // Una deadline si propaga **come deadline**. I figli ereditano la
        // deadline del padre (`child_token`), quindi quando scade e la stessa
        // scadenza a chiudere tutto l'albero: marcarli `Parent` faceva
        // dipendere la ragione dall'ordine di scheduling — `Deadline` se il
        // figlio registrava la propria scadenza per primo, `Parent` se
        // arrivava prima la propagazione — e i provider traducono le due in
        // `Timeout` e `Cancelled`. La stessa scadenza diventava due errori
        // pubblici diversi.
        //
        // `Parent` resta per cio che e davvero una decisione del padre: una
        // `cancel()` esplicita, o una propagazione gia parentale.
        let inherited = match reason {
            CancellationReason::Deadline => CancellationReason::Deadline,
            CancellationReason::Requested | CancellationReason::Parent => {
                CancellationReason::Parent
            }
        };
        pending.extend(children.into_iter().map(|child| (child, inherited)));
    }
}

/// Quante volte un singolo future insiste per armare il worker.
///
/// Il retry cooperativo — `wake_by_ref` prima di `Pending` — serve a non
/// restare sospesi quando il worker non parte. Illimitato pero diventa
/// un'attesa attiva: su un executor a thread singolo un future che si
/// risveglia da solo a ogni giro puo affamare gli altri task, compresi quelli
/// che libererebbero le risorse necessarie a creare il thread. Con un tetto
/// il costo e limitato a pochi giri; dopo, la deadline degrada a cio che era
/// prima che questo scheduler esistesse — osservata al prossimo poll — che
/// non e una regressione ma lo stato precedente.
const MAX_ARM_ATTEMPTS: u8 = 8;

pub struct Cancelled<'a> {
    token: &'a CancellationToken,
    state: Arc<WaiterState>,
    registered: bool,
    /// Tentativi di armare il worker gia spesi da **questo** future.
    arm_attempts: u8,
}

impl Future for Cancelled<'_> {
    type Output = CancellationReason;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(reason) = self.token.reason() {
            return Poll::Ready(reason);
        }

        {
            let mut waiters = lock_recover(&self.token.inner.waiters);
            waiters.retain(|entry| entry.owner.strong_count() > 0);
            if let Some(entry) = waiters.iter_mut().find(|entry| {
                entry
                    .owner
                    .upgrade()
                    .is_some_and(|owner| Arc::ptr_eq(&owner, &self.state))
            }) {
                if !entry.waker.will_wake(context.waker()) {
                    entry.waker.clone_from(context.waker());
                }
            } else {
                waiters.push(WaiterEntry {
                    owner: Arc::downgrade(&self.state),
                    waker: context.waker().clone(),
                });
            }
        }
        self.registered = true;

        // Chi sta per sospendersi su una deadline futura e esattamente chi ha
        // bisogno che il worker esista. Se il suo avvio era fallito — limiti
        // di thread del processo — si riprova qui.
        //
        // Se **anche** questo tentativo fallisce, restituire `Pending` senza
        // altro lascerebbe il future sospeso per sempre: nessuno e tenuto a
        // ripolarlo, e senza worker nessuno lo svegliera. In quel caso si
        // richiede subito un nuovo poll, cosi la deadline viene osservata da
        // `reason()` al piu tardi quando scade.
        //
        // E' un'attesa attiva, e si paga solo quando il processo non riesce a
        // creare un thread — uno stato in cui il costo di qualche poll in piu
        // e preferibile a un'operazione che non finisce mai. Nel caso normale
        // costa un `Mutex` gia sbloccato.
        let needs_cooperative_retry = if self.arm_attempts < MAX_ARM_ATTEMPTS
            && self
                .token
                .inner
                .deadline
                .is_some_and(|at| at > Instant::now())
        {
            self.arm_attempts += 1;
            !scheduler().ensure_worker()
        } else {
            false
        };

        if let Some(reason) = self.token.reason() {
            self.token.remove_waiter(&self.state);
            self.registered = false;
            Poll::Ready(reason)
        } else {
            if needs_cooperative_retry {
                context.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }
}

impl Drop for Cancelled<'_> {
    fn drop(&mut self) {
        if self.registered {
            self.token.remove_waiter(&self.state);
        }
    }
}

#[cfg(test)]
#[path = "cancellation_tests.rs"]
mod tests;
