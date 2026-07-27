use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
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

    #[must_use]
    pub fn with_deadline(deadline: Instant) -> Self {
        Self::new_inner(Some(deadline))
    }

    fn new_inner(deadline: Option<Instant>) -> Self {
        Self {
            inner: Arc::new(Inner {
                reason: AtomicU8::new(0),
                deadline,
                waiters: Mutex::new(Vec::new()),
                children: Mutex::new(Vec::new()),
            }),
        }
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
                waiter.waker.wake();
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
        pending.extend(
            children
                .into_iter()
                .map(|child| (child, CancellationReason::Parent)),
        );
    }
}

pub struct Cancelled<'a> {
    token: &'a CancellationToken,
    state: Arc<WaiterState>,
    registered: bool,
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

        if let Some(reason) = self.token.reason() {
            self.token.remove_waiter(&self.state);
            self.registered = false;
            Poll::Ready(reason)
        } else {
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
mod tests {
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

    #[test]
    fn child_deadline_cannot_weaken_parent_and_has_distinct_reason() {
        let parent_deadline = Instant::now();
        let parent = CancellationToken::with_deadline(parent_deadline);
        let child = parent.child_token_with_deadline(
            parent_deadline.checked_add(std::time::Duration::from_secs(1)),
        );
        assert_eq!(child.deadline(), Some(parent_deadline));
        assert_eq!(child.reason(), Some(CancellationReason::Deadline));

        let explicit = CancellationToken::new();
        explicit.cancel_due_to_deadline();
        assert_eq!(explicit.reason(), Some(CancellationReason::Deadline));
    }
}
