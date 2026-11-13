use super::*;
use plenora_database_core::arrow::{RecordBatch, Schema};
use plenora_database_core::provider::ProviderFuture;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Server {
    responses: VecDeque<Result<Option<RecordBatch>>>,
    dropped: Arc<AtomicUsize>,
    entered: Option<Arc<tokio::sync::Notify>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
impl BatchStream for Server {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }
    fn next_batch<'a>(
        &'a mut self,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            if let Some(entered) = &self.entered {
                entered.notify_one();
                std::future::pending::<()>().await;
            }
            self.responses.pop_front().unwrap_or(Ok(None))
        })
    }
}
fn reader(
    responses: Vec<Result<Option<RecordBatch>>>,
    entered: Option<Arc<tokio::sync::Notify>>,
) -> (Reader, Arc<AtomicUsize>) {
    crate::build_runtime().unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    (
        Reader::new(
            Box::new(Server {
                responses: responses.into(),
                dropped: dropped.clone(),
                entered,
            }),
            CancellationToken::new(),
        ),
        dropped,
    )
}
fn batch() -> RecordBatch {
    RecordBatch::new_empty(Arc::new(Schema::empty()))
}

#[test]
fn server_failures_preserve_all_axes_at_every_lazy_position_and_release() {
    let rt = crate::build_runtime().unwrap();
    for category in [
        ErrorCategory::Execution,
        ErrorCategory::Io,
        ErrorCategory::Cancelled,
        ErrorCategory::Timeout,
        ErrorCategory::Crs,
    ] {
        for prefix in 0..3 {
            let mut error = DatabaseError::new(category, ErrorPhase::Read, None, "errore lettura");
            error.remote_effect = plenora_database_core::RemoteEffect::Unknown;
            error.retry = plenora_database_core::RetryDisposition::Quarantine;
            let mut responses = vec![Ok(Some(batch())); prefix];
            responses.push(Err(error.clone()));
            let (reader, dropped) = reader(responses, None);
            rt.block_on(async {
                for _ in 0..prefix {
                    assert!(reader.next().await.unwrap().is_some());
                }
                assert_eq!(reader.next().await.unwrap_err(), error);
                assert_eq!(dropped.load(Ordering::SeqCst), 1);
                reader.released().await;
                assert_eq!(reader.next().await.unwrap_err(), error);
                assert_eq!(reader.schema_bytes().unwrap_err(), error);
                assert_eq!(reader.state.error(), Some(error));
                assert_eq!(dropped.load(Ordering::SeqCst), 1);
            });
        }
    }
}
#[test]
fn eof_releases_without_losing_schema_or_becoming_cancelled() {
    let (reader, dropped) = reader(vec![Ok(Some(batch()))], None);
    crate::runtime().block_on(async {
        assert!(reader.next().await.unwrap().is_some());
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        reader.released().await;
        assert!(reader.state.error().is_none());
        assert!(reader.schema_bytes().is_ok());
        assert!(reader.next().await.unwrap().is_none());
    });
}
#[test]
fn dropping_await_before_poll_or_in_flight_releases_and_preserves_interruption() {
    for polled in [false, true] {
        let entered = Arc::new(tokio::sync::Notify::new());
        let (reader, dropped) = reader(vec![], Some(entered.clone()));
        crate::runtime().block_on(async {
            let mut flight = reader.flight();
            if polled {
                let task = tokio::spawn(async move {
                    let result = flight.reader.next().await;
                    flight.finish();
                    result
                });
                entered.notified().await;
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                drop(flight);
            }
            reader.released().await;
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            assert_eq!(
                reader.state.error().unwrap().category,
                ErrorCategory::Cancelled
            );
        });
    }
}
#[test]
fn deadline_and_close_wake_pending_server_without_parent_cancellation() {
    for deadline in [false, true] {
        let entered = Arc::new(tokio::sync::Notify::new());
        let (mut reader, dropped) = reader(vec![], Some(entered.clone()));
        let parent = CancellationToken::new();
        reader.cancellation = parent.child_token();
        crate::runtime().block_on(async {
            let worker = reader.clone();
            let task = tokio::spawn(async move { worker.next().await });
            entered.notified().await;
            if deadline {
                reader.cancellation.cancel_due_to_deadline();
            } else {
                reader.close();
            }
            let error = task.await.unwrap().unwrap_err();
            assert_eq!(
                error.category,
                if deadline {
                    ErrorCategory::Timeout
                } else {
                    ErrorCategory::Cancelled
                }
            );
            reader.released().await;
            assert_eq!(reader.state.error(), Some(error));
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            assert!(parent.reason().is_none());
        });
    }
}
#[test]
fn close_before_publication_wins_over_a_ready_batch() {
    let state = StreamState::default();
    state.close();
    assert_eq!(
        state.publish(Ok(Some(batch()))).unwrap_err().category,
        ErrorCategory::Cancelled
    );
}

// Windows links libpython for extension-module tests; Unix exercises the wheel.
#[cfg(windows)]
#[test]
fn actual_python_sync_and_async_consumers_receive_structured_lazy_errors() {
    use pyo3::prelude::*;
    use pyo3::types::PyModule;
    crate::build_runtime().unwrap();
    Python::initialize();
    Python::attach(|py| {
        let probe = PyModule::from_code(
            py,
            c"
import asyncio

def axes(error):
    return error.category, error.phase, error.remote_effect, error.retry['kind']

def sync(source):
    assert next(source)
    try:
        next(source)
    except Exception as error:
        source.close()
        assert axes(source.stream_state['error']) == axes(error)
        return axes(error)
    raise AssertionError('missing failure')

async def consume(source):
    assert await source.__anext__()
    try:
        await source.__anext__()
    except Exception as error:
        await source.aclose()
        assert axes(source.stream_state['error']) == axes(error)
        return axes(error)
    raise AssertionError('missing failure')

def asynchronous(source):
    loop = asyncio.SelectorEventLoop()
    try:
        return loop.run_until_complete(consume(source))
    finally:
        loop.close()
",
            c"stream_probe.py",
            c"stream_probe",
        )
        .unwrap();
        for (category, name) in [
            (ErrorCategory::Execution, "execution"),
            (ErrorCategory::Io, "io"),
            (ErrorCategory::Cancelled, "cancelled"),
            (ErrorCategory::Timeout, "timeout"),
        ] {
            for asynchronous in [false, true] {
                let mut error =
                    DatabaseError::new(category, ErrorPhase::Read, None, "errore lettura");
                error.remote_effect = plenora_database_core::RemoteEffect::Unknown;
                error.retry = plenora_database_core::RetryDisposition::Quarantine;
                let dropped = Arc::new(AtomicUsize::new(0));
                let server = Box::new(Server {
                    responses: vec![Ok(Some(batch())), Err(error)].into(),
                    dropped: dropped.clone(),
                    entered: None,
                });
                let source = if asynchronous {
                    Py::new(
                        py,
                        crate::arrow_reader::AsyncBatchReader::new(
                            server,
                            CancellationToken::new(),
                        ),
                    )
                    .unwrap()
                    .into_any()
                } else {
                    Py::new(
                        py,
                        crate::arrow_reader::BatchReader::new(server, CancellationToken::new()),
                    )
                    .unwrap()
                    .into_any()
                };
                let axes = probe
                    .getattr(if asynchronous { "asynchronous" } else { "sync" })
                    .unwrap()
                    .call1((source,))
                    .unwrap()
                    .extract::<(String, String, String, String)>()
                    .unwrap();
                assert_eq!(
                    axes,
                    (
                        name.to_owned(),
                        "read".to_owned(),
                        "unknown".to_owned(),
                        "quarantine".to_owned()
                    )
                );
                assert_eq!(dropped.load(Ordering::SeqCst), 1);
            }
        }
    });
}

#[test]
fn cancellation_of_python_finalization_after_eof_is_structured() {
    let (reader, dropped) = reader(vec![], None);
    crate::runtime().block_on(async {
        assert!(reader.next().await.unwrap().is_none());
        let error = reader.interruption();
        assert_eq!(error.category, ErrorCategory::Cancelled);
        reader.released().await;
        assert_eq!(reader.state.error(), Some(error));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    });
}
