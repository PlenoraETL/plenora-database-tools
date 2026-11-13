//! One lifecycle for sync/async readers; failures are recorded before leaving Rust.
use crate::stream_state::StreamState;
use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::provider::BatchStream;
use plenora_database_core::{
    CancellationReason, CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result,
};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Reader {
    inner: Arc<Mutex<Option<Box<dyn BatchStream>>>>,
    pub schema: SchemaRef,
    pub cancellation: CancellationToken,
    pub state: StreamState,
}

impl Reader {
    pub fn new(stream: Box<dyn BatchStream>, parent: CancellationToken) -> Self {
        let state = StreamState::default();
        state.report(stream.read_loss());
        Self {
            schema: stream.schema(),
            inner: Arc::new(Mutex::new(Some(stream))),
            cancellation: parent,
            state,
        }
    }

    pub fn close(&self) {
        let _runtime = crate::runtime().enter();
        self.cancellation.cancel();
        self.state.close();
        if let Ok(mut guard) = self.inner.try_lock() {
            drop(guard.take());
        } else {
            let inner = self.inner.clone();
            crate::runtime().spawn(async move {
                drop(inner.lock().await.take());
            });
        }
    }

    pub async fn released(&self) {
        self.close();
        drop(self.inner.lock().await.take());
    }

    pub fn mapping_failure(&self) -> DatabaseError {
        self.local_failure(DatabaseError::new(
            ErrorCategory::DataMapping,
            ErrorPhase::Read,
            None,
            "stream Arrow SDK non leggibile",
        ))
    }

    pub fn interruption(&self) -> DatabaseError {
        self.local_failure(StreamState::interrupted())
    }

    fn local_failure(&self, error: DatabaseError) -> DatabaseError {
        let error = self.state.record_failure(error);
        self.close();
        error
    }

    pub fn schema_bytes(&self) -> Result<Vec<u8>> {
        if let Some(error) = self.state.error() {
            return Err(error);
        }
        crate::arrow_reader::schema_to_ipc_bytes(&self.schema).map_err(|error| {
            let error = self.state.record_failure(error);
            self.close();
            error
        })
    }

    pub async fn next(&self) -> Result<Option<Vec<u8>>> {
        let mut guard = self.inner.lock().await;
        if let Some(error) = self.state.error() {
            drop(guard.take());
            return Err(error);
        }
        let Some(stream) = guard.as_mut() else {
            return Ok(None);
        };
        let result = if let Some(reason) = self.cancellation.reason() {
            Err(Self::cancelled(reason))
        } else {
            tokio::select! {
                biased;
                result = plenora_database_core::arrow_validation::validated_batch(
                    stream.next_batch(&self.cancellation), ErrorPhase::Read) => result,
                reason = self.cancellation.cancelled() => Err(Self::cancelled(reason)),
            }
        };
        self.state.report(stream.read_loss());
        let result = result.and_then(|batch| {
            batch
                .as_ref()
                .map(crate::arrow_reader::batch_to_ipc_bytes)
                .transpose()
        });
        let result = self.state.publish(result);
        if !matches!(result, Ok(Some(_))) {
            drop(guard.take());
        }
        drop(guard);
        result.map_err(|error| self.state.record_failure(error))
    }

    fn cancelled(reason: CancellationReason) -> DatabaseError {
        DatabaseError::new(
            if reason == CancellationReason::Deadline {
                ErrorCategory::Timeout
            } else {
                ErrorCategory::Cancelled
            },
            ErrorPhase::Read,
            None,
            "lettura Arrow annullata",
        )
    }

    pub fn flight(&self) -> Flight {
        Flight {
            reader: self.clone(),
            armed: true,
        }
    }
}

/// Dropping a cancelled Python await releases the stream even before its first poll.
pub struct Flight {
    pub reader: Reader,
    armed: bool,
}
impl Flight {
    pub const fn finish(&mut self) {
        self.armed = false;
    }
}
impl Drop for Flight {
    fn drop(&mut self) {
        if self.armed {
            self.reader.close();
        }
    }
}

#[cfg(test)]
#[path = "stream_reader_tests.rs"]
mod tests;
