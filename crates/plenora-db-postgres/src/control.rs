use plenora_database_core::CancellationToken;

pub async fn select_with_cancellation<T, F>(
    future: F,
    cancellation: &CancellationToken,
) -> Option<T>
where
    F: std::future::Future<Output = T>,
{
    tokio::pin!(future);
    tokio::select! {
        result = &mut future => Some(result),
        _reason = cancellation.cancelled() => None,
    }
}
