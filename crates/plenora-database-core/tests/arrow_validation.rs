use plenora_database_core::arrow::array::{
    types::Int8Type, Array, DictionaryArray, Int8Array, StringArray,
};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::arrow_validation::{validate_batch, validated_batch, validated_input};
use plenora_database_core::provider::{BatchStream, ProviderFuture};
use plenora_database_core::{CancellationToken, ErrorCategory, ErrorPhase};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

fn batch() -> RecordBatch {
    let keys = Int8Array::from(vec![0]);
    let values = Arc::new(StringArray::from(vec!["PAYLOAD_MUST_NOT_LEAK"]));
    let dictionary = DictionaryArray::<Int8Type>::try_new(keys, values).expect("dizionario");
    RecordBatch::try_from_iter([("v", Arc::new(dictionary) as Arc<dyn Array>)])
        .expect("schema valido")
}

struct Input(Option<RecordBatch>);
impl BatchStream for Input {
    fn schema(&self) -> plenora_database_core::arrow::SchemaRef {
        self.0.as_ref().expect("batch").schema()
    }
    fn next_batch<'a>(
        &'a mut self,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(std::future::ready(Ok(self.0.take())))
    }
}

fn ready<T>(future: impl Future<Output = T>) -> T {
    let waker = Waker::noop();
    match std::pin::pin!(future)
        .as_mut()
        .poll(&mut Context::from_waker(waker))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("fixture sincrono"),
    }
}

// I buffer malformati reali e gli ingressi custom sono esercitati nel binding
// da write_arrow_ingress_tests.rs (FFI PyArrow, senza unsafe nel workspace).
#[test]
fn upstream_errors_are_preserved_and_valid_host_batches_are_delivered() {
    let cancellation = CancellationToken::new();
    let error = plenora_database_core::DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Prepare,
        None,
        "dati Arrow non validi",
    );
    let error = ready(validated_batch(
        Box::pin(std::future::ready(Err(error))),
        ErrorPhase::Read,
    ))
    .expect_err("nessuna consegna dopo errore");
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert_eq!(error.phase, ErrorPhase::Prepare);
    assert_eq!(error.message, "dati Arrow non validi");
    validate_batch(&batch(), ErrorPhase::Prepare).expect("dizionario valido");
    let mut input = validated_input(Box::new(Input(Some(batch()))));
    assert!(ready(input.next_batch(&cancellation))
        .expect("valido")
        .is_some());
}

#[test]
#[ignore = "misura locale esplicita; nessuna soglia temporale in CI"]
fn benchmark_original_batch_validation() {
    let values = Arc::new(StringArray::from(vec!["abcdefghij"; 2_000_000]));
    let batch = RecordBatch::try_from_iter([("v", values as Arc<dyn Array>)]).expect("batch");
    let cancellation = CancellationToken::new();
    for checked in [false, true] {
        let mut samples = Vec::new();
        for repeat in 0..21 {
            let mut input = Input(Some(batch.clone()));
            let start = std::time::Instant::now();
            let result = if checked {
                ready(validated_batch(
                    input.next_batch(&cancellation),
                    ErrorPhase::Read,
                ))
            } else {
                ready(input.next_batch(&cancellation))
            };
            std::hint::black_box(result.expect("valido"));
            if repeat > 0 {
                samples.push(start.elapsed());
            }
        }
        samples.sort();
        println!("validated={checked} rows=2000000 median={:?}", samples[10]);
    }
}
