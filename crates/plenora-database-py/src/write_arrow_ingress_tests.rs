//! Buffer esterni reali, importati validi via FFI e poi corrotti dal produttore.
//! Si cambia soltanto un offset/indice, mantenendo allocazione e lifetime: il
//! test non dereferenzia indirizzi fuori buffer e non introduce unsafe.
//! Windows collega libpython anche con extension-module; sugli altri sistemi
//! questa prova embedded non viene compilata (la suite SDK prova gli IPC).
use arrow_pyarrow::FromPyArrow;
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::arrow_validation::{
    validate_array, validate_batch, validated_batch, validated_input,
};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::{BatchStream, ProviderFuture};
use plenora_database_core::{CancellationToken, ErrorCategory, ErrorPhase};
use pyo3::prelude::*;
use pyo3::types::PyModule;

struct Input(Option<RecordBatch>, SchemaRef);
impl BatchStream for Input {
    fn schema(&self) -> SchemaRef {
        self.1.clone()
    }
    fn next_batch<'a>(
        &'a mut self,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(std::future::ready(Ok(self.0.take())))
    }
}

fn assert_redacted(error: plenora_database_core::DatabaseError, phase: ErrorPhase) {
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert_eq!(error.phase, phase);
    assert_eq!(error.message, "dati Arrow non validi");
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "matrice unica: ogni buffer attraversa gli stessi confini e le stesse asserzioni"
)]
fn malformed_ffi_buffers_reach_custom_streams_and_direct_helpers() {
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::from_code(
            py,
            c"
import pyarrow as pa
import struct

def make(kind):
    global storage, batch, bad
    if kind == 'dictionary':
        storage = bytearray([0])
        keys = pa.Array.from_buffers(pa.int8(), 1, [None, pa.py_buffer(storage)])
        array = pa.DictionaryArray.from_arrays(keys, pa.array(['PAYLOAD_MUST_NOT_LEAK']))
        bad = bytes([7])
    else:
        payload = b'PAYLOAD_MUST_NOT_LEAK'
        end = 1 if kind == 'list' else len(payload)
        storage = bytearray(struct.pack('<ii', 0, end))
        if kind == 'list':
            array = pa.Array.from_buffers(pa.list_(pa.string()), 1,
                [None, pa.py_buffer(storage)], children=[pa.array([payload.decode()])])
        else:
            dtype = pa.string() if kind == 'string' else pa.binary()
            array = pa.Array.from_buffers(dtype, 1,
                [None, pa.py_buffer(storage), pa.py_buffer(payload)])
        bad = struct.pack('<ii', 0, end + 7)
    batch = pa.record_batch([array], names=['v'])
    batch.validate(full=True)
    return batch

def corrupt():
    storage[:] = bad
",
            c"arrow_ingress_fixture.py",
            c"arrow_ingress_fixture",
        )
        .expect("fixture Python");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let cancellation = CancellationToken::new();
        for kind in ["string", "binary", "list", "dictionary"] {
            let original = module
                .getattr("make")
                .expect("make")
                .call1((kind,))
                .expect("batch Python");
            let batch = RecordBatch::from_pyarrow_bound(&original).expect("FFI valido");
            validate_batch(&batch, ErrorPhase::Prepare)
                .expect("controllo positivo prima della corruzione");
            module
                .getattr("corrupt")
                .expect("corrupt")
                .call0()
                .expect("corruzione buffer esterno");
            let array = batch.column(0).as_ref();
            assert!(
                array.to_data().validate_full().is_err(),
                "il buffer originale deve essere realmente malformato"
            );
            assert_redacted(
                validate_array(array, ErrorPhase::Prepare).expect_err("array"),
                ErrorPhase::Prepare,
            );
            assert_redacted(
                validate_batch(&batch, ErrorPhase::Write).expect_err("batch"),
                ErrorPhase::Write,
            );
            // Il future e Ok(Some(batch malformato)), mai un errore predisposto.
            for phase in [ErrorPhase::Read, ErrorPhase::Write] {
                let mut input = Input(Some(batch.clone()), batch.schema());
                assert_redacted(
                    runtime
                        .block_on(validated_batch(input.next_batch(&cancellation), phase))
                        .expect_err("confine provider"),
                    phase,
                );
            }
            // Lo stream restituito dal callback host passa al provider custom
            // attraverso lo stesso adapter usato da RuntimeBinding::write.
            let host_callback =
                || Box::new(Input(Some(batch.clone()), batch.schema())) as Box<dyn BatchStream>;
            let mut input = validated_input(host_callback());
            assert_redacted(
                runtime
                    .block_on(input.next_batch(&cancellation))
                    .expect_err("callback host"),
                ErrorPhase::Write,
            );
            let guard = plenora_database_core::store_rewrite::RewriteGuard::validation_only(
                ProviderKind::Postgres,
            );
            assert_redacted(guard.check(&batch).expect_err("rewrite"), ErrorPhase::Write);
            let budget = plenora_database_core::resource::ResourceBudget::new(
                plenora_database_core::resource::ResourceLimits::default(),
            )
            .expect("budget");
            let mut keys = plenora_database_core::write_keys::UniqueInputKeys::new(
                &batch.schema(),
                &[],
                plenora_database_core::write_keys::NullKeys::Distinct,
            )
            .expect("tracker");
            assert_redacted(
                keys.admit(&batch, &budget).expect_err("chiavi"),
                ErrorPhase::Write,
            );
            assert_redacted(
                plenora_database_engine::ValidatedWriteArray::new(array)
                    .err()
                    .expect("codec"),
                ErrorPhase::Write,
            );
            assert_redacted(
                plenora_database_engine::arrow_parameter_value(
                    array,
                    batch.schema().field(0),
                    0,
                    ' ',
                )
                .expect_err("parametro"),
                ErrorPhase::Write,
            );
            assert_redacted(
                plenora_database_engine::arrow_binary_value(array, array.data_type(), 0)
                    .expect_err("binario"),
                ErrorPhase::Write,
            );
            if kind == "binary" {
                let binary = array
                    .as_any()
                    .downcast_ref::<plenora_database_core::arrow::array::BinaryArray>()
                    .expect("binary");
                let result = plenora_database_engine::runtime::inspect_spatial_arrays(
                    [(0, binary)],
                    100,
                    1000,
                    10,
                    |error, _, _| error,
                    |_, _, _| Ok(()),
                );
                assert_redacted(result.expect_err("helper spatial"), ErrorPhase::Write);
            }
            assert_redacted(
                runtime
                    .block_on(plenora_database_engine::public_ops::summarize_stream(
                        ProviderKind::Postgres,
                        &mut Input(Some(batch.clone()), batch.schema()),
                        &cancellation,
                    ))
                    .expect_err("riepilogo"),
                ErrorPhase::Read,
            );
        }
    });
}
