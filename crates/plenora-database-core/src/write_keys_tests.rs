use super::*;
use crate::resource::ResourceLimits;
use arrow_array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow_schema::{Field, Schema};
use std::sync::Arc;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]))
}

fn batch(ids: &[Option<i64>], labels: &[Option<&str>]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(labels.to_vec())) as ArrayRef,
        ],
    )
    .expect("batch")
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

fn keys(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

#[test]
fn le_mode_per_chiavi_sono_tre() {
    for mode in [
        WriteMode::Create,
        WriteMode::Append,
        WriteMode::Replace,
        WriteMode::TruncateInsert,
    ] {
        assert!(!keyed_mode(mode), "{mode:?}");
    }
    for mode in [
        WriteMode::Update,
        WriteMode::DeleteByKeys,
        WriteMode::Upsert,
    ] {
        assert!(keyed_mode(mode), "{mode:?}");
    }
}

/// S7 della rilettura del 25/10: Update `[(1,x),(1,y)]` dava `confirmed 1
/// skipped 1` su `MySQL` e `confirmed 2` su PostgreSQL e SQL Server.
#[test]
fn una_chiave_ripetuta_nello_stesso_batch_rifiuta() {
    let budget = budget();
    let mut seen =
        UniqueInputKeys::new(&schema(), &keys(&["id"]), NullKeys::Distinct).expect("chiavi");
    let error = seen
        .admit(
            &batch(&[Some(1), Some(1)], &[Some("x"), Some("y")]),
            &budget,
        )
        .expect_err("ripetuta");
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert!(error.message.contains(DUPLICATE_INPUT_KEYS));
    assert!(!error.message.contains('1'), "nessun dato nel messaggio");
}

#[test]
fn una_chiave_ripetuta_in_un_batch_successivo_rifiuta() {
    let budget = budget();
    let mut seen =
        UniqueInputKeys::new(&schema(), &keys(&["id"]), NullKeys::Distinct).expect("chiavi");
    seen.admit(
        &batch(&[Some(1), Some(2)], &[Some("x"), Some("y")]),
        &budget,
    )
    .expect("distinte");
    assert!(seen
        .admit(
            &batch(&[Some(3), Some(2)], &[Some("z"), Some("w")]),
            &budget
        )
        .is_err());
}

#[test]
fn le_chiavi_composte_si_confrontano_per_tupla() {
    let budget = budget();
    let mut seen = UniqueInputKeys::new(&schema(), &keys(&["id", "label"]), NullKeys::Distinct)
        .expect("chiavi");
    seen.admit(
        &batch(
            &[Some(1), Some(1), Some(2)],
            &[Some("a"), Some("b"), Some("a")],
        ),
        &budget,
    )
    .expect("tuple distinte");
    assert!(seen
        .admit(&batch(&[Some(1)], &[Some("b")]), &budget)
        .is_err());
}

/// Le lunghezze davanti alle stringhe: `("ab","c")` e `("a","bc")` sono due
/// chiavi.
#[test]
fn le_stringhe_non_si_confondono_concatenate() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let input = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["ab", "a"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["c", "bc"])) as ArrayRef,
        ],
    )
    .expect("batch");
    let mut seen =
        UniqueInputKeys::new(&schema, &keys(&["a", "b"]), NullKeys::Distinct).expect("chiavi");
    seen.admit(&input, &budget()).expect("due chiavi");
}

#[test]
fn le_chiavi_con_null_non_si_registrano() {
    let budget = budget();
    let mut seen =
        UniqueInputKeys::new(&schema(), &keys(&["id"]), NullKeys::Distinct).expect("chiavi");
    seen.admit(&batch(&[None, None], &[Some("x"), Some("y")]), &budget)
        .expect("NULL non trova righe");
}

/// Quinta lettura del 25/10: con `UNIQUE NULLS NOT DISTINCT (k)` l'Upsert di
/// `[(NULL,'uno'),(NULL,'due')]` dava `confirmed 2` con una sola riga.
#[test]
fn le_chiavi_null_uguali_per_l_indice_sono_ripetute() {
    let budget = budget();
    let mut seen =
        UniqueInputKeys::new(&schema(), &keys(&["id"]), NullKeys::Equal).expect("chiavi");
    let error = seen
        .admit(&batch(&[None, None], &[Some("uno"), Some("due")]), &budget)
        .expect_err("due NULL per un indice NULLS NOT DISTINCT");
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert!(error.message.contains(DUPLICATE_INPUT_KEYS));
    // Un NULL e un valore restano due chiavi.
    let mut seen =
        UniqueInputKeys::new(&schema(), &keys(&["id"]), NullKeys::Equal).expect("chiavi");
    seen.admit(&batch(&[None, Some(0)], &[Some("a"), Some("b")]), &budget)
        .expect("NULL e 0 sono due chiavi");
    // Fra batch diversi.
    assert!(seen.admit(&batch(&[None], &[Some("c")]), &budget).is_err());
}

/// Oracle: una chiave tutta NULL non e nell'indice, una in parte NULL si.
#[test]
fn oracle_distingue_soltanto_le_chiavi_tutte_null() {
    let budget = budget();
    let composite = keys(&["id", "label"]);
    let mut seen =
        UniqueInputKeys::new(&schema(), &composite, NullKeys::EqualUnlessAllNull).expect("chiavi");
    seen.admit(&batch(&[None, None], &[None, None]), &budget)
        .expect("chiavi tutte NULL distinte");
    let error = seen
        .admit(&batch(&[Some(1), Some(1)], &[None, None]), &budget)
        .expect_err("(1,NULL) due volte");
    assert_eq!(error.category, ErrorCategory::DataMapping);
    let mut distinct =
        UniqueInputKeys::new(&schema(), &composite, NullKeys::Distinct).expect("chiavi");
    distinct
        .admit(&batch(&[Some(1), Some(1)], &[None, None]), &budget)
        .expect("MySQL: (1,NULL) due volte non e ripetuta");
}

#[test]
fn i_float_uguali_per_il_server_sono_la_stessa_chiave() {
    let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, false)]));
    for pair in [[0.0, -0.0], [f64::NAN, -f64::NAN]] {
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(pair.to_vec())) as ArrayRef],
        )
        .expect("batch");
        let mut seen =
            UniqueInputKeys::new(&schema, &keys(&["f"]), NullKeys::Distinct).expect("chiavi");
        assert!(seen.admit(&input, &budget()).is_err(), "{pair:?}");
    }
}

#[test]
fn un_tipo_non_confrontabile_si_rifiuta_prima() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "l",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    )]));
    let error =
        UniqueInputKeys::new(&schema, &keys(&["l"]), NullKeys::Distinct).expect_err("lista");
    assert_eq!(error.category, ErrorCategory::Unsupported);
}

#[test]
fn la_memoria_delle_chiavi_si_prenota_e_si_restituisce() {
    let budget = ResourceBudget::new(ResourceLimits {
        memory_bytes: 1_000,
        cell_bytes: 1_000,
        ..ResourceLimits::default()
    })
    .expect("budget");
    let mut seen =
        UniqueInputKeys::new(&schema(), &keys(&["id"]), NullKeys::Distinct).expect("chiavi");
    seen.admit(&batch(&[Some(1)], &[Some("x")]), &budget)
        .expect("una chiave");
    assert!(budget.remaining(ResourceKind::MemoryBytes) < 1_000);
    let many = (100..164).map(Some).collect::<Vec<_>>();
    let labels = vec![Some("x"); many.len()];
    let error = seen
        .admit(&batch(&many, &labels), &budget)
        .expect_err("oltre il budget");
    assert_eq!(error.category, ErrorCategory::ResourceLimit);
    drop(seen);
    assert_eq!(budget.remaining(ResourceKind::MemoryBytes), 1_000);
}

struct Batches(Vec<RecordBatch>);

impl BatchStream for Batches {
    fn schema(&self) -> SchemaRef {
        schema()
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        let next = (!self.0.is_empty()).then(|| self.0.remove(0));
        Box::pin(std::future::ready(Ok(next)))
    }
}

fn drain(mut stream: Box<dyn BatchStream>) -> Result<usize> {
    let cancellation = CancellationToken::new();
    let mut batches = 0;
    loop {
        let next = futures_executor(stream.next_batch(&cancellation))?;
        if next.is_none() {
            return Ok(batches);
        }
        batches += 1;
    }
}

/// Un executor minimo: gli stream di prova sono pronti subito.
fn futures_executor<T>(future: ProviderFuture<'_, T>) -> Result<T> {
    use std::task::{Context, Poll, Waker};
    let mut future = future;
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("stream di prova non pronto"),
    }
}

#[test]
fn lo_stream_rifiuta_soltanto_le_mode_per_chiavi() {
    let input = || {
        Box::new(Batches(vec![
            batch(&[Some(1)], &[Some("x")]),
            batch(&[Some(1)], &[Some("y")]),
        ])) as Box<dyn BatchStream>
    };
    let budget = budget();
    let append = guard_input_keys(WriteMode::Append, &[], input(), &budget, NullKeys::Distinct)
        .expect("append");
    assert_eq!(drain(append).expect("append ammessa"), 2);
    for mode in [
        WriteMode::Update,
        WriteMode::DeleteByKeys,
        WriteMode::Upsert,
    ] {
        let guarded = guard_input_keys(mode, &keys(&["id"]), input(), &budget, NullKeys::Distinct)
            .expect("guardia");
        let error = drain(guarded).expect_err("ripetuta");
        assert_eq!(error.category, ErrorCategory::DataMapping, "{mode:?}");
    }
}

/// Seconda lettura del 25/10: con `(1 -> SET k=2)` e `(2 -> SET v)` riga per
/// riga, la seconda trovava la riga appena spostata (`confirmed 2` con la
/// chiave 2 assente).
#[test]
fn update_non_aggiorna_una_chiave() {
    use crate::loss::MappingPolicy;
    use crate::plan::{ObjectRef, TransactionProfile, WriteOperation};
    let operation = |mode: WriteMode, update_columns: &[&str]| WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: None,
            object: "t".to_owned(),
        },
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: keys(&["k"]),
        update_columns: keys(update_columns),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };
    let error = refuse_key_in_update_columns(&operation(WriteMode::Update, &["k", "v"]))
        .expect_err("chiave aggiornata");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(error.message, KEY_IN_UPDATE_COLUMNS);
    refuse_key_in_update_columns(&operation(WriteMode::Update, &["v"]))
        .expect("colonna non chiave");
    refuse_key_in_update_columns(&operation(WriteMode::Upsert, &[])).expect("altre mode");
}

#[test]
fn gli_spazi_finali_non_distinguono_due_chiavi() {
    let budget = budget();
    let mut seen =
        UniqueInputKeys::new(&schema(), &keys(&["label"]), NullKeys::Distinct).expect("chiavi");
    let error = seen
        .admit(
            &batch(&[Some(1), Some(2)], &[Some("a"), Some("a  ")]),
            &budget,
        )
        .expect_err("stessa voce dell'indice con il riempimento");
    assert!(error.message.contains(DUPLICATE_INPUT_KEYS));
    let mut seen =
        UniqueInputKeys::new(&schema(), &keys(&["label"]), NullKeys::Distinct).expect("chiavi");
    // Gli spazi iniziali e gli altri caratteri bianchi distinguono ancora.
    seen.admit(
        &batch(
            &[Some(1), Some(2), Some(3)],
            &[Some("a"), Some(" a"), Some("a\t")],
        ),
        &budget,
    )
    .expect("distinte");
}

#[test]
fn gli_zeri_finali_non_distinguono_due_chiavi_binarie() {
    use arrow_array::BinaryArray;
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Binary, true)]));
    let batch = |values: Vec<&[u8]>| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BinaryArray::from(values)) as ArrayRef],
        )
        .expect("batch")
    };
    let mut seen =
        UniqueInputKeys::new(&schema, &keys(&["k"]), NullKeys::Distinct).expect("chiavi");
    let error = seen
        .admit(&batch(vec![b"\x01", b"\x01\x00"]), &budget())
        .expect_err("SQL Server confronta 0x01 = 0x0100");
    assert!(error.message.contains(DUPLICATE_INPUT_KEYS));
    let mut seen =
        UniqueInputKeys::new(&schema, &keys(&["k"]), NullKeys::Distinct).expect("chiavi");
    seen.admit(&batch(vec![b"\x01", b"\x00\x01", b""]), &budget())
        .expect("gli zeri iniziali distinguono");
}
