use super::*;
use mysql_async::consts::ColumnType;
use plenora_database_core::arrow::array::Int64Array;
use plenora_database_core::arrow::Schema;
use plenora_database_core::resource::ResourceLimits;
use plenora_database_core::RetryDisposition;

/// Stima conservativa di una riga di sole colonne intere: per ciascuna
/// `CONSERVATIVE_CELL_BYTES` piu i 32 byte di payload numerico.
const INTEGER_ROW_BYTES: u64 = 4 * (CONSERVATIVE_CELL_BYTES + 32);

fn integer_column(name: &str) -> crate::MysqlColumnSpec {
    crate::MysqlColumnSpec {
        name: name.to_owned(),
        native_type: "bigint".to_owned(),
        native_declaration: "bigint".to_owned(),
        nullable: false,
        collation: None,
        kind: MysqlColumnKind::I64,
        spatial_srid: None,
        spatial_srid_declared: false,
    }
}

fn binary_column(name: &str) -> crate::MysqlColumnSpec {
    crate::MysqlColumnSpec {
        name: name.to_owned(),
        native_type: "varbinary".to_owned(),
        native_declaration: "varbinary(65535)".to_owned(),
        nullable: false,
        collation: None,
        kind: MysqlColumnKind::Binary,
        spatial_srid: None,
        spatial_srid_declared: false,
    }
}

fn wire_columns(columns: &[crate::MysqlColumnSpec]) -> Arc<[mysql_async::Column]> {
    columns
        .iter()
        .map(|column| {
            let column_type = if column.kind == MysqlColumnKind::Binary {
                ColumnType::MYSQL_TYPE_BLOB
            } else {
                ColumnType::MYSQL_TYPE_LONGLONG
            };
            mysql_async::Column::new(column_type).with_name(column.name.as_bytes())
        })
        .collect()
}

/// Righe di sole colonne intere, con l'identificatore replicato su ogni
/// colonna: l'ordine sorgente resta leggibile in qualunque batch.
fn integer_rows(columns: &[crate::MysqlColumnSpec], count: i64) -> Vec<Row> {
    let wire = wire_columns(columns);
    (1..=count)
        .map(|id| {
            let values = columns.iter().map(|_| Value::Int(id)).collect::<Vec<_>>();
            mysql_common::row::new_row(values, Arc::clone(&wire))
        })
        .collect()
}

fn test_schema(columns: &[crate::MysqlColumnSpec]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(crate::MysqlColumnSpec::arrow_field)
            .collect::<Vec<_>>(),
    ))
}

/// Costruisce lo stream sopra un worker sintetico che rispetta lo stesso
/// protocollo a domanda del worker `MySQL`: una riga per ogni richiesta.
fn spawn_stream(
    columns: Vec<crate::MysqlColumnSpec>,
    rows: Vec<Row>,
    budget: &ResourceBudget,
) -> MysqlBatchStream {
    let (sender, receiver) = mpsc::channel(ROW_CHANNEL_CAPACITY);
    let (demand_sender, mut demand_receiver) = mpsc::channel(ROW_CHANNEL_CAPACITY);
    let worker_task = tokio::spawn(async move {
        for row in rows {
            if demand_receiver.recv().await.is_none() {
                return;
            }
            if sender.send(Ok(row)).await.is_err() {
                return;
            }
        }
    });
    let schema = test_schema(&columns);
    let column_count = u64::try_from(columns.len()).expect("colonne rappresentabili");
    MysqlBatchStream {
        receiver,
        profile: &crate::profile::MYSQL_PROFILE,
        demand_sender,
        columns,
        crs_checks: Vec::new(),
        schema,
        batch_rows: DEFAULT_BATCH_ROWS,
        budget: budget.clone(),
        cancellation: CancellationToken::new(),
        deadline_task: tokio::spawn(async {}),
        worker_task: Some(worker_task),
        _operation_lease: budget
            .try_lease(ResourceKind::ConcurrentOperations, 1)
            .expect("lease operazione"),
        _columns_lease: budget
            .try_lease(ResourceKind::Columns, column_count)
            .expect("lease colonne"),
        state: MysqlStreamState::Active,
        pending: None,
        read_diagnostics: ReadDiagnosticsTracker::new(ReadDiagnosticsPolicy::default())
            .expect("tracker"),
    }
}

async fn collect_batches(
    stream: &mut MysqlBatchStream,
    cancellation: &CancellationToken,
) -> Vec<Vec<i64>> {
    let mut batches = Vec::new();
    while let Some(batch) = stream
        .next_batch(cancellation)
        .await
        .expect("batch consegnato")
    {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("prima colonna intera");
        batches.push((0..column.len()).map(|row| column.value(row)).collect());
    }
    batches
}

fn bounded_budget(memory_bytes: u64) -> ResourceBudget {
    ResourceBudget::new(ResourceLimits {
        memory_bytes,
        output_bytes: memory_bytes,
        cell_bytes: memory_bytes.min(65_536),
        ..ResourceLimits::default()
    })
    .expect("budget di prova")
}

/// Con i limiti di default e quattro colonne il batch raccoglie tutte le
/// righe disponibili. Il residuo non viene piu confrontato con il massimo
/// teorico `cell_bytes × colonne`, che da solo bastava a chiudere il batch
/// dopo una riga sola.
#[tokio::test]
async fn default_limits_batch_many_rows_over_four_columns() {
    let columns = vec![
        integer_column("a"),
        integer_column("b"),
        integer_column("c"),
        integer_column("d"),
    ];
    let rows = integer_rows(&columns, 64);
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget default");
    let mut stream = spawn_stream(columns, rows, &budget);
    let cancellation = CancellationToken::new();
    assert_eq!(
        collect_batches(&mut stream, &cancellation).await,
        vec![(1..=64).collect::<Vec<i64>>()]
    );
}

/// La riga che non entra nel residuo corrente apre il batch successivo
/// nella sua posizione sorgente, senza perdite ne duplicazioni.
#[tokio::test]
async fn a_row_that_does_not_fit_opens_the_next_batch() {
    let columns = vec![
        integer_column("a"),
        integer_column("b"),
        integer_column("c"),
        integer_column("d"),
    ];
    let rows = integer_rows(&columns, 270);
    // 100 000 / 384 = 260 righe intere nel primo batch; la 261esima
    // eccede il residuo ed e rinviata.
    let admitted = i64::try_from(100_000 / INTEGER_ROW_BYTES).expect("righe ammesse");
    let budget = bounded_budget(100_000);
    let mut stream = spawn_stream(columns, rows, &budget);
    let cancellation = CancellationToken::new();
    assert_eq!(
        collect_batches(&mut stream, &cancellation).await,
        vec![
            (1..=admitted).collect::<Vec<i64>>(),
            (admitted + 1..=270).collect::<Vec<i64>>(),
        ]
    );
}

/// Una riga sola oltre il budget del batch non ha un batch successivo dove
/// essere rinviata: fallisce `ResourceLimit` invece di produrre un batch
/// vuoto o un ciclo che la ripropone.
#[tokio::test]
async fn a_single_row_over_the_batch_budget_fails_with_resource_limit() {
    let columns = vec![
        integer_column("id"),
        binary_column("payload"),
        integer_column("a"),
        integer_column("b"),
    ];
    let wire = wire_columns(&columns);
    let row = mysql_common::row::new_row(
        vec![
            Value::Int(1),
            Value::Bytes(vec![0x41; 60_000]),
            Value::Int(1),
            Value::Int(1),
        ],
        wire,
    );
    let budget = bounded_budget(100_000);
    let mut stream = spawn_stream(columns, vec![row], &budget);
    let cancellation = CancellationToken::new();
    for _ in 0..2 {
        assert_eq!(
            stream
                .next_batch(&cancellation)
                .await
                .expect_err("riga oltre il budget del batch")
                .category,
            ErrorCategory::ResourceLimit
        );
    }
}

/// Il carry-over non altera ne l'ordine ne il conteggio delle righe
/// consegnate, qualunque sia il punto in cui cade il confine del batch.
#[tokio::test]
async fn rows_keep_their_order_and_count_across_batch_boundaries() {
    let columns = vec![
        integer_column("a"),
        integer_column("b"),
        integer_column("c"),
        integer_column("d"),
    ];
    let rows = integer_rows(&columns, 250);
    let budget = bounded_budget(40_000);
    let mut stream = spawn_stream(columns, rows, &budget);
    let cancellation = CancellationToken::new();
    let batches = collect_batches(&mut stream, &cancellation).await;
    assert!(batches.len() > 1, "confine di batch non attraversato");
    assert!(batches.iter().all(|batch| !batch.is_empty()));
    assert_eq!(batches.concat(), (1..=250).collect::<Vec<i64>>());
}

/// La cancellazione con una riga trattenuta restituisce al budget memoria
/// esattamente la quota che la riga aveva prenotato.
#[tokio::test]
async fn cancellation_with_a_pending_row_returns_its_memory_lease() {
    let columns = vec![
        integer_column("a"),
        integer_column("b"),
        integer_column("c"),
        integer_column("d"),
    ];
    let rows = integer_rows(&columns, 270);
    let budget = bounded_budget(100_000);
    let mut stream = spawn_stream(columns, rows, &budget);
    let cancellation = CancellationToken::new();
    let first = stream
        .next_batch(&cancellation)
        .await
        .expect("primo batch")
        .expect("batch non vuoto");
    assert_eq!(
        u64::try_from(first.num_rows()).expect("righe rappresentabili"),
        100_000 / INTEGER_ROW_BYTES
    );
    let parked = budget.remaining(ResourceKind::MemoryBytes);
    cancellation.cancel();
    assert_eq!(
        stream
            .next_batch(&cancellation)
            .await
            .expect_err("cancellazione con riga trattenuta")
            .category,
        ErrorCategory::Cancelled
    );
    assert_eq!(
        budget.remaining(ResourceKind::MemoryBytes),
        parked + INTEGER_ROW_BYTES
    );
}

#[test]
fn invalid_batch_size_is_rejected_before_io() {
    assert_eq!(
        validate_batch_rows(0).expect_err("zero batch").category,
        ErrorCategory::InvalidPlan
    );
    assert_eq!(
        validate_batch_rows(MAX_BATCH_ROWS + 1)
            .expect_err("oversized batch")
            .category,
        ErrorCategory::InvalidPlan
    );
}

#[test]
fn reservation_fails_before_allocation_when_rows_are_exhausted() {
    let budget = ResourceBudget::new(ResourceLimits {
        rows: 1,
        ..ResourceLimits::default()
    })
    .expect("budget");
    let consumed = budget.try_lease(ResourceKind::Rows, 1).expect("row lease");
    consumed.commit(1).expect("commit row");
    assert_eq!(
        reserve_batch(&budget, 1, &[])
            .expect_err("exhausted")
            .category,
        ErrorCategory::ResourceLimit
    );
}

#[test]
fn terminal_stream_error_is_sticky_instead_of_becoming_eof() {
    let error = read_error(
        ErrorCategory::Protocol,
        ErrorPhase::Read,
        "errore terminale",
    );
    let state = MysqlStreamState::Failed(error.clone());
    for _ in 0..2 {
        assert_eq!(
            state
                .terminal_result()
                .expect("stato terminale")
                .expect_err("errore sticky"),
            error
        );
    }
}

#[test]
fn builder_capacity_is_bounded_by_the_byte_budget_before_allocation() {
    assert_eq!(
        bounded_buffer_capacity(8_192, 128, 1).expect("due righe conservative"),
        2
    );
    assert_eq!(
        bounded_buffer_capacity(1, 63, 1)
            .expect_err("budget inferiore a una riga")
            .category,
        ErrorCategory::ResourceLimit
    );
}

fn spatial_column(name: &str) -> crate::MysqlColumnSpec {
    crate::MysqlColumnSpec {
        name: name.to_owned(),
        native_type: "geometry".to_owned(),
        native_declaration: "geometry".to_owned(),
        nullable: true,
        collation: None,
        kind: MysqlColumnKind::Geometry,
        spatial_srid: None,
        spatial_srid_declared: false,
    }
}

/// Un difetto di conversione osservato mentre il batch è in costruzione
/// pubblica l'indice sorgente assoluto e la colonna del piano.
#[test]
fn a_read_conversion_defect_publishes_the_absolute_source_index() {
    let mut tracker =
        ReadDiagnosticsTracker::new(ReadDiagnosticsPolicy::default()).expect("tracker");
    tracker
        .publish_batch(8_192)
        .expect("primo batch pubblicato");
    let columns = [spatial_column("shape"), spatial_column("footprint")];

    let error = attribute_conversion_defect(
        &tracker,
        &columns,
        read_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Read,
            "valore MySQL non rappresentabile",
        ),
        Some(17),
        Some(1),
    );
    assert_eq!(error.phase, ErrorPhase::Read);
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
    let report = error.row_diagnostics().expect("diagnostica MySQL");
    report.validate().expect("documento valido");
    assert_eq!(
        serde_json::to_value(report).expect("documento serializzabile"),
        serde_json::json!({
            "contract": "plenora-row-diagnostics-v1",
            "scope": "read",
            "index_basis": "source_row_zero_based",
            "completeness": "partial",
            "knowledge_limits": [
                "read.batches_already_published",
                "read.scan_stopped_at_first_defect"
            ],
            "observed_total": 1,
            "counts": {"conversion.value_not_representable": 1},
            "examples_limit": 10,
            "examples_truncated": false,
            "examples": [{
                "source_index": 8_209,
                "cause": "conversion.value_not_representable",
                "column": "footprint"
            }]
        })
    );
}

/// Una colonna fuori dal piano non produce un nome inventato e un errore
/// che non è un difetto di conversione non riceve una riga sorgente.
#[test]
fn unattributable_read_failures_never_invent_provenance() {
    let tracker = ReadDiagnosticsTracker::new(ReadDiagnosticsPolicy::default()).expect("tracker");
    let columns = [spatial_column("shape")];

    let error = attribute_conversion_defect(
        &tracker,
        &columns,
        read_error(ErrorCategory::DataMapping, ErrorPhase::Read, "difetto"),
        Some(3),
        Some(9),
    );
    let report = error.row_diagnostics().expect("diagnostica MySQL");
    assert_eq!(report.examples[0].source_index, 3);
    assert!(report.examples[0].column.is_none());

    let unknown_row = attribute_conversion_defect(
        &tracker,
        &columns,
        read_error(ErrorCategory::DataMapping, ErrorPhase::Read, "difetto"),
        None,
        Some(0),
    );
    let report = unknown_row.row_diagnostics().expect("diagnostica MySQL");
    assert_eq!(
        report.completeness,
        plenora_database_core::row_diagnostics::Completeness::Unknown,
    );
    assert!(report.examples.is_empty(), "nessun indice inventato");

    let budget = attribute_conversion_defect(
        &tracker,
        &columns,
        DatabaseError::resource_limit("budget MySQL esaurito"),
        Some(3),
        Some(0),
    );
    assert_eq!(budget.category, ErrorCategory::ResourceLimit);
    assert!(budget.row_diagnostics().is_none());
}

#[tokio::test]
async fn expired_resource_deadline_maps_to_timeout() {
    let budget = ResourceBudget::new(ResourceLimits {
        duration_ms: 1,
        ..ResourceLimits::default()
    })
    .expect("budget breve");
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    assert_eq!(
        ensure_active_read_budget(&budget, &crate::profile::MYSQL_PROFILE)
            .expect_err("deadline scaduta")
            .category,
        ErrorCategory::Timeout
    );
}
