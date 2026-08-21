use crate::profile::{ProductProfile, MYSQL_PROFILE};
use crate::{MysqlConfig, MysqlPool};
use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::capabilities::ProviderCapabilities;
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{Operation, ProviderKind, ReadOperation, WriteOperation};
use plenora_database_core::provider::{
    BatchStream, ConnectionInfo, Inspection, ParameterBag, PreparedWrite, Provider, ProviderFuture,
    SecretString,
};
use plenora_database_core::query::QueryOperation;
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::resource::ResourceKind;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

static WRITE_EXECUTION_SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct CachedPool {
    secret_fingerprint: [u8; 32],
    pool: Arc<MysqlPool>,
}

pub struct MysqlProvider {
    config: MysqlConfig,
    max_connections: usize,
    cached_pool: Mutex<Option<CachedPool>>,
    /// Il prodotto che questo provider serve.
    ///
    /// Non e configurabile e non si deduce dal server: lo fissa il
    /// costruttore. Un provider che scoprisse il proprio prodotto
    /// connettendosi sarebbe una selezione automatica, che ADR 0014 esclude
    /// — il consumatore sceglie il provider, e il provider sa gia cosa
    /// serve. Il riconoscimento alla probe verifica quella scelta, non la
    /// compie.
    profile: &'static dyn ProductProfile,
}

// Il profilo non compare nel `Debug`: quell'output e superficie
// osservata, e questa fase non ne cambia nemmeno una riga. Il
// prodotto servito si legge dal provider, non da qui.
#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for MysqlProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MysqlProvider")
            .field("config", &self.config)
            .field("max_connections", &self.max_connections)
            .field(
                "pool_initialized",
                &lock_recover(&self.cached_pool).is_some(),
            )
            .finish()
    }
}

impl MysqlProvider {
    /// Costruisce un provider con pool lazy e configurazione validata.
    ///
    /// # Errors
    ///
    /// Fallisce se configurazione o limiti del pool non sono validi.
    pub fn new(config: MysqlConfig, max_connections: usize) -> Result<Self> {
        Self::with_profile(config, max_connections, &MYSQL_PROFILE)
    }

    /// Il costruttore reale: nessun `MysqlProvider` esiste senza un profilo.
    ///
    /// `new` non ha un ramo alternativo, e non deve averlo: se la
    /// costruzione potesse saltare il profilo, esisterebbe un provider le cui
    /// decisioni di prodotto sono quelle sparse nel codice invece che quelle
    /// dichiarate. E il seam da cui entrera il secondo provider.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn with_profile(
        config: MysqlConfig,
        max_connections: usize,
        profile: &'static dyn ProductProfile,
    ) -> Result<Self> {
        // Anche il costruttore e un bordo: qui il provider non esiste ancora,
        // ma il profilo si', e sono gli unici errori che un consumatore vede
        // senza aver mai toccato il server. Uscivano con il segnaposto.
        crate::profile::attributed(profile, config.validate_for_product(profile.product()))?;
        if max_connections == 0 {
            let mut error = provider_error(
                ErrorCategory::InvalidConfiguration,
                ErrorPhase::Validate,
                format!("provider {} con pool a capacita zero", profile.product()),
            );
            error.provider = Some(profile.kind());
            return Err(error);
        }
        Ok(Self {
            config,
            max_connections,
            cached_pool: Mutex::new(None),
            profile,
        })
    }

    fn pool_for(&self, secret: &SecretString) -> Result<Arc<MysqlPool>> {
        let fingerprint: [u8; 32] = Sha256::digest(secret.expose().as_bytes()).into();
        let mut cached = lock_recover(&self.cached_pool);
        if let Some(candidate) = cached.as_ref() {
            if candidate.secret_fingerprint == fingerprint {
                return Ok(Arc::clone(&candidate.pool));
            }
        }
        let config = self.config.clone().with_password(secret.clone());
        // Il pool eredita il profilo del provider, e con esso ogni sessione
        // che ne esce: `MysqlPool::new` forzerebbe quello MySQL, e un secondo
        // provider avrebbe connessioni che si dichiarano del prodotto
        // sbagliato fin dalla probe.
        let pool = Arc::new(MysqlPool::new_with_profile(
            &config,
            self.max_connections,
            self.profile,
        )?);
        *cached = Some(CachedPool {
            secret_fingerprint: fingerprint,
            pool: Arc::clone(&pool),
        });
        drop(cached);
        Ok(pool)
    }

    fn validate_source(&self, source: &plenora_database_core::plan::ObjectRef) -> Result<()> {
        let product = self.profile.product();
        if source
            .catalog
            .as_deref()
            .is_some_and(|catalog| catalog != self.config.database())
        {
            return Err(unsupported(format!(
                "accesso cross-database {product} non supportato dal provider"
            )));
        }
        Ok(())
    }

    async fn inspect_operation(
        &self,
        pool: &MysqlPool,
        operation: &Operation,
        cancellation: &CancellationToken,
    ) -> Result<Inspection> {
        let product = self.profile.product();
        let mut session = pool.checkout(cancellation).await?;
        let result = match operation {
            Operation::DatabaseListCatalogs => Ok(Inspection {
                operation: "database.list_catalogs".to_owned(),
                document: json!({"catalogs": [self.config.database()]}),
            }),
            Operation::DatabaseListSchemas { source } => {
                if let Some(source) = source {
                    self.validate_source(source)?;
                }
                let schemas = crate::catalog::list_schemas_with_profile(
                    &mut session,
                    self.profile,
                    cancellation,
                )
                .await?;
                Ok(Inspection {
                    operation: "database.list_schemas".to_owned(),
                    document: json!({"schemas": schemas}),
                })
            }
            Operation::DatabaseListObjects { source } => {
                if let Some(source) = source {
                    self.validate_source(source)?;
                }
                let schema = source
                    .as_ref()
                    .and_then(|value| value.schema.as_deref())
                    .unwrap_or_else(|| self.config.database());
                let objects = crate::catalog::list_objects_with_profile(
                    &mut session,
                    schema,
                    self.profile,
                    cancellation,
                )
                .await?;
                Ok(Inspection {
                    operation: "database.list_objects".to_owned(),
                    document: json!({"schema": schema, "objects": objects}),
                })
            }
            Operation::DatabaseDescribeObject { source } => {
                self.validate_source(source)?;
                let schema = source
                    .schema
                    .as_deref()
                    .unwrap_or_else(|| self.config.database());
                let description = crate::catalog::describe_object_with_profile(
                    &mut session,
                    schema,
                    &source.object,
                    self.profile,
                    cancellation,
                )
                .await?;
                Ok(Inspection {
                    operation: "database.describe_object".to_owned(),
                    document: json!(description),
                })
            }
            _ => Err(unsupported(format!(
                "operazione inspect non supportata da {product}"
            ))),
        };
        drop(session);
        result
    }
}

impl Provider for MysqlProvider {
    fn kind(&self) -> ProviderKind {
        self.profile.kind()
    }

    fn test_connection<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        Box::pin(async move {
            // Bordo: da qui in poi l'attribuzione e quella del
            // profilo, non il segnaposto con cui l'errore e nato.
            let outcome = async move {
                let pool = self.pool_for(secret)?;
                let mut session = pool.checkout(cancellation).await?;
                let probe = crate::catalog::probe_server_with_profile(
                    &mut session,
                    self.profile,
                    cancellation,
                )
                .await?;
                drop(session);
                Ok(ConnectionInfo {
                    provider: self.profile.kind(),
                    server_version: probe.product_version,
                    connection_identity: Some(probe.database),
                })
            }
            .await;
            crate::profile::attributed(self.profile, outcome)
        })
    }

    fn probe_capabilities<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        Box::pin(async move {
            // Bordo: da qui in poi l'attribuzione e quella del
            // profilo, non il segnaposto con cui l'errore e nato.
            let outcome = async move {
                let pool = self.pool_for(secret)?;
                let mut session = pool.checkout(cancellation).await?;
                let probe = crate::catalog::probe_server_with_profile(
                    &mut session,
                    self.profile,
                    cancellation,
                )
                .await?;
                drop(session);
                Ok(self.profile.capabilities(probe.product_version))
            }
            .await;
            crate::profile::attributed(self.profile, outcome)
        })
    }

    fn inspect<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a Operation,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        Box::pin(async move {
            // Bordo: da qui in poi l'attribuzione e quella del
            // profilo, non il segnaposto con cui l'errore e nato.
            let outcome = async move {
                let pool = self.pool_for(secret)?;
                self.inspect_operation(&pool, operation, cancellation).await
            }
            .await;
            crate::profile::attributed(self.profile, outcome)
        })
    }

    fn read<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a ReadOperation,
        parameters: &'a ParameterBag,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async move {
            // Bordo: da qui in poi l'attribuzione e quella del
            // profilo, non il segnaposto con cui l'errore e nato.
            let outcome = async move {
                self.validate_source(&operation.source)?;
                let pool = self.pool_for(secret)?;
                let mut effective = operation.clone();
                effective
                    .source
                    .schema
                    .get_or_insert_with(|| self.config.database().to_owned());
                crate::read::read_operation_with_profile(
                    &pool,
                    &effective,
                    parameters,
                    crate::DEFAULT_BATCH_ROWS,
                    self.profile,
                    budget,
                    cancellation,
                )
                .await
            }
            .await;
            crate::profile::attributed(self.profile, outcome)
        })
    }

    fn query<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a QueryOperation,
        parameters: &'a ParameterBag,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async move {
            // Bordo: da qui in poi l'attribuzione e quella del
            // profilo, non il segnaposto con cui l'errore e nato.
            let outcome = async move {
                let pool = self.pool_for(secret)?;
                crate::read::query_operation_with_profile(
                    &pool,
                    self.config.database(),
                    operation,
                    parameters,
                    crate::DEFAULT_BATCH_ROWS,
                    self.profile,
                    budget,
                    cancellation,
                )
                .await
            }
            .await;
            crate::profile::attributed(self.profile, outcome)
        })
    }

    fn prepare_write<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a WriteOperation,
        input_schema: SchemaRef,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        let product = self.profile.product();
        Box::pin(async move {
            // Bordo: da qui in poi l'attribuzione e quella del
            // profilo, non il segnaposto con cui l'errore e nato.
            let outcome = async move {
                budget.ensure_active()?;
                let effective_cancellation =
                    crate::read::BudgetCancellation::new(cancellation, budget);
                let token = effective_cancellation.token();
                let plan = crate::write::MysqlWritePlan::compile_with_profile(
                    &input_schema,
                    operation,
                    self.config.database(),
                    self.profile,
                )?;
                let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
                let column_count = u64::try_from(input_schema.fields().len()).map_err(|_| {
                    provider_error(
                        ErrorCategory::ResourceLimit,
                        ErrorPhase::Prepare,
                        format!("numero colonne {product} non rappresentabile"),
                    )
                })?;
                let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
                let loss_report = if matches!(
                    operation.mode,
                    plenora_database_core::plan::WriteMode::Create
                        | plenora_database_core::plan::WriteMode::DeleteByKeys
                        | plenora_database_core::plan::WriteMode::Update
                ) {
                    // Create: target non esiste ancora — skip describe.
                    // DeleteByKeys: schema Arrow è keys-only; il preflight
                    // standard rifiuterebbe le colonne target non nello schema.
                    // In entrambi i casi LossReport vuoto (schema matcha per
                    // costruzione).
                    plenora_database_core::loss::LossReport {
                        schema_version: 2,
                        policy: operation.mapping_policy,
                        losses: Vec::new(),
                    }
                } else {
                    let pool = self.pool_for(secret)?;
                    let mut session = pool.checkout(token).await?;
                    let target_schema = operation
                        .target
                        .schema
                        .as_deref()
                        .unwrap_or_else(|| self.config.database());
                    let target = crate::catalog::describe_object_with_profile(
                        &mut session,
                        target_schema,
                        &operation.target.object,
                        self.profile,
                        token,
                    )
                    .await?;
                    let report = plan.preflight(&target, self.profile)?;
                    drop(session);
                    report
                };
                Ok(PreparedWrite {
                    operation: operation.clone(),
                    input_schema,
                    loss_report,
                    budget: budget.clone(),
                    operation_lease,
                    columns_lease,
                })
            }
            .await;
            crate::profile::attributed(self.profile, outcome)
        })
    }

    fn write<'a>(
        &'a self,
        secret: &'a SecretString,
        prepared: PreparedWrite,
        input: Box<dyn BatchStream>,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, WriteOutcome> {
        Box::pin(async move {
            // Bordo: l'esito della scrittura porta l'attribuzione del
            // profilo, sia quando riesce sia quando fallisce. Un outcome
            // committato che dichiarasse il prodotto sbagliato sarebbe una
            // riga di contabilita falsa, non un dettaglio diagnostico.
            let outcome =
                execute_mysql_write(self, secret, prepared, input, budget, cancellation).await;
            crate::profile::attributed(self.profile, outcome).map(|mut outcome| {
                outcome.provider = self.profile.kind();
                outcome
            })
        })
    }

    fn begin_transaction<'a>(
        &'a self,
        secret: &'a SecretString,
        options: &'a plenora_database_core::transaction::TransactionOptions,
        _budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn plenora_database_core::transaction::TransactionScope>> {
        Box::pin(async move {
            // Bordo: da qui in poi l'attribuzione e quella del
            // profilo, non il segnaposto con cui l'errore e nato.
            let outcome = async move {
                let pool = self.pool_for(secret)?;
                let session = pool.checkout(cancellation).await?;
                let transaction =
                    crate::transaction::MysqlTransaction::begin(session, options, cancellation)
                        .await?;
                Ok(Box::new(transaction)
                    as Box<
                        dyn plenora_database_core::transaction::TransactionScope,
                    >)
            }
            .await;
            crate::profile::attributed(self.profile, outcome)
        })
    }

    fn execute_ddl<'a>(
        &'a self,
        secret: &'a SecretString,
        sql: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            // Bordo: da qui in poi l'attribuzione e quella del
            // profilo, non il segnaposto con cui l'errore e nato.
            let outcome = async move {
                let pool = self.pool_for(secret)?;
                let mut session = pool.checkout(cancellation).await?;
                // Text protocol via `exec_control`, non prepared via `exec_write`,
                // per due ragioni distinte:
                //
                // 1. MySQL rifiuta parte del DDL nel prepared statement protocol
                //    (errore 1295), quindi `exec_write` chiuderebbe statement che
                //    il server accetta senza problemi in text protocol;
                // 2. il DDL MySQL fa autocommit. Su cancellazione o timeout
                //    `exec_write` dichiara `RemoteEffect::None` — "non e successo
                //    nulla" — mentre lo statement puo essersi gia committato.
                //    `exec_control` dichiara `Unknown`, che e la sola cosa vera
                //    quando la connessione cade mentre un DDL autocommit e in
                //    volo.
                //
                // La fase e `Write`: qui si esegue, non si prepara. Con `Prepare`
                // il consumer leggerebbe il fallimento come pre-esecuzione, cioe
                // senza effetto remoto possibile.
                session
                    .exec_control(sql, ErrorPhase::Write, cancellation)
                    .await
            }
            .await;
            crate::profile::attributed(self.profile, outcome)
        })
    }
}

#[allow(clippy::too_many_lines)]
async fn execute_mysql_write(
    provider: &MysqlProvider,
    secret: &SecretString,
    prepared: PreparedWrite,
    input: Box<dyn BatchStream>,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<WriteOutcome> {
    if !prepared.budget.is_same_budget(budget) {
        return Err(provider_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Write,
            "budget sostituito fra prepare e write",
        ));
    }
    budget.ensure_active()?;
    let PreparedWrite {
        operation,
        input_schema: prepared_schema,
        loss_report: prepared_loss,
        budget: _prepared_budget,
        operation_lease: _operation_lease,
        columns_lease: _columns_lease,
    } = prepared;
    let schema = input.schema();
    if schema.as_ref() != prepared_schema.as_ref() {
        return Err(provider_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Write,
            "schema stream diverso dallo schema preparato",
        ));
    }
    let plan = crate::write::MysqlWritePlan::compile_with_profile(
        &schema,
        &operation,
        provider.config.database(),
        provider.profile,
    )?;
    let effective_cancellation = crate::read::BudgetCancellation::new(cancellation, budget);
    let token = effective_cancellation.token();
    let pool = provider.pool_for(secret)?;
    let mut session = pool.checkout(token).await?;
    let target_schema = operation
        .target
        .schema
        .as_deref()
        .unwrap_or_else(|| provider.config.database());

    // Prepare target step per Create: verifica NON exists, poi CREATE TABLE.
    // Le altre mode non emettono DDL — Replace usa DELETE, Update una
    // TEMPORARY table che muore con la sessione.
    let ddl_residue = if operation.mode == plenora_database_core::plan::WriteMode::Create {
        let pre_check = crate::catalog::describe_object_with_profile(
            &mut session,
            target_schema,
            &operation.target.object,
            provider.profile,
            token,
        )
        .await;
        match pre_check {
            Ok(_) => {
                return Err(provider_error(
                    ErrorCategory::Conflict,
                    ErrorPhase::Prepare,
                    "target già esistente (mode='create')",
                ));
            }
            Err(err) if err.category == ErrorCategory::NotFound => {}
            Err(other) => return Err(other),
        }
        let ddl = crate::write::build_create_table_sql(
            &schema,
            &operation,
            provider.config.database(),
            provider.profile,
        )?;
        session
            .exec_control(&ddl, ErrorPhase::Prepare, token)
            .await?;
        // Da qui in poi la tabella esiste sul server e nessun ROLLBACK la
        // rimuove: il DDL MySQL fa commit implicito.
        crate::write::DdlResidue::CreatedTable
    } else {
        crate::write::DdlResidue::None
    };

    // Il resto della scrittura ha molte uscite — describe, preflight, apertura
    // transazione, scrittura, commit — e ciascuna, dopo la DDL, direbbe il
    // falso da sola. Passano tutte da qui, e il residuo si stampa una volta
    // sul risultato.
    let result = execute_mysql_write_after_ddl(
        provider,
        &mut session,
        input,
        &operation,
        &schema,
        &plan,
        prepared_loss,
        target_schema,
        budget,
        token,
    )
    .await;
    drop(session);
    crate::write::stamp_ddl_residue(result, ddl_residue)
}

/// Il corpo della scrittura dopo l'eventuale DDL di preparazione.
///
/// Separato da `execute_mysql_write` per una ragione sola: dare alle sue
/// uscite un unico punto di ritorno su cui il chiamante possa dichiarare cosa
/// e rimasto sul server.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn execute_mysql_write_after_ddl(
    provider: &MysqlProvider,
    session: &mut crate::MysqlSession,
    mut input: Box<dyn BatchStream>,
    operation: &plenora_database_core::plan::WriteOperation,
    schema: &SchemaRef,
    plan: &crate::write::MysqlWritePlan,
    prepared_loss: plenora_database_core::loss::LossReport,
    target_schema: &str,
    budget: &ResourceBudget,
    token: &CancellationToken,
) -> Result<WriteOutcome> {
    let product = session.profile().product();
    let target = crate::catalog::describe_object_with_profile(
        session,
        target_schema,
        &operation.target.object,
        provider.profile,
        token,
    )
    .await?;
    // Skip preflight compare per Create (target appena creato dallo schema)
    // e DeleteByKeys (schema keys-only, preflight standard non applicabile).
    // Il target deve comunque esistere ed essere BASE TABLE InnoDB — questo
    // è validato implicitamente dal describe_object che precede.
    if !matches!(
        operation.mode,
        plenora_database_core::plan::WriteMode::Create
            | plenora_database_core::plan::WriteMode::DeleteByKeys
            | plenora_database_core::plan::WriteMode::Update
    ) && plan.preflight(&target, provider.profile)? != prepared_loss
    {
        return Err(provider_error(
            ErrorCategory::Schema,
            ErrorPhase::Prepare,
            format!("preflight {product} cambiato fra prepare e write"),
        ));
    }

    // v1.2 — Update: crea staging TEMPORARY TABLE. Il bulk INSERT
    // sotto scriverà in staging invece del target; dopo, UPDATE JOIN.
    let update_staging_quoted: Option<String> = if operation.mode
        == plenora_database_core::plan::WriteMode::Update
    {
        let seed = format!(
            "{}-{}",
            std::process::id(),
            WRITE_EXECUTION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let staging_name = plan.staging_temp_name(&seed);
        let staging_ddl = crate::write::build_temp_staging_sql(
            schema,
            &staging_name,
            provider.config.database(),
            provider.profile,
        )?;
        session
            .exec_control(&staging_ddl, ErrorPhase::Prepare, token)
            .await?;
        let quoted = crate::write::quote_staging_name(&staging_name, provider.config.database())?;
        Some(quoted)
    } else {
        None
    };

    // Replace: il DELETE viene reso qui ma eseguito dentro la transazione,
    // subito prima del bulk insert. Renderlo prima di aprire la transazione
    // evita di lasciarne una aperta se il target non e un identificatore
    // valido.
    let replace_delete_sql = if operation.mode == plenora_database_core::plan::WriteMode::Replace {
        Some(crate::write::build_delete_all_sql(
            operation,
            provider.config.database(),
        )?)
    } else {
        None
    };
    // Quando la sorgente dichiara quante righe produrrà, la scrittura può
    // partizionare l'input fra righe rifiutate, annullate e mai tentate: solo
    // allora il percorso diagnostico riga per riga ha un input_total da
    // pubblicare, e il costo di uno statement per riga è giustificato.
    //
    // v1.2: Row-scoped diagnostics ha semantica valida SOLO per Append —
    // Upsert MySQL ritorna affected_rows=2 per UPDATE, che il validatore
    // per-row rifiuta ("conteggio incoerente"). Per Create/TruncateInsert
    // il diagnostic path è tecnicamente supportabile ma bulk INSERT è
    // preferibile (throughput). Semplifichiamo: solo Append attiva diagnostic.
    let diagnostic_input = if operation.mode == plenora_database_core::plan::WriteMode::Append {
        input
            .declared_input_rows()
            .map(|rows| validate_diagnostic_input(schema, rows, input.row_diagnostics_policy()))
            .transpose()?
    } else {
        None
    };
    let execution_id = start_write_transaction(session, token).await?;
    // Replace: svuota il target dentro la stessa transazione del bulk insert.
    // Nessun DDL, quindi identita dell'oggetto, indici, FK, trigger, check,
    // default, grant e AUTO_INCREMENT restano quelli del target — e un
    // fallimento successivo riporta indietro anche le righe cancellate.
    if let Some(delete_sql) = replace_delete_sql.as_deref() {
        if let Err(error) = session
            .exec_write(
                delete_sql,
                mysql_async::Params::Empty,
                ErrorPhase::Write,
                token,
            )
            .await
        {
            return Err(rollback_after_failure(session, error, &execution_id).await);
        }
    }
    if let Some((declared_rows, policy)) = diagnostic_input {
        let result = diagnostic_mysql_write(
            session,
            input.as_mut(),
            schema,
            plan,
            budget,
            token,
            policy,
            declared_rows,
            &execution_id,
        )
        .await;
        return result;
    }
    let effective_staging = update_staging_quoted.as_deref();
    let progress = match write_input_batches(
        session,
        input.as_mut(),
        schema,
        plan,
        operation.mode,
        effective_staging,
        budget,
        token,
    )
    .await
    {
        Ok(progress) => progress,
        Err(error) => {
            return Err(rollback_after_failure(session, error, &execution_id).await);
        }
    };

    // v1.2 — Update: dopo aver riempito staging, esegui UPDATE JOIN.
    // Il numero di righe aggiornate rimpiazza `progress.inserted` per il
    // RowCounts finale (il committed_outcome_for_mode Update usa
    // affected = updated_target_rows).
    let mut final_affected = progress.inserted;
    if operation.mode == plenora_database_core::plan::WriteMode::Update {
        if let Some(staging) = update_staging_quoted.as_deref() {
            let update_sql = plan.render_update_from_staging(staging);
            match session
                .exec_write(
                    &update_sql,
                    mysql_async::Params::Empty,
                    ErrorPhase::Write,
                    token,
                )
                .await
            {
                Ok(updated) => {
                    final_affected = updated;
                }
                Err(error) => {
                    return Err(rollback_after_failure(session, error, &execution_id).await);
                }
            }
        }
    }

    let result = match session
        .exec_transaction(
            crate::session::MysqlTransactionCommand::Commit,
            ErrorPhase::Commit,
            token,
        )
        .await
    {
        Ok(()) => crate::write::committed_outcome_for_mode(
            execution_id,
            progress.received,
            final_affected,
            operation.mode,
        ),
        Err(error) => {
            session.discard().await;
            crate::write::commit_failure(error, execution_id, progress.received)
        }
    };
    result
}

fn validate_diagnostic_input(
    schema: &SchemaRef,
    declared_rows: u64,
    policy: plenora_database_core::row_diagnostics::RowDiagnosticsPolicy,
) -> Result<(
    u64,
    plenora_database_core::row_diagnostics::RowDiagnosticsPolicy,
)> {
    plenora_database_core::row_diagnostics::WriteDiagnosticsTracker::new(
        declared_rows,
        policy.clone(),
    )?;
    for field in [
        policy.key_field.as_deref(),
        policy.constraint_column.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if schema.field_with_name(field).is_err() {
            return Err(provider_error(
                ErrorCategory::InvalidPlan,
                ErrorPhase::Prepare,
                "policy row-scoped riferita a un campo assente dallo schema preparato",
            ));
        }
    }
    Ok((declared_rows, policy))
}

async fn start_write_transaction(
    session: &mut crate::MysqlSession,
    cancellation: &CancellationToken,
) -> Result<String> {
    let execution_id = format!(
        "mysql-{}-{}",
        std::process::id(),
        WRITE_EXECUTION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    session
        .exec_transaction(
            crate::session::MysqlTransactionCommand::Start,
            ErrorPhase::Write,
            cancellation,
        )
        .await?;
    Ok(execution_id)
}

/// Scrittura diagnostica: uno statement per riga sorgente.
///
/// Il rifiuto chiude la transazione e viaggia come errore che trasporta il
/// documento `plenora-row-diagnostics-v1`; un input applicato per intero
/// procede al commit come il percorso normale.
#[allow(clippy::too_many_arguments)]
async fn diagnostic_mysql_write(
    session: &mut crate::MysqlSession,
    input: &mut dyn BatchStream,
    schema: &SchemaRef,
    plan: &crate::write::MysqlWritePlan,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    policy: plenora_database_core::row_diagnostics::RowDiagnosticsPolicy,
    declared_rows: u64,
    execution_id: &str,
) -> Result<WriteOutcome> {
    // Il codice MySQL prova la classe; la colonna arriva esclusivamente dal
    // contratto dichiarato della sorgente, già verificato contro lo schema
    // preparato prima dell'apertura della transazione.
    let constraint_column = policy.constraint_column.clone();
    let mut tracker = plenora_database_core::row_diagnostics::WriteDiagnosticsTracker::new(
        declared_rows,
        policy,
    )?;
    let applied;
    let diagnosed = {
        let mut writer = crate::row_diagnostics::MysqlRowWriter::new(
            session,
            plan,
            input,
            schema,
            budget,
            cancellation,
            constraint_column,
        );
        let diagnosed = plenora_database_core::row_diagnostics::diagnose_row_scoped_write(
            &mut writer,
            &mut tracker,
        )
        .await;
        applied = writer.applied();
        diagnosed
    };
    match diagnosed {
        // Il seam ha già annullato la transazione: l'evidenza raccolta è
        // dentro il documento e non va rifatta qui.
        Ok(Some(outcome)) => Err(outcome.into_error(
            Some(crate::profile::PROVISIONAL_KIND),
            Some(execution_id.to_owned()),
        )?),
        Ok(None) => match session
            .exec_transaction(
                crate::session::MysqlTransactionCommand::Commit,
                ErrorPhase::Commit,
                cancellation,
            )
            .await
        {
            Ok(()) => {
                crate::write::committed_outcome(execution_id.to_owned(), declared_rows, applied)
            }
            Err(error) => {
                session.discard().await;
                crate::write::commit_failure(error, execution_id.to_owned(), declared_rows)
            }
        },
        Err(error) => Err(rollback_after_failure(session, error, execution_id).await),
    }
}

#[derive(Default)]
struct WriteProgress {
    received: u64,
    inserted: u64,
}

#[allow(clippy::too_many_arguments)]
async fn write_input_batches(
    session: &mut crate::MysqlSession,
    input: &mut dyn BatchStream,
    schema: &SchemaRef,
    plan: &crate::write::MysqlWritePlan,
    mode: plenora_database_core::plan::WriteMode,
    staging_override: Option<&str>,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<WriteProgress> {
    let product = session.profile().product();
    let mut progress = WriteProgress::default();
    loop {
        let batch = match input.next_batch(cancellation).await {
            Ok(Some(batch)) => batch,
            Ok(None) => break,
            Err(mut error) => {
                error.phase = ErrorPhase::Write;
                error.provider = Some(crate::profile::PROVISIONAL_KIND);
                return Err(error);
            }
        };
        crate::write::validate_batch_schema(&batch, schema)?;
        if batch.num_rows() == 0 {
            continue;
        }
        let batch_rows = u64::try_from(batch.num_rows()).map_err(|_| {
            provider_error(
                ErrorCategory::ResourceLimit,
                ErrorPhase::Write,
                format!("righe batch {product} non rappresentabili"),
            )
        })?;
        plan.validate_spatial_batch(&batch, budget)?;
        let row_lease = budget.try_lease(ResourceKind::Rows, batch_rows)?;
        progress.received = progress.received.checked_add(batch_rows).ok_or_else(|| {
            provider_error(
                ErrorCategory::ResourceLimit,
                ErrorPhase::Write,
                format!("overflow conteggio righe {product}"),
            )
        })?;
        let affected = write_batch_chunks_for_mode(
            session,
            &batch,
            plan,
            mode,
            staging_override,
            cancellation,
        )
        .await?;
        progress.inserted = progress.inserted.checked_add(affected).ok_or_else(|| {
            provider_error(
                ErrorCategory::ResourceLimit,
                ErrorPhase::Write,
                format!("overflow righe inserite {product}"),
            )
        })?;
        row_lease.commit(batch_rows)?;
    }
    Ok(progress)
}

/// Mode-aware — dispatch tra INSERT bulk (Append/Create/TruncateInsert/
/// Upsert), DELETE by keys, INSERT verso staging (Update).
///
/// `staging_override`: se Some, il SQL punta a staging invece del target
/// reale (usato per Update — accumulazione dati in staging table prima
/// del successivo UPDATE JOIN).
async fn write_batch_chunks_for_mode(
    session: &mut crate::MysqlSession,
    batch: &plenora_database_core::arrow::RecordBatch,
    plan: &crate::write::MysqlWritePlan,
    mode: plenora_database_core::plan::WriteMode,
    staging_override: Option<&str>,
    cancellation: &CancellationToken,
) -> Result<u64> {
    let product = session.profile().product();
    let mut affected_total = 0_u64;
    let mut start = 0_usize;
    while start < batch.num_rows() {
        let rows = plan.rows_per_statement().min(batch.num_rows() - start);
        let parameters = plan.bind_chunk(batch, start, rows)?;
        let sql = if mode == plenora_database_core::plan::WriteMode::DeleteByKeys {
            plan.render_delete_by_keys(rows)?
        } else if let Some(staging) = staging_override {
            plan.render_insert_into_staging(staging, rows)?
        } else {
            plan.render_insert(rows)?
        };
        let affected = session
            .exec_write(&sql, parameters, ErrorPhase::Write, cancellation)
            .await?;
        affected_total = affected_total.checked_add(affected).ok_or_else(|| {
            provider_error(
                ErrorCategory::ResourceLimit,
                ErrorPhase::Write,
                format!("overflow righe write {product}"),
            )
        })?;
        start += rows;
    }
    Ok(affected_total)
}

/// Annulla la transazione e costruisce l'errore del fallimento pre-commit.
///
/// Non dichiara i residui di DDL: quelli li stampa
/// `crate::write::stamp_ddl_residue` sul valore di ritorno di
/// `execute_mysql_write_after_ddl`, che e l'unico punto attraversato da tutte
/// le uscite.
async fn rollback_after_failure(
    session: &mut crate::MysqlSession,
    error: DatabaseError,
    execution_id: &str,
) -> DatabaseError {
    let cleanup_cancellation = CancellationToken::new();
    let confirmed = session
        .exec_transaction(
            crate::session::MysqlTransactionCommand::Rollback,
            ErrorPhase::Rollback,
            &cleanup_cancellation,
        )
        .await
        .is_ok();
    if !confirmed {
        session.discard().await;
    }
    crate::write::rolled_back_error(error, confirmed, execution_id)
}

fn unsupported(message: impl Into<String>) -> DatabaseError {
    provider_error(ErrorCategory::Unsupported, ErrorPhase::Prepare, message)
}

fn provider_error(
    category: ErrorCategory,
    phase: ErrorPhase,
    message: impl Into<String>,
) -> DatabaseError {
    DatabaseError {
        category,
        phase,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(crate::profile::PROVISIONAL_KIND),
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::plan::{ComparisonOperator, ObjectRef};
    use plenora_database_core::provider::ParameterValue;
    use plenora_database_core::query::{
        ColumnRef, JoinKind, QueryExpression, QueryJoin, QueryLock, QueryLockStrength,
        QueryLockWait, QueryProjection, QuerySource, ScalarFunction,
    };
    use plenora_database_core::resource::ResourceLimits;
    use std::collections::BTreeMap;

    const fn assert_provider<T: Provider>() {}

    fn parameterized_query() -> QueryOperation {
        QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("warehouse".to_owned()),
                    object: "events".to_owned(),
                },
                alias: None,
            }),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "event_id".to_owned(),
                    },
                },
                alias: None,
            }],
            joins: Vec::new(),
            filter: Some(QueryExpression::Compare {
                left: Box::new(QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "event_id".to_owned(),
                    },
                }),
                operator: ComparisonOperator::Eq,
                right: Box::new(QueryExpression::Parameter {
                    name: "wanted".to_owned(),
                }),
            }),
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: None,
            row_offset: None,
            locking: None,
        }
    }

    #[tokio::test]
    async fn query_renders_and_binds_before_reaching_the_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let parameters = ParameterBag::new(BTreeMap::from([
            ("wanted".to_owned(), ParameterValue::I64(7)),
            ("unused".to_owned(), ParameterValue::I64(9)),
        ]));
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let cancellation = CancellationToken::new();
        let outcome = provider
            .query(
                &SecretString::new("unique-secret"),
                &parameterized_query(),
                &parameters,
                &budget,
                &cancellation,
            )
            .await;
        let Err(error) = outcome else {
            panic!("ParameterBag con parametro non usato accettato");
        };
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    #[tokio::test]
    async fn query_honours_cancellation_before_reaching_the_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let parameters = ParameterBag::new(BTreeMap::from([(
            "wanted".to_owned(),
            ParameterValue::I64(7),
        )]));
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let outcome = provider
            .query(
                &SecretString::new("unique-secret"),
                &parameterized_query(),
                &parameters,
                &budget,
                &cancellation,
            )
            .await;
        let Err(error) = outcome else {
            panic!("token gia cancellato accettato");
        };
        assert_eq!(error.category, ErrorCategory::Cancelled);
    }

    #[tokio::test]
    async fn query_keeps_unqualified_ast_fail_closed_before_the_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let mut operation = parameterized_query();
        operation.locking = Some(QueryLock {
            strength: QueryLockStrength::Update,
            relations: Vec::new(),
            wait: QueryLockWait::NoWait,
        });
        let parameters = ParameterBag::new(BTreeMap::from([(
            "wanted".to_owned(),
            ParameterValue::I64(7),
        )]));
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let cancellation = CancellationToken::new();
        let outcome = provider
            .query(
                &SecretString::new("unique-secret"),
                &operation,
                &parameters,
                &budget,
                &cancellation,
            )
            .await;
        let Err(error) = outcome else {
            panic!("locking esplicito non qualificato accettato");
        };
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    /// Il bind di HAVING deve essere estratto e richiesto come ogni altro:
    /// la mancanza del valore va vista prima di aprire la connessione.
    #[tokio::test]
    async fn query_demands_the_having_bind_before_reaching_the_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let mut operation = parameterized_query();
        let events = QueryExpression::Scalar {
            function: ScalarFunction::Count,
            arguments: vec![QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "event_id".to_owned(),
                },
            }],
        };
        operation.projection = vec![
            QueryProjection {
                expression: QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "actor_id".to_owned(),
                    },
                },
                alias: None,
            },
            QueryProjection {
                expression: events.clone(),
                alias: Some("events".to_owned()),
            },
        ];
        operation.group_by = vec![QueryExpression::Column {
            column: ColumnRef {
                relation: None,
                field: "actor_id".to_owned(),
            },
        }];
        operation.having = Some(QueryExpression::Compare {
            left: Box::new(events),
            operator: ComparisonOperator::Gte,
            right: Box::new(QueryExpression::Parameter {
                name: "floor".to_owned(),
            }),
        });
        let parameters = ParameterBag::new(BTreeMap::from([(
            "wanted".to_owned(),
            ParameterValue::I64(7),
        )]));
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let cancellation = CancellationToken::new();
        let outcome = provider
            .query(
                &SecretString::new("unique-secret"),
                &operation,
                &parameters,
                &budget,
                &cancellation,
            )
            .await;
        let Err(error) = outcome else {
            panic!("bind HAVING mancante accettato");
        };
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    /// Il bind della clausola ON e posizionale come ogni altro e precede
    /// quello di WHERE: la sua assenza deve essere vista prima di aprire la
    /// connessione, non al `COM_STMT_EXECUTE`.
    #[tokio::test]
    async fn query_demands_the_join_on_bind_before_reaching_the_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let qualified = |relation: &str, field: &str| QueryExpression::Column {
            column: ColumnRef {
                relation: Some(relation.to_owned()),
                field: field.to_owned(),
            },
        };
        let mut operation = parameterized_query();
        operation.source = Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("warehouse".to_owned()),
                object: "events".to_owned(),
            },
            alias: Some("e".to_owned()),
        });
        operation.projection = vec![QueryProjection {
            expression: qualified("a", "name"),
            alias: Some("actor".to_owned()),
        }];
        operation.joins = vec![QueryJoin {
            kind: JoinKind::Inner,
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("warehouse".to_owned()),
                    object: "actors".to_owned(),
                },
                alias: Some("a".to_owned()),
            }),
            derived_source: None,
            lateral: false,
            on: Some(QueryExpression::And {
                arguments: vec![
                    QueryExpression::Compare {
                        left: Box::new(qualified("e", "actor_id")),
                        operator: ComparisonOperator::Eq,
                        right: Box::new(qualified("a", "actor_id")),
                    },
                    QueryExpression::Compare {
                        left: Box::new(qualified("a", "tier")),
                        operator: ComparisonOperator::Gte,
                        right: Box::new(QueryExpression::Parameter {
                            name: "tier".to_owned(),
                        }),
                    },
                ],
            }),
        }];
        operation.filter = Some(QueryExpression::Compare {
            left: Box::new(qualified("e", "event_id")),
            operator: ComparisonOperator::Eq,
            right: Box::new(QueryExpression::Parameter {
                name: "wanted".to_owned(),
            }),
        });
        let parameters = ParameterBag::new(BTreeMap::from([(
            "wanted".to_owned(),
            ParameterValue::I64(7),
        )]));
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let cancellation = CancellationToken::new();
        let outcome = provider
            .query(
                &SecretString::new("unique-secret"),
                &operation,
                &parameters,
                &budget,
                &cancellation,
            )
            .await;
        let Err(error) = outcome else {
            panic!("bind della clausola ON mancante accettato");
        };
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    fn append_write_operation() -> WriteOperation {
        WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("warehouse".to_owned()),
                object: "events".to_owned(),
            },
            mode: plenora_database_core::plan::WriteMode::Append,
            mapping_policy: plenora_database_core::loss::MappingPolicy::Strict,
            transaction_profile: plenora_database_core::plan::TransactionProfile::SingleTransaction,
            keys: Vec::new(),
            update_columns: Vec::new(),
            srid_policy: None,
            create_spatial_index: false,
            allow_partial: false,
        }
    }

    fn append_input_schema() -> SchemaRef {
        Arc::new(plenora_database_core::arrow::Schema::new_with_metadata(
            vec![plenora_database_core::arrow::Field::new(
                "id",
                plenora_database_core::arrow::DataType::Int64,
                false,
            )],
            BTreeMap::from([(
                plenora_database_core::protocol::CONTRACT_VERSION_KEY.to_owned(),
                plenora_database_core::protocol::CONTRACT_VERSION.to_owned(),
            )])
            .into_iter()
            .collect(),
        ))
    }

    struct EmptyBatchStream(SchemaRef);

    impl BatchStream for EmptyBatchStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.0)
        }

        fn next_batch<'a>(
            &'a mut self,
            _cancellation: &'a plenora_database_core::CancellationToken,
        ) -> plenora_database_core::provider::ProviderFuture<
            'a,
            Option<plenora_database_core::arrow::RecordBatch>,
        > {
            Box::pin(async { Ok(None) })
        }
    }

    #[test]
    fn invalid_row_diagnostics_policy_is_rejected_before_transaction_setup() {
        let schema = append_input_schema();
        let policy = plenora_database_core::row_diagnostics::RowDiagnosticsPolicy::default();
        assert!(validate_diagnostic_input(&schema, 0, policy.clone()).is_err());

        let mut zero_examples = policy;
        zero_examples.examples_limit = 0;
        assert!(validate_diagnostic_input(&schema, 1, zero_examples).is_err());

        let missing_field = plenora_database_core::row_diagnostics::RowDiagnosticsPolicy {
            key_field: Some("missing_key".to_owned()),
            constraint_column: Some("missing_constraint_column".to_owned()),
            examples_limit: 10,
        };
        assert!(validate_diagnostic_input(&schema, 1, missing_field).is_err());

        let declared = plenora_database_core::row_diagnostics::RowDiagnosticsPolicy {
            key_field: Some("id".to_owned()),
            constraint_column: Some("id".to_owned()),
            examples_limit: 10,
        };
        let (_, validated) = validate_diagnostic_input(&schema, 1, declared)
            .expect("campi dichiarati presenti nello schema preparato");
        assert_eq!(validated.constraint_column.as_deref(), Some("id"));
    }

    fn prepared_write_for_test(budget: &ResourceBudget, input_schema: SchemaRef) -> PreparedWrite {
        PreparedWrite {
            operation: append_write_operation(),
            input_schema,
            loss_report: plenora_database_core::loss::LossReport {
                schema_version: 2,
                policy: plenora_database_core::loss::MappingPolicy::Strict,
                losses: Vec::new(),
            },
            budget: budget.clone(),
            operation_lease: budget
                .try_lease(
                    plenora_database_core::resource::ResourceKind::ConcurrentOperations,
                    1,
                )
                .expect("lease operazione"),
            columns_lease: budget
                .try_lease(plenora_database_core::resource::ResourceKind::Columns, 1)
                .expect("lease colonne"),
        }
    }

    /// Il piano di scrittura e compilato prima di aprire la connessione: una
    /// forma non qualificata non deve arrivare al server.
    #[tokio::test]
    async fn prepare_write_rejects_unqualified_operations_before_the_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let mut operation = append_write_operation();
        // v1.2: 7/7 modes ora qualificati. Uso mapping_policy=Lossy che
        // resta Unsupported (finché il loss preflight non è qualificato).
        operation.mapping_policy = plenora_database_core::loss::MappingPolicy::Lossy;
        let outcome = provider
            .prepare_write(
                &SecretString::new("unique-secret"),
                &operation,
                append_input_schema(),
                &budget,
                &CancellationToken::new(),
            )
            .await;
        let Err(error) = outcome else {
            panic!("update MySQL non qualificato accettato");
        };
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    /// Un token gia cancellato chiude prima del checkout: non esiste una
    /// connessione da quarantinare.
    #[tokio::test]
    async fn prepare_write_honours_cancellation_before_the_network() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let outcome = provider
            .prepare_write(
                &SecretString::new("unique-secret"),
                &append_write_operation(),
                append_input_schema(),
                &budget,
                &cancellation,
            )
            .await;
        let Err(error) = outcome else {
            panic!("token gia cancellato accettato");
        };
        assert_eq!(error.category, ErrorCategory::Cancelled);
    }

    /// Le lease di prepare non sono trasferibili: un budget diverso da quello
    /// che ha prodotto il piano non puo eseguire la scrittura.
    #[tokio::test]
    async fn write_rejects_a_budget_that_did_not_prepare_it() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let prepared_budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let prepared = prepared_write_for_test(&prepared_budget, append_input_schema());
        let foreign = ResourceBudget::new(ResourceLimits::default()).expect("budget estraneo");
        let error = provider
            .write(
                &SecretString::new("unique-secret"),
                prepared,
                Box::new(EmptyBatchStream(append_input_schema())),
                &foreign,
                &CancellationToken::new(),
            )
            .await
            .expect_err("budget estraneo");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
    }

    #[tokio::test]
    async fn write_rejects_a_stream_schema_different_from_prepare() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 1).expect("provider");
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let prepared = prepared_write_for_test(&budget, append_input_schema());
        let renamed = Arc::new(plenora_database_core::arrow::Schema::new_with_metadata(
            vec![plenora_database_core::arrow::Field::new(
                "renamed",
                plenora_database_core::arrow::DataType::Int64,
                false,
            )],
            BTreeMap::from([(
                plenora_database_core::protocol::CONTRACT_VERSION_KEY.to_owned(),
                plenora_database_core::protocol::CONTRACT_VERSION.to_owned(),
            )])
            .into_iter()
            .collect(),
        ));
        let error = provider
            .write(
                &SecretString::new("unique-secret"),
                prepared,
                Box::new(EmptyBatchStream(renamed)),
                &budget,
                &CancellationToken::new(),
            )
            .await
            .expect_err("schema stream diverso");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Write);
    }

    #[test]
    fn provider_surface_is_typed_and_fail_closed() {
        assert_provider::<MysqlProvider>();
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 2).expect("provider");
        assert_eq!(provider.kind(), ProviderKind::Mysql);
        let rendered = format!("{provider:?}");
        assert!(!rendered.contains("unique-secret"));
    }

    #[test]
    fn the_provider_is_always_built_through_a_profile() {
        let config = MysqlConfig::new(
            "mysql.example.test",
            "warehouse",
            "loader",
            SecretString::new("unique-secret"),
        );
        let provider = MysqlProvider::new(config, 2).expect("provider");
        assert_eq!(provider.profile.product(), "MySQL");
        assert_eq!(provider.profile.kind(), ProviderKind::Mysql);

        // La guardia strutturale: un secondo punto di costruzione sarebbe un
        // provider senza profilo dichiarato, e il test comportamentale sopra
        // non lo vedrebbe. Gli aghi si compongono a runtime perche scritti
        // per intero comparirebbero in questo stesso file, e la guardia si
        // troverebbe da sola.
        //
        // Due forme, perche un literal si scrive in due modi, e presidiarne
        // uno solo e stato l'errore della prima stesura: una costruzione
        // dentro un `Ok` con turbofish passava indisturbata sotto un ago che
        // cercava la sola forma senza.
        let source = include_str!("provider.rs");
        let brace = " {";
        let by_self = format!("Self{brace}");
        // Un tipo di ritorno non e una costruzione: scontarlo tiene vero il
        // messaggio dell'asserzione invece di allargare la guardia a un caso
        // che non riguarda il profilo.
        let returns_self = format!("-> Self{brace}");
        let constructions = source.matches(by_self.as_str()).count()
            - source.matches(returns_self.as_str()).count();
        assert_eq!(
            constructions, 1,
            "il provider deve avere un solo punto di costruzione"
        );
        // La forma per nome compare anche in dichiarazione e negli `impl`:
        // li e legittima, altrove sarebbe una seconda costruzione.
        let by_name = format!("MysqlProvider{brace}");
        for at in source.match_indices(by_name.as_str()).map(|(at, _)| at) {
            let preceding = source[..at].trim_end();
            assert!(
                preceding.ends_with("struct")
                    || preceding.ends_with("impl")
                    || preceding.ends_with("for"),
                "costruzione di MysqlProvider per nome fuori da with_profile"
            );
        }
        let with_profile = source
            .find("fn with_profile(")
            .expect("with_profile deve esistere");
        let built = source
            .find(by_self.as_str())
            .expect("la costruzione deve esistere");
        assert!(
            built > with_profile,
            "l'unica costruzione deve stare dentro with_profile"
        );
    }

    #[test]
    fn published_spatial_capabilities_match_generic_geometry_contract() {
        let capabilities = crate::profile::MYSQL_PROFILE
            .capabilities("9.7.2".to_owned())
            .spatial;
        assert!(capabilities.read_wkb);
        assert!(capabilities.write_wkb);
        assert!(capabilities.geometry);
        assert!(capabilities.mixed_geometry_types);
        assert_eq!(
            capabilities.dimensions,
            vec![plenora_database_core::geometry::Dimensions::Xy]
        );
        assert!(!capabilities.spatial_index);
    }
}
