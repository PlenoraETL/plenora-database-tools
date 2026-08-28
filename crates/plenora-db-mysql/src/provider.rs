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
use plenora_database_core::relational::QueryOperation;
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::resource::ResourceKind;
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use plenora_database_engine::{validate_prepared_budget, ContractLeases};
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

/// Il profilo che il costruttore pubblico di un provider seleziona.
///
/// La dichiarazione appartiene al **tipo**, non al crate, e la ragione e la
/// cardinalita che arriva: questo crate pubblichera due provider, e una
/// costante sola non potrebbe descriverli entrambi. Con l'associazione al
/// tipo, `MariadbProvider` portera la propria il giorno che esiste, senza che
/// niente qui debba cambiare.
///
/// Chi la legge, e per cosa:
///
/// * il costruttore pubblico, che usa `Self::PROFILE` invece di scegliere;
/// * `the_published_profile_is_the_one_the_constructor_selects`, che verifica
///   che il provider costruito sia davvero quello — la dichiarazione da sola
///   potrebbe dire una cosa e il costruttore farne un'altra;
/// * `docs/STATO.md`, che da qui sa quali dichiarazioni di capability un
///   consumatore puo raggiungere.
///
/// Gli altri profili del crate esistono per la misura e restano interni:
/// `with_profile` e `pub(crate)`, e finche nessun tipo esportato li dichiara
/// non c'e modo pubblico di ottenerli.
// `pub(crate)` e non privato al modulo, e non e ridondante come sembra: il
// secondo provider di questo crate lo implementera, e non e detto che nasca
// qui dentro. Un tratto che vale per il crate si dichiara per il crate.
#[allow(clippy::redundant_pub_crate)]
pub(crate) trait PublishedProfile {
    const PROFILE: &'static dyn ProductProfile;
}

impl PublishedProfile for MysqlProvider {
    const PROFILE: &'static dyn ProductProfile = &MYSQL_PROFILE;
}

impl MysqlProvider {
    /// Costruisce un provider con pool lazy e configurazione validata.
    ///
    /// # Errors
    ///
    /// Fallisce se configurazione o limiti del pool non sono validi.
    pub fn new(config: MysqlConfig, max_connections: usize) -> Result<Self> {
        Self::with_profile(config, max_connections, Self::PROFILE)
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
                self.profile.capabilities(probe.product_version).published()
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
        Box::pin(async move {
            // Bordo: da qui in poi l'attribuzione e quella del
            // profilo, non il segnaposto con cui l'errore e nato.
            let outcome = async move {
                budget.ensure_active()?;
                let effective_cancellation =
                    crate::read::BudgetCancellation::new(cancellation, budget)?;
                let token = effective_cancellation.token();
                let plan = crate::write::MysqlWritePlan::compile_with_profile(
                    &input_schema,
                    operation,
                    self.config.database(),
                    self.profile,
                )?;
                let (operation_lease, columns_lease) =
                    ContractLeases::acquire(budget, input_schema.fields().len())?.into_parts();
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
                Ok(PreparedWrite::new(
                    operation.clone(),
                    input_schema,
                    loss_report,
                    budget.clone(),
                    operation_lease,
                    columns_lease,
                )
                .with_driver_state(plan))
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
    mut prepared: PreparedWrite,
    input: Box<dyn BatchStream>,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<WriteOutcome> {
    validate_prepared_budget(&prepared.budget, budget)?;
    budget.ensure_active()?;
    let schema = input.schema();
    if schema.as_ref() != prepared.input_schema.as_ref() {
        return Err(provider_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Write,
            "schema stream diverso dallo schema preparato",
        ));
    }
    let plan = prepared
        .take_driver_state::<crate::write::MysqlWritePlan>()
        .ok_or_else(|| {
            DatabaseError::invalid_plan("stato prepared write MySQL assente o incompatibile")
        })?;
    let operation = &prepared.operation;
    let effective_cancellation = crate::read::BudgetCancellation::new(cancellation, budget)?;
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
            operation,
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
        operation,
        &schema,
        &plan,
        prepared.loss_report.clone(),
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

    // Update scrive il bulk nella tabella temporanea e applica poi UPDATE JOIN
    // al target.
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
    // La diagnostica per riga ha semantica valida solo per Append:
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

    // Dopo il caricamento dello staging, Update esegue UPDATE JOIN.
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
    DatabaseError::new(
        category,
        phase,
        Some(crate::profile::PROVISIONAL_KIND),
        message,
    )
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Il provider pubblico di `MariaDB`.
///
/// Condivide l'implementazione interna con `MysqlProvider`, ma usa un profilo
/// di prodotto distinto per identificazione, SQL, errori e capability. In tal
/// modo le differenze misurate non si disperdono in rami nel codice comune e
/// ciascun provider pubblica soltanto il proprio contratto verificato.
/// dichiara le stesse sei write mode di `MySQL`, e ciascuna ha le proprie tre
/// sonde su tre riferimenti fissati per digest.
///
/// # Non c'e selezione automatica
///
/// `MysqlProvider` continua a rifiutare `MariaDB` alla probe, e questo rifiuta
/// `MySQL` con la stessa simmetria. Un provider che si adattasse al server che
/// trova sceglierebbe per il consumatore, e lo farebbe nel punto in cui il
/// consumatore non sta guardando: chi dichiara `mysql` e finisce su `MariaDB`
/// ha un problema di configurazione, non una comodita da assecondare.
///
/// # Perche un newtype e non una feature del primo
///
/// Il prodotto e un fatto del **tipo**, non un parametro: e cio che permette a
/// `PublishedProfile` di dire quale profilo un costruttore pubblico seleziona,
/// e a una guardia di verificare che il provider costruito sia davvero quello.
/// Un parametro avrebbe rimesso la scelta a runtime, cioe dove nessuno la
/// verifica.
///
/// La configurazione resta `MysqlConfig`: i due prodotti parlano lo stesso
/// protocollo e la stessa connessione, e un tipo gemello che differisse solo
/// nel nome divergerebbe alla prima correzione applicata a uno solo. Cio che
/// diverge e nel profilo, che e il posto in cui questa ADR ha deciso di
/// tenerlo.
pub struct MariadbProvider(MysqlProvider);

impl PublishedProfile for MariadbProvider {
    const PROFILE: &'static dyn ProductProfile = &crate::profile::MARIADB_PROFILE;
}

impl std::fmt::Debug for MariadbProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Delega, ma con il proprio nome: il `Debug` e superficie osservata, e
        // un `MariadbProvider` che si stampasse `MysqlProvider` direbbe il
        // falso proprio nel punto in cui qualcuno sta cercando di capire quale
        // dei due ha in mano.
        formatter
            .debug_tuple("MariadbProvider")
            .field(&self.0)
            .finish()
    }
}

impl MariadbProvider {
    /// Costruisce il provider `MariaDB` con pool lazy e configurazione
    /// validata.
    ///
    /// # Errors
    ///
    /// Come [`MysqlProvider::new`]: configurazione o limiti del pool non
    /// validi. L'errore e attribuito a `MariaDB`, non al provider gemello.
    pub fn new(config: MysqlConfig, max_connections: usize) -> Result<Self> {
        Ok(Self(MysqlProvider::with_profile(
            config,
            max_connections,
            Self::PROFILE,
        )?))
    }
}

// La delega e per intero e senza rami: se una sola operazione passasse da
// un'altra parte, il provider pubblico e quello misurato sarebbero due cose
// diverse, e la misura non direbbe piu niente su cio che il consumatore usa.
impl Provider for MariadbProvider {
    fn kind(&self) -> ProviderKind {
        self.0.kind()
    }

    fn test_connection<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        self.0.test_connection(secret, cancellation)
    }

    fn probe_capabilities<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        self.0.probe_capabilities(secret, cancellation)
    }

    fn inspect<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a Operation,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        self.0.inspect(secret, operation, cancellation)
    }

    fn read<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a ReadOperation,
        parameters: &'a ParameterBag,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        self.0
            .read(secret, operation, parameters, budget, cancellation)
    }

    fn query<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a QueryOperation,
        parameters: &'a ParameterBag,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        self.0
            .query(secret, operation, parameters, budget, cancellation)
    }

    fn prepare_write<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a WriteOperation,
        input_schema: SchemaRef,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        self.0
            .prepare_write(secret, operation, input_schema, budget, cancellation)
    }

    fn write<'a>(
        &'a self,
        secret: &'a SecretString,
        prepared: PreparedWrite,
        input: Box<dyn BatchStream>,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, WriteOutcome> {
        self.0.write(secret, prepared, input, budget, cancellation)
    }

    fn begin_transaction<'a>(
        &'a self,
        secret: &'a SecretString,
        options: &'a plenora_database_core::transaction::TransactionOptions,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn plenora_database_core::transaction::TransactionScope>> {
        self.0
            .begin_transaction(secret, options, budget, cancellation)
    }

    fn execute_ddl<'a>(
        &'a self,
        secret: &'a SecretString,
        sql: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        self.0.execute_ddl(secret, sql, cancellation)
    }
}

#[cfg(test)]
#[path = "provider_tests.rs"]
mod tests;
