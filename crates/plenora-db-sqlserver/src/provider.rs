use crate::read::MAX_CONFIGURED_BATCH_ROWS;
use crate::read::{read_operation, read_query_operation};
use crate::write::{
    prepare_write_with_external_contract_leases, prepare_write_with_options as prepare_driver_write,
};
use crate::{
    describe_object, list_objects, list_schemas, probe_server, write_prepared, SqlServerConfig,
    SqlServerInsertMode, SqlServerPool, SqlServerSchemaEvolution,
};
use plenora_database_core::capabilities::{
    ProviderCapabilities, ProviderLimits, ReadCapabilities, SpatialCapabilities,
    TransactionCapabilities, TransactionScope, WriteCapabilities,
};
use plenora_database_core::geometry::Dimensions;
use plenora_database_core::geometry::SpatialSemantics;
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{
    ObjectRef, Operation, ProviderKind, ReadOperation, WriteOperation,
};
use plenora_database_core::provider::{
    BatchStream, ConnectionInfo, Inspection, ParameterBag, PreparedWrite, Provider, ProviderFuture,
    SecretString,
};
use plenora_database_core::query::QueryOperation;
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tiberius::Query;

struct CachedPool {
    secret_fingerprint: [u8; 32],
    pool: Arc<SqlServerPool>,
}

/// Adattatore `SQL Server` per il contratto comune Plenora.
///
/// L'endpoint, l'utente e le policy TLS provengono da [`SqlServerConfig`].
/// Il [`SecretString`] ricevuto dai metodi [`Provider`] sostituisce sempre la
/// password della configurazione: il piano conserva soltanto `connection_ref`
/// e il secret entra al confine runtime.
///
/// Le capability restano fail-closed. In questa baseline il trait comune
/// espone catalogo, read tabellare senza trasformazioni e le modalità write
/// già provate dal data path TDS. Query relazionali e opzioni read non ancora
/// collegate restituiscono `Unsupported`.
pub struct SqlServerProvider {
    config: SqlServerConfig,
    batch_rows: usize,
    max_connections: usize,
    insert_mode: SqlServerInsertMode,
    schema_evolution: SqlServerSchemaEvolution,
    cached_pool: Mutex<Option<CachedPool>>,
}

impl std::fmt::Debug for SqlServerProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqlServerProvider")
            .field("config", &self.config)
            .field("batch_rows", &self.batch_rows)
            .field("max_connections", &self.max_connections)
            .field("insert_mode", &self.insert_mode)
            .field("schema_evolution", &self.schema_evolution)
            .field(
                "pool_initialized",
                &lock_recover(&self.cached_pool).is_some(),
            )
            .finish()
    }
}

impl SqlServerProvider {
    /// Costruisce un provider bounded senza aprire connessioni.
    ///
    /// # Errors
    ///
    /// Fallisce per configurazione invalida, batch nullo/eccessivo o pool a
    /// capacità zero.
    pub fn new(config: SqlServerConfig, batch_rows: usize, max_connections: usize) -> Result<Self> {
        config.validate()?;
        if batch_rows == 0 || batch_rows > MAX_CONFIGURED_BATCH_ROWS {
            return Err(provider_error(
                ErrorCategory::InvalidConfiguration,
                ErrorPhase::Validate,
                "batch_rows SQL Server fuori dal profilo bounded",
            ));
        }
        if max_connections == 0 {
            return Err(provider_error(
                ErrorCategory::InvalidConfiguration,
                ErrorPhase::Validate,
                "pool SQL Server con capacità zero",
            ));
        }
        Ok(Self {
            config,
            batch_rows,
            max_connections,
            insert_mode: SqlServerInsertMode::Prepared,
            schema_evolution: SqlServerSchemaEvolution::Disabled,
            cached_pool: Mutex::new(None),
        })
    }

    /// Seleziona esplicitamente il codec write. Il default resta
    /// [`SqlServerInsertMode::Prepared`].
    #[must_use]
    pub const fn with_insert_mode(mut self, insert_mode: SqlServerInsertMode) -> Self {
        self.insert_mode = insert_mode;
        self
    }

    #[must_use]
    pub const fn with_schema_evolution(
        mut self,
        schema_evolution: SqlServerSchemaEvolution,
    ) -> Self {
        self.schema_evolution = schema_evolution;
        self
    }

    fn pool_for(&self, secret: &SecretString) -> Result<Arc<SqlServerPool>> {
        let fingerprint: [u8; 32] = Sha256::digest(secret.expose().as_bytes()).into();
        let mut cached = lock_recover(&self.cached_pool);
        if let Some(candidate) = cached.as_ref() {
            if candidate.secret_fingerprint == fingerprint {
                return Ok(Arc::clone(&candidate.pool));
            }
        }
        let config = self.config.clone().with_password(secret.clone());
        let pool = SqlServerPool::new(config, self.max_connections)?;
        *cached = Some(CachedPool {
            secret_fingerprint: fingerprint,
            pool: Arc::clone(&pool),
        });
        drop(cached);
        Ok(pool)
    }

    fn validate_source(&self, source: &ObjectRef) -> Result<()> {
        if source
            .catalog
            .as_deref()
            .is_some_and(|catalog| catalog != self.config.database())
        {
            return Err(unsupported(
                ErrorPhase::Prepare,
                "accesso cross-database SQL Server non supportato dal provider",
            ));
        }
        Ok(())
    }

    async fn inspect_operation(
        &self,
        pool: &Arc<SqlServerPool>,
        operation: &Operation,
        cancellation: &CancellationToken,
    ) -> Result<Inspection> {
        let mut pooled = pool.checkout(cancellation).await?;
        let inspection = match operation {
            Operation::DatabaseListCatalogs => {
                let probe = probe_server(pooled.session_mut()?, cancellation).await?;
                Ok(Inspection {
                    operation: "database.list_catalogs".to_owned(),
                    document: json!({"catalogs": [probe.database]}),
                })
            }
            Operation::DatabaseListSchemas { source } => {
                if let Some(source) = source {
                    self.validate_source(source)?;
                }
                let schemas = list_schemas(pooled.session_mut()?, cancellation).await?;
                Ok(Inspection {
                    operation: "database.list_schemas".to_owned(),
                    document: json!({"schemas": schemas}),
                })
            }
            Operation::DatabaseListObjects { source } => {
                if let Some(source) = source {
                    self.validate_source(source)?;
                }
                let schema = source.as_ref().and_then(|item| item.schema.as_deref());
                let objects = list_objects(pooled.session_mut()?, schema, cancellation).await?;
                Ok(Inspection {
                    operation: "database.list_objects".to_owned(),
                    document: json!({"schema": schema, "objects": objects}),
                })
            }
            Operation::DatabaseDescribeObject { source } => {
                self.validate_source(source)?;
                let schema = source.schema.as_deref().unwrap_or("dbo");
                let description =
                    describe_object(pooled.session_mut()?, schema, &source.object, cancellation)
                        .await?;
                Ok(Inspection {
                    operation: "database.describe_object".to_owned(),
                    document: json!({
                        "columns": description.columns,
                        "schema_token": description.token,
                        "relation": {
                            "catalog": description.catalog,
                            "schema": description.schema,
                            "name": description.name,
                            "kind": description.kind,
                            "temporal_type": description.temporal_type,
                            "memory_optimized": description.memory_optimized,
                            "durability": description.durability,
                        },
                        "constraints": description.constraints,
                        "indexes": description.indexes,
                    }),
                })
            }
            _ => Err(unsupported(
                ErrorPhase::Probe,
                "operazione di introspezione SQL Server non supportata",
            )),
        };
        drop(pooled);
        inspection
    }
}

impl Provider for SqlServerProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Sqlserver
    }

    fn test_connection<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        Box::pin(async move {
            ensure_not_cancelled(cancellation, ErrorPhase::Connect)?;
            let pool = self.pool_for(secret)?;
            let mut pooled = pool.checkout(cancellation).await?;
            let probe = probe_server(pooled.session_mut()?, cancellation).await?;
            drop(pooled);
            Ok(ConnectionInfo {
                provider: ProviderKind::Sqlserver,
                server_version: probe.product_version,
                connection_identity: Some(probe.database),
            })
        })
    }

    fn probe_capabilities<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        Box::pin(async move {
            ensure_not_cancelled(cancellation, ErrorPhase::Probe)?;
            let pool = self.pool_for(secret)?;
            let mut pooled = pool.checkout(cancellation).await?;
            let probe = probe_server(pooled.session_mut()?, cancellation).await?;
            drop(pooled);
            ProviderCapabilities {
                schema_version: 2,
                provider: ProviderKind::Sqlserver,
                provider_version: probe.product_version.clone(),
                extension_versions: BTreeMap::new(),
                reads: ReadCapabilities {
                    streaming: true,
                    server_cursor: false,
                    // Come gli altri tre: la finestra si rende, e la
                    // bandiera la governa. Qui la forma del dialetto e
                    // `OFFSET n ROWS FETCH NEXT m ROWS ONLY`, e il tetto
                    // lascia `TOP` — le due forme non convivono.
                    pagination: true,
                    projection: true,
                    filter: true,
                    ordering: true,
                    resumable: false,
                },
                writes: WriteCapabilities {
                    create: true,
                    append: true,
                    truncate_insert: true,
                    update: true,
                    upsert: true,
                    replace: true,
                    delete_by_keys: true,
                    bulk: true,
                    array_binding: false,
                    returning: false,
                    rollback_on_failure: true,
                },
                transactions: TransactionCapabilities {
                    single_transaction: true,
                    // Aperta insieme allo scope transazionale, e non prima: la
                    // ragione per cui era chiusa — scritta nel contratto —
                    // diceva «non espone affatto uno scope transazionale,
                    // quindi non c'e niente su cui chiamarli», ed era vera
                    // finche lo scope non c'era.
                    //
                    // La capability promette `SAVE TRANSACTION` e il
                    // `ROLLBACK` a un punto, e T-SQL ha entrambi. Il rilascio
                    // non e fra le due, e infatti resta rifiutato: T-SQL non
                    // ha `RELEASE SAVEPOINT`, e simularlo con un `Ok(())`
                    // farebbe credere irraggiungibile un punto che invece si
                    // raggiunge ancora.
                    savepoints: true,
                    transactional_ddl: true,
                    staged_swap: true,
                    scope: TransactionScope::Transaction,
                },
                spatial: spatial_capabilities(&probe),
                limits: ProviderLimits {
                    // Il limite SQL Server e in **caratteri** — 128, per
                    // `nvarchar(128)` — mentre questo campo e in byte. I due
                    // non si convertono: 129 caratteri ASCII sono 129 byte,
                    // quindi passavano il 256 dichiarato qui e venivano poi
                    // respinti dal controllo di dialetto in
                    // `plenora_database_core::identifier`. La capability
                    // prometteva cio che il core negava.
                    //
                    // Stessa risposta gia data da MySQL, che ha lo stesso
                    // problema: finche il contratto non esprime i caratteri,
                    // l'unica risposta onesta e "non dichiarato".
                    max_identifier_bytes: None,
                    max_bind_parameters: Some(crate::MAX_BIND_PARAMETERS as u64),
                    max_statement_bytes: None,
                    max_batch_rows: Some(MAX_CONFIGURED_BATCH_ROWS as u64),
                    max_payload_bytes: None,
                },
            }
            .published()
        })
    }

    /// Apre una transazione applicativa.
    ///
    /// # Perche esiste, e da quando
    ///
    /// Il documento capability di questo provider dichiara
    /// `transactions.scope = Transaction`, e il contratto dice che devono
    /// sovrascrivere questo metodo «soltanto i provider che pubblicano scope
    /// pari a Transaction». Questo provider lo pubblicava e non lo
    /// sovrascriveva: il default rispondeva `Unsupported`, e la capability era
    /// una promessa che nessuno poteva mantenere.
    ///
    /// Nessun consumatore ci arrivava — il CLI generico non apre transazioni,
    /// le prove live usano le primitive TDS direttamente — quindi il difetto e
    /// rimasto invisibile finche il SDK Python non ha raggiunto questo motore.
    /// L'ha trovato la prima riga di Python che ha provato a usarlo.
    ///
    /// # Errors
    ///
    /// Se il pool non consegna una sessione, se il server rifiuta il livello
    /// di isolamento, o se il piano chiede la sola lettura, che su SQL Server
    /// non ha una forma dichiarativa.
    fn begin_transaction<'a>(
        &'a self,
        secret: &'a SecretString,
        options: &'a plenora_database_core::transaction::TransactionOptions,
        _budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn plenora_database_core::transaction::TransactionScope>> {
        Box::pin(async move {
            crate::transaction::validate_options(options)?;
            let pool = self.pool_for(secret)?;
            let session = pool.checkout(cancellation).await?;
            let transaction =
                crate::transaction::SqlServerTransaction::begin(session, options, cancellation)
                    .await?;
            Ok(Box::new(transaction)
                as Box<
                    dyn plenora_database_core::transaction::TransactionScope,
                >)
        })
    }

    fn inspect<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a Operation,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        Box::pin(async move {
            ensure_not_cancelled(cancellation, ErrorPhase::Probe)?;
            let pool = self.pool_for(secret)?;
            self.inspect_operation(&pool, operation, cancellation).await
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
            ensure_not_cancelled(cancellation, ErrorPhase::Read)?;
            self.validate_source(&operation.source)?;
            let pool = self.pool_for(secret)?;
            read_operation(
                &pool,
                operation,
                parameters,
                self.batch_rows,
                budget,
                cancellation,
            )
            .await
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
            ensure_not_cancelled(cancellation, ErrorPhase::Read)?;
            let pool = self.pool_for(secret)?;
            read_query_operation(
                &pool,
                self.config.database(),
                operation,
                parameters,
                self.batch_rows,
                budget,
                cancellation,
            )
            .await
        })
    }

    fn prepare_write<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a WriteOperation,
        input_schema: plenora_database_core::arrow::SchemaRef,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        Box::pin(async move {
            ensure_not_cancelled(cancellation, ErrorPhase::Prepare)?;
            self.validate_source(&operation.target)?;
            let pool = self.pool_for(secret)?;
            let driver_prepared = prepare_driver_write(
                &pool,
                operation,
                Arc::clone(&input_schema),
                budget,
                cancellation,
                self.insert_mode,
                self.schema_evolution,
            )
            .await?;
            let loss_report = driver_prepared.loss_report().clone();
            drop(driver_prepared);

            let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
            let column_count = u64::try_from(input_schema.fields().len())
                .map_err(|_| DatabaseError::resource_limit("numero colonne non rappresentabile"))?;
            let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
            Ok(PreparedWrite {
                operation: operation.clone(),
                input_schema,
                loss_report,
                budget: budget.clone(),
                operation_lease,
                columns_lease,
            })
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
            if !prepared.budget.is_same_budget(budget) {
                return Err(DatabaseError::invalid_plan(
                    "il budget di write non coincide con quello usato in prepare_write",
                ));
            }
            ensure_not_cancelled(cancellation, ErrorPhase::Write)?;
            self.validate_source(&prepared.operation.target)?;
            let input_schema = input.schema();
            if let Some(input_total) = input.declared_input_rows() {
                crate::write::validate_diagnostic_request(
                    &prepared.input_schema,
                    &input_schema,
                    &prepared.operation,
                    self.insert_mode,
                    input_total,
                    input.row_diagnostics_policy(),
                )?;
            }
            let pool = self.pool_for(secret)?;
            let driver_prepared = prepare_write_with_external_contract_leases(
                &pool,
                &prepared.operation,
                input_schema,
                budget,
                cancellation,
                self.insert_mode,
                self.schema_evolution,
            )
            .await?;
            let result = write_prepared(driver_prepared, input, cancellation).await;
            drop(prepared);
            result
        })
    }

    fn execute_ddl<'a>(
        &'a self,
        secret: &'a SecretString,
        sql: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async move {
            ensure_not_cancelled(cancellation, ErrorPhase::Write)?;
            let pool = self.pool_for(secret)?;
            let mut session = pool.checkout(cancellation).await?;
            let outcome = session
                .session_mut()?
                .execute_query(Query::new(sql.to_owned()), ErrorPhase::Write, cancellation)
                .await;
            drop(session);
            outcome.map(|_| ()).map_err(ddl_execution_error)
        })
    }
}

/// Dopo l'invio di DDL non si puo dedurre dal solo errore se SQL Server abbia
/// applicato zero, uno o piu statement del batch. Il bordo pubblico conserva
/// quindi l'incertezza e obbliga il chiamante a verificare lo stato remoto.
const fn ddl_execution_error(mut error: DatabaseError) -> DatabaseError {
    error.remote_effect = RemoteEffect::Unknown;
    error.retry = RetryDisposition::RequiresRecovery;
    error
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Interrompe **prima** dell'I/O, dicendo quale causa ha interrotto.
///
/// Sta davanti a test connection, capability, inspect, read, query, prepare e
/// write: costruire qui `Cancelled` a mano faceva uscire come annullamento
/// esplicito una deadline che il resto del provider — e gli altri due
/// provider — riportano come `Timeout`.
fn ensure_not_cancelled(cancellation: &CancellationToken, phase: ErrorPhase) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(DatabaseError::interrupted(
            cancellation,
            Some(ProviderKind::Sqlserver),
            phase,
            "operazione SQL Server interrotta prima dell'I/O",
        ))
    } else {
        Ok(())
    }
}

fn unsupported(phase: ErrorPhase, message: &'static str) -> DatabaseError {
    DatabaseError::unsupported(ProviderKind::Sqlserver, phase, message)
}

fn provider_error(
    category: ErrorCategory,
    phase: ErrorPhase,
    message: &'static str,
) -> DatabaseError {
    DatabaseError {
        category,
        phase,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(ProviderKind::Sqlserver),
        execution_id: None,
        message: message.to_owned(),
        diagnostics: None,
    }
}

/// Il blocco spatial delle capability, dal solo esito della sonda.
///
/// # Perche una funzione e non un letterale in mezzo al documento
///
/// Perche cosi si puo interrogare senza un server, ed e cio che ha fatto
/// emergere l'incoerenza che questa funzione corregge.
///
/// `geometry` e `geography` erano sondate — il provider contempla che i due UDT
/// non ci siano — mentre `read_wkb`, `write_wkb`, `dimensions` e l'elenco delle
/// funzioni erano costanti. Un server senza tipi spaziali avrebbe percio
/// pubblicato «non ho geometrie» e insieme «so leggere WKB, conosco quattro
/// profili dimensionali e ventinove funzioni». Nessuna delle due meta e
/// sbagliata da sola; insieme non descrivono niente.
///
/// E' la stessa classe che `PostgreSQL` aveva gia corretto, e il commento la
/// nomina per esteso: li `read_wkb` e `write_wkb` sono `geometry &&
/// callable(...)`, perche la funzione di trasporto puo mancare anche dove il
/// tipo c'e. Su SQL Server il trasporto non puo mancare — `.AsBinaryZM()` e
/// `STGeomFromWKB` appartengono al tipo — ma il **tipo** si, e la condizione
/// che restava da scrivere era quella.
fn spatial_capabilities(probe: &crate::catalog::SqlServerProbe) -> SpatialCapabilities {
    let geometry = probe.geometry_type_id.is_some();
    let geography = probe.geography_type_id.is_some();
    // Senza nessuno dei due UDT non c'e una superficie spatial di cui parlare,
    // e il blocco si chiude per intero invece di restare vero a meta.
    let spatial = geometry || geography;
    // Una voce per semantica dichiarata, e la voce di `geometry` e piu lunga:
    // sette funzioni del contratto esistono su quel tipo e non sull'altro —
    // `STCentroid`, `STEnvelope`, `STBoundary`, `STPointOnSurface`,
    // `STIsSimple`, `STTouches`, `STCrosses` — e il censimento le
    // ha misurate una per una.
    //
    // Finche il contratto pubblicava una lista sola, quelle sette restavano
    // chiuse: offrirle accanto a `geography` sarebbe stata una promessa che li
    // non regge. Ora la semantica ce l'hanno scritta accanto.
    let mut functions_by_semantics = BTreeMap::new();
    if geometry {
        functions_by_semantics.insert(
            SpatialSemantics::Geometry,
            crate::query::GEOMETRY_ONLY_SPATIAL_FUNCTIONS
                .iter()
                .chain(crate::query::VERIFIED_SPATIAL_FUNCTIONS)
                .copied()
                .collect(),
        );
    }
    if geography {
        functions_by_semantics.insert(
            SpatialSemantics::Geography,
            crate::query::VERIFIED_SPATIAL_FUNCTIONS.to_vec(),
        );
    }
    SpatialCapabilities {
        // L'SRID viaggia **dentro** il valore: `geometry` e `geography` sono
        // UDT che se lo portano dietro, e `.STSrid` lo rende senza interrogare
        // un catalogo. Non c'e niente da dichiarare.
        requires_declared_crs: false,
        read_wkb: spatial,
        write_wkb: spatial,
        geometry,
        geography,
        // La presenza dei due UDT e la condizione, non la prova. Il DDL vero —
        // `GEOMETRY_AUTO_GRID` con bounding box calcolato sui dati,
        // `GEOGRAPHY_AUTO_GRID` senza — lo attraversa
        // `live_create_and_replace_round_trip_all_reference_types`, che dopo il
        // create **e** dopo il replace staged rilegge il catalogo e pretende i
        // due indici con il loro schema di tessellazione: un percorso che
        // smettesse di emettere la DDL non renderebbe questo flag falso, ma
        // farebbe cadere quella prova.
        spatial_index: geometry && geography,
        // Stessa forma, e per un po' e stata una deduzione soltanto: `geometry`
        // e `geography` sono UDT non vincolati a un singolo tipo geometrico — a
        // differenza di una colonna `POINT` di MySQL — e da li si concludeva
        // che i tipi misti reggessero. Il ragionamento e solido, ed e
        // esattamente cio che su MySQL aveva tenuto in piedi per mesi undici
        // funzioni mai utilizzabili.
        //
        // Ora c'e la misura:
        // `live_mixed_geometry_types_share_one_column_on_both_semantics` scrive
        // `Point`, `LineString` e `Polygon` nella stessa colonna, in un batch
        // solo, e si fa dire dal server il tipo di ogni riga riletta.
        mixed_geometry_types: spatial,
        // Le quattro dimensioni non sono un elenco di comodo:
        // `live_spatial_write_round_trips_z_m_and_zm_losslessly` scrive `xyz`,
        // `xym` e `xyzm` su entrambe le semantiche e pretende il ritorno **byte
        // per byte**, e `Xy` la attraversa mezzo repository.
        dimensions: if spatial {
            vec![
                Dimensions::Xy,
                Dimensions::Xyz,
                Dimensions::Xym,
                Dimensions::Xyzm,
            ]
        } else {
            Vec::new()
        },
        // La lista e misurata da
        // `live_every_verified_spatial_function_is_crossed` su **entrambe** le
        // semantiche, e delimitata da
        // `live_the_spatial_census_leaves_no_usable_function_unexplained`, che
        // attraversa tutte e settantadue quelle del catalogo. Sono metodi di un
        // tipo: senza il tipo non c'e niente da offrire.
        functions: plenora_database_core::capabilities::intersect_spatial_functions(
            &functions_by_semantics,
        ),
        functions_by_semantics,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CertificatePolicy;

    /// Una sonda con o senza i due UDT, e nient'altro di variabile.
    fn probe_with(geometry: Option<i32>, geography: Option<i32>) -> crate::catalog::SqlServerProbe {
        crate::catalog::SqlServerProbe {
            product_version: "16.0.4255.1".to_owned(),
            product_level: "RTM".to_owned(),
            edition: "Developer Edition (64-bit)".to_owned(),
            engine_edition: 3,
            hadr_enabled: false,
            database: "dataflow_test".to_owned(),
            compatibility_level: 160,
            collation: "Latin1_General_100_CI_AS_SC".to_owned(),
            read_committed_snapshot: false,
            snapshot_isolation_state: 0,
            geometry_type_id: geometry,
            geography_type_id: geography,
            polybase_installed: false,
        }
    }

    #[test]
    fn the_guaranteed_list_is_the_intersection_of_what_each_semantics_offers() {
        // L'invariante che rende il campo nuovo leggibile: `functions` non e
        // una terza lista scritta a mano accanto alle due, e cio che vale
        // ovunque. La calcola il core da `functions_by_semantics`, e questa
        // prova pretende che il conto torni con cio che le due liste dicono.
        let both = spatial_capabilities(&probe_with(Some(240), Some(241)));
        assert_eq!(both.functions_by_semantics.len(), 2);
        let on_geometry = &both.functions_by_semantics[&SpatialSemantics::Geometry];
        let on_geography = &both.functions_by_semantics[&SpatialSemantics::Geography];
        assert_eq!(
            on_geometry.len(),
            crate::query::VERIFIED_SPATIAL_FUNCTIONS.len()
                + crate::query::GEOMETRY_ONLY_SPATIAL_FUNCTIONS.len()
        );
        assert_eq!(
            on_geography.len(),
            crate::query::VERIFIED_SPATIAL_FUNCTIONS.len()
        );
        // L'intersezione **e** la lista garantita, e nessuna delle sette la
        // raggiunge.
        assert_eq!(
            both.functions,
            crate::query::VERIFIED_SPATIAL_FUNCTIONS.to_vec()
        );
        for function in crate::query::GEOMETRY_ONLY_SPATIAL_FUNCTIONS {
            assert!(on_geometry.contains(function), "{function:?}");
            assert!(!on_geography.contains(function), "{function:?}");
            assert!(!both.functions.contains(function), "{function:?}");
        }
    }

    #[test]
    fn one_semantics_alone_publishes_only_its_own_list() {
        // Una chiave per una semantica non dichiarata sarebbe una promessa su
        // un tipo che il prodotto dice di non avere.
        let only_geometry = spatial_capabilities(&probe_with(Some(240), None));
        assert_eq!(only_geometry.functions_by_semantics.len(), 1);
        assert!(only_geometry
            .functions_by_semantics
            .contains_key(&SpatialSemantics::Geometry));
        // Con una semantica sola, l'intersezione **e** quella lista: le sette
        // diventano garantite, perche non c'e un secondo tipo su cui possano
        // mancare.
        for function in crate::query::GEOMETRY_ONLY_SPATIAL_FUNCTIONS {
            assert!(only_geometry.functions.contains(function), "{function:?}");
        }
    }

    #[test]
    fn without_the_spatial_types_the_whole_spatial_block_closes() {
        // `geometry` e `geography` erano sondate — il provider contempla che i
        // due UDT non ci siano — e accanto stavano quattro affermazioni
        // costanti: so leggere WKB, so scriverlo, conosco quattro profili
        // dimensionali, offro ventinove funzioni. Su un server senza tipi
        // spaziali il documento diceva le due meta insieme, e insieme non
        // descrivono niente.
        //
        // E' la classe che PostgreSQL aveva gia corretto, dove `read_wkb` e
        // `write_wkb` sono `geometry && callable(...)`.
        let closed = spatial_capabilities(&probe_with(None, None));
        assert!(!closed.geometry && !closed.geography);
        assert!(!closed.read_wkb, "so leggere WKB di quale geometria?");
        assert!(!closed.write_wkb);
        assert!(!closed.spatial_index);
        assert!(!closed.mixed_geometry_types);
        assert!(closed.dimensions.is_empty());
        assert!(closed.functions.is_empty());
    }

    #[test]
    fn one_spatial_type_is_enough_to_transport_wkb_and_not_enough_to_index() {
        // Le due condizioni sono diverse e vanno tenute diverse: il trasporto
        // WKB appartiene al tipo, quindi uno solo basta; l'indice spaziale il
        // provider lo emette per **entrambe** le semantiche nella stessa DDL,
        // quindi ne pretende due.
        let only_geometry = spatial_capabilities(&probe_with(Some(240), None));
        assert!(only_geometry.read_wkb && only_geometry.write_wkb);
        assert!(only_geometry.geometry && !only_geometry.geography);
        assert!(!only_geometry.spatial_index);
        assert!(!only_geometry.functions.is_empty());

        let both = spatial_capabilities(&probe_with(Some(240), Some(241)));
        assert!(both.spatial_index);
        assert_eq!(
            both.functions,
            crate::query::VERIFIED_SPATIAL_FUNCTIONS.to_vec()
        );
        assert_eq!(both.dimensions.len(), 4);
    }

    fn provider() -> SqlServerProvider {
        let config = SqlServerConfig::new(
            "sql.example.test",
            "warehouse",
            "loader",
            SecretString::new("constructor-secret"),
        )
        .with_certificate_policy(CertificatePolicy::TrustServerCertificate);
        SqlServerProvider::new(config, 1_024, 4).expect("provider")
    }

    #[test]
    fn implements_common_provider_contract_type() {
        const fn assert_provider<T: Provider>() {}
        assert_provider::<SqlServerProvider>();
        assert_eq!(provider().kind(), ProviderKind::Sqlserver);
    }

    #[tokio::test]
    async fn pre_cancelled_connection_fails_without_network() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = provider()
            .test_connection(&SecretString::new("runtime-secret"), &cancellation)
            .await
            .expect_err("cancelled");
        assert_eq!(error.category, ErrorCategory::Cancelled);
        assert_eq!(error.phase, ErrorPhase::Connect);
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert_eq!(error.retry, RetryDisposition::Never);
        assert_eq!(error.provider, Some(ProviderKind::Sqlserver));
    }

    #[tokio::test]
    async fn pre_cancelled_ddl_fails_without_network_and_without_remote_effect() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = provider()
            .execute_ddl(
                &SecretString::new("runtime-secret"),
                "CREATE TABLE should_not_run (id int)",
                &cancellation,
            )
            .await
            .expect_err("cancelled");
        assert_eq!(error.category, ErrorCategory::Cancelled);
        assert_eq!(error.phase, ErrorPhase::Write);
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert_eq!(error.retry, RetryDisposition::Never);
    }

    #[tokio::test]
    async fn unsupported_transaction_options_fail_before_pool_checkout() {
        use plenora_database_core::resource::ResourceLimits;
        use plenora_database_core::transaction::{AccessMode, TransactionOptions};

        let options = TransactionOptions {
            access_mode: Some(AccessMode::ReadOnly),
            ..TransactionOptions::default()
        };
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let outcome = provider()
            .begin_transaction(
                &SecretString::new("runtime-secret"),
                &options,
                &budget,
                &CancellationToken::new(),
            )
            .await;
        let Err(error) = outcome else {
            panic!("l'opzione deve fallire senza tentare la rete");
        };
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
        assert_eq!(error.remote_effect, RemoteEffect::None);
    }

    #[test]
    fn ddl_failure_after_dispatch_requires_remote_recovery() {
        let error = provider_error(
            ErrorCategory::Execution,
            ErrorPhase::Write,
            "DDL SQL Server rifiutato",
        );
        let classified = ddl_execution_error(error);
        assert_eq!(classified.remote_effect, RemoteEffect::Unknown);
        assert_eq!(classified.retry, RetryDisposition::RequiresRecovery);
        assert_eq!(classified.message, "DDL SQL Server rifiutato");
    }

    #[test]
    fn provider_debug_redacts_constructor_and_runtime_state() {
        let rendered = format!("{:?}", provider());
        assert!(!rendered.contains("constructor-secret"));
        assert!(!rendered.contains("loader"));
        assert!(rendered.contains("[REDACTED]"));
    }
}
