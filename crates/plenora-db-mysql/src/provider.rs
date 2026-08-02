use crate::{describe_object, list_objects, list_schemas, probe_server, MysqlConfig, MysqlPool};
use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::capabilities::{
    ProviderCapabilities, ProviderLimits, ReadCapabilities, SpatialCapabilities,
    TransactionCapabilities, TransactionScope, WriteCapabilities,
};
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{Operation, ProviderKind, ReadOperation, WriteOperation};
use plenora_database_core::provider::{
    BatchStream, ConnectionInfo, Inspection, ParameterBag, PreparedWrite, Provider, ProviderFuture,
    SecretString,
};
use plenora_database_core::query::QueryOperation;
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

struct CachedPool {
    secret_fingerprint: [u8; 32],
    pool: Arc<MysqlPool>,
}

pub struct MysqlProvider {
    config: MysqlConfig,
    max_connections: usize,
    cached_pool: Mutex<Option<CachedPool>>,
}

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
        config.validate()?;
        if max_connections == 0 {
            return Err(provider_error(
                ErrorCategory::InvalidConfiguration,
                ErrorPhase::Validate,
                "provider MySQL con pool a capacita zero",
            ));
        }
        Ok(Self {
            config,
            max_connections,
            cached_pool: Mutex::new(None),
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
        let pool = Arc::new(MysqlPool::new(&config, self.max_connections)?);
        *cached = Some(CachedPool {
            secret_fingerprint: fingerprint,
            pool: Arc::clone(&pool),
        });
        drop(cached);
        Ok(pool)
    }

    fn validate_source(&self, source: &plenora_database_core::plan::ObjectRef) -> Result<()> {
        if source.layer_id.is_some() {
            return Err(unsupported("layer_id non appartiene al provider MySQL"));
        }
        if source
            .catalog
            .as_deref()
            .is_some_and(|catalog| catalog != self.config.database())
        {
            return Err(unsupported(
                "accesso cross-database MySQL non supportato dal provider",
            ));
        }
        Ok(())
    }

    async fn inspect_operation(
        &self,
        pool: &MysqlPool,
        operation: &Operation,
        cancellation: &CancellationToken,
    ) -> Result<Inspection> {
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
                let schemas = list_schemas(&mut session, cancellation).await?;
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
                let objects = list_objects(&mut session, schema, cancellation).await?;
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
                let description =
                    describe_object(&mut session, schema, &source.object, cancellation).await?;
                Ok(Inspection {
                    operation: "database.describe_object".to_owned(),
                    document: json!(description),
                })
            }
            _ => Err(unsupported("operazione inspect non supportata da MySQL")),
        };
        drop(session);
        result
    }
}

impl Provider for MysqlProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Mysql
    }

    fn test_connection<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        Box::pin(async move {
            let pool = self.pool_for(secret)?;
            let mut session = pool.checkout(cancellation).await?;
            let probe = probe_server(&mut session, cancellation).await?;
            drop(session);
            Ok(ConnectionInfo {
                provider: ProviderKind::Mysql,
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
            let pool = self.pool_for(secret)?;
            let mut session = pool.checkout(cancellation).await?;
            let probe = probe_server(&mut session, cancellation).await?;
            drop(session);
            Ok(ProviderCapabilities {
                schema_version: 1,
                provider: ProviderKind::Mysql,
                provider_version: probe.product_version,
                extension_versions: BTreeMap::new(),
                reads: ReadCapabilities {
                    streaming: true,
                    server_cursor: false,
                    pagination: false,
                    object_id_windows: false,
                    projection: true,
                    filter: true,
                    ordering: true,
                    resumable: false,
                },
                writes: WriteCapabilities {
                    create: false,
                    append: false,
                    update: false,
                    upsert: false,
                    replace: false,
                    delete_by_keys: false,
                    bulk: false,
                    array_binding: false,
                    returning: false,
                    apply_edits: false,
                    rollback_on_failure: false,
                    use_global_ids: false,
                },
                transactions: TransactionCapabilities {
                    single_transaction: false,
                    savepoints: false,
                    transactional_ddl: false,
                    staged_swap: false,
                    scope: TransactionScope::None,
                },
                spatial: mysql_spatial_capabilities(),
                limits: ProviderLimits {
                    max_identifier_bytes: Some(crate::MAX_IDENTIFIER_CHARACTERS as u64),
                    max_bind_parameters: Some(crate::MAX_BIND_PARAMETERS as u64),
                    max_statement_bytes: None,
                    max_batch_rows: Some(crate::MAX_BATCH_ROWS as u64),
                    max_payload_bytes: None,
                    max_record_count: None,
                },
            })
        })
    }

    fn inspect<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a Operation,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        Box::pin(async move {
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
            self.validate_source(&operation.source)?;
            let pool = self.pool_for(secret)?;
            let mut effective = operation.clone();
            effective
                .source
                .schema
                .get_or_insert_with(|| self.config.database().to_owned());
            crate::read_operation(
                &pool,
                &effective,
                parameters,
                crate::DEFAULT_BATCH_ROWS,
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
            let pool = self.pool_for(secret)?;
            crate::query_operation(
                &pool,
                self.config.database(),
                operation,
                parameters,
                crate::DEFAULT_BATCH_ROWS,
                budget,
                cancellation,
            )
            .await
        })
    }

    fn prepare_write<'a>(
        &'a self,
        _secret: &'a SecretString,
        _operation: &'a WriteOperation,
        _input_schema: SchemaRef,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        Box::pin(async { Err(unsupported("write MySQL non ancora qualificata")) })
    }

    fn write<'a>(
        &'a self,
        _secret: &'a SecretString,
        _prepared: PreparedWrite,
        _input: Box<dyn BatchStream>,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, WriteOutcome> {
        Box::pin(async { Err(unsupported("write MySQL non ancora qualificata")) })
    }
}

fn unsupported(message: impl Into<String>) -> DatabaseError {
    provider_error(ErrorCategory::Unsupported, ErrorPhase::Prepare, message)
}

fn mysql_spatial_capabilities() -> SpatialCapabilities {
    SpatialCapabilities {
        read_wkb: true,
        write_wkb: false,
        geometry: true,
        geography: false,
        spatial_index: false,
        mixed_geometry_types: true,
        dimensions: vec![plenora_database_core::geometry::Dimensions::Xy],
        functions: Vec::new(),
    }
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
        provider: Some(ProviderKind::Mysql),
        execution_id: None,
        message: message.into(),
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

    const fn assert_provider<T: Provider>() {}

    fn parameterized_query() -> QueryOperation {
        QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("warehouse".to_owned()),
                    object: "events".to_owned(),
                    layer_id: None,
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
                layer_id: None,
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
                    layer_id: None,
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
    fn published_spatial_capabilities_match_generic_geometry_contract() {
        let capabilities = mysql_spatial_capabilities();
        assert!(capabilities.geometry);
        assert!(capabilities.mixed_geometry_types);
    }
}
