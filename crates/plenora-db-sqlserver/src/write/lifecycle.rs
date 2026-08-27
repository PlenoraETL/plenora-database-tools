use crate::SqlServerSession;
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::{Dialect, DialectCapabilities, Identifier, ObjectName, Renderer};
use tiberius::{FromSql, Query, Row};

pub(super) fn derived_object_name(base: &str, role: &str, nonce: u64) -> Result<String> {
    let suffix = format!("__pln_{role}_{:x}_{nonce:x}", std::process::id());
    let available = crate::MAX_IDENTIFIER_CHARACTERS
        .checked_sub(suffix.chars().count())
        .ok_or_else(|| {
            lifecycle_error(
                ErrorCategory::ResourceLimit,
                ErrorPhase::Prepare,
                "suffix lifecycle SQL Server non rappresentabile",
            )
        })?;
    let prefix = base.chars().take(available).collect::<String>();
    let value = format!("{prefix}{suffix}");
    Identifier::new(value.clone())?;
    Ok(value)
}

pub(super) async fn object_exists(
    session: &mut SqlServerSession,
    schema: &str,
    object: &str,
    cancellation: &CancellationToken,
) -> Result<bool> {
    let mut query = Query::new(
        "SELECT COUNT_BIG(*) FROM sys.objects AS o \
         JOIN sys.schemas AS s ON s.schema_id = o.schema_id \
         WHERE s.name = @P1 AND o.name = @P2;",
    );
    query.bind(schema.to_owned());
    query.bind(object.to_owned());
    let row = one_row(
        session
            .execute_query(query, ErrorPhase::Prepare, cancellation)
            .await?,
        "esistenza target",
        ErrorPhase::Prepare,
    )?;
    Ok(required::<i64>(&row, 0, "conteggio esistenza", ErrorPhase::Prepare)? > 0)
}

pub(super) async fn validate_replace_external_state(
    session: &mut SqlServerSession,
    schema: &str,
    object: &str,
    phase: ErrorPhase,
    cancellation: &CancellationToken,
) -> Result<()> {
    let feature_row = one_row(
        session
            .execute_query(
                Query::new(
                    "SELECT CAST(CASE WHEN COL_LENGTH('sys.tables', 'ledger_type') IS NULL \
                     THEN 0 ELSE 1 END AS bit), \
                     CAST(CASE WHEN COL_LENGTH('sys.partitions', 'xml_compression') IS NULL \
                     THEN 0 ELSE 1 END AS bit);",
                ),
                phase,
                cancellation,
            )
            .await?,
        "feature catalogo replace",
        phase,
    )?;
    let has_ledger = required::<bool>(&feature_row, 0, "ledger_type", phase)?;
    let has_xml_compression = required::<bool>(&feature_row, 1, "xml_compression", phase)?;
    let mut query = Query::new(replace_external_state_sql(has_ledger, has_xml_compression));
    query.bind(schema.to_owned());
    query.bind(object.to_owned());
    let row = one_row(
        session.execute_query(query, phase, cancellation).await?,
        "dipendenze replace",
        phase,
    )?;
    let names = [
        "foreign key entranti",
        "trigger",
        "permessi object-level",
        "dipendenze SQL",
        "proprieta estese",
        "row-level security",
        "change tracking",
        "colonne sparse/column-set/encrypted/hidden/masked",
        "CDC/replica/FileTable/stretch/external/graph/ledger",
        "partition scheme",
        "compressione dati o XML",
        "statistiche utente",
        "indice full-text",
        "specifiche database audit",
    ];
    for (index, name) in names.iter().enumerate() {
        if required::<i64>(&row, index, name, phase)? > 0 {
            return Err(lifecycle_error(
                ErrorCategory::Unsupported,
                phase,
                format!("replace SQL Server non preserva {name}"),
            ));
        }
    }
    Ok(())
}

fn replace_external_state_sql(has_ledger: bool, has_xml_compression: bool) -> String {
    let ledger = if has_ledger {
        " OR ledger_type <> 0"
    } else {
        ""
    };
    let xml_compression = if has_xml_compression {
        " OR xml_compression <> 0"
    } else {
        ""
    };
    format!(
        r"
DECLARE @object_id int =
(
    SELECT o.object_id
    FROM sys.objects AS o
    JOIN sys.schemas AS s ON s.schema_id = o.schema_id
    WHERE s.name = @P1 AND o.name = @P2 AND o.type = 'U'
);
SELECT
    (SELECT COUNT_BIG(*) FROM sys.foreign_keys WHERE referenced_object_id = @object_id),
    (SELECT COUNT_BIG(*) FROM sys.triggers WHERE parent_id = @object_id),
    (SELECT COUNT_BIG(*) FROM sys.database_permissions
      WHERE class = 1 AND major_id = @object_id),
    (SELECT COUNT_BIG(*) FROM sys.sql_expression_dependencies
      WHERE referenced_id = @object_id),
    (SELECT COUNT_BIG(*) FROM sys.extended_properties
      WHERE class = 1 AND major_id = @object_id),
    (SELECT COUNT_BIG(*) FROM sys.security_predicates
      WHERE target_object_id = @object_id),
    (SELECT COUNT_BIG(*) FROM sys.change_tracking_tables
      WHERE object_id = @object_id),
    (SELECT COUNT_BIG(*) FROM sys.columns
      WHERE object_id = @object_id
        AND (is_sparse = 1 OR is_column_set = 1 OR encryption_type IS NOT NULL
             OR is_hidden = 1 OR is_masked = 1)),
    (SELECT COUNT_BIG(*)
       FROM sys.tables
      WHERE object_id = @object_id
        AND (is_tracked_by_cdc = 1 OR is_replicated = 1
             OR has_replication_filter = 1 OR is_merge_published = 1
             OR is_sync_tran_subscribed = 1 OR is_filetable = 1
             OR is_remote_data_archive_enabled = 1 OR is_external = 1
             OR is_node = 1 OR is_edge = 1{ledger})),
    (SELECT COUNT_BIG(*)
       FROM sys.indexes AS i
       JOIN sys.data_spaces AS ds ON ds.data_space_id = i.data_space_id
      WHERE i.object_id = @object_id AND ds.type = 'PS'),
    (SELECT COUNT_BIG(*)
       FROM sys.partitions
      WHERE object_id = @object_id
        AND (data_compression <> 0{xml_compression})),
    (SELECT COUNT_BIG(*) FROM sys.stats
      WHERE object_id = @object_id AND user_created = 1),
    (SELECT COUNT_BIG(*) FROM sys.fulltext_indexes WHERE object_id = @object_id),
    (SELECT COUNT_BIG(*) FROM sys.database_audit_specification_details
      WHERE major_id = @object_id);
"
    )
}

pub(super) async fn publish_replacement(
    session: &mut SqlServerSession,
    schema: &str,
    object: &str,
    staging_object: &str,
    backup_object: &str,
    cancellation: &CancellationToken,
) -> Result<()> {
    let renderer = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: false,
        },
    );
    let schema_id = Identifier::new(schema.to_owned())?;
    let original_id = Identifier::new(object.to_owned())?;
    let staging_id = Identifier::new(staging_object.to_owned())?;
    let backup_id = Identifier::new(backup_object.to_owned())?;
    let original = renderer.quote_object(&ObjectName {
        catalog: None,
        schema: Some(schema_id.clone()),
        object: original_id,
    })?;
    let staging = renderer.quote_object(&ObjectName {
        catalog: None,
        schema: Some(schema_id.clone()),
        object: staging_id,
    })?;
    let backup = renderer.quote_object(&ObjectName {
        catalog: None,
        schema: Some(schema_id),
        object: backup_id,
    })?;
    let publish = PublishStatement::new(&original, &staging, &backup, object, backup_object);
    let mut query = Query::new(publish.sql);
    for value in publish.binds {
        query.bind(value);
    }
    session.execute_write_query(query, cancellation).await?;
    Ok(())
}

/// Lo statement che pubblica una `Replace`, con i suoi parametri in ordine.
///
/// Il ciclo e uno solo, e questa struttura e il posto dove si legge per
/// intero: il target prende il nome del backup, lo staging prende il nome del
/// target, e il backup — che a quel punto porta i dati vecchi — viene
/// eliminato. Non c'e un istante in cui il nome del target non esiste, e non
/// c'e un percorso in cui il `DROP` colpisce qualcosa di diverso dal backup.
///
/// Vive separata da [`publish_replacement`] perche quella chiede una sessione:
/// una garanzia che si puo verificare solo con un server acceso e una garanzia
/// che nessuno verifica.
struct PublishStatement {
    sql: String,
    binds: Vec<String>,
}

impl PublishStatement {
    fn new(
        original_quoted: &str,
        staging_quoted: &str,
        backup_quoted: &str,
        object: &str,
        backup_object: &str,
    ) -> Self {
        Self {
            sql: format!(
                "EXEC sys.sp_rename @P1, @P2, N'OBJECT'; \
                 EXEC sys.sp_rename @P3, @P4, N'OBJECT'; \
                 DROP TABLE {backup_quoted};"
            ),
            // `sp_rename` vuole il nome vecchio qualificato e quello nuovo
            // nudo: il secondo e un nome, non un riferimento.
            binds: vec![
                original_quoted.to_owned(),
                backup_object.to_owned(),
                staging_quoted.to_owned(),
                object.to_owned(),
            ],
        }
    }
}

fn one_row(mut results: Vec<Vec<Row>>, context: &str, phase: ErrorPhase) -> Result<Row> {
    if results.len() != 1 {
        return Err(lifecycle_error(
            ErrorCategory::Protocol,
            phase,
            format!("{context} SQL Server con result set inattesi"),
        ));
    }
    let mut rows = results.pop().ok_or_else(|| {
        lifecycle_error(
            ErrorCategory::Protocol,
            phase,
            format!("{context} SQL Server senza result set"),
        )
    })?;
    if rows.len() != 1 {
        return Err(lifecycle_error(
            ErrorCategory::Protocol,
            phase,
            format!("{context} SQL Server con cardinalita inattesa"),
        ));
    }
    rows.pop().ok_or_else(|| {
        lifecycle_error(
            ErrorCategory::Protocol,
            phase,
            format!("{context} SQL Server senza riga"),
        )
    })
}

fn required<'a, T>(row: &'a Row, index: usize, name: &str, phase: ErrorPhase) -> Result<T>
where
    T: FromSql<'a>,
{
    row.try_get(index)
        .map_err(|_| {
            lifecycle_error(
                ErrorCategory::Protocol,
                phase,
                format!("tipo lifecycle incompatibile: {name}"),
            )
        })?
        .ok_or_else(|| {
            lifecycle_error(
                ErrorCategory::Protocol,
                phase,
                format!("campo lifecycle assente: {name}"),
            )
        })
}

fn lifecycle_error(
    category: ErrorCategory,
    phase: ErrorPhase,
    message: impl Into<String>,
) -> DatabaseError {
    DatabaseError::new(
        category,
        phase,
        Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        message,
    )
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
