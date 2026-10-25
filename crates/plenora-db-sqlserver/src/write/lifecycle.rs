use crate::SqlServerSession;
use plenora_database_core::plan::ProviderKind;
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

/// La regola R su SQL Server (`plenora_database_core::row_visibility`) per
/// ogni mode su un target esistente tranne Replace:
///
/// - un trigger `INSTEAD OF` per uno degli eventi della mode riceve le righe
///   al posto del target: l'`OUTPUT` dello statement (con cui la scrittura
///   conta) e `@@ROWCOUNT` contano cio che il trigger ha ricevuto, non cio
///   che ha scritto. Misurato il 25/10 su SQL Server 2022: Upsert con
///   `INSTEAD OF UPDATE` vuoto → `updated 1` e riga invariata, con `INSTEAD
///   OF INSERT` vuoto → `inserted 1` e nessuna riga; `DeleteByKeys` con
///   `INSTEAD OF DELETE` → `skipped` per una chiave presente;
/// - un predicato di filtro RLS nasconde righe a `Update` e `DeleteByKeys`,
///   che le pubblicherebbero `skipped` come chiavi assenti (per un INSERT un
///   predicato di blocco rifiuta con un errore, non in silenzio);
/// - con il profilo `TdsBulk` (`bulk`), `INSERT BULK` di `tiberius-ng` 0.13.1
///   non passa `CHECK_CONSTRAINTS`, `FIRE_TRIGGERS` ne `KEEP_NULLS`, e il
///   driver non ha modo di passarli: vincoli `CHECK` e foreign key non sono
///   verificati (e restano `is_not_trusted`), i trigger `INSERT` non
///   scattano, e un `NULL` dell'ingresso puo diventare il default della
///   colonna. Un target con uno di questi rifiuta il profilo (`path_bypass`);
///   il ripiego silenzioso sul profilo preparato non c'e, chi lo vuole lo
///   sceglie.
///
/// Replace qui non passa: pubblica uno staging con lo swap e non cancella
/// righe, e le sue condizioni (nessun trigger, nessuna policy) sono in
/// [`validate_replace_external_state`].
///
/// I fatti si leggono sulla chiusura del target: il target e, se e una
/// vista, gli oggetti da cui dipende (`sys.sql_expression_dependencies`,
/// ricorsivamente). Una dipendenza non risolta o un oggetto che non e una
/// tabella o una vista (funzione, sinonimo, tabella esterna) sono opachi.
///
/// # Limite dichiarato
///
/// - **Regola.** La visibilita dei metadati di SQL Server nasconde una
///   security policy a chi non ha permessi su di essa: senza `VIEW
///   DEFINITION` sul database il catalogo non prova l'assenza di filtri, e la
///   regola rifiuta (`unreadable`).
/// - **Ambito.** Append, `TruncateInsert`, Upsert, Update, `DeleteByKeys` SQL
///   Server; il profilo `TdsBulk` per Append e `TruncateInsert`.
/// - **Hazard.** Un ruolo senza quel permesso non puo scrivere un target
///   esistente; `TdsBulk` non si usa su tabelle con vincoli `CHECK`, foreign
///   key, trigger `INSERT` o default.
/// - **Rientro.** Una prova dell'assenza di policy che non dipenda dalla
///   visibilita dei metadati; un driver che passi le opzioni di `INSERT
///   BULK`.
pub(super) async fn validate_row_visibility(
    session: &mut SqlServerSession,
    schema: &str,
    object: &str,
    mode: plenora_database_core::plan::WriteMode,
    bulk: bool,
    phase: ErrorPhase,
    cancellation: &CancellationToken,
) -> Result<()> {
    use plenora_database_core::plan::WriteMode;
    use plenora_database_core::row_visibility::{
        mode_events, refuse_unfaithful_write, RowFilters, WriteEvent,
    };
    let events = mode_events(mode);
    if events.is_empty() || mode == WriteMode::Replace {
        return Ok(());
    }
    // `sys.trigger_events.type_desc` degli eventi, fra virgole: il filtro
    // e `CHARINDEX(',' + type_desc + ',', @P3) > 0`.
    let names = events
        .iter()
        .map(|event| match event {
            WriteEvent::Insert => "INSERT",
            WriteEvent::Update => "UPDATE",
            WriteEvent::Delete => "DELETE",
        })
        .collect::<Vec<_>>();
    let mut query = Query::new(ROW_VISIBILITY_SQL);
    query.bind(schema.to_owned());
    query.bind(object.to_owned());
    query.bind(format!(",{},", names.join(",")));
    let row = one_row(
        session.execute_query(query, phase, cancellation).await?,
        "visibilita delle righe",
        phase,
    )?;
    let fact = |index: usize, name: &str| -> Result<bool> {
        Ok(required::<i64>(&row, index, name, phase)? > 0)
    };
    if !fact(0, "target risolto")? {
        return Ok(());
    }
    refuse_unfaithful_write(
        ProviderKind::Sqlserver,
        mode,
        phase,
        RowFilters {
            unreadable: !fact(1, "permesso VIEW DEFINITION")?,
            row_security: fact(2, "predicati di filtro")?,
            rewrite_rule: false,
            instead_trigger: fact(3, "trigger INSTEAD OF")?,
            // SQL Server non ha trigger BEFORE: l'unico trigger che sopprime
            // righe e l'`INSTEAD OF`, letto sopra per ogni evento della mode.
            row_trigger: false,
            path_bypass: bulk && fact(6, "garanzie scavalcate dal bulk")?,
            opaque_relation: fact(4, "dipendenze opache")?,
            view: fact(5, "vista")?,
        },
    )
}

const ROW_VISIBILITY_SQL: &str = r"
DECLARE @object_id int =
(
    SELECT o.object_id
    FROM sys.objects AS o
    JOIN sys.schemas AS s ON s.schema_id = o.schema_id
    WHERE s.name = @P1 AND o.name = @P2
);
DECLARE @closure TABLE (object_id int PRIMARY KEY);
WITH closure AS
(
    SELECT @object_id AS object_id
    UNION ALL
    SELECT d.referenced_id
    FROM closure AS c
    JOIN sys.objects AS v ON v.object_id = c.object_id AND v.type = 'V'
    JOIN sys.sql_expression_dependencies AS d ON d.referencing_id = c.object_id
    WHERE d.referenced_id IS NOT NULL
)
INSERT INTO @closure (object_id)
SELECT DISTINCT object_id FROM closure WHERE object_id IS NOT NULL;
SELECT
    CAST(CASE WHEN @object_id IS NULL THEN 0 ELSE 1 END AS bigint),
    CAST(ISNULL(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DEFINITION'), 0) AS bigint),
    (SELECT COUNT_BIG(*)
       FROM sys.security_predicates AS p
       JOIN sys.security_policies AS sp ON sp.object_id = p.object_id
      WHERE p.target_object_id IN (SELECT object_id FROM @closure)
        AND sp.is_enabled = 1 AND p.predicate_type = 0),
    (SELECT COUNT_BIG(*)
       FROM sys.triggers AS t
       JOIN sys.trigger_events AS e ON e.object_id = t.object_id
      WHERE t.parent_id IN (SELECT object_id FROM @closure)
        AND t.is_instead_of_trigger = 1 AND t.is_disabled = 0
        AND CHARINDEX(',' + e.type_desc + ',', @P3) > 0),
    (SELECT COUNT_BIG(*)
       FROM @closure AS c
       LEFT JOIN sys.objects AS o ON o.object_id = c.object_id
      WHERE o.object_id IS NULL OR o.type NOT IN ('U', 'V'))
    + (SELECT COUNT_BIG(*)
         FROM sys.sql_expression_dependencies AS d
         JOIN sys.objects AS v ON v.object_id = d.referencing_id AND v.type = 'V'
        WHERE d.referencing_id IN (SELECT object_id FROM @closure)
          AND d.referenced_id IS NULL),
    (SELECT COUNT_BIG(*) FROM sys.objects WHERE object_id = @object_id AND type = 'V'),
    (SELECT COUNT_BIG(*) FROM sys.check_constraints
      WHERE parent_object_id IN (SELECT object_id FROM @closure) AND is_disabled = 0)
    + (SELECT COUNT_BIG(*) FROM sys.foreign_keys
        WHERE parent_object_id IN (SELECT object_id FROM @closure) AND is_disabled = 0)
    + (SELECT COUNT_BIG(*)
         FROM sys.triggers AS t
         JOIN sys.trigger_events AS e ON e.object_id = t.object_id
        WHERE t.parent_id IN (SELECT object_id FROM @closure)
          AND t.is_disabled = 0 AND e.type_desc = 'INSERT')
    + (SELECT COUNT_BIG(*) FROM sys.default_constraints
        WHERE parent_object_id IN (SELECT object_id FROM @closure));
";

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
    DatabaseError::new(category, phase, Some(ProviderKind::Sqlserver), message)
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
