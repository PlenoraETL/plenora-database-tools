use crate::MysqlSession;
use mysql_async::prelude::FromValue;
use mysql_async::{Params, Row, Value};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlProbe {
    pub product_version: String,
    pub database: String,
    pub version_comment: String,
    pub lower_case_table_names: u64,
    pub sql_mode: String,
    pub time_zone: String,
    pub transaction_isolation: String,
    pub tls_cipher: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlObjectSummary {
    pub schema: String,
    pub name: String,
    pub kind: String,
    pub engine: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlColumn {
    pub name: String,
    pub ordinal: u64,
    pub data_type: String,
    pub native_declaration: String,
    pub nullable: bool,
    pub default_expression: Option<String>,
    pub character_set: Option<String>,
    pub collation: Option<String>,
    pub numeric_precision: Option<u64>,
    pub numeric_scale: Option<u64>,
    pub datetime_precision: Option<u64>,
    pub spatial_srid: Option<u32>,
    pub extra: String,
    pub generation_expression: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MysqlSchemaToken(pub String);

/// Un indice (PRIMARY / UNIQUE / non-unique) osservato su una tabella.
///
/// Serve al preflight Upsert: `ON DUPLICATE KEY UPDATE` scatta su ogni
/// PK/unique index, non solo sulle `keys` dichiarate, quindi la policy
/// fail-closed deve poter confrontare le colonne di ciascun unique index
/// con le keys dell'operazione.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlIndex {
    pub name: String,
    /// `true` per PRIMARY e UNIQUE (`NON_UNIQUE` = 0).
    pub unique: bool,
    /// `true` se ogni parte dell'indice è una colonna semplice. `false` se
    /// almeno una parte è un'espressione (functional index, `MySQL` 8.0+):
    /// in quel caso `columns` non descrive l'indice per intero e non è
    /// confrontabile con le keys.
    pub column_backed: bool,
    /// Colonne dell'indice in ordine `SEQ_IN_INDEX`. Parziale/vuoto quando
    /// `column_backed` è `false`.
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlObjectDescription {
    pub schema: String,
    pub name: String,
    pub kind: String,
    pub engine: Option<String>,
    pub columns: Vec<MysqlColumn>,
    pub indexes: Vec<MysqlIndex>,
    pub token: MysqlSchemaToken,
}

/// Bypass del riconoscimento MariaDB riservato alle prove differenziali.
/// Permette di misurare il percorso MySQL contro MariaDB senza modificare
/// il comportamento dei costruttori pubblici.
#[cfg(test)]
static MARIADB_REJECTION_BYPASS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Attiva il bypass per il resto del processo di test.
///
/// Globale e non per-thread perche il provider gira su un runtime tokio
/// multi-thread: un interruttore legato al thread del test non sarebbe
/// visibile dentro il task che apre la connessione.
/// Attiva il bypass finche la guardia resta viva, e lo spegne al `Drop`.
///
/// Un interruttore che si accende e basta lascerebbe il rifiuto disattivato
/// per il resto del processo di test: qualunque altro test eseguito dopo, in
/// quel binario, vedrebbe un provider che accetta `MariaDB` senza che nessuno
/// lo abbia chiesto. Lo scope lo rende un fatto locale alla misura.
// `pub(crate)` in un modulo privato e cio che serve: la misura vive in un
// modulo fratello e non deve poterlo chiamare nessun altro.
#[allow(clippy::redundant_pub_crate)]
#[cfg(test)]
pub(crate) struct MariadbRejectionBypass;

#[allow(clippy::redundant_pub_crate)]
#[cfg(test)]
impl MariadbRejectionBypass {
    pub(crate) fn engage() -> Self {
        MARIADB_REJECTION_BYPASS.store(true, std::sync::atomic::Ordering::SeqCst);
        Self
    }
}

#[cfg(test)]
impl Drop for MariadbRejectionBypass {
    fn drop(&mut self) {
        MARIADB_REJECTION_BYPASS.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
fn mariadb_rejection_bypassed() -> bool {
    MARIADB_REJECTION_BYPASS.load(std::sync::atomic::Ordering::SeqCst)
}

#[cfg(not(test))]
const fn mariadb_rejection_bypassed() -> bool {
    false
}

/// Rileva versione, sessione deterministica e cifratura del server.
///
/// # Errors
///
/// Fallisce se il probe non e univoco, la sessione non e cifrata o il server
/// restituisce valori non rappresentabili.
pub async fn probe_server(
    session: &mut MysqlSession,
    cancellation: &CancellationToken,
) -> Result<MysqlProbe> {
    probe_server_with_profile(session, &crate::profile::MYSQL_PROFILE, cancellation).await
}

/// La probe, con il profilo che decide quale prodotto e accettabile.
///
/// La forma pubblica sopra resta legata a un solo prodotto perche e API: chi
/// la chiama sta chiedendo la probe `MySQL`. Il provider passa invece il
/// proprio profilo, ed e da qui che un secondo prodotto entrera senza
/// toccare la firma esportata.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn probe_server_with_profile(
    session: &mut MysqlSession,
    profile: &dyn crate::profile::ProductProfile,
    cancellation: &CancellationToken,
) -> Result<MysqlProbe> {
    // Verifica l'identita prima delle capability: i nomi delle variabili
    // di sessione dipendono dal prodotto e dalla versione. Una versione non
    // qualificata viene rifiutata prima di interrogare quelle variabili.
    let mut identity = session
        .query_rows(
            "SELECT VERSION() AS product_version, \
             @@version_comment AS version_comment",
            ErrorPhase::Probe,
            cancellation,
        )
        .await?;
    if identity.len() != 1 {
        return Err(mapping_error(format!(
            "probe {} priva di una riga univoca",
            profile.product()
        )));
    }
    let identity = identity.remove(0);
    let product_version: String = required(&identity, "product_version", "product_version")?;
    let version_comment: String = required(&identity, "version_comment", "version_comment")?;

    // Il riconoscimento del prodotto, e la ragione per cui rifiutarlo,
    // stanno nel profilo. Qui resta il punto in cui il rifiuto scatta, e con
    // esso il bypass di test, che non si sposta: e questo il punto che
    // attraversa, ed e accanto a questo che va letto.
    if !mariadb_rejection_bypassed() {
        if let Some(rejection) =
            profile.foreign_product_rejection(&product_version, &version_comment)
        {
            return Err(rejection);
        }
    }

    // La verifica della versione resta attiva anche con il bypass di test
    // del riconoscimento del prodotto.
    if let Some(rejection) =
        crate::profile::unqualified_version_rejection(profile, &product_version)
    {
        return Err(rejection);
    }

    // Solo ora le variabili di sessione: da qui in poi la versione e nota e
    // qualificata, quindi un 1193 sarebbe una divergenza vera e non
    // l'ombra di una versione che nessuno ha misurato.
    let mut rows = session
        .query_rows(
            &format!(
                "SELECT DATABASE() AS database_name, \
                 @@lower_case_table_names AS lower_case_table_names, \
                 @@sql_mode AS sql_mode, @@time_zone AS time_zone, \
                 {} AS transaction_isolation",
                profile.session_isolation_variable()
            ),
            ErrorPhase::Probe,
            cancellation,
        )
        .await?;
    if rows.len() != 1 {
        return Err(mapping_error(format!(
            "sessione {} priva di una riga univoca",
            profile.product()
        )));
    }
    let row = rows.remove(0);
    let tls_rows = session
        .query_rows(
            "SHOW STATUS LIKE 'Ssl_cipher'",
            ErrorPhase::Probe,
            cancellation,
        )
        .await?;
    let tls_cipher: String = tls_rows
        .first()
        .map(|value| required::<String, _>(value, 1, "Ssl_cipher"))
        .transpose()?
        .unwrap_or_default();
    if tls_cipher.is_empty() {
        return Err(DatabaseError::new(
            ErrorCategory::Authentication,
            ErrorPhase::Probe,
            Some(crate::profile::PROVISIONAL_KIND),
            format!(
                "connessione {} priva di cifratura TLS negoziata",
                profile.product()
            ),
        ));
    }
    Ok(MysqlProbe {
        product_version,
        database: required(&row, "database_name", "database_name")?,
        version_comment,
        lower_case_table_names: required(&row, "lower_case_table_names", "lower_case_table_names")?,
        sql_mode: required(&row, "sql_mode", "sql_mode")?,
        time_zone: required(&row, "time_zone", "time_zone")?,
        transaction_isolation: required(&row, "transaction_isolation", "transaction_isolation")?,
        tls_cipher,
    })
}

/// Elenca gli schemi visibili alla connessione corrente.
///
/// # Errors
///
/// Propaga errori di protocollo, autorizzazione, timeout e cancellazione.
pub async fn list_schemas(
    session: &mut MysqlSession,
    cancellation: &CancellationToken,
) -> Result<Vec<String>> {
    list_schemas_with_profile(session, &crate::profile::MYSQL_PROFILE, cancellation).await
}

/// Gli schemi, con il profilo che decide come interrogarli.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn list_schemas_with_profile(
    session: &mut MysqlSession,
    profile: &dyn crate::profile::ProductProfile,
    cancellation: &CancellationToken,
) -> Result<Vec<String>> {
    // Il catalogo pubblico esclude gli schemi di sistema. Un consumer che ne
    // ha bisogno deve interrogare `information_schema.schemata` direttamente.
    session
        .query_rows(profile.schemas_query(), ErrorPhase::Probe, cancellation)
        .await?
        .iter()
        .map(|row| required(row, "schema_name", "schema_name"))
        .collect()
}

/// Elenca tabelle e viste di uno schema usando parametri preparati.
///
/// # Errors
///
/// Propaga errori di protocollo, autorizzazione, timeout e cancellazione.
pub async fn list_objects(
    session: &mut MysqlSession,
    schema: &str,
    cancellation: &CancellationToken,
) -> Result<Vec<MysqlObjectSummary>> {
    list_objects_with_profile(
        session,
        schema,
        &crate::profile::MYSQL_PROFILE,
        cancellation,
    )
    .await
}

/// Gli oggetti di uno schema, con il profilo che decide come interrogarli.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn list_objects_with_profile(
    session: &mut MysqlSession,
    schema: &str,
    profile: &dyn crate::profile::ProductProfile,
    cancellation: &CancellationToken,
) -> Result<Vec<MysqlObjectSummary>> {
    session
        .exec_rows(
            profile.objects_query(),
            Params::Positional(vec![Value::from(schema)]),
            ErrorPhase::Probe,
            cancellation,
        )
        .await?
        .iter()
        .map(|row| {
            Ok(MysqlObjectSummary {
                schema: required(row, "table_schema", "table_schema")?,
                name: required(row, "table_name", "table_name")?,
                kind: required(row, "table_type", "table_type")?,
                engine: optional(row, "engine", "engine")?,
            })
        })
        .collect()
}

/// Descrive un oggetto e produce un token stabile del suo schema.
///
/// # Errors
///
/// Fallisce se l'oggetto non esiste, e ambiguo o contiene metadati non
/// rappresentabili.
pub async fn describe_object(
    session: &mut MysqlSession,
    schema: &str,
    name: &str,
    cancellation: &CancellationToken,
) -> Result<MysqlObjectDescription> {
    describe_object_with_profile(
        session,
        schema,
        name,
        &crate::profile::MYSQL_PROFILE,
        cancellation,
    )
    .await
}

/// La descrizione di un oggetto, con il profilo che decide come interrogarlo.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn describe_object_with_profile(
    session: &mut MysqlSession,
    schema: &str,
    name: &str,
    profile: &dyn crate::profile::ProductProfile,
    cancellation: &CancellationToken,
) -> Result<MysqlObjectDescription> {
    let product = profile.product();
    let objects = session
        .exec_rows(
            profile.object_query(),
            Params::Positional(vec![Value::from(schema), Value::from(name)]),
            ErrorPhase::Probe,
            cancellation,
        )
        .await?;
    if objects.len() != 1 {
        return Err(not_found(format!(
            "oggetto {product} non trovato o ambiguo"
        )));
    }
    let object = &objects[0];
    let column_rows = session
        .exec_rows(
            profile.object_columns_query(),
            Params::Positional(vec![Value::from(schema), Value::from(name)]),
            ErrorPhase::Probe,
            cancellation,
        )
        .await?;
    if column_rows.is_empty() {
        return Err(mapping_error(format!(
            "oggetto {product} senza colonne osservabili"
        )));
    }
    let columns = column_rows
        .iter()
        .map(|row| {
            let nullable: String = required(row, "is_nullable", "is_nullable")?;
            Ok(MysqlColumn {
                name: required(row, "column_name", "column_name")?,
                ordinal: required(row, "ordinal_position", "ordinal_position")?,
                data_type: required(row, "data_type", "data_type")?,
                native_declaration: required(row, "column_type", "column_type")?,
                nullable: nullable == "YES",
                default_expression: optional(row, "column_default", "column_default")?,
                character_set: optional(row, "character_set_name", "character_set_name")?,
                collation: optional(row, "collation_name", "collation_name")?,
                numeric_precision: optional(row, "numeric_precision", "numeric_precision")?,
                numeric_scale: optional(row, "numeric_scale", "numeric_scale")?,
                datetime_precision: optional(row, "datetime_precision", "datetime_precision")?,
                spatial_srid: optional(row, "srs_id", "srs_id")?,
                extra: required(row, "extra", "extra")?,
                generation_expression: required(
                    row,
                    "generation_expression",
                    "generation_expression",
                )?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    // Indici: PRIMARY/UNIQUE/non-unique con le loro colonne. Ordinati per
    // (INDEX_NAME, SEQ_IN_INDEX) così che le parti di ogni indice siano
    // contigue e in ordine. `EXPRESSION` (MySQL 8.0+) è non-NULL per le
    // parti funzionali: in quel caso COLUMN_NAME è NULL e l'indice non è
    // confrontabile per colonne.
    let index_rows = session
        .exec_rows(
            profile.object_indexes_query(),
            Params::Positional(vec![Value::from(schema), Value::from(name)]),
            ErrorPhase::Probe,
            cancellation,
        )
        .await?;
    let indexes = build_indexes(&index_rows, profile)?;
    let schema_name: String = required(object, "table_schema", "table_schema")?;
    let object_name: String = required(object, "table_name", "table_name")?;
    let kind: String = required(object, "table_type", "table_type")?;
    let engine = optional(object, "engine", "engine")?;
    let token = schema_token(
        &schema_name,
        &object_name,
        &kind,
        engine.as_deref(),
        &columns,
        &indexes,
    )?;
    Ok(MysqlObjectDescription {
        schema: schema_name,
        name: object_name,
        kind,
        engine,
        columns,
        indexes,
        token,
    })
}

/// Aggrega le righe di `information_schema.statistics` (una per parte di
/// indice) in una lista di `MysqlIndex`. Le righe arrivano già ordinate per
/// `(INDEX_NAME, SEQ_IN_INDEX)`.
fn build_indexes(
    rows: &[Row],
    profile: &dyn crate::profile::ProductProfile,
) -> Result<Vec<MysqlIndex>> {
    let product = profile.product();
    let mut indexes: Vec<MysqlIndex> = Vec::new();
    for row in rows {
        let name: String = required(row, "index_name", "index_name")?;
        let non_unique: i64 = required(row, "non_unique", "non_unique")?;
        let column: Option<String> = optional(row, "column_name", "column_name")?;
        let expression: Option<String> = optional(row, "expression", "expression")?;
        // Nuovo indice se il nome cambia rispetto all'ultimo accumulato.
        if indexes.last().map(|last| last.name.as_str()) != Some(name.as_str()) {
            indexes.push(MysqlIndex {
                name,
                unique: non_unique == 0,
                column_backed: true,
                columns: Vec::new(),
            });
        }
        let current = indexes
            .last_mut()
            .ok_or_else(|| mapping_error(format!("aggregazione indici {product} incoerente")))?;
        match column {
            Some(column_name) => current.columns.push(column_name),
            // Parte funzionale (espressione non nulla, colonna nulla):
            // l'indice non è più confrontabile per colonne. Che il prodotto
            // pubblichi o meno queste parti lo dice il profilo; che una parte
            // senza colonna né espressione sia un errore no, perché sarebbe
            // un indice di cui non sappiamo dire nulla.
            None if expression.is_some() && profile.reports_functional_index_parts() => {
                current.column_backed = false;
            }
            None => {
                return Err(mapping_error(format!(
                    "parte di indice {product} senza colonna né espressione"
                )))
            }
        }
    }
    Ok(indexes)
}

fn schema_token(
    schema: &str,
    name: &str,
    kind: &str,
    engine: Option<&str>,
    columns: &[MysqlColumn],
    indexes: &[MysqlIndex],
) -> Result<MysqlSchemaToken> {
    // Indici inclusi nel token: una modifica agli indici (es. drop del PK,
    // aggiunta di un unique index) fra prepare ed esecuzione deve cambiare
    // il token e non passare inosservata al preflight Upsert.
    let encoded = plenora_database_core::fingerprint::canonical_json_sha256(&(
        schema, name, kind, engine, columns, indexes,
    ))
    .map_err(|_| mapping_error("serializzazione token schema fallita"))?;
    Ok(MysqlSchemaToken(encoded))
}

fn required<T, I>(row: &Row, index: I, field: &str) -> Result<T>
where
    T: FromValue,
    I: mysql_async::prelude::ColumnIndex,
{
    match row.get_opt::<T, _>(index) {
        Some(Ok(value)) => Ok(value),
        Some(Err(_)) => Err(mapping_error(format!(
            "campo catalogo {field} non convertibile"
        ))),
        None => Err(mapping_error(format!("campo catalogo {field} assente"))),
    }
}

fn optional<T, I>(row: &Row, index: I, field: &str) -> Result<Option<T>>
where
    T: FromValue,
    I: mysql_async::prelude::ColumnIndex,
{
    match row.get_opt::<Option<T>, _>(index) {
        Some(Ok(value)) => Ok(value),
        Some(Err(_)) => Err(mapping_error(format!(
            "campo catalogo {field} non convertibile"
        ))),
        None => Err(mapping_error(format!("campo catalogo {field} assente"))),
    }
}

/// Verifica che lo SRID `srid`, derivato da `EPSG:<srid>` (GEO-008), sia nel
/// catalogo del server come quell'identita: `ORGANIZATION = 'EPSG'` e
/// `ORGANIZATION_COORDSYS_ID = srid`.
///
/// Un SRS definito dall'utente con lo stesso numero e un altro CRS, e uno
/// SRID assente farebbe fallire la DDL o la scrittura con un errore che non
/// dice `crs`: si rifiuta qui, prima di ogni effetto.
///
/// # Errors
///
/// `Crs` per uno SRID assente o registrato con un'altra identita.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn verify_epsg_srid(
    session: &mut MysqlSession,
    srid: u32,
    product: &str,
    cancellation: &CancellationToken,
) -> Result<()> {
    let rows = session
        .exec_rows(
            "SELECT ORGANIZATION AS organization, \
                    ORGANIZATION_COORDSYS_ID AS organization_coordsys_id \
             FROM INFORMATION_SCHEMA.ST_SPATIAL_REFERENCE_SYSTEMS WHERE SRS_ID = ?",
            Params::Positional(vec![Value::from(srid)]),
            ErrorPhase::Probe,
            cancellation,
        )
        .await?;
    let registered = match rows.as_slice() {
        [row] => {
            let organization: Option<String> = optional(row, "organization", "organization")?;
            let code: Option<u32> =
                optional(row, "organization_coordsys_id", "organization_coordsys_id")?;
            organization.is_some_and(|name| name.eq_ignore_ascii_case("EPSG")) && code == Some(srid)
        }
        _ => false,
    };
    if registered {
        Ok(())
    } else {
        Err(DatabaseError::new(
            ErrorCategory::Crs,
            ErrorPhase::Probe,
            Some(crate::profile::PROVISIONAL_KIND),
            format!(
                "SRID derivato da crs_id EPSG assente dal catalogo {product} o registrato \
                 con un'altra identita"
            ),
        ))
    }
}

/// Le colonne geometriche di `schema.object` il cui SRID il catalogo
/// **dichiara** senza che il server lo vincoli, confrontate con quello che la
/// scrittura scrive in ciascuna (`expected`: nome della colonna, SRID).
///
/// Serve dove il profilo la chiede
/// (`ProductProfile::declared_column_srids_query`): su `MariaDB`
/// `REF_SYSTEM_ID=4326` si dichiara nella DDL, il registro OGC lo pubblica
/// (`GEOMETRY_COLUMNS.SRID`), ma un `INSERT` di un valore 3857 nella colonna
/// e accettato (misurato su 11.4 il 25/10). La regola delle sonde vuole che
/// un adapter il cui server non vincola lo SRID della colonna lo confronti
/// lui, prima della scrittura, come la scrittura `PostgreSQL` con il typmod
/// (GEO-009). Uno SRID 0 e una colonna non vincolata: niente da confrontare.
///
/// # Errors
///
/// `crs` per una colonna dichiarata in un altro SRID; gli errori del catalogo.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn verify_declared_column_srids(
    session: &mut MysqlSession,
    profile: &dyn crate::profile::ProductProfile,
    schema: &str,
    object: &str,
    expected: &[(String, u32)],
    cancellation: &CancellationToken,
) -> Result<()> {
    let Some(query) = profile.declared_column_srids_query() else {
        return Ok(());
    };
    if expected.is_empty() {
        return Ok(());
    }
    let rows = session
        .exec_rows(
            query,
            Params::Positional(vec![Value::from(schema), Value::from(object)]),
            ErrorPhase::Prepare,
            cancellation,
        )
        .await?;
    for row in &rows {
        let column: String = required(row, "column_name", "column_name")?;
        let declared: Option<u32> = optional(row, "srid", "srid")?;
        let Some(declared) = declared.filter(|srid| *srid != 0) else {
            continue;
        };
        if expected
            .iter()
            .any(|(name, srid)| *name == column && *srid != declared)
        {
            return Err(DatabaseError::new(
                ErrorCategory::Crs,
                ErrorPhase::Prepare,
                Some(profile.kind()),
                format!(
                    "SRID target {} incompatibile col contratto Arrow",
                    profile.product()
                ),
            ));
        }
    }
    Ok(())
}

/// Le colonne delle relazioni `relations` (schema, oggetto; lo schema
/// assente e `database`), nell'ordine della tabella e con il flag
/// geometrico: la regola degli operandi geometrici (`spatial_operand`) le
/// giudica a mondo chiuso prima di preparare una query.
///
/// Si leggono con `SHOW COLUMNS`, che risolve il nome come lo risolve la
/// query: una tabella `TEMPORARY` della sessione che ne nasconde una di base
/// omonima, e le maiuscole secondo `lower_case_table_names`. Fino al 25/10 si
/// leggeva `INFORMATION_SCHEMA.COLUMNS`, che elenca la tabella di base: con
/// `t(g INT)` e una `TEMPORARY t(g POINT)`, `ORDER BY t.g` si giudicava sulla
/// colonna intera e il server ordinava la geometria. Una tabella assente
/// fallisce come fallirebbe la query.
///
/// # Errors
///
/// Gli errori del catalogo.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn relation_columns(
    session: &mut MysqlSession,
    profile: &dyn crate::profile::ProductProfile,
    database: &str,
    relations: &[(Option<String>, String)],
    cancellation: &CancellationToken,
) -> Result<plenora_database_core::spatial_operand::OperandCatalog> {
    use plenora_database_core::spatial_operand::OperandCatalog;
    let mut catalog = OperandCatalog::new(profile.kind());
    for (schema, object) in relations {
        let wanted_schema = schema.as_deref().unwrap_or(database);
        let columns = show_columns(session, profile, wanted_schema, object, cancellation).await?;
        catalog.insert(schema.clone(), object.clone(), columns);
    }
    Ok(catalog)
}

/// Il catalogo di [`relation_columns`] dentro una transazione: il database
/// di default e quello della connessione (`DATABASE()`), letto soltanto se
/// una relazione non nomina lo schema. Senza database di default una
/// relazione senza schema non si conosce, e nessun suo riferimento si
/// risolve. E la lettura di `TransactionScope::operand_catalog`.
///
/// # Errors
///
/// Gli errori del catalogo.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn transaction_operand_catalog(
    session: &mut MysqlSession,
    relations: &[(Option<String>, String)],
    cancellation: &CancellationToken,
) -> Result<plenora_database_core::spatial_operand::OperandCatalog> {
    use plenora_database_core::spatial_operand::{OperandCatalog, RelationColumns};
    let profile = session.profile();
    let database = if relations.iter().any(|(schema, _)| schema.is_none()) {
        let rows = session
            .query_rows("SELECT DATABASE()", ErrorPhase::Prepare, cancellation)
            .await?;
        match rows.first() {
            Some(row) => optional::<String, _>(row, 0, "DATABASE()")?,
            None => None,
        }
    } else {
        None
    };
    let Some(database) = database else {
        let qualified = relations
            .iter()
            .filter(|(schema, _)| schema.is_some())
            .cloned()
            .collect::<Vec<_>>();
        let mut catalog = if qualified.is_empty() {
            OperandCatalog::new(profile.kind())
        } else {
            relation_columns(session, profile, "", &qualified, cancellation).await?
        };
        for (schema, object) in relations.iter().filter(|(schema, _)| schema.is_none()) {
            catalog.insert(schema.clone(), object.clone(), RelationColumns::Unknown);
        }
        return Ok(catalog);
    };
    relation_columns(session, profile, &database, relations, cancellation).await
}

/// I tipi delle colonne di una relazione, con precisione, scala, lunghezza e
/// precisione temporale (`information_schema.COLUMNS`), per l'ammissione dei
/// letterali portable (`plenora_database_core::operand_admission`). `None`
/// per una relazione che il catalogo non conosce.
///
/// # Errors
///
/// Gli errori del catalogo in fase `prepare`.
#[allow(clippy::redundant_pub_crate)]
pub(crate) async fn transaction_column_types(
    session: &mut MysqlSession,
    relation: &(Option<String>, String),
    cancellation: &CancellationToken,
) -> Result<Option<plenora_database_core::operand_admission::RelationTypes>> {
    use plenora_database_core::operand_admission::{CatalogType, Charset};
    let rows = session
        .exec_rows(
            session.profile().column_types_query(),
            mysql_async::Params::Positional(vec![
                relation
                    .0
                    .as_ref()
                    .map_or(mysql_async::Value::NULL, |schema| {
                        mysql_async::Value::Bytes(schema.as_bytes().to_vec())
                    }),
                mysql_async::Value::Bytes(relation.1.as_bytes().to_vec()),
            ]),
            ErrorPhase::Prepare,
            cancellation,
        )
        .await?;
    if rows.is_empty() {
        return Ok(None);
    }
    rows.iter()
        .map(|row| {
            let name = optional::<String, _>(row, 0, "COLUMN_NAME")?
                .ok_or_else(|| mapping_error("campo catalogo COLUMN_NAME nullo".to_owned()))?;
            let catalog = CatalogType {
                name: optional::<String, _>(row, 1, "DATA_TYPE")?.unwrap_or_default(),
                declaration: optional::<String, _>(row, 2, "COLUMN_TYPE")?,
                length: optional::<u64, _>(row, 3, "CHARACTER_MAXIMUM_LENGTH")?,
                precision: optional::<u64, _>(row, 4, "NUMERIC_PRECISION")?,
                scale: optional::<i64, _>(row, 5, "NUMERIC_SCALE")?,
                datetime_precision: optional::<u64, _>(row, 6, "DATETIME_PRECISION")?,
                // La lunghezza in caratteri; il charset della colonna decide
                // quali caratteri conserva (`utf8mb3` soltanto il piano di
                // base, `latin1` e Windows-1252).
                length_unit: plenora_database_core::operand_admission::LengthUnit::Characters,
                charset: match optional::<String, _>(row, 7, "CHARACTER_SET_NAME")?.as_deref() {
                    None | Some("utf8mb4") => Charset::Unicode,
                    Some("utf8mb3" | "utf8") => Charset::UnicodeBmp,
                    Some("latin1") => Charset::Windows1252,
                    Some(_) => Charset::Ascii,
                },
            };
            Ok((name, catalog))
        })
        .collect::<Result<_>>()
        .map(Some)
}

/// I tipi binari di `SHOW COLUMNS` (il nome prima della lunghezza): un
/// `CHAR` o `VARCHAR` con `CHARACTER SET binary` si descrive `binary` o
/// `varbinary`.
const BINARY_TYPES: [&str; 6] = [
    "binary",
    "varbinary",
    "tinyblob",
    "blob",
    "mediumblob",
    "longblob",
];

/// Il tipo (il nome prima della lunghezza, senza maiuscole) e binario.
#[allow(clippy::redundant_pub_crate)]
pub(crate) fn is_binary_type(data_type: &str) -> bool {
    let base = data_type
        .split(|character: char| !character.is_ascii_alphabetic())
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    BINARY_TYPES.contains(&base.as_str())
}

/// Una vista con quel nome, senza maiuscole (`lower_case_table_names` decide
/// se il server le distingue; senza, il giudizio e piu severo, non meno).
const VIEW_SQL: &str = "SELECT 1 FROM INFORMATION_SCHEMA.TABLES \
    WHERE LOWER(TABLE_SCHEMA) = LOWER(?) AND LOWER(TABLE_NAME) = LOWER(?) \
    AND TABLE_TYPE IN ('VIEW', 'SYSTEM VIEW')";

/// `SHOW COLUMNS` di una relazione, risolta come la risolve la query.
async fn show_columns(
    session: &mut MysqlSession,
    profile: &dyn crate::profile::ProductProfile,
    schema: &str,
    object: &str,
    cancellation: &CancellationToken,
) -> Result<plenora_database_core::spatial_operand::RelationColumns> {
    use plenora_database_core::identifier::{quote_identifier, IdentifierDialect};
    use plenora_database_core::spatial_operand::{
        catalog_column_is_geometric, ColumnCarrier, RelationOrigin,
    };
    let sql = format!(
        "SHOW COLUMNS FROM {}.{}",
        quote_identifier(IdentifierDialect::Mysql, schema)?,
        quote_identifier(IdentifierDialect::Mysql, object)?
    );
    let rows = session
        .query_rows(&sql, ErrorPhase::Prepare, cancellation)
        .await?;
    // La regola B (`spatial_operand::catalog_column_is_geometric`): una vista
    // del nome, anche soltanto senza maiuscole. `SHOW COLUMNS` legge una
    // tabella `TEMPORARY` che ne nasconde una omonima, che `TABLES` non
    // elenca: una vista omonima la fa giudicare derivata, un rifiuto in piu.
    let origin = if rows.is_empty() {
        RelationOrigin::Base
    } else {
        let views = session
            .exec_rows(
                VIEW_SQL,
                Params::Positional(vec![Value::from(schema), Value::from(object)]),
                ErrorPhase::Prepare,
                cancellation,
            )
            .await?;
        if views.is_empty() {
            RelationOrigin::Base
        } else {
            RelationOrigin::Derived
        }
    };
    let mut columns = Vec::with_capacity(rows.len());
    for row in &rows {
        let column: String = required(row, 0, "Field")?;
        let declared: String = required(row, 1, "Type")?;
        let base = declared
            .split(|character: char| !character.is_ascii_alphabetic())
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let carrier = if profile.is_spatial_native_type(&base) {
            ColumnCarrier::Geometry
        } else if is_binary_type(&base) {
            ColumnCarrier::Binary
        } else {
            ColumnCarrier::Scalar
        };
        columns.push((column, catalog_column_is_geometric(origin, carrier)));
    }
    Ok(if columns.is_empty() {
        plenora_database_core::spatial_operand::RelationColumns::Absent
    } else {
        plenora_database_core::spatial_operand::RelationColumns::Known(columns)
    })
}

fn mapping_error(message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Probe,
        Some(crate::profile::PROVISIONAL_KIND),
        message,
    )
}

fn not_found(message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::NotFound,
        ErrorPhase::Probe,
        Some(crate::profile::PROVISIONAL_KIND),
        message,
    )
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
