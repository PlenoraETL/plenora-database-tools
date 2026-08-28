//! Fasi offline `parse -> validate -> fingerprint -> prepare` + helper di
//! orchestrazione runtime (retry, ...).
//!
//! L'esecuzione remota resta negli adapter provider, senza `match` sui
//! provider nell'executor.

pub mod engine;
pub mod result;
pub mod retry;
pub mod runtime;
pub use engine::{Engine, EngineStatistics, Session, SessionRowStream, SessionTransaction};
pub use result::QueryResult;
pub use retry::{retry_with_policy, RetryPolicy};
pub use runtime::{
    inspect_spatial_arrays, validate_prepared_budget, ContractLeases, DeadlineGuard,
    ReadBatchReservation, WriteResourceReservation,
};

use plenora_database_core::capabilities::{ProviderCapabilities, TransactionScope};
use plenora_database_core::plan::{
    FilterExpression, ObjectRef, Operation, Plan, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::relational::SpatialFunction;
use plenora_database_core::{DatabaseError, Result};
use sha2::{Digest, Sha256};

const HEX: &[u8; 16] = b"0123456789abcdef";

#[derive(Debug, Clone)]
pub struct ValidatedPlan {
    plan: Plan,
    fingerprint: String,
}

impl ValidatedPlan {
    #[must_use]
    pub const fn plan(&self) -> &Plan {
        &self.plan
    }

    #[must_use]
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

#[derive(Debug, Clone)]
pub struct PreparedPlan {
    validated: ValidatedPlan,
    capabilities: ProviderCapabilities,
}

impl PreparedPlan {
    #[must_use]
    pub const fn validated(&self) -> &ValidatedPlan {
        &self.validated
    }

    #[must_use]
    pub const fn capabilities(&self) -> &ProviderCapabilities {
        &self.capabilities
    }
}

/// Deserializza e valida un piano entro il limite hard di parsing.
///
/// # Errors
///
/// Restituisce un errore redatto se il JSON è invalido, troppo grande oppure
/// viola un'invariante del contratto.
pub fn parse_and_validate(input: &[u8]) -> Result<ValidatedPlan> {
    let hard_limit = plenora_database_core::limits::Limits::default().max_plan_json_bytes;
    if input.len() > hard_limit {
        return Err(DatabaseError::invalid_plan(
            "piano oltre il limite di parsing",
        ));
    }
    let plan: Plan = serde_json::from_slice(input)
        .map_err(|_| DatabaseError::invalid_plan("JSON piano non valido"))?;
    validate(plan)
}

/// Valida un piano già deserializzato e ne calcola il fingerprint.
///
/// # Errors
///
/// Restituisce `InvalidPlan` per versioni, limiti, riferimenti, operazioni o
/// profili incompatibili.
pub fn validate(plan: Plan) -> Result<ValidatedPlan> {
    if plan.schema_version != 2 {
        return Err(DatabaseError::invalid_plan(
            "schema_version del piano non supportata",
        ));
    }
    plan.limits.validate()?;
    validate_connection_ref(&plan.connection_ref)?;
    enforce_connection_reference_policy(&plan.connection_ref)?;
    validate_operation(&plan)?;
    let canonical = serde_json::to_vec(&plan)
        .map_err(|_| DatabaseError::invalid_plan("piano non serializzabile"))?;
    let digest = Sha256::digest(canonical);
    let mut fingerprint = String::with_capacity(digest.len() * 2);
    for byte in digest {
        fingerprint.push(char::from(HEX[usize::from(byte >> 4)]));
        fingerprint.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(ValidatedPlan { plan, fingerprint })
}

/// Una dichiarazione di CRS vale dove serve, e **solo** li.
///
/// I provider il cui catalogo l'SRID lo sa — `PostgreSQL` da
/// `geometry_columns`, `MySQL` da `information_schema` — pubblicano
/// `requires_declared_crs = false`, e accettare li una dichiarazione vorrebbe
/// dire tenere due fonti per lo stesso fatto: quando divergono, nessuna delle
/// due e piu quella giusta.
///
/// Il rifiuto sta nell'engine e non nei provider perche riguarda la coerenza
/// fra il piano e cio che il provider pubblica, che e la domanda che questo
/// modulo decide. Un provider potrebbe ignorare il campo in silenzio, e il
/// chiamante crederebbe di aver dichiarato qualcosa.
fn validate_declared_crs(
    capabilities: &ProviderCapabilities,
    read: &plenora_database_core::plan::ReadOperation,
) -> Result<()> {
    if read.declared_crs.is_empty() {
        return Ok(());
    }
    require(
        capabilities,
        capabilities.spatial.requires_declared_crs,
        "declared_crs",
    )?;
    let mut seen = std::collections::BTreeSet::new();
    for declaration in &read.declared_crs {
        // Zero e l'«indefinito» OGC, cioe cio che il registro di MariaDB
        // risponde da solo. Dichiararlo non aggiunge nulla a quel che il
        // catalogo dice, e da l'aria di averlo fatto.
        if declaration.srid == 0 {
            return Err(DatabaseError::invalid_plan(
                "declared_crs non ammette SRID 0: e l'indefinito OGC, cioe cio che il catalogo dice gia da solo",
            ));
        }
        if !seen.insert(declaration.column.as_str()) {
            return Err(DatabaseError::invalid_plan(
                "declared_crs dichiara due volte la stessa colonna",
            ));
        }
    }
    Ok(())
}

/// Verifica che le capability runtime possano eseguire il piano.
///
/// # Che cosa questo confronto e, e che cosa non e
///
/// Confronta il piano con il **documento capability**, e nient'altro. E' una
/// condizione necessaria, non sufficiente: un provider puo rifiutare in
/// prepare-time un piano che qui passa, perche applica regole di lifecycle che
/// il contratto v2 non sa esprimere — `MySQL` accetta il solo profilo
/// `single_transaction`, `SQL Server` ammette `staged_swap` soltanto per
/// `Replace`. Rappresentarle qui richiederebbe campi che il contratto non ha,
/// cioe una nuova major.
///
/// Un esito positivo va quindi letto come "nessuna capability pubblicizzata
/// contraddice il piano", non come "il provider lo eseguira".
///
/// # Errors
///
/// Restituisce `InvalidPlan` per un documento capability di un'altra major,
/// senza `provider_version` o riferito a un altro provider; `Unsupported`
/// quando una capability che il piano usa non e pubblicizzata.
pub fn prepare(
    validated: ValidatedPlan,
    capabilities: ProviderCapabilities,
) -> Result<PreparedPlan> {
    // La major del documento si verifica prima di leggerne il contenuto: un
    // documento di un'altra versione ha campi con lo stesso nome e un altro
    // significato, e prepararci sopra un piano vorrebbe dire confrontarlo con
    // promesse che nessuno ha fatto.
    // Validazione completa, non due controlli scelti a mano: major,
    // lunghezze del contratto, duplicati, limiti a zero e combinazioni
    // contraddittorie. Vive in `plenora-database-core` perche sia una sola,
    // condivisa con il testkit.
    capabilities.validate()?;
    if validated.plan.provider != capabilities.provider {
        return Err(DatabaseError::invalid_plan(
            "capability riferite a un provider diverso",
        ));
    }
    match &validated.plan.operation {
        Operation::DatabaseRead { read } => {
            require(
                &capabilities,
                capabilities.reads.streaming,
                "read streaming",
            )?;
            if !read.projection.is_empty() {
                require(&capabilities, capabilities.reads.projection, "projection")?;
            }
            if !read.order_by.is_empty() {
                require(&capabilities, capabilities.reads.ordering, "order_by")?;
            }
            // `row_limit` non e legato a `reads.pagination`, e non e una
            // dimenticanza: un tetto non e una finestra. I provider che
            // pubblicano `pagination = false` rendono comunque il limite, e
            // legarli qui rifiuterebbe piani legittimi che i loro test live
            // usano da sempre.
            //
            // `row_offset` si, ed e cio che rende quella bandiera qualcosa
            // invece di una dichiarazione. Il campo e nuovo, quindi nessun
            // piano esistente porta un offset e nessuno viene rifiutato da
            // questa riga per la prima volta.
            if read.row_offset.is_some() {
                require(&capabilities, capabilities.reads.pagination, "row_offset")?;
                // La finestra pretende un ordinamento, e la regola sta qui e
                // non nei provider perche riguarda il **piano**: un offset su
                // un risultato non ordinato non e riproducibile su nessun
                // motore, e due letture consecutive possono rendere righe
                // diverse. `MySQL` la applicava gia per il tetto, PostgreSQL
                // no: metterla qui la rende una sola.
                if read.order_by.is_empty() {
                    return Err(DatabaseError::invalid_plan(
                        "row_offset richiede order_by: una finestra su un risultato non \
                         ordinato non e riproducibile",
                    ));
                }
            }
            validate_declared_crs(&capabilities, read)?;
            if let Some(filter) = &read.filter {
                require(&capabilities, capabilities.reads.filter, "filter")?;
                let mut used = Vec::new();
                collect_spatial_functions(filter, &mut used);
                for function in used {
                    if !capabilities.spatial.functions.contains(&function) {
                        return Err(DatabaseError::unsupported(
                            capabilities.provider,
                            plenora_database_core::ErrorPhase::Prepare,
                            "il piano usa una funzione spatial che il provider non pubblicizza",
                        ));
                    }
                }
            }
        }
        Operation::DatabaseWrite { write } => {
            // Prima le contraddizioni interne, poi le capability: un piano
            // incoerente deve produrre `InvalidPlan` indipendentemente da cio
            // che il provider pubblica.
            reject_contradictory_write(write)?;
            validate_write_capability(write.mode, &capabilities)?;
            if write.create_spatial_index {
                require(
                    &capabilities,
                    capabilities.spatial.spatial_index,
                    "create_spatial_index",
                )?;
            }
            // `allow_partial = false` chiede che un fallimento non lasci righe
            // a meta. E' esattamente cio che `rollback_on_failure` promette:
            // senza, il piano domanda una garanzia che il provider non ha.
            if !write.allow_partial {
                require(
                    &capabilities,
                    capabilities.writes.rollback_on_failure,
                    "allow_partial=false",
                )?;
            }
            match write.transaction_profile {
                TransactionProfile::SingleTransaction => require(
                    &capabilities,
                    capabilities.transactions.single_transaction,
                    "transaction_profile=single_transaction",
                )?,
                TransactionProfile::StagedSwap => require(
                    &capabilities,
                    capabilities.transactions.staged_swap,
                    "transaction_profile=staged_swap",
                )?,
                TransactionProfile::ChunkCommitted => require(
                    &capabilities,
                    capabilities.transactions.scope != TransactionScope::None,
                    "transaction_profile=chunk_committed",
                )?,
                // `read_only` su una scrittura e una contraddizione, ed e
                // `reject_contradictory_write` a chiuderla: qui non c'e una
                // capability da interrogare. `best_effort_ddl` e la scelta che
                // si fa *quando* il DDL non e transazionale, e nessuna
                // bandiera di v2 puo negarla senza inventarne la semantica.
                TransactionProfile::ReadOnly | TransactionProfile::BestEffortDdl => {}
            }
        }
        // Le operazioni di sola introspezione non hanno una capability
        // dedicata nel contratto v2: ogni provider che esiste le espone.
        Operation::DatabaseTestConnection
        | Operation::DatabaseListCatalogs
        | Operation::DatabaseListSchemas { .. }
        | Operation::DatabaseListObjects { .. }
        | Operation::DatabaseDescribeObject { .. } => {}
    }
    Ok(PreparedPlan {
        validated,
        capabilities,
    })
}

/// Rifiuta le scritture che si contraddicono da sole.
///
/// Sono incoerenze **interne al piano**, non promesse mancate del provider:
/// nessuna capability puo renderle vere, e nessun provider puo eseguirle come
/// scritte. Chiuderle qui non richiede campi nuovi nel contratto e non
/// introduce un `match` sul provider — cosa che questo crate evita per
/// costruzione.
///
/// Il controllo avviene prima delle capability per impedire che un piano
/// incoerente venga attribuito a un limite del provider.
///
/// # Errors
///
/// `InvalidPlan`, con la contraddizione nominata.
fn reject_contradictory_write(write: &WriteOperation) -> Result<()> {
    // Un profilo di sola lettura su un'operazione di scrittura.
    if write.transaction_profile == TransactionProfile::ReadOnly {
        return Err(DatabaseError::invalid_plan(
            "transaction_profile=read_only su un'operazione di scrittura",
        ));
    }

    // `chunk_committed` **e** una scrittura a pezzi confermati: un
    // fallimento a meta strada lascia i pezzi gia confermati. Chiedere
    // insieme `allow_partial = false` domanda le due cose insieme.
    if write.transaction_profile == TransactionProfile::ChunkCommitted && !write.allow_partial {
        return Err(DatabaseError::invalid_plan(
            "transaction_profile=chunk_committed con allow_partial=false: \
             i commit intermedi sono parziali per costruzione",
        ));
    }

    // Lo staged swap scrive in un oggetto di appoggio e poi lo scambia con il
    // target: sostituisce cio che c'era. Su una mode che aggiunge o modifica
    // righe esistenti non c'e niente da scambiare.
    if write.transaction_profile == TransactionProfile::StagedSwap
        && !matches!(
            write.mode,
            WriteMode::Create | WriteMode::Replace | WriteMode::TruncateInsert
        )
    {
        return Err(DatabaseError::invalid_plan(
            "transaction_profile=staged_swap con una mode che non sostituisce \
             il contenuto del target",
        ));
    }

    // `create_spatial_index` **non** e qui, di proposito. Sembrava una
    // contraddizione — un indice si crea insieme all'oggetto che lo porta — ma
    // e una regola di lifecycle del singolo provider, e i provider non
    // concordano: PostgreSQL lo ammette solo con `Create`, perche `Replace`
    // scrive dentro il target esistente che ha gia i suoi indici; `SQL Server`
    // lo qualifica per `Create` e `Replace`, e ha un test live che esegue
    // `Replace + StagedSwap + create_spatial_index`.
    //
    // Codificarne una versione qui significherebbe scegliere il comportamento di un
    // provider per tutti. Il contratto v2 non ha un
    // campo che esprima quale mode costruisce il target, quindi la regola resta
    // dove e verificabile — nel preflight di ciascun provider.

    Ok(())
}

/// Nega il piano quando la capability che gli serve non e pubblicizzata.
///
/// Il messaggio nomina la richiesta del piano, non il valore del dato: e
/// contesto operativo, e serve a capire *quale* clausola ha fermato la
/// preparazione.
fn require(capabilities: &ProviderCapabilities, granted: bool, requested: &str) -> Result<()> {
    if granted {
        return Ok(());
    }
    Err(DatabaseError::unsupported(
        capabilities.provider,
        plenora_database_core::ErrorPhase::Prepare,
        format!("il piano richiede `{requested}`, che il provider non pubblicizza"),
    ))
}

/// Raccoglie le funzioni spatial citate da un filtro, a qualunque profondita.
fn collect_spatial_functions(filter: &FilterExpression, out: &mut Vec<SpatialFunction>) {
    match filter {
        FilterExpression::And { args } | FilterExpression::Or { args } => {
            for arg in args {
                collect_spatial_functions(arg, out);
            }
        }
        FilterExpression::Spatial { function, .. } if !out.contains(function) => {
            out.push(*function);
        }
        _ => {}
    }
}

/// Cio che `contracts/v2` dichiara su `connection_ref`: stringa non vuota, al
/// piu 256 **caratteri**, senza NUL.
///
/// `maxLength` di JSON Schema conta code point, quindi la validazione usa
/// `chars().count()` e non la lunghezza UTF-8 in byte.
fn validate_connection_ref(connection_ref: &str) -> Result<()> {
    if connection_ref.is_empty() {
        return Err(DatabaseError::invalid_plan("connection_ref vuoto"));
    }
    if connection_ref.chars().count() > 256 {
        return Err(DatabaseError::invalid_plan(
            "connection_ref oltre la lunghezza del contratto",
        ));
    }
    if connection_ref.contains('\0') {
        return Err(DatabaseError::invalid_plan("connection_ref con NUL"));
    }
    Ok(())
}

/// Questo runtime accetta solo riferimenti **indiretti** alla connessione.
///
/// E una restrizione dichiarata, non una lettura del contratto: la v2 ammette
/// qualunque stringa, quindi un piano con una DSN in chiaro e valido e viene
/// rifiutato lo stesso. Il motivo e che il piano viene serializzato per
/// calcolarne l'impronta, finisce nei log di esecuzione e negli artefatti di
/// evidenza: una credenziale scritta dentro sarebbe copiata in tutti.
///
/// Sta separata da [`validate_connection_ref`] perche le due cose non sono la
/// stessa: la prima dice cosa il contratto ammette, questa cosa il prodotto
/// esegue. Tenerle insieme faceva chiamare "piano non valido" un documento che
/// il contratto accetta, e nascondeva il fatto che la regola e nostra. Se
/// dovesse diventare normativa, il posto e `contracts/v3/`.
fn enforce_connection_reference_policy(connection_ref: &str) -> Result<()> {
    let indirect = ["env:", "secret:", "connection:"]
        .iter()
        .any(|prefix| connection_ref.starts_with(prefix));
    if indirect {
        return Ok(());
    }
    Err(DatabaseError::invalid_plan(
        "questo runtime esegue solo connection_ref indiretti: env:, secret: o connection:",
    ))
}

fn validate_operation(plan: &Plan) -> Result<()> {
    match &plan.operation {
        Operation::DatabaseDescribeObject { source } => {
            validate_object(source, &plan.limits)?;
        }
        Operation::DatabaseListSchemas { source } | Operation::DatabaseListObjects { source } => {
            if let Some(source) = source {
                validate_object(source, &plan.limits)?;
            }
        }
        Operation::DatabaseRead { read } => {
            validate_object(&read.source, &plan.limits)?;
            for field in &read.projection {
                validate_identifier(field, plan.limits.max_identifier_bytes)?;
            }
            for order in &read.order_by {
                validate_identifier(&order.field, plan.limits.max_identifier_bytes)?;
            }
            if let Some(filter) = &read.filter {
                let mut nodes = 0;
                validate_filter(filter, 1, &mut nodes, &plan.limits)?;
            }
        }
        Operation::DatabaseWrite { write } => {
            validate_write(write, plan)?;
        }
        Operation::DatabaseTestConnection | Operation::DatabaseListCatalogs => {}
    }
    Ok(())
}

fn validate_write(write: &WriteOperation, plan: &Plan) -> Result<()> {
    validate_object(&write.target, &plan.limits)?;
    for key in &write.keys {
        validate_identifier(key, plan.limits.max_identifier_bytes)?;
    }
    for field in &write.update_columns {
        validate_identifier(field, plan.limits.max_identifier_bytes)?;
    }
    if matches!(
        write.mode,
        WriteMode::Update | WriteMode::Upsert | WriteMode::DeleteByKeys
    ) && write.keys.is_empty()
    {
        return Err(DatabaseError::invalid_plan(
            "la modalità write richiede almeno una chiave",
        ));
    }
    if write.mode == WriteMode::Update && write.update_columns.is_empty() {
        return Err(DatabaseError::invalid_plan(
            "update richiede update_columns",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn validate_filter(
    expression: &FilterExpression,
    depth: usize,
    nodes: &mut usize,
    limits: &plenora_database_core::limits::Limits,
) -> Result<()> {
    *nodes += 1;
    if depth > limits.max_filter_depth || *nodes > limits.max_filter_nodes {
        return Err(DatabaseError::invalid_plan(
            "filtro oltre i limiti di profondità o nodi",
        ));
    }
    match expression {
        FilterExpression::And { args } | FilterExpression::Or { args } => {
            if args.is_empty() {
                return Err(DatabaseError::invalid_plan("gruppo filtro vuoto"));
            }
            for arg in args {
                validate_filter(arg, depth + 1, nodes, limits)?;
            }
        }
        FilterExpression::Eq { field, parameter }
        | FilterExpression::Ne { field, parameter }
        | FilterExpression::Lt { field, parameter }
        | FilterExpression::Lte { field, parameter }
        | FilterExpression::Gt { field, parameter }
        | FilterExpression::Gte { field, parameter }
        | FilterExpression::Like {
            field, parameter, ..
        } => {
            validate_identifier(field, limits.max_identifier_bytes)?;
            validate_parameter(parameter)?;
        }
        FilterExpression::IsNull { field } | FilterExpression::IsNotNull { field } => {
            validate_identifier(field, limits.max_identifier_bytes)?;
        }
        FilterExpression::In { field, parameters } => {
            validate_identifier(field, limits.max_identifier_bytes)?;
            if parameters.is_empty() || parameters.len() > 1_024 {
                return Err(DatabaseError::invalid_plan(
                    "IN richiede da 1 a 1024 parametri",
                ));
            }
            for parameter in parameters {
                validate_parameter(parameter)?;
            }
        }
        FilterExpression::Between {
            field,
            lower_parameter,
            upper_parameter,
        } => {
            validate_identifier(field, limits.max_identifier_bytes)?;
            validate_parameter(lower_parameter)?;
            validate_parameter(upper_parameter)?;
        }
        FilterExpression::Spatial {
            function,
            field,
            geometry_parameter,
            distance_parameter,
        } => {
            validate_identifier(field, limits.max_identifier_bytes)?;
            if let Some(parameter) = geometry_parameter {
                validate_parameter(parameter)?;
            }
            if let Some(parameter) = distance_parameter {
                validate_parameter(parameter)?;
            }
            match function {
                function if function.is_unary_predicate() => {
                    if geometry_parameter.is_some() || distance_parameter.is_some() {
                        return Err(DatabaseError::invalid_plan(
                            "predicato spatial unario con parametri inattesi",
                        ));
                    }
                }
                SpatialFunction::DWithin => {
                    if geometry_parameter.is_none() || distance_parameter.is_none() {
                        return Err(DatabaseError::invalid_plan(
                            "d_within richiede geometria e distanza",
                        ));
                    }
                }
                function if function.is_binary_predicate() => {
                    if geometry_parameter.is_none() || distance_parameter.is_some() {
                        return Err(DatabaseError::invalid_plan(
                            "predicato spatial binario non valido",
                        ));
                    }
                }
                _ => {
                    return Err(DatabaseError::invalid_plan(
                        "funzione spatial non utilizzabile come filtro",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_object(
    object: &ObjectRef,
    limits: &plenora_database_core::limits::Limits,
) -> Result<()> {
    if let Some(catalog) = &object.catalog {
        validate_identifier(catalog, limits.max_identifier_bytes)?;
    }
    if let Some(schema) = &object.schema {
        validate_identifier(schema, limits.max_identifier_bytes)?;
    }
    validate_identifier(&object.object, limits.max_identifier_bytes)
}

/// `common.schema.json#/$defs/identifier` dice `maxLength: 256`, e
/// `maxLength` di JSON Schema conta **code point**.
///
/// Il confronto usa code point, non byte UTF-8, e resta quindi compatibile con
/// identificatori Unicode e con il limite in caratteri di SQL Server.
///
/// `Limits::max_identifier_bytes` non compare in `plan.schema.json`, che ha
/// `additionalProperties: false`: nessun piano puo dichiararlo, e il suo 256 e
/// il tetto del contratto scritto in un campo il cui nome dice byte. Qui vale
/// come numero di caratteri, che e cio che il contratto intende.
fn validate_identifier(value: &str, max_characters: usize) -> Result<()> {
    if value.is_empty() || value.contains('\0') || value.chars().count() > max_characters {
        return Err(DatabaseError::invalid_plan(
            "identificatore vuoto, con NUL o oltre limite",
        ));
    }
    Ok(())
}

/// I nomi dei parametri passano dalla stessa `$defs/identifier` degli
/// oggetti, e `maxLength` la conta in code point.
///
/// Il limite usa i code point, non i byte UTF-8. Questo vale anche per i nomi
/// referenziati dai filtri.
fn validate_parameter(value: &str) -> Result<()> {
    if value.is_empty() || value.contains('\0') || value.chars().count() > 256 {
        return Err(DatabaseError::invalid_plan("nome parametro non valido"));
    }
    Ok(())
}

fn validate_write_capability(mode: WriteMode, capabilities: &ProviderCapabilities) -> Result<()> {
    let supported = match mode {
        WriteMode::Create => capabilities.writes.create,
        WriteMode::Append => capabilities.writes.append,
        WriteMode::TruncateInsert => capabilities.writes.truncate_insert,
        WriteMode::Replace => capabilities.writes.replace,
        WriteMode::Update => capabilities.writes.update,
        WriteMode::Upsert => capabilities.writes.upsert,
        WriteMode::DeleteByKeys => capabilities.writes.delete_by_keys,
    };
    if supported {
        Ok(())
    } else {
        Err(DatabaseError::unsupported(
            capabilities.provider,
            plenora_database_core::ErrorPhase::Prepare,
            "modalità write non pubblicizzata dal provider",
        ))
    }
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

/// Ogni bandiera del contratto o governa qualcosa, o dichiara di non farlo.
///
/// Ogni campo delle strutture capability deve stare in uno solo fra due
/// insiemi: quelli consultati dall'engine e quelli esplicitamente descrittivi.
/// Un campo in nessuno dei due insiemi è una promessa senza controllo; uno in
/// entrambi rende invece obsoleta la classificazione descrittiva.
#[cfg(test)]
#[path = "lib_capability_surface.rs"]
mod capability_surface;
