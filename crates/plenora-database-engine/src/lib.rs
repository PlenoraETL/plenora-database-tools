//! Fasi offline `parse -> validate -> fingerprint -> prepare` + helper di
//! orchestrazione runtime (retry, ...).
//!
//! L'esecuzione remota sarà aggiunta dagli adapter provider senza introdurre
//! `match` sui provider nell'executor.

pub mod retry;
pub use retry::{retry_with_policy, RetryPolicy};

use plenora_database_core::capabilities::{ProviderCapabilities, TransactionScope};
use plenora_database_core::plan::{
    FilterExpression, ObjectRef, Operation, Plan, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::query::SpatialFunction;
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
            // **Prima** le contraddizioni interne, poi le capability. Un piano
            // che si contraddice e sbagliato in se, e la sua diagnosi non deve
            // dipendere da cosa il provider pubblichi: con l'ordine invertito
            // una scrittura `read_only` in Append otteneva `Unsupported`
            // invece di `InvalidPlan` non appena `writes.append` era false, e
            // il chiamante ne deduceva "provider incapace" invece di "piano da
            // correggere" — due strategie di recupero diverse.
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
/// Il confronto con le capability, da solo, le lasciava passare: un piano
/// `read_only` che scrive, o `chunk_committed` che pretende di non lasciare
/// righe a meta, otteneva `prepared` e veniva poi rifiutato dal provider, o
/// peggio eseguito con un lifecycle diverso da quello chiesto.
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
    // Codificarne una versione qui significava scegliere il comportamento di un
    // provider per tutti: la prima stesura restringeva a `Create` e rifiutava
    // prima della rete un piano `SQL Server` valido. Il contratto v2 non ha un
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
        FilterExpression::Spatial { function, .. } => {
            if !out.contains(function) {
                out.push(*function);
            }
        }
        _ => {}
    }
}

/// Cio che `contracts/v2` dichiara su `connection_ref`: stringa non vuota, al
/// piu 256 **caratteri**, senza NUL.
///
/// La lunghezza si contava in byte. `maxLength` di JSON Schema conta code
/// point, quindi un riferimento di duecento caratteri accentati — conforme al
/// contratto, e prodotto da chiunque nomini una variabile in una lingua che non
/// sia l'inglese — veniva dichiarato "piano non valido" da questa
/// implementazione, e da nessun'altra.
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
/// Il confronto era su `String::len()`, cioe byte. Un nome di 65 caratteri CJK
/// pesa 195 byte e sta dentro il contratto; uno di 100 caratteri accentati ne
/// pesa 200. Sopra i 128 caratteri multibyte il tetto in byte mordeva prima di
/// quello vero, e mordeva **piu** stretto del limite reale di SQL Server, che
/// e 128 caratteri Unicode e non byte — tanto che il provider pubblica
/// `max_identifier_bytes: None` proprio per non promettere un numero in byte.
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
/// Era rimasta `value.len()`: la correzione precedente aveva toccato
/// `validate_identifier` e non questa, che pero governa **tutti** i nomi
/// referenziati dai filtri. Centoventinove `é` sono 129 code point e 258 byte:
/// dentro il contratto, fuori da questo controllo.
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
mod tests {
    use super::*;

    /// Le quattro operazioni di inspect stanno in un solo sottoschema, dove
    /// solo `id` e obbligatorio e `source` e ammesso per tutte.
    ///
    /// Serde invece pretende `source` per `database.describe_object`: senza,
    /// non c'e oggetto da descrivere. Un `{"id": "database.describe_object"}`
    /// e percio un documento dentro il contratto e fuori da questo lettore.
    ///
    /// Non si chiude allargando Serde — accettarlo vorrebbe dire rimandare a
    /// runtime un errore che oggi si prende alla lettura — ne stringendo lo
    /// schema, che e pubblicato e la major non si restringe. Il posto dove
    /// separare i quattro sottoschemi e `contracts/v3/`.
    #[test]
    fn describe_object_without_a_source_is_within_the_contract_and_outside_this_reader() {
        let bytes = include_bytes!(
            "../../../contracts/v2/examples/unconsumable-plan-describe-without-source.json"
        );
        serde_json::from_slice::<Plan>(bytes)
            .expect_err("documento conforme allo schema e non consumabile");
    }

    /// L'altro verso **non** e una divergenza, ed e utile dirlo.
    ///
    /// Un `database.list_catalogs` con `source` e ammesso dallo schema, e il
    /// lettore lo accetta ignorando il campo: `deny_unknown_fields` non
    /// raggiunge le varianti unitarie di un enum con tag interno. Non e una
    /// falla — il contratto permette quel documento, e la variante non ha un
    /// oggetto su cui operare — ma e un comportamento che si legge male dal
    /// solo `deny_unknown_fields`, e resta scritto qui.
    #[test]
    fn list_catalogs_ignores_a_source_the_contract_allows() {
        let plan: Plan = serde_json::from_str(
            r#"{"schema_version":2,"connection_ref":"env:DSN","provider":"postgres",
                "operation":{"id":"database.list_catalogs",
                             "source":{"schema":"public","object":"eventi"}}}"#,
        )
        .expect("il contratto ammette il campo, e il lettore non lo rifiuta");
        assert_eq!(plan.operation, Operation::DatabaseListCatalogs);
    }

    /// Un limite oltre `u64` e conforme allo schema v2 — `minimum` senza
    /// `maximum` — e illeggibile da questa implementazione.
    ///
    /// E lo stesso confine gia fissato per il documento capability, e vale per
    /// ogni intero del contratto: dopo il ripristino del dominio storico della
    /// v2 non esiste piu alcun massimo dichiarato, quindi la divergenza fra
    /// cio che il contratto ammette e cio che `u64` rappresenta e permanente
    /// finche non arriva una major che la scriva.
    #[test]
    fn a_plan_limit_beyond_u64_is_within_the_contract_and_outside_this_reader() {
        let bytes =
            include_bytes!("../../../contracts/v2/examples/unconsumable-plan-limit-over-u64.json");
        serde_json::from_slice::<Plan>(bytes)
            .expect_err("un limite oltre u64 non e rappresentabile qui");
    }

    /// Una finestra senza ordinamento sta dentro il contratto e fuori da
    /// questo lettore.
    ///
    /// Lo schema non lega i due campi — potrebbe, con una condizione, ma la
    /// regola non e sintattica: e la ragione per cui la finestra esiste. Due
    /// letture consecutive di un risultato non ordinato possono rendere righe
    /// diverse, quindi `row_offset` senza `order_by` descrive una pagina che
    /// nessuno puo ripetere.
    ///
    /// Il rifiuto e in `prepare` e non nei provider: riguarda il piano, e
    /// `MySQL` lo applicava gia al tetto mentre `PostgreSQL` no. Metterlo qui
    /// lo rende uno.
    #[test]
    fn an_offset_without_an_ordering_is_within_the_contract_and_refused_here() {
        let bytes = include_bytes!(
            "../../../contracts/v2/examples/unconsumable-plan-offset-without-order.json"
        );
        let validated = parse_and_validate(bytes).expect("il contratto ammette il documento");
        let error = prepare(validated, postgres_capabilities())
            .expect_err("una finestra senza ordinamento non e riproducibile");
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );
        assert!(
            error.message.contains("row_offset richiede order_by"),
            "il messaggio non dice cosa manca: {}",
            error.message
        );
    }

    /// Una finestra chiesta a chi non la pubblica viene rifiutata.
    ///
    /// E' cio che rende `reads.pagination` una bandiera invece di una
    /// dichiarazione: prima nessuna riga la consultava, e un provider che
    /// avesse pubblicato `false` avrebbe ricevuto l'offset lo stesso.
    #[test]
    fn an_offset_is_refused_by_a_provider_that_does_not_publish_pagination() {
        let bytes = include_bytes!(
            "../../../contracts/v2/examples/unconsumable-plan-offset-without-order.json"
        );
        let validated = parse_and_validate(bytes).expect("il contratto ammette il documento");
        let mut capabilities = postgres_capabilities();
        capabilities.reads.pagination = false;
        let error =
            prepare(validated, capabilities).expect_err("il provider non pubblica la paginazione");
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::Unsupported,
            "atteso un rifiuto di capability, non di piano: {}",
            error.message
        );
    }

    /// `maxLength: 256` conta code point. Un riferimento di 256 caratteri
    /// accentati pesa 512 byte ed e dentro il contratto.
    #[test]
    fn a_connection_ref_of_256_characters_is_within_the_contract() {
        let reference = format!("env:{}", "e\u{300}".repeat(126));
        assert_eq!(reference.chars().count(), 256);
        assert!(
            reference.len() > 256,
            "il caso serve solo se i byte eccedono"
        );
        validate_connection_ref(&reference).expect("256 caratteri sono ammessi");
        enforce_connection_reference_policy(&reference).expect("prefisso indiretto");

        let too_long = format!("env:{}", "e\u{300}".repeat(127));
        validate_connection_ref(&too_long).expect_err("257 caratteri no");
    }

    /// La politica di questo runtime e piu stretta del contratto, e i due
    /// giudizi restano distinti: il piano con la DSN in chiaro **e** un
    /// documento v2 valido, e viene rifiutato lo stesso.
    #[test]
    fn an_inline_dsn_is_contract_valid_and_still_refused_here() {
        let inline = "postgres://user:password@host/db";
        validate_connection_ref(inline).expect("il contratto v2 lo ammette");
        enforce_connection_reference_policy(inline)
            .expect_err("questo runtime non esegue credenziali in chiaro");
    }

    #[test]
    fn parses_the_postgres_contract_example() {
        let bytes = include_bytes!("../../../contracts/v2/examples/plan-postgres-read.json");
        let validated = parse_and_validate(bytes).expect("valid plan");
        assert_eq!(validated.fingerprint().len(), 64);
        assert_eq!(
            validated.plan().provider,
            plenora_database_core::plan::ProviderKind::Postgres
        );
    }

    // ------------------------------------------------------------------
    //  prepare: la matrice piano -> capability
    // ------------------------------------------------------------------

    const CONTRACT_READ_PLAN: &[u8] =
        include_bytes!("../../../contracts/v2/examples/plan-postgres-read.json");

    /// Spegne una capability. Il nome accompagna il veto perche il messaggio
    /// di fallimento dica *quale* bandiera non ha morso.
    type Veto = (&'static str, fn(&mut ProviderCapabilities));

    fn postgres_capabilities() -> ProviderCapabilities {
        serde_json::from_slice(include_bytes!(
            "../../../contracts/v2/examples/capabilities-postgres.json"
        ))
        .expect("documento capability del contratto")
    }

    fn prepare_plan(plan: &[u8], capabilities: ProviderCapabilities) -> Result<PreparedPlan> {
        prepare(
            parse_and_validate(plan).expect("piano valido"),
            capabilities,
        )
    }

    #[test]
    fn the_contract_plan_prepares_against_the_contract_capabilities() {
        prepare_plan(CONTRACT_READ_PLAN, postgres_capabilities())
            .expect("il piano del contratto deve preparare contro le capability del contratto");
    }

    /// Ogni capability che il piano usa deve poterlo fermare da sola.
    ///
    /// `prepare` confrontava soltanto `reads.streaming` e la write mode:
    /// projection, filter e ordering erano dichiarabili `false` senza che
    /// nulla cambiasse, cioe la capability non significava niente. Il test
    /// disabilita una bandiera per volta, cosi una dimenticanza futura non si
    /// nasconde dietro le altre.
    #[test]
    fn every_read_capability_the_plan_uses_can_veto_it() {
        let vetoes: [Veto; 4] = [
            ("streaming", |c| c.reads.streaming = false),
            ("projection", |c| c.reads.projection = false),
            ("filter", |c| c.reads.filter = false),
            ("ordering", |c| c.reads.ordering = false),
        ];
        for (name, veto) in vetoes {
            let mut capabilities = postgres_capabilities();
            veto(&mut capabilities);
            let error = prepare_plan(CONTRACT_READ_PLAN, capabilities).unwrap_err();
            assert_eq!(
                error.category,
                plenora_database_core::ErrorCategory::Unsupported,
                "`{name}` a false deve fermare il piano"
            );
        }
    }

    /// Il piano del contratto non usa `row_limit`: la relativa capability non
    /// deve poterlo fermare. Una matrice che rifiuta troppo e sbagliata quanto
    /// una che rifiuta troppo poco.
    #[test]
    fn a_capability_the_plan_does_not_use_does_not_veto_it() {
        let mut capabilities = postgres_capabilities();
        capabilities.reads.pagination = false;
        capabilities.spatial.functions.clear();
        prepare_plan(CONTRACT_READ_PLAN, capabilities)
            .expect("il piano non usa row_limit ne funzioni spatial");
    }

    fn write_plan_with_mode(
        profile: &str,
        allow_partial: bool,
        spatial_index: bool,
        mode: &str,
    ) -> Vec<u8> {
        format!(
            concat!(
                r#"{{"schema_version": 2,"#,
                r#""connection_ref": "env:PLENORA_DATABASE_DSN","#,
                r#""provider": "postgres","#,
                r#""operation": {{"#,
                r#""id": "database.write","#,
                r#""target": {{"schema": "public", "object": "events"}},"#,
                r#""mode": "{}","#,
                r#""mapping_policy": "strict","#,
                r#""transaction_profile": "{}","#,
                r#""allow_partial": {},"#,
                r#""create_spatial_index": {}"#,
                r#"}}}}"#
            ),
            mode, profile, allow_partial, spatial_index
        )
        .into_bytes()
    }

    #[test]
    fn every_write_capability_the_plan_uses_can_veto_it() {
        // `create`, non `append`: l'indice spaziale si chiede solo a chi crea
        // il target, e un piano che lo chiedesse altrove sarebbe fermato dalla
        // contraddizione interna prima di arrivare alle capability — cioe
        // questo test non proverebbe piu cio che dichiara.
        let plan = write_plan_with_mode("single_transaction", false, true, "create");
        prepare_plan(&plan, postgres_capabilities()).expect("capability complete");

        let vetoes: [Veto; 4] = [
            ("create", |c| c.writes.create = false),
            ("rollback_on_failure", |c| {
                c.writes.rollback_on_failure = false;
            }),
            ("single_transaction", |c| {
                // Il documento deve restare **valido**, altrimenti il test
                // misura la validazione invece del veto: `scope = transaction`
                // e `staged_swap` richiedono entrambi `single_transaction`, e
                // lasciarli accesi produrrebbe un documento contraddittorio
                // rifiutato prima del confronto col piano.
                c.transactions.single_transaction = false;
                c.transactions.savepoints = false;
                c.transactions.staged_swap = false;
                c.transactions.scope = TransactionScope::Statement;
            }),
            ("spatial_index", |c| c.spatial.spatial_index = false),
        ];
        for (name, veto) in vetoes {
            let mut capabilities = postgres_capabilities();
            veto(&mut capabilities);
            let error = prepare_plan(&plan, capabilities).unwrap_err();
            assert_eq!(
                error.category,
                plenora_database_core::ErrorCategory::Unsupported,
                "`{name}` a false deve fermare il piano"
            );
        }
    }

    /// Le contraddizioni interne al piano si chiudono senza capability.
    ///
    /// Nessun documento capability puo renderle vere e nessun provider puo
    /// eseguirle come scritte: prima ottenevano `prepared` e venivano poi
    /// rifiutate a valle, o — peggio — eseguite con un lifecycle diverso da
    /// quello chiesto.
    #[test]
    fn a_write_that_contradicts_itself_is_rejected() {
        // (profilo, allow_partial, indice spaziale, mode, cosa si contraddice)
        let contradictions = [
            (
                "read_only",
                true,
                false,
                "append",
                "profilo di sola lettura",
            ),
            (
                "chunk_committed",
                false,
                false,
                "append",
                "commit intermedi con allow_partial=false",
            ),
            (
                "staged_swap",
                true,
                false,
                "append",
                "staged swap su una mode che non sostituisce",
            ),
        ];
        for (profile, allow_partial, spatial_index, mode, what) in contradictions {
            let plan = write_plan_with_mode(profile, allow_partial, spatial_index, mode);
            let error = prepare_plan(&plan, postgres_capabilities()).unwrap_err();
            assert_eq!(
                error.category,
                plenora_database_core::ErrorCategory::InvalidPlan,
                "{what} deve essere rifiutato dal piano, non dalle capability"
            );
        }
    }

    /// Le stesse forme, senza la contraddizione, restano ammesse.
    #[test]
    fn the_same_shapes_without_the_contradiction_still_prepare() {
        for (profile, allow_partial, spatial_index, mode) in [
            ("chunk_committed", true, false, "append"),
            ("staged_swap", true, false, "replace"),
            ("single_transaction", false, true, "create"),
        ] {
            let plan = write_plan_with_mode(profile, allow_partial, spatial_index, mode);
            prepare_plan(&plan, postgres_capabilities())
                .unwrap_or_else(|error| panic!("{profile}/{mode} respinto: {error:?}"));
        }
    }

    /// `prepare` non accetta piu un documento capability che il contratto
    /// rifiuta: la validazione e quella completa del core, non due controlli
    /// scelti a mano.
    #[test]
    fn a_capability_document_that_violates_the_contract_is_rejected() {
        let violations: [Veto; 4] = [
            ("major sbagliata", |c| c.schema_version = 1),
            // Vuota davvero: `minLength: 1` e un vincolo che lo schema
            // enuncia. Una versione di soli spazi lo supera, quindi non e una
            // violazione del contratto e non appartiene a questa lista.
            ("provider_version vuota", |c| {
                c.provider_version = String::new();
            }),
            ("limite esplicito a zero", |c| {
                c.limits.max_bind_parameters = Some(0);
            }),
            ("funzioni spatial duplicate", |c| {
                c.spatial.functions =
                    vec![SpatialFunction::Intersects, SpatialFunction::Intersects];
            }),
        ];
        for (what, break_it) in violations {
            let mut capabilities = postgres_capabilities();
            break_it(&mut capabilities);
            let error = prepare_plan(CONTRACT_READ_PLAN, capabilities).unwrap_err();
            assert_eq!(
                error.category,
                plenora_database_core::ErrorCategory::InvalidPlan,
                "{what} deve fermare la preparazione"
            );
        }
    }

    #[test]
    fn staged_swap_requires_the_staged_swap_capability() {
        // `replace`: lo staged swap sostituisce il contenuto del target, e su
        // una mode che non lo sostituisce e una contraddizione del piano.
        let plan = write_plan_with_mode("staged_swap", true, false, "replace");
        prepare_plan(&plan, postgres_capabilities()).expect("capability complete");

        let mut capabilities = postgres_capabilities();
        capabilities.transactions.staged_swap = false;
        assert_eq!(
            prepare_plan(&plan, capabilities)
                .expect_err("staged_swap non pubblicizzato")
                .category,
            plenora_database_core::ErrorCategory::Unsupported
        );
    }

    /// Un piano che chiede una funzione spatial non pubblicizzata si ferma.
    #[test]
    fn an_unadvertised_spatial_function_vetoes_the_plan() {
        let plan = br#"{
          "schema_version": 2,
          "connection_ref": "env:PLENORA_DATABASE_DSN",
          "provider": "postgres",
          "operation": {
            "id": "database.read",
            "source": {"schema": "public", "object": "events"},
            "filter": {
              "op": "spatial",
              "function": "intersects",
              "field": "geom",
              "geometry_parameter": "reference"
            }
          }
        }"#;
        // Il documento capability del contratto non elenca funzioni spatial:
        // `functions` e opzionale e li e assente, quindi di suo non ne
        // garantisce nessuna. Il test dichiara cio che gli serve invece di
        // dipendere da quel documento.
        let mut advertising = postgres_capabilities();
        advertising.spatial.functions = vec![SpatialFunction::Intersects];
        prepare_plan(plan, advertising).expect("intersects pubblicizzata");

        let mut silent = postgres_capabilities();
        silent
            .spatial
            .functions
            .retain(|function| *function != SpatialFunction::Intersects);
        assert_eq!(
            prepare_plan(plan, silent)
                .expect_err("intersects non pubblicizzata")
                .category,
            plenora_database_core::ErrorCategory::Unsupported
        );
    }

    #[test]
    fn fingerprint_is_stable() {
        let bytes = include_bytes!("../../../contracts/v2/examples/plan-postgres-read.json");
        let first = parse_and_validate(bytes).expect("first");
        let second = parse_and_validate(bytes).expect("second");
        assert_eq!(first.fingerprint(), second.fingerprint());
    }

    #[test]
    fn rejects_inline_dsn() {
        let bytes = br#"{
          "schema_version": 2,
          "connection_ref": "postgres://user:password@host/db",
          "provider": "postgres",
          "operation": {"id": "database.test_connection"}
        }"#;
        let error = parse_and_validate(bytes).expect_err("inline DSN");
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );
    }
}

/// Ogni bandiera del contratto o governa qualcosa, o dichiara di non farlo.
///
/// # Il difetto che questa guardia esiste per rendere visibile
///
/// Sei campi del documento capability non erano consultati da nessuna riga di
/// questo file, e nessuno lo diceva: `server_cursor`, `pagination`,
/// `resumable`, `bulk`, `array_binding`, `returning`. Non erano decorativi per
/// scelta — erano decorativi e basta, senza documentazione, e la loro assenza
/// di significato aveva gia prodotto tre esiti diversi sullo stesso campo:
/// `pagination` pubblicato `true` da due provider, `false` da altri due, e
/// reso identico dallo stesso renderer per tutti e quattro.
///
/// Una bandiera che nessuno legge non e neutra. Un consumatore la legge, e ci
/// costruisce sopra una decisione che nessun controllo verifichera mai.
///
/// # Cosa pretende
///
/// Che ogni campo delle tre strutture stia in **esattamente uno** dei due
/// insiemi: quelli che questo file consulta, e quelli dichiarati descrittivi
/// qui sotto. La terza possibilita — «non e in nessuno dei due» — e quella
/// che era la norma, ed e indistinguibile da «qualcuno se n'e dimenticato».
///
/// La seconda meta e altrettanto importante: un campo dichiarato descrittivo
/// che poi **viene** consultato e una dichiarazione scaduta, e la guardia la
/// rifiuta invece di lasciarla invecchiare.
#[cfg(test)]
mod capability_surface {
    /// I campi che questo file non consulta, e perche. Il motivo non e
    /// ornamentale: e la differenza fra una scelta e una dimenticanza.
    const DESCRIPTIVE: &[(&str, &str)] = &[
        (
            "server_cursor",
            "nessun piano chiede un cursore nominato: aprirlo vorrebbe dire prima              un'operazione nel contratto che lo domandi",
        ),
        (
            "resumable",
            "riprendere richiede un punto di ripresa che il contratto non ha",
        ),
        (
            "bulk",
            "la forma della scrittura la sceglie il provider, non il chiamante: non              esiste un piano che chieda l'una o l'altra",
        ),
        (
            "array_binding",
            "nessuna forma di piano lega un array a un parametro solo",
        ),
        (
            "returning",
            "`WriteOutcome` conta righe e non le trasporta: aprirlo sarebbe una major              del contratto, non una bandiera",
        ),
        (
            "savepoints",
            "un savepoint non si chiede in un piano: lo usa chi tiene lo scope in mano",
        ),
        (
            "transactional_ddl",
            "descrive cosa resta dopo un rollback, e il chiamante lo riceve nell'esito              — `Partial` invece di `RolledBack` — non come rifiuto in prepare",
        ),
    ];

    /// I campi dichiarati dalle tre strutture, letti dal contratto.
    fn declared_fields() -> Vec<(&'static str, &'static str)> {
        let source = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../plenora-database-core/src/capabilities.rs"
        ));
        let mut fields = Vec::new();
        for structure in [
            "ReadCapabilities",
            "WriteCapabilities",
            "TransactionCapabilities",
        ] {
            let head = format!("pub struct {structure} {{");
            let at = source
                .find(head.as_str())
                .unwrap_or_else(|| panic!("{structure} non dichiarata nel contratto"));
            let body = &source[at + head.len()..];
            let end = body
                .find(
                    "
}",
                )
                .unwrap_or(body.len());
            for line in body[..end].lines() {
                if let Some(rest) = line.trim().strip_prefix("pub ") {
                    if let Some((name, _)) = rest.split_once(':') {
                        fields.push((
                            structure,
                            Box::leak(name.trim().to_owned().into_boxed_str()) as &'static str,
                        ));
                    }
                }
            }
        }
        assert!(fields.len() >= 20, "lettura dei campi fallita: {fields:?}");
        fields
    }

    #[test]
    fn every_capability_flag_is_enforced_or_declared_descriptive() {
        // Il file si legge da se: cio che conta e se il **codice** nomina il
        // campo, e la parte di test di questo file non e codice che gira in
        // produzione.
        let source = include_str!("lib.rs");
        let production = source
            .split_once(
                "
#[cfg(test)]",
            )
            .map_or(source, |(head, _)| head);
        // I commenti non consultano niente, e questa riga esiste perche la
        // prima stesura della guardia ha detto il contrario: `pagination` e
        // nominata in un commento che spiega **perche** non viene consultata,
        // e la guardia l'ha letta come una consultazione. Un campo citato in
        // prosa resta un campo che nessun controllo fa rispettare.
        let code: String = production
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        let declared: Vec<&str> = DESCRIPTIVE.iter().map(|(name, _)| *name).collect();

        for (structure, field) in declared_fields() {
            // `scope` non e un booleano e si consulta per confronto: il
            // riconoscimento e lo stesso, cerca il nome del campo.
            let enforced = code.contains(&format!(".{field}"));
            let described = declared.contains(&field);
            assert!(
                enforced || described,
                "{structure}::{field} non e consultata da nessuna riga dell'engine \
                 e non dichiara di essere descrittiva: e una promessa che nessun \
                 controllo fa rispettare"
            );
            assert!(
                !(enforced && described),
                "{structure}::{field} e dichiarata descrittiva ma l'engine la \
                 consulta: la dichiarazione e scaduta"
            );
        }
    }

    #[test]
    fn the_descriptive_declaration_does_not_outlive_the_fields() {
        let fields: Vec<&str> = declared_fields().into_iter().map(|(_, f)| f).collect();
        for (name, reason) in DESCRIPTIVE {
            assert!(
                fields.contains(name),
                "{name} e dichiarata descrittiva ma non esiste piu nel contratto"
            );
            assert!(
                reason.len() > 30,
                "{name}: la dichiarazione deve dire il motivo, non ripetere il nome"
            );
        }
    }
}
