//! La regola A su PostgreSQL (`plenora_database_core::write_admission`): la
//! chiave d'ammissione di una colonna e la sua ricerca nel fixture misurato.
//!
//! # La chiave
//!
//! PostgreSQL ha gia la classificazione per **foglie** del tipo
//! (`super::canonical`): domini, array, range, multirange e composti si
//! attraversano fino ai tipi base ed enum. La chiave d'ammissione e quella
//! della foglia:
//!
//! - il tipo Arrow dell'ingresso (`write_admission::arrow_key`);
//! - la foglia base con il typmod, come la scrive `pg_catalog.format_type`
//!   (`numeric(10,2)`, `timestamp(3) without time zone`, `interval day`), con
//!   la lunghezza di `character varying(n)`, `character(n)`, `bit(n)` e `bit
//!   varying(n)` in forma simbolica e la lunghezza nella dimensione (il
//!   limite dichiarato delle dimensioni su rappresentanti, nel core); ogni enum
//!   e `enum` (sotto, il limite dichiarato);
//! - la forma: `scalare`, `array`, `range`, `annidato` (dentro un composto, un
//!   multirange, un array di range, o una colonna con piu foglie): cambia cio
//!   che si salva, perche il valore arriva al server come letterale composto
//!   dalla scrittura;
//! - l'encoding del database (`server_encoding`).
//!
//! La forma della chiave e `forma:tipo;encoding` (`scalare:integer;UTF8`).
//! La major del server e i fatti si leggono nella stessa sessione delle
//! foglie: nel preflight in una transazione che si annulla, nella transazione
//! della scrittura e nel dry run con la DSN. Le colonne spatial hanno le
//! proprie verifiche (`plenora_database_core::geometry_value`) e sono escluse,
//! come in Oracle.
//!
//! Per Create, e per le colonne che l'evoluzione dello schema aggiungerebbe,
//! il preflight crea in una transazione che si annulla una tabella temporanea
//! con le stesse dichiarazioni (`CREATE TEMPORARY TABLE`, il tipo risolto con
//! il percorso della sessione, come `CREATE TABLE`) e ne legge le foglie con
//! la stessa lettura del catalogo: la chiave e quella della colonna che la
//! scrittura creera.
//!
//! # Limite dichiarato: la foglia dentro un dominio
//!
//! - **Regola**: un dominio ha la chiave del suo tipo base: la misura di
//!   `scalare:integer` vale per un dominio su `integer`.
//! - **Ambito**: i domini, a ogni profondita (il cammino delle foglie li
//!   attraversa senza cambiare la forma).
//! - **Hazard**: il valore arriva al server con il tipo base (`super::prepared_codec`,
//!   `ThroughDomains`; `COPY` lo legge con la funzione d'ingresso del tipo
//!   base); i vincoli del dominio possono soltanto rifiutare un valore, non
//!   riscriverlo. Un dominio con un default o un trigger che riscrive non
//!   esiste in PostgreSQL; un trigger del target si, ed e fuori dalla regola
//!   A per ogni tipo (la regola R dichiara cio che decide il server).
//! - **Rientro**: la misura dei domini come chiavi proprie.
//!
//! # Limite dichiarato: gli enum
//!
//! - **Regola**: ogni enum ha la chiave `enum`: la misura di un enum della
//!   sonda vale per ogni enum.
//! - **Ambito**: le foglie con `typtype = 'e'`.
//! - **Hazard**: `enum_in` accetta soltanto un'etichetta esatta del tipo e la
//!   salva come tale, o rifiuta: un enum con etichette diverse da quelle della
//!   sonda non riscrive niente di diverso, ma non e misurato lui stesso.
//! - **Rientro**: la misura per tipo.
//!
//! # Limite dichiarato: l'encoding e le collation
//!
//! - **Regola**: la chiave porta l'encoding del database; la collation non
//!   cambia cio che si salva (cambia il confronto, che e la regola K,
//!   `super::visibility`).
//! - **Ambito**: le major misurate con `UTF8` (le immagini qualificate); un
//!   database con un altro encoding non ha chiavi misurate e si rifiuta con
//!   ogni politica.
//! - **Hazard**: piu stretto.
//! - **Rientro**: la misura di un database con un altro encoding.
//!
//! # Limite dichiarato: la tabella temporanea di Create
//!
//! - **Regola**: le foglie delle colonne che la scrittura creera si leggono
//!   da una tabella temporanea con le stesse dichiarazioni, in una
//!   transazione del preflight che si annulla.
//! - **Ambito**: Create e le colonne aggiunte dall'evoluzione dello schema.
//! - **Hazard**: piu stretto: un ruolo senza il privilegio `TEMPORARY` sul
//!   database (concesso a `PUBLIC` per difetto) non scrive con Create; un
//!   tipo che la sessione risolvesse diversamente fra il preflight e la
//!   scrittura (un DDL concorrente) si rilegge comunque nella transazione
//!   della scrittura, dalla colonna creata, con la stessa ammissione.
//! - **Rientro**: le foglie dalla dichiarazione risolta con `to_regtype` e il
//!   typmod di `to_regtypemod` (dalla 16), senza tabella.
//!
//! # Limite dichiarato: il dry run offline
//!
//! - **Regola**: l'ammissione e una classe del catalogo: la decide il dry run
//!   con la DSN (che esegue `prepare_write`), non quello offline.
//! - **Ambito**: `dry_run_write` senza segreto, `--dry-run --offline` della
//!   CLI, che lo dichiarano (`loss_report: None`, `dry_run_offline`).
//! - **Hazard**: un dry run offline verde non dice che la coppia di tipi e
//!   misurata.
//! - **Rientro**: nessuno senza il server: la major e le foglie sono del
//!   server.

use super::canonical::{self, LeafType, SessionShape};
use super::plan::{StoreRule, WriteColumnPlan};
use crate::error::{classify_error, public_error};
use crate::search_path::SessionPath;
use arrow_schema::SchemaRef;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{ProviderKind, WriteMode, WriteOperation};
use plenora_database_core::store_rewrite::ColumnRewrites;
use plenora_database_core::write_admission::{
    arrow_key, AdmissionKey, AdmissionTable, TargetDimensions,
};
use plenora_database_core::{ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::Identifier;
use std::collections::BTreeMap;
use std::sync::OnceLock;
use tokio_postgres::GenericClient;

/// Il fixture misurato della regola A per PostgreSQL.
pub const FIXTURE: &[u8] =
    include_bytes!("../../../../tests/fixtures/scrittura_server_postgres.json");

/// La tabella delle misure, letta una volta.
///
/// # Errors
///
/// `internal` se il fixture incorporato non e valido.
pub fn admission_table() -> Result<&'static AdmissionTable> {
    static TABLE: OnceLock<Result<AdmissionTable>> = OnceLock::new();
    TABLE
        .get_or_init(|| AdmissionTable::parse(ProviderKind::Postgres, FIXTURE))
        .as_ref()
        .map_err(Clone::clone)
}

/// La major del server e l'encoding del database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerFacts {
    pub major: String,
    pub version: String,
    pub encoding: String,
}

const SERVER_FACTS_SQL: &str = "SELECT pg_catalog.current_setting('server_version_num'), \
     pg_catalog.current_setting('server_version'), pg_catalog.current_setting('server_encoding')";

/// La tabella temporanea con cui il preflight legge le foglie delle colonne
/// che la scrittura creera.
const CREATED_TABLE: &str = "pln_ammissione_w";

const TEMP_SCHEMA_SQL: &str = "SELECT n.nspname::pg_catalog.text FROM pg_catalog.pg_namespace n \
     WHERE n.oid = pg_catalog.pg_my_temp_schema()";

/// Legge la major e l'encoding.
///
/// # Errors
///
/// L'errore classificato della lettura, in `phase`; `unsupported` se la
/// versione non si legge nella forma attesa: la regola A non si applica, e la
/// scrittura non parte.
pub async fn server_facts<C: GenericClient + Sync>(
    client: &C,
    phase: ErrorPhase,
) -> Result<ServerFacts> {
    let row = client
        .query_one(SERVER_FACTS_SQL, &[])
        .await
        .map_err(|error| classify_error(phase, &error))?;
    let number: String = row.get(0);
    let major = number
        .parse::<u32>()
        .ok()
        .map(|number| number / 10_000)
        .filter(|major| *major > 0)
        .ok_or_else(|| {
            public_error(
                ErrorCategory::Unsupported,
                phase,
                false,
                "versione PostgreSQL non letta: la regola A non e provata",
            )
        })?;
    Ok(ServerFacts {
        major: major.to_string(),
        version: row.get(1),
        encoding: row.get(2),
    })
}

const fn shape_label(shape: SessionShape) -> &'static str {
    match shape {
        SessionShape::Scalar => "scalare",
        SessionShape::Array => "array",
        SessionShape::Range => "range",
        SessionShape::Nested => "annidato",
    }
}

/// La foglia base con la lunghezza in forma simbolica, e la lunghezza.
fn symbolic(leaf: &LeafType) -> (String, Option<u64>) {
    if leaf.typtype == "e" {
        return ("enum".to_owned(), None);
    }
    for prefix in ["character varying(", "character(", "bit varying(", "bit("] {
        if let Some((digits, tail)) = leaf
            .format
            .strip_prefix(prefix)
            .and_then(|rest| rest.split_once(')'))
        {
            if let Ok(length) = digits.parse::<u64>() {
                return (format!("{prefix}n){tail}"), Some(length));
            }
        }
    }
    (leaf.format.clone(), None)
}

/// La chiave d'ammissione della colonna con le foglie `leaves` per
/// l'ingresso `arrow`, e le dimensioni con cui si istanziano le classi
/// misurate.
#[must_use]
pub(super) fn admission_key(
    arrow: &arrow_schema::DataType,
    leaves: &[LeafType],
    server: &ServerFacts,
) -> (AdmissionKey, TargetDimensions) {
    let (native, dimension) = match leaves {
        [leaf] => {
            let (base, dimension) = symbolic(leaf);
            (format!("{}:{base}", shape_label(leaf.shape)), dimension)
        }
        // Piu foglie (un composto): mai misurata.
        _ => (
            format!(
                "annidato:{}",
                leaves
                    .iter()
                    .map(|leaf| leaf.format.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            None,
        ),
    };
    (
        AdmissionKey {
            arrow: arrow_key(arrow),
            nativo: format!("{native};{}", server.encoding),
            dimensione: dimension,
        },
        TargetDimensions {
            chars: dimension,
            bytes: None,
        },
    )
}

/// Le riscritture misurate di ogni colonna non spatial dell'ingresso che ha
/// le foglie in `leaves`.
///
/// # Errors
///
/// `unsupported` (regola A) per una chiave o una major non misurata, o
/// misurata come rifiutata, nella fase `phase`; `internal` per una classe
/// misurata che non si applica al tipo Arrow.
fn admit(
    operation: &WriteOperation,
    schema: &SchemaRef,
    plans: &[WriteColumnPlan],
    leaves: &BTreeMap<String, Vec<LeafType>>,
    server: &ServerFacts,
    phase: ErrorPhase,
) -> Result<Vec<ColumnRewrites>> {
    let in_phase = |mut error: plenora_database_core::DatabaseError| {
        error.phase = phase;
        error
    };
    #[cfg(test)]
    if plenora_database_testkit::write_probe::is_probe_table(&operation.target.object) {
        return Ok(Vec::new());
    }
    #[cfg(not(test))]
    let _ = operation;
    let measured = admission_table()
        .map_err(in_phase)?
        .server(&server.major)
        .map_err(in_phase)?;
    let mut columns = Vec::new();
    for (index, (field, plan)) in schema.fields().iter().zip(plans).enumerate() {
        if plan.is_spatial() {
            continue;
        }
        let Some(column) = leaves.get(field.name()) else {
            continue;
        };
        let (key, dimensions) = admission_key(field.data_type(), column, server);
        let rewrites = measured.admit(&key, dimensions).map_err(in_phase)?;
        if !rewrites
            .iter()
            .all(|rewrite| rewrite.admits(field.data_type()))
        {
            return Err(public_error(
                ErrorCategory::Internal,
                phase,
                false,
                "regola A: riscrittura misurata su un tipo Arrow a cui non si applica",
            ));
        }
        columns.push(ColumnRewrites {
            field: index,
            target_type: key.nativo,
            rewrites,
        });
    }
    Ok(columns)
}

/// Assegna ai piani le riscritture misurate: la politica e le chiavi a ogni
/// colonna, le riscritture alle colonne di `columns` (le altre conservano le
/// proprie, per esempio quelle che il preflight ha letto per una colonna che
/// la scrittura aggiunge).
pub(super) fn apply(
    plans: &mut [WriteColumnPlan],
    schema: &SchemaRef,
    operation: &WriteOperation,
    columns: &[ColumnRewrites],
) {
    let strict = operation.mapping_policy == MappingPolicy::Strict;
    for (index, (plan, field)) in plans.iter_mut().zip(schema.fields()).enumerate() {
        let rewrites = columns
            .iter()
            .find(|column| column.field == index)
            .map_or_else(
                || std::mem::take(&mut plan.store.rewrites),
                |column| column.rewrites.clone(),
            );
        plan.store = StoreRule {
            rewrites,
            strict,
            key: operation.keys.iter().any(|key| key == field.name()),
        };
    }
}

/// La regola A nella transazione della scrittura o nel dry run con la DSN:
/// le foglie `leaves` lette dal target in questa sessione, la major letta qui.
///
/// # Errors
///
/// Come [`admit`], e l'errore classificato della lettura della major.
pub(super) async fn assign<C: GenericClient + Sync>(
    client: &C,
    operation: &WriteOperation,
    schema: &SchemaRef,
    plans: &mut [WriteColumnPlan],
    leaves: &BTreeMap<String, Vec<LeafType>>,
    phase: ErrorPhase,
) -> Result<()> {
    let server = server_facts(client, phase).await?;
    let columns = admit(operation, schema, plans, leaves, &server, phase)?;
    apply(plans, schema, operation, &columns);
    Ok(())
}

/// Le chiavi d'ammissione delle colonne `(nome, tipo Arrow)` del target, come
/// le calcola il preflight: per la sonda (`write_admission_live_tests`).
///
/// # Errors
///
/// L'errore classificato della lettura.
#[cfg(test)]
pub async fn target_keys(
    client: &tokio_postgres::Client,
    operation: &WriteOperation,
    schema: &SchemaRef,
) -> Result<Vec<Option<(AdmissionKey, TargetDimensions)>>> {
    let phase = ErrorPhase::Prepare;
    let read = |error: tokio_postgres::Error| classify_error(phase, &error);
    client.batch_execute("BEGIN").await.map_err(read)?;
    let result = async {
        let server = server_facts(client, phase).await?;
        SessionPath::close(client, phase).await?;
        let (_, leaves) = canonical::rules_with_leaves(client, operation, schema, phase).await?;
        Ok(schema
            .fields()
            .iter()
            .map(|field| {
                leaves
                    .get(field.name())
                    .map(|column| admission_key(field.data_type(), column, &server))
            })
            .collect())
    }
    .await;
    client.batch_execute("ROLLBACK").await.map_err(read)?;
    result
}

/// La regola A nel preflight: le riscritture misurate di ogni colonna, in una
/// transazione che si annulla, con il percorso chiuso della regola Q2. Le
/// colonne che il target non ha (tutte, per Create) si leggono da una tabella
/// temporanea con le dichiarazioni che la scrittura userebbe.
///
/// # Errors
///
/// `unsupported` (regola A) per una chiave o una major non misurata, o
/// misurata come rifiutata; l'errore classificato della lettura, o della
/// creazione della tabella temporanea (una dichiarazione che il server non
/// risolve, che `CREATE TABLE` rifiuterebbe allo stesso modo).
pub async fn preflight(
    client: &tokio_postgres::Client,
    operation: &WriteOperation,
    schema: &SchemaRef,
) -> Result<Vec<ColumnRewrites>> {
    let phase = ErrorPhase::Prepare;
    let plans = super::compile_schema_plan(schema, operation)?;
    let read = |error: tokio_postgres::Error| classify_error(phase, &error);
    client.batch_execute("BEGIN").await.map_err(read)?;
    let result = async {
        let server = server_facts(client, phase).await?;
        let paths = SessionPath::close(client, phase).await?;
        let mut leaves = if operation.mode == WriteMode::Create {
            BTreeMap::new()
        } else {
            canonical::rules_with_leaves(client, operation, schema, phase)
                .await?
                .1
        };
        let renderer = super::sql::renderer();
        let missing = schema
            .fields()
            .iter()
            .zip(&plans)
            .filter(|(field, plan)| !plan.is_spatial() && !leaves.contains_key(field.name()))
            .map(|(field, plan)| {
                let name = renderer.quote_identifier(&Identifier::new(field.name().clone())?)?;
                Ok(format!("{name} {}", plan.postgres_type))
            })
            .collect::<Result<Vec<_>>>()?;
        if !missing.is_empty() {
            let sql = format!(
                "CREATE TEMPORARY TABLE {CREATED_TABLE} ({}) ON COMMIT DROP",
                missing.join(", ")
            );
            paths
                .on_target(client, async {
                    client.batch_execute(&sql).await.map_err(read)
                })
                .await?;
            let temporary: String = client
                .query_one(TEMP_SCHEMA_SQL, &[])
                .await
                .map_err(read)?
                .get(0);
            let mut created = operation.clone();
            created.target.schema = Some(temporary);
            CREATED_TABLE.clone_into(&mut created.target.object);
            let (_, declared) =
                canonical::rules_with_leaves(client, &created, schema, phase).await?;
            for (name, column) in declared {
                leaves.entry(name).or_insert(column);
            }
        }
        admit(operation, schema, &plans, &leaves, &server, phase)
    }
    .await;
    client.batch_execute("ROLLBACK").await.map_err(read)?;
    result
}

#[cfg(test)]
#[path = "admission_tests.rs"]
mod tests;
