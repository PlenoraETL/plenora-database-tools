//! Identita logica persistente nel commento di colonna, nella transazione write.
use crate::error::{classify_error, public_error};
use arrow_schema::SchemaRef;
use plenora_database_core::plan::{WriteMode, WriteOperation};
#[cfg(test)]
use plenora_database_core::protocol::canonical_field_id;
use plenora_database_core::{ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::{Dialect, DialectCapabilities, Identifier, ObjectName, Renderer};
use tokio_postgres::{Client, Transaction};

const COLUMN_COMMENTS: &str =
    "SELECT a.attname::pg_catalog.text, pg_catalog.col_description(a.attrelid, a.attnum) \
     FROM pg_catalog.pg_attribute a \
     WHERE a.attrelid = $1::pg_catalog.text::pg_catalog.regclass \
     AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum";

/// La prima riga e riservata; il resto del commento resta byte per byte.
/// Un'intestazione riservata corrotta non diventa un nuovo id di provenienza.
/// Limite dichiarato: se il commento o il prefisso vengono rimossi a mano,
/// la colonna equivale a una mai registrata e usa il fallback di provenienza.
/// Il commento non autentica l'identita e non rileva tale manomissione.
///
/// L'id si restituisce nella forma canonica (GEO-016: senza zeri iniziali).
///
/// # Deviazione dichiarata (GEO-016, contracts/adoption-source.json)
///
/// Le versioni precedenti registravano l'id cosi come arrivava, e `007` era
/// un ingresso ammesso: un commento gia scritto in quella forma resta
/// leggibile e si pubblica canonico (`7`), invece di rendere la tabella
/// illeggibile. Un ingresso nuovo resta severo: `validate_schema_contract`
/// rifiuta `007` prima che arrivi alla registrazione. Rientra quando nessun
/// commento registrato dalle versioni precedenti puo piu essere letto.
pub fn registered_id(comment: Option<&str>) -> Result<Option<String>> {
    crate::column_metadata::decode(comment)?;
    crate::column_metadata::split_identity(comment).map(|(id, _)| id)
}

#[cfg(test)]
fn invalid_comment() -> plenora_database_core::DatabaseError {
    public_error(
        ErrorCategory::DataMapping,
        ErrorPhase::Prepare,
        false,
        "metadati identita colonna PostgreSQL non validi",
    )
}

#[cfg(test)]
fn registration(comment: Option<&str>, incoming: &str) -> Result<Option<String>> {
    if let Some(existing) = registered_id(comment)? {
        if Some(existing) != canonical_field_id(incoming) {
            // Entrambi in forma canonica: `7` e `007` sono lo stesso id.
            return Err(public_error(
                ErrorCategory::Conflict,
                ErrorPhase::Write,
                false,
                "identita del campo incompatibile con la colonna PostgreSQL",
            ));
        }
        return Ok(None);
    }
    // Si registra la forma canonica, quella che la lettura pubblica.
    let incoming = canonical_field_id(incoming).ok_or_else(invalid_comment)?;
    Ok(Some(format!(
        "{}{incoming}\n{}",
        crate::column_metadata::ID_PREFIX,
        comment.unwrap_or("")
    )))
}

/// Plans every final comment before DDL. The executor already holds the table lock;
/// dry run uses the same routine without claiming a locked snapshot.
pub async fn prepare<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    operation: &WriteOperation,
    schema: &SchemaRef,
    plan: &crate::geometry_properties::SchemaPlan,
) -> Result<Vec<crate::geometry_properties::Registration>> {
    if operation.mode == WriteMode::DeleteByKeys {
        return Ok(Vec::new());
    }
    let target = quoted_target(&renderer(), operation)?;
    let comments: Vec<(String, Option<String>)> = if operation.mode == WriteMode::Create {
        Vec::new()
    } else {
        client
            .query(COLUMN_COMMENTS, &[&target])
            .await
            .map_err(|error| classify_error(ErrorPhase::Write, &error))?
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    };
    for field in schema.fields() {
        if let Some((_, comment)) = comments.iter().find(|(name, _)| name == field.name()) {
            check_legacy_adoption(client, operation, field, &target, comment.as_deref()).await?;
        }
    }
    plan.registrations(schema, &comments)
}

pub async fn register(
    transaction: &Transaction<'_>,
    operation: &WriteOperation,
    registrations: &[crate::geometry_properties::Registration],
    evaluation: &mut crate::write::effective::Evaluation,
) -> Result<()> {
    let renderer = renderer();
    let target = quoted_target(&renderer, operation)?;
    for registration in registrations {
        if let Some(updated) = &registration.comment {
            // COMMENT cannot bind: quote the value on the server, identifiers in the renderer.
            let literal: String = transaction
                .query_one(
                    "SELECT pg_catalog.quote_literal($1::pg_catalog.text)",
                    &[updated],
                )
                .await
                .map_err(|error| classify_error(ErrorPhase::Write, &error))?
                .get(0);
            let column =
                renderer.quote_identifier(&Identifier::new(registration.column.clone())?)?;
            if let Some(position) = registration.input_position {
                evaluation.sent(position)?;
            }
            transaction
                .batch_execute(&format!("COMMENT ON COLUMN {target}.{column} IS {literal}"))
                .await
                .map_err(|error| classify_error(ErrorPhase::Write, &error))?;
        } else if let Some(position) = registration.input_position {
            evaluation.sent(position)?;
        }
        if let Some(position) = registration.input_position {
            evaluation.applied(position, 1)?;
        }
    }
    Ok(())
}

/// La decisione di [`register`] senza registrare, per il dry run: legge i
/// commenti delle colonne del target e rifiuta come la scrittura un id
/// divergente o non registrabile. Senza `LOCK`: un dry run e una previsione,
/// e un append concorrente puo ancora cambiare il commento prima della
/// scrittura, che resta l'unica a decidere sotto lock.
///
/// `may_lock` dice se l'utente puo prendere il `LOCK` di [`register`]
/// (`UPDATE`, `DELETE` o `TRUNCATE` sulla tabella), `owner` se puo scrivere
/// `COMMENT ON COLUMN` (la proprieta della tabella): senza, la scrittura
/// fallisce `authorization` in fase `write`, e il dry run lo dice nello
/// stesso punto.
pub async fn check(
    client: &Client,
    operation: &WriteOperation,
    schema: &SchemaRef,
    may_lock: bool,
    owner: bool,
    plan: &crate::geometry_properties::SchemaPlan,
) -> Result<()> {
    if operation.mode == WriteMode::DeleteByKeys {
        return Ok(());
    }
    let denied = || {
        public_error(
            ErrorCategory::Authorization,
            ErrorPhase::Write,
            false,
            "permesso PostgreSQL insufficiente",
        )
    };
    if !may_lock {
        return Err(denied());
    }
    let registrations = prepare(client, operation, schema, plan).await?;
    if !owner && registrations.iter().any(|r| r.comment.is_some()) {
        return Err(denied());
    }

    Ok(())
}

async fn check_legacy_adoption<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    operation: &WriteOperation,
    field: &arrow_schema::Field,
    target: &str,
    comment: Option<&str>,
) -> Result<()> {
    if !crate::field_contract::FieldContract::parse(field)?
        .canonical
        .spatial
        || crate::column_metadata::decode(comment)?.is_some()
        || matches!(
            operation.mode,
            WriteMode::Create | WriteMode::Replace | WriteMode::TruncateInsert
        )
    {
        return Ok(());
    }
    let populated: bool = client
        .query_one(&format!("SELECT EXISTS (SELECT 1 FROM {target})"), &[])
        .await
        .map_err(|error| classify_error(ErrorPhase::Write, &error))?
        .get(0);
    if populated {
        return Err(public_error(
            ErrorCategory::Conflict,
            ErrorPhase::Write,
            false,
            "registrazione geometrica di un target legacy popolato non attestabile",
        ));
    }
    Ok(())
}

const fn renderer() -> Renderer {
    Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
}

fn quoted_target(renderer: &Renderer, operation: &WriteOperation) -> Result<String> {
    renderer.quote_object(&ObjectName {
        catalog: None,
        schema: Some(Identifier::new(crate::target_schema::schema_of(
            &operation.target,
        )?)?),
        object: Identifier::new(operation.target.object.clone())?,
    })
}

#[cfg(test)]
#[path = "field_identity_tests.rs"]
mod tests;
