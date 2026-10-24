//! Identita logica persistente nel commento di colonna, nella transazione write.
use crate::error::{classify_error, public_error};
use arrow_schema::SchemaRef;
use plenora_database_core::plan::{WriteMode, WriteOperation};
use plenora_database_core::protocol::{canonical_field_id, FIELD_ID};
use plenora_database_core::{ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::{Dialect, DialectCapabilities, Identifier, ObjectName, Renderer};
use tokio_postgres::Transaction;

const PREFIX: &str = "plenora.field_id.v1:";

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
    let Some(value) = comment.and_then(|text| text.strip_prefix(PREFIX)) else {
        return Ok(None);
    };
    let (id, _) = value.split_once('\n').ok_or_else(invalid_comment)?;
    canonical_field_id(id).ok_or_else(invalid_comment).map(Some)
}

fn invalid_comment() -> plenora_database_core::DatabaseError {
    public_error(
        ErrorCategory::DataMapping,
        ErrorPhase::Prepare,
        false,
        "metadati identita colonna PostgreSQL non validi",
    )
}

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
        "{PREFIX}{incoming}\n{}",
        comment.unwrap_or("")
    )))
}

pub async fn register(
    transaction: &Transaction<'_>,
    operation: &WriteOperation,
    schema: &SchemaRef,
) -> Result<()> {
    if operation.mode == WriteMode::DeleteByKeys
        || !schema
            .fields()
            .iter()
            .any(|field| field.metadata().contains_key(FIELD_ID))
    {
        return Ok(());
    }
    let renderer = Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    );
    let target = renderer.quote_object(&ObjectName {
        catalog: None,
        schema: Some(Identifier::new(
            operation.target.schema.as_deref().unwrap_or("public"),
        )?),
        object: Identifier::new(operation.target.object.clone())?,
    })?;
    // Serializza registrazione e confronto anche fra append concorrenti.
    transaction
        .batch_execute(&format!("LOCK TABLE {target} IN SHARE ROW EXCLUSIVE MODE"))
        .await
        .map_err(|error| classify_error(ErrorPhase::Write, &error))?;
    for field in schema.fields() {
        let Some(incoming) = field.metadata().get(FIELD_ID) else {
            continue;
        };
        let row = transaction
            .query_one(
                "SELECT col_description(a.attrelid, a.attnum) FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = $1::text::regclass AND a.attname = $2 \
             AND a.attnum > 0 AND NOT a.attisdropped",
                &[&target, field.name()],
            )
            .await
            .map_err(|error| classify_error(ErrorPhase::Write, &error))?;
        let comment: Option<String> = row.get(0);
        if let Some(updated) = registration(comment.as_deref(), incoming)? {
            // COMMENT non accetta bind: il server quota il valore con le proprie
            // regole, incluse le impostazioni sulle stringhe e i backslash.
            let literal: String = transaction
                .query_one("SELECT quote_literal($1::text)", &[&updated])
                .await
                .map_err(|error| classify_error(ErrorPhase::Write, &error))?
                .get(0);
            let column = renderer.quote_identifier(&Identifier::new(field.name().clone())?)?;
            transaction
                .batch_execute(&format!("COMMENT ON COLUMN {target}.{column} IS {literal}"))
                .await
                .map_err(|error| classify_error(ErrorPhase::Write, &error))?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "field_identity_tests.rs"]
mod tests;
