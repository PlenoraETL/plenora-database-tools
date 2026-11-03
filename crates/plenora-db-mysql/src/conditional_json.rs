//! Il controllo condizionale accetta solo SQL semplice su relazioni senza JSON.
//! Un SQL opaco non prova il tipo degli operandi: si rifiuta prima del savepoint.
use crate::session::MysqlSession;
use plenora_database_core::spatial_operand::RelationColumns;
use plenora_database_core::{CancellationToken, DatabaseError, Result};

fn refusal() -> DatabaseError {
    DatabaseError::invalid_plan("confronto conditional_update JSON o SQL non verificabile: usare un piano relazionale tipizzato")
}

/// Riconosce un sottoinsieme conservativo, senza funzioni, subquery o join.
/// I nomi mantengono case e quoting; ogni forma esterna si rifiuta.
fn relation(sql: &str, update: bool) -> Result<(Option<String>, String)> {
    if sql.bytes().any(|b| {
        !b.is_ascii() || matches!(b, b'(' | b')' | b'\'' | b'"' | b'@' | b'#' | b';' | b'\\')
    }) || sql.contains("--")
        || sql.contains("/*")
    {
        return Err(refusal());
    }
    let words = sql.split_whitespace().collect::<Vec<_>>();
    let forbidden = [
        "JOIN", "UNION", "WITH", "EXISTS", "SELECT", "FROM", "UPDATE", "DELETE", "INSERT", "TABLE",
        "JSON",
    ];
    let position = if update {
        if !words
            .first()
            .is_some_and(|w| w.eq_ignore_ascii_case("UPDATE"))
        {
            return Err(refusal());
        }
        1
    } else {
        if !words
            .first()
            .is_some_and(|w| w.eq_ignore_ascii_case("SELECT"))
        {
            return Err(refusal());
        }
        words
            .iter()
            .position(|w| w.eq_ignore_ascii_case("FROM"))
            .ok_or_else(refusal)?
            + 1
    };
    let target = words.get(position).ok_or_else(refusal)?;
    let rest = words.get(position + 1..).ok_or_else(refusal)?;
    if update && !rest.first().is_some_and(|w| w.eq_ignore_ascii_case("SET")) {
        return Err(refusal());
    }
    if !update && !rest.is_empty() && !rest[0].eq_ignore_ascii_case("WHERE") {
        return Err(refusal());
    }
    // Tokenizzare anche la punteggiatura evita UNION/SELECT senza spazi.
    let tail = rest.join(" ");
    if tail
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .any(|w| {
            forbidden
                .iter()
                .any(|keyword| w.eq_ignore_ascii_case(keyword))
        })
    {
        return Err(refusal());
    }
    let parts = target
        .split('.')
        .map(|part| {
            let name = part
                .strip_prefix('`')
                .and_then(|s| s.strip_suffix('`'))
                .unwrap_or(part);
            if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                Err(refusal())
            } else {
                Ok(name.to_owned())
            }
        })
        .collect::<Result<Vec<_>>>()?;
    match parts.as_slice() {
        [object] => Ok((None, object.clone())),
        [schema, object] => Ok((Some(schema.clone()), object.clone())),
        _ => Err(refusal()),
    }
}

/// Verifica il target e il probe prima di qualunque mutazione.
///
/// # Errors
/// `InvalidPlan` per JSON o SQL opaco; propaga gli errori del catalogo.
pub async fn verify(
    session: &mut MysqlSession,
    sql: &str,
    update: bool,
    cancellation: &CancellationToken,
) -> Result<()> {
    let target = relation(sql, update)?;
    let catalog = crate::catalog::transaction_operand_catalog(
        session,
        std::slice::from_ref(&target),
        &[],
        cancellation,
    )
    .await?;
    let Some(RelationColumns::Known(columns)) = catalog.relation(target.0.as_ref(), &target.1)
    else {
        return Err(refusal());
    };
    if columns
        .iter()
        .any(|column| column.column_type.identity.as_deref() == Some("json"))
    {
        return Err(refusal());
    }
    Ok(())
}

#[cfg(test)]
#[path = "conditional_json_tests.rs"]
mod tests;
