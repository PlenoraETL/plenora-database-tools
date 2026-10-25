//! La regola V su PostgreSQL: un valore che la scrittura manda come testo e
//! che il server interpreta secondo la sessione (`DateStyle`,
//! `IntervalStyle`, `TimeZone`, `lc_monetary`) deve essere nella forma
//! canonica che non ne dipende, o la scrittura si rifiuta.
//!
//! Misurato il 25/10 su PostgreSQL 16 (rilettura indipendente):
//!
//! - `daterange` con il limite `'01/02/2026'`: 2 gennaio con `DateStyle =
//!   'ISO, MDY'`, 1 febbraio con `'ISO, DMY'`, entrambi `committed`;
//! - `tstzrange` con il limite `'2026-01-02 10:00:00'`, senza offset: l'istante
//!   cambiava con il `TimeZone` del ruolo;
//! - un `interval` scritto da `CopyText` cambiava segno con `IntervalStyle =
//!   sql_standard` (corretto in `binary_codec::interval_text`, che ora mette
//!   il segno su ogni campo).
//!
//! # La regola
//!
//! La transazione della scrittura (e il dry run con la DSN) legge dal
//! catalogo, per ogni colonna dell'ingresso, se il tipo della colonna arriva —
//! attraverso domini, array, range e tipi composti — a uno dei tipi che
//! dipendono dalla sessione ([`rules`], per oid fisso: `date`, `timestamp`,
//! `timestamptz`, `timetz`, `interval`, `money`). Poi ogni valore non nullo
//! passa da [`check`] prima della codifica, in ogni profilo:
//!
//! - testo libero (`Utf8`, gli elementi di una lista `Utf8`, i limiti di un
//!   range): soltanto la forma canonica del tipo foglia — date ISO
//!   `AAAA-MM-GG`; timestamp ISO senza zona; `timestamptz` e `timetz` con la
//!   zona esplicita (`Z` o `+hh[:mm]`); `infinity`/`-infinity`; intervalli in
//!   forma ISO 8601 (`P...`) o con il segno esplicito su ogni campo dopo un
//!   primo campo negativo;
//! - un array o un range scritto come un unico letterale di testo, un tipo
//!   composto che contiene un tipo della sessione, `money` (il separatore
//!   decimale dipende da `lc_monetary`): rifiutati;
//! - valori Arrow tipizzati che la scrittura formatta lei stessa: canonici
//!   per costruzione, tranne le conversioni che passano dal `TimeZone`
//!   (un timestamp senza zona, una data o un `time` verso `timestamptz` o
//!   `timetz`, ammesse soltanto da una politica di mapping non stretta):
//!   rifiutate.
//!
//! La sessione **non** si fissa: `TimeZone` e `DateStyle` decidono anche i
//! default e i trigger del target (`current_date`, `localtimestamp`, una data
//! formattata in testo), e fissarli cambierebbe in silenzio cio che il target
//! scrive. La forma canonica non dipende dalla sessione, e non la tocca.
//!
//! # Limite dichiarato
//!
//! - **Regola**: un valore testuale verso un tipo della sessione e canonico,
//!   o la scrittura si rifiuta.
//! - **Ambito**: scrittura e dry run con la DSN, per le colonne del target
//!   lette dal catalogo; il dry run senza DSN, e quello di Create, conoscono
//!   soltanto i tipi di `pg_catalog` che il piano dichiara ([`declared`]),
//!   non i domini ne i tipi composti.
//! - **Hazard**: si rifiutano anche forme che non dipendono dalla sessione
//!   ma non sono nell'elenco (una zona per nome, `Europe/Rome`; una data con
//!   il nome del mese; `today`, che pero dipende dall'ora). Un tipo composto
//!   con un campo della sessione si rifiuta sempre, anche se il valore e
//!   canonico.
//!   Le regole si leggono una volta, prima del primo valore e senza rileggerle
//!   dopo il lock: un `ALTER COLUMN ... TYPE` concorrente fra la lettura e il
//!   primo statement lascia la colonna con la regola del tipo di prima.
//! - **Rientro**: la verifica per posizione dei campi di un composto, i
//!   domini e i composti dichiarati risolti dal catalogo anche senza DSN, e
//!   le regole rilette con la regola R dopo il lock.

use super::mapping_error;
use super::plan::{ColumnSemantics, WriteColumnPlan};
use super::value_codec::range_value;
use crate::error::{classify_error, public_error};
use arrow_array::{Array, ListArray, StringArray, StructArray};
use arrow_schema::{DataType, SchemaRef};
use plenora_database_core::plan::WriteOperation;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::BTreeMap;
use tokio_postgres::GenericClient;

/// Il tipo foglia che la sessione interpreta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SessionLeaf {
    Date,
    Timestamp,
    TimestampTz,
    TimeTz,
    Interval,
    Money,
}

/// Dove sta il tipo foglia nel tipo della colonna.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SessionShape {
    Scalar,
    Array,
    Range,
    /// Dentro un tipo composto, un multirange o un array di range: il testo
    /// non si verifica per posizione.
    Nested,
}

/// La regola V di una colonna.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SessionRule {
    pub(super) leaf: SessionLeaf,
    pub(super) shape: SessionShape,
}

/// Il motivo pubblico del rifiuto.
pub(super) const NOT_CANONICAL: &str = "valore non canonico per un tipo che il server \
     interpreta secondo la sessione (DateStyle, IntervalStyle, TimeZone, lc_monetary)";

fn refused() -> DatabaseError {
    public_error(
        ErrorCategory::DataMapping,
        ErrorPhase::Write,
        false,
        &format!("scrittura rifiutata: {NOT_CANONICAL}"),
    )
}

/// I tipi foglia per oid fisso del catalogo: `money` 790, `date` 1082,
/// `timestamp` 1114, `timestamptz` 1184, `interval` 1186, `timetz` 1266.
const RULES_SQL: &str = "\
WITH RECURSIVE target AS (
    SELECT CASE WHEN $1::pg_catalog.text IS NULL
                THEN pg_catalog.to_regclass(pg_catalog.quote_ident($2::pg_catalog.text))::pg_catalog.oid
           ELSE (SELECT c.oid FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1::pg_catalog.text AND c.relname = $2::pg_catalog.text) END AS oid
), walk(name, oid, shape, depth) AS (
    SELECT a.attname::pg_catalog.text, a.atttypid, 'scalar'::pg_catalog.text, 0
      FROM target t JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid
     WHERE a.attnum > 0 AND NOT a.attisdropped
       AND a.attname::pg_catalog.text = ANY ($3::pg_catalog.text[])
    UNION ALL
    SELECT w.name, x.next, x.shape, w.depth + 1 FROM walk w CROSS JOIN LATERAL (
        SELECT y.typbasetype AS next, w.shape AS shape
          FROM pg_catalog.pg_type y WHERE y.oid = w.oid AND y.typtype = 'd'
        UNION ALL
        SELECT y.typelem, CASE WHEN w.shape = 'scalar' THEN 'array' ELSE 'nested' END
          FROM pg_catalog.pg_type y
         WHERE y.oid = w.oid AND y.typtype = 'b' AND y.typcategory = 'A' AND y.typelem <> 0
        UNION ALL
        SELECT r.rngsubtype, CASE WHEN w.shape = 'scalar' THEN 'range' ELSE 'nested' END
          FROM pg_catalog.pg_range r WHERE r.rngtypid = w.oid
        UNION ALL
        SELECT r.rngsubtype, 'nested' FROM pg_catalog.pg_range r WHERE r.rngmultitypid = w.oid
        UNION ALL
        SELECT a.atttypid, 'nested'
          FROM pg_catalog.pg_type y JOIN pg_catalog.pg_attribute a ON a.attrelid = y.typrelid
         WHERE y.oid = w.oid AND y.typtype = 'c' AND a.attnum > 0 AND NOT a.attisdropped
    ) x
    WHERE w.depth < 32
)
SELECT name, shape, oid::pg_catalog.int8 FROM walk
 WHERE oid = ANY ('{790,1082,1114,1184,1186,1266}'::pg_catalog.oid[])";

/// Le regole V delle colonne dell'ingresso sul target, per nome. Un target
/// che non si risolve non ne ha: lo statement lo rifiutera da se.
///
/// # Errors
///
/// L'errore classificato della lettura, in `phase`.
pub(super) async fn rules<C: GenericClient + Sync>(
    client: &C,
    operation: &WriteOperation,
    schema: &SchemaRef,
    phase: ErrorPhase,
) -> Result<BTreeMap<String, SessionRule>> {
    let names = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let rows = client
        .query(
            RULES_SQL,
            &[&operation.target.schema, &operation.target.object, &names],
        )
        .await
        .map_err(|error| classify_error(phase, &error))?;
    let mut rules = BTreeMap::new();
    for row in rows {
        let name: String = row.get(0);
        let shape = match row.get::<_, String>(1).as_str() {
            "scalar" => SessionShape::Scalar,
            "array" => SessionShape::Array,
            "range" => SessionShape::Range,
            _ => SessionShape::Nested,
        };
        let leaf = match row.get::<_, i64>(2) {
            790 => SessionLeaf::Money,
            1082 => SessionLeaf::Date,
            1114 => SessionLeaf::Timestamp,
            1184 => SessionLeaf::TimestampTz,
            1186 => SessionLeaf::Interval,
            _ => SessionLeaf::TimeTz,
        };
        // Due foglie per una colonna arrivano soltanto da un composto, che e
        // gia `Nested`: la prima basta.
        rules
            .entry(name)
            .and_modify(|rule: &mut SessionRule| {
                if shape == SessionShape::Nested {
                    rule.shape = SessionShape::Nested;
                }
            })
            .or_insert(SessionRule { leaf, shape });
    }
    Ok(rules)
}

/// Assegna a ogni piano la regola della sua colonna.
pub(super) fn assign(
    plans: &mut [WriteColumnPlan],
    schema: &SchemaRef,
    rules: &BTreeMap<String, SessionRule>,
) {
    for (plan, field) in plans.iter_mut().zip(schema.fields()) {
        plan.session_rule = rules.get(field.name()).copied();
    }
}

/// Assegna a ogni piano la regola che dichiara ([`declared`]).
pub(super) fn assign_declared(plans: &mut [WriteColumnPlan]) {
    for plan in plans {
        plan.session_rule = declared(plan);
    }
}

/// La regola V che il piano dichiara da se, per i tipi nativi di
/// `pg_catalog` che porta nei metadati (`plenora.native_type`): quella che
/// la colonna avra dopo `CREATE TABLE`, e quella che il dry run senza DSN
/// conosce. I domini e i tipi composti dichiarati non si risolvono senza il
/// catalogo.
pub(super) fn declared(plan: &WriteColumnPlan) -> Option<SessionRule> {
    let declaration = plan.native_declaration_sql.as_deref()?;
    let (base, array) = declaration
        .strip_suffix("[]")
        .map_or((declaration, false), |base| (base, true));
    let (leaf, range) = match base {
        "date" => (SessionLeaf::Date, false),
        "timestamp without time zone" => (SessionLeaf::Timestamp, false),
        "timestamp with time zone" => (SessionLeaf::TimestampTz, false),
        "time with time zone" => (SessionLeaf::TimeTz, false),
        "interval" => (SessionLeaf::Interval, false),
        "daterange" => (SessionLeaf::Date, true),
        "tsrange" => (SessionLeaf::Timestamp, true),
        "tstzrange" => (SessionLeaf::TimestampTz, true),
        _ => return None,
    };
    let shape = match (array, range) {
        (false, false) => SessionShape::Scalar,
        (true, false) => SessionShape::Array,
        (false, true) => SessionShape::Range,
        (true, true) => SessionShape::Nested,
    };
    Some(SessionRule { leaf, shape })
}

/// Il valore non nullo di `array` alla riga `row` contro la regola del piano.
///
/// # Errors
///
/// `data_mapping` con [`NOT_CANONICAL`].
pub(super) fn check(plan: &WriteColumnPlan, array: &dyn Array, row: usize) -> Result<()> {
    let Some(rule) = plan.session_rule else {
        return Ok(());
    };
    if rule.leaf == SessionLeaf::Money || rule.shape == SessionShape::Nested {
        return Err(refused());
    }
    let accepted = match (rule.shape, array.data_type()) {
        (SessionShape::Scalar, DataType::Utf8) => canonical(rule.leaf, string(array, row)?),
        (SessionShape::Scalar, DataType::Date32) => rule.leaf != SessionLeaf::TimestampTz,
        (SessionShape::Scalar, DataType::Timestamp(_, None) | DataType::Time64(_)) => {
            !matches!(rule.leaf, SessionLeaf::TimestampTz | SessionLeaf::TimeTz)
        }
        // Un array o un range scritto come un unico letterale di testo cade
        // nell'ultimo ramo: rifiutato.
        (SessionShape::Array, DataType::List(_)) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(mapping_error)?;
            let values = list.value(row);
            values
                .as_any()
                .downcast_ref::<StringArray>()
                .is_none_or(|items| {
                    (0..items.len()).all(|index| {
                        items.is_null(index) || canonical(rule.leaf, items.value(index))
                    })
                })
        }
        (SessionShape::Range, DataType::Struct(_)) if plan.semantics == ColumnSemantics::Range => {
            let range = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(mapping_error)?;
            let value = range_value(range, row)?;
            let lower = value
                .lower
                .as_deref()
                .is_none_or(|bound| canonical(rule.leaf, bound));
            let upper = value
                .upper
                .as_deref()
                .is_none_or(|bound| canonical(rule.leaf, bound));
            lower && upper
        }
        // I valori Arrow tipizzati che la scrittura formatta da se.
        (
            _,
            DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Binary
            | DataType::Interval(_)
            | DataType::Timestamp(_, Some(_)),
        ) => true,
        // Una lista o una struttura che non corrisponde alla forma della
        // colonna (un composto verso una `date`, una lista verso un range):
        // il suo testo arriverebbe al server senza verifica.
        _ => false,
    };
    if accepted {
        Ok(())
    } else {
        Err(refused())
    }
}

fn string(array: &dyn Array, row: usize) -> Result<&str> {
    Ok(array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(mapping_error)?
        .value(row))
}

/// La forma canonica di `text` per il tipo foglia.
pub(super) fn canonical(leaf: SessionLeaf, text: &str) -> bool {
    let infinite = || text == "infinity" || text == "-infinity";
    match leaf {
        SessionLeaf::Date => {
            infinite() || date_end(text.as_bytes()).is_some_and(|end| bc_end(text, end))
        }
        SessionLeaf::Timestamp => infinite() || timestamp(text, false),
        SessionLeaf::TimestampTz => infinite() || timestamp(text, true),
        SessionLeaf::TimeTz => time_end(text.as_bytes(), 0)
            .and_then(|end| zone_end(text.as_bytes(), end))
            .is_some_and(|end| end == text.len()),
        SessionLeaf::Interval => interval(text),
        SessionLeaf::Money => false,
    }
}

fn digits(bytes: &[u8], from: usize, count: usize) -> Option<usize> {
    let end = from.checked_add(count)?;
    bytes
        .get(from..end)?
        .iter()
        .all(u8::is_ascii_digit)
        .then_some(end)
}

/// `AAAA-MM-GG` (anni di quattro o piu cifre), con la fine.
fn date_end(bytes: &[u8]) -> Option<usize> {
    let year = bytes
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if year < 4 || bytes.get(year) != Some(&b'-') {
        return None;
    }
    let month = digits(bytes, year + 1, 2)?;
    if bytes.get(month) != Some(&b'-') {
        return None;
    }
    digits(bytes, month + 1, 2)
}

/// `hh:mm[:ss[.f...]]` da `from`, con la fine.
fn time_end(bytes: &[u8], from: usize) -> Option<usize> {
    let hours = digits(bytes, from, 2)?;
    if bytes.get(hours) != Some(&b':') {
        return None;
    }
    let minutes = digits(bytes, hours + 1, 2)?;
    if bytes.get(minutes) != Some(&b':') {
        return Some(minutes);
    }
    let seconds = digits(bytes, minutes + 1, 2)?;
    if bytes.get(seconds) != Some(&b'.') {
        return Some(seconds);
    }
    let fraction = bytes[seconds + 1..]
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    (fraction > 0).then_some(seconds + 1 + fraction)
}

/// `[ ]Z` o `[ ]+hh[[:]mm[[:]ss]]` da `from`, con la fine.
fn zone_end(bytes: &[u8], from: usize) -> Option<usize> {
    let from = if bytes.get(from) == Some(&b' ') {
        from + 1
    } else {
        from
    };
    match bytes.get(from) {
        Some(b'Z') => Some(from + 1),
        Some(b'+' | b'-') => {
            let mut end = digits(bytes, from + 1, 2)?;
            for _ in 0..2 {
                let next = if bytes.get(end) == Some(&b':') {
                    end + 1
                } else {
                    end
                };
                match digits(bytes, next, 2) {
                    Some(after) => end = after,
                    None if next == end => break,
                    None => return None,
                }
            }
            Some(end)
        }
        _ => None,
    }
}

/// La fine del testo, o ` BC` e poi la fine.
fn bc_end(text: &str, end: usize) -> bool {
    end == text.len() || &text[end..] == " BC"
}

fn timestamp(text: &str, zoned: bool) -> bool {
    let bytes = text.as_bytes();
    let Some(date) = date_end(bytes) else {
        return false;
    };
    if !matches!(bytes.get(date), Some(b'T' | b' ')) {
        return false;
    }
    let Some(time) = time_end(bytes, date + 1) else {
        return false;
    };
    if zoned {
        zone_end(bytes, time).is_some_and(|end| bc_end(text, end))
    } else {
        bc_end(text, time)
    }
}

/// Un intervallo la cui lettura non dipende da `IntervalStyle`: la forma ISO
/// 8601 con le designazioni (`P1Y2M3DT4H5M6S`, segni ammessi sui numeri),
/// oppure una forma in cui il primo campo numerico non e negativo o ogni
/// campo numerico dopo il primo porta il proprio segno (`sql_standard`
/// applica il segno del primo campo ai campi che non ne hanno uno).
fn interval(text: &str) -> bool {
    if let Some(rest) = text.strip_prefix('P') {
        return iso_interval(rest);
    }
    // Il server divide i campi anche sulla punteggiatura e su ogni spazio, e
    // attacca un segno isolato alla parola dopo (`- 1 day`): un testo che
    // divide diversamente da qui si rifiuta invece di indovinarlo.
    let bytes = text.as_bytes();
    let separated = bytes.iter().enumerate().all(|(index, byte)| {
        byte.is_ascii_alphanumeric()
            || matches!(byte, b'.' | b':' | b'@')
            || *byte == b' '
            || (matches!(byte, b'+' | b'-')
                && bytes
                    .get(index + 1)
                    .is_some_and(|next| next.is_ascii_digit() || *next == b'.'))
    });
    if !separated {
        return false;
    }
    let mut numeric = text.split_ascii_whitespace().filter(|word| {
        word.trim_start_matches(['+', '-'])
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_digit() || *byte == b'.')
    });
    let Some(first) = numeric.next() else {
        return true;
    };
    !first.starts_with('-') || numeric.all(|word| word.starts_with(['+', '-']))
}

/// I campi ISO 8601 di `part` nell'ordine di `designators`, ciascuno con un
/// numero (segno ammesso) e la sua designazione.
fn iso_fields(part: &str, designators: &[u8]) -> Option<usize> {
    let bytes = part.as_bytes();
    let mut index = 0;
    let mut allowed = designators;
    let mut count = 0;
    while index < bytes.len() {
        if matches!(bytes[index], b'+' | b'-') {
            index += 1;
        }
        let number = bytes[index..]
            .iter()
            .take_while(|byte| byte.is_ascii_digit() || **byte == b'.')
            .count();
        if number == 0 {
            return None;
        }
        index += number;
        let designator = *bytes.get(index)?;
        let position = allowed.iter().position(|item| *item == designator)?;
        allowed = &allowed[position + 1..];
        index += 1;
        count += 1;
    }
    Some(count)
}

fn iso_interval(rest: &str) -> bool {
    match rest.split_once('T') {
        None => iso_fields(rest, b"YMWD").is_some_and(|count| count > 0),
        Some((date, time)) => {
            iso_fields(date, b"YMWD").is_some()
                && iso_fields(time, b"HMS").is_some_and(|count| count > 0)
        }
    }
}

#[cfg(test)]
#[path = "canonical_tests.rs"]
mod tests;
