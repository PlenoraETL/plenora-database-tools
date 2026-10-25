//! Prova di fedelta dei parametri bindati, comune ai provider.
//!
//! La regola che misura: ogni variante di `ParameterValue`, su ogni provider,
//! o ha un binding esatto — il server riceve proprio quel valore, lo conserva
//! e lo ritrova con `col = ?` — oppure e rifiutata con un errore esplicito in
//! fase `Prepare`, prima che lo statement parta. Una conversione implicita
//! del server che cambia il valore e un difetto silenzioso, non un binding.
//!
//! Il caso si misura su tre percorsi indipendenti, perche ciascuno ha il
//! proprio codificatore: lo statement nativo del chiamante (`execute` e
//! `query` della transazione), l'AST portable compilato per il dialetto, e il
//! filtro della lettura plan-based. Ogni percorso apre la propria transazione
//! e la annulla: la tabella sonda resta vuota fra un caso e l'altro.
//!
//! Il modulo non panica: restituisce un resoconto, e il test live del
//! provider lo trasforma in un verdetto.

use plenora_database_core::plan::{FilterExpression, ObjectRef, ReadOperation};
use plenora_database_core::portable::{
    and, compile_portable, eq, Expression, InsertStatement, PortableStatement, Projection,
    SelectStatement, TableRef,
};
use plenora_database_core::provider::{ParameterBag, ParameterValue, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions, TransactionScope};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Row};
use std::collections::BTreeMap;

/// Lo statement nativo che il provider scrive per la tabella sonda.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeSql {
    /// `INSERT (ID, <col>)` con due segnaposto: id e valore.
    Insert,
    /// La colonna della riga `ID = ?`.
    Select,
    /// `COUNT(*)` delle righe con `ID = ?` e `<col> = ?`.
    Count,
    /// La rappresentazione che il server conserva (esadecimale dei byte,
    /// testo canonico), per la riga `ID = ?`.
    Raw,
    /// `DELETE` della riga `ID = ?`, per il percorso di lettura.
    Delete,
}

/// Il percorso misurato.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FidelityPath {
    Native,
    Portable,
    Read,
}

/// Cio che il caso deve provare.
#[derive(Debug, Clone, PartialEq)]
pub enum Expect {
    /// Conservato e riletto come `read`, ritrovato da `col = ?` con lo stesso
    /// valore, mai da nessuno dei `distinct`.
    Exact {
        read: ParameterValue,
        raw: Option<ParameterValue>,
        distinct: Vec<ParameterValue>,
    },
    /// Conservato e riletto come `read`, senza confronti: un limite
    /// dichiarato in cui il server assegna il valore esatto a una colonna
    /// piu stretta (`plenora_database_core::parameter_fidelity`). Soltanto
    /// sul percorso nativo.
    Assigned { read: ParameterValue },
    /// Rifiutato con questa categoria, senza scrivere; sul percorso di lettura
    /// basta il rifiuto prima dello stream (`Prepare` o `Validate`).
    Refused(ErrorCategory),
}

impl Expect {
    #[must_use]
    pub const fn exact(read: ParameterValue) -> Self {
        Self::Exact {
            read,
            raw: None,
            distinct: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_raw(self, raw: ParameterValue) -> Self {
        match self {
            Self::Exact { read, distinct, .. } => Self::Exact {
                read,
                raw: Some(raw),
                distinct,
            },
            other => other,
        }
    }

    #[must_use]
    pub fn distinct_from(self, value: ParameterValue) -> Self {
        match self {
            Self::Exact {
                read,
                raw,
                mut distinct,
            } => {
                distinct.push(value);
                Self::Exact {
                    read,
                    raw,
                    distinct,
                }
            }
            other => other,
        }
    }
}

/// Un caso: una variante, una colonna, un esito atteso, i percorsi su cui
/// vale.
#[derive(Debug, Clone)]
pub struct Case {
    pub label: &'static str,
    pub column: &'static str,
    pub value: ParameterValue,
    pub expect: Expect,
    pub paths: Vec<FidelityPath>,
}

impl Case {
    #[must_use]
    pub fn new(
        label: &'static str,
        column: &'static str,
        value: ParameterValue,
        expect: Expect,
    ) -> Self {
        Self {
            label,
            column,
            value,
            expect,
            paths: vec![
                FidelityPath::Native,
                FidelityPath::Portable,
                FidelityPath::Read,
            ],
        }
    }

    #[must_use]
    pub fn only(mut self, paths: &[FidelityPath]) -> Self {
        self.paths = paths.to_vec();
        self
    }
}

/// Esito di un caso su un percorso; `Err` descrive la discrepanza.
#[derive(Debug, Clone)]
pub struct Outcome {
    pub label: &'static str,
    pub path: FidelityPath,
    pub result: std::result::Result<(), String>,
}

/// Il bersaglio della prova: provider, tabella sonda, statement nativi.
pub struct Target<'a> {
    pub provider: &'a dyn Provider,
    pub secret: &'a SecretString,
    /// I limiti di ogni percorso: ciascuno riceve un budget nuovo, perche
    /// la durata di un budget unico scadrebbe a meta della prova.
    pub limits: ResourceLimits,
    /// Tabella con una colonna intera `ID` e una colonna per tipo.
    pub table: ObjectRef,
    /// Il nome della colonna `ID` come la scrive il dialetto nel portable e
    /// nel filtro della lettura.
    pub id_column: &'static str,
    pub native: &'a (dyn Fn(NativeSql, &str) -> String + Sync),
}

/// Esegue tutti i casi su tutti i percorsi dichiarati e ne raccoglie gli
/// esiti, senza fermarsi al primo.
pub async fn run(target: &Target<'_>, cases: &[Case]) -> Vec<Outcome> {
    let mut outcomes = Vec::new();
    for (index, case) in cases.iter().enumerate() {
        let id = i32::try_from(index).unwrap_or(i32::MAX).saturating_add(1);
        for path in &case.paths {
            let result = match path {
                FidelityPath::Native => native(target, case, id).await,
                FidelityPath::Portable => portable(target, case, id).await,
                FidelityPath::Read => read(target, case, id).await,
            };
            outcomes.push(Outcome {
                label: case.label,
                path: *path,
                result,
            });
        }
    }
    outcomes
}

/// Le righe del resoconto, una per esito, e l'elenco delle discrepanze.
#[must_use]
pub fn report(outcomes: &[Outcome]) -> (String, Vec<String>) {
    let mut text = String::new();
    let mut failures = Vec::new();
    for outcome in outcomes {
        let line = match &outcome.result {
            Ok(()) => format!("ok    {:?} {}", outcome.path, outcome.label),
            Err(reason) => {
                failures.push(format!("{:?} {}: {reason}", outcome.path, outcome.label));
                format!("FAIL  {:?} {}: {reason}", outcome.path, outcome.label)
            }
        };
        text.push_str(&line);
        text.push('\n');
    }
    (text, failures)
}

type Probe = std::result::Result<(), String>;

/// Uguaglianza di valore: un `F64` si confronta per bit, cosi `-0.0` non e
/// `0.0` e un `NaN` e se stesso.
fn same(left: &ParameterValue, right: &ParameterValue) -> bool {
    match (left, right) {
        (ParameterValue::F64(left), ParameterValue::F64(right)) => {
            left.to_bits() == right.to_bits()
        }
        _ => left == right,
    }
}

/// I confronti di un caso: il valore stesso ritrova la riga — un `NULL` no,
/// per la logica a tre valori di SQL — e ogni valore distinto non la ritrova.
fn comparisons<'v>(
    value: &'v ParameterValue,
    distinct: &'v [ParameterValue],
) -> impl Iterator<Item = (&'v ParameterValue, usize)> {
    let own = usize::from(!matches!(value, ParameterValue::Null { .. }));
    std::iter::once((value, own)).chain(distinct.iter().map(|other| (other, 0)))
}

fn budget(target: &Target<'_>) -> std::result::Result<ResourceBudget, String> {
    ResourceBudget::new(target.limits.clone())
        .map_err(|error| format!("budget: {}", describe(&error)))
}

async fn begin(target: &Target<'_>) -> std::result::Result<Box<dyn TransactionScope>, String> {
    let budget = budget(target)?;
    target
        .provider
        .begin_transaction(
            target.secret,
            &TransactionOptions::default(),
            &budget,
            &CancellationToken::new(),
        )
        .await
        .map_err(|error| format!("apertura transazione: {}", describe(&error)))
}

fn describe(error: &DatabaseError) -> String {
    format!("{:?}/{:?}: {}", error.category, error.phase, error.message)
}

fn refused(result: &std::result::Result<u64, DatabaseError>, category: ErrorCategory) -> Probe {
    match result {
        Err(error) if error.category == category => Ok(()),
        Err(error) => Err(format!(
            "rifiuto atteso {category:?}, ottenuto {}",
            describe(error)
        )),
        Ok(_) => Err(format!(
            "rifiuto atteso {category:?}, il valore e stato scritto"
        )),
    }
}

fn count_of(rows: &[Row]) -> std::result::Result<u64, String> {
    let value = rows
        .first()
        .and_then(|row| row.values().first())
        .ok_or_else(|| "conteggio senza righe".to_owned())?;
    match value {
        ParameterValue::I32(count) => {
            u64::try_from(*count).map_err(|_| "conteggio negativo".into())
        }
        ParameterValue::I64(count) => {
            u64::try_from(*count).map_err(|_| "conteggio negativo".into())
        }
        ParameterValue::Decimal(text) => text
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("conteggio non intero: {text}")),
        other => Err(format!("conteggio di tipo inatteso: {other:?}")),
    }
}

async fn query_first(
    scope: &mut dyn TransactionScope,
    statement: Statement,
) -> std::result::Result<ParameterValue, String> {
    let rows = scope
        .query(&statement, &CancellationToken::new())
        .await
        .map_err(|error| format!("rilettura: {}", describe(&error)))?;
    rows.first()
        .and_then(|row| row.values().first())
        .cloned()
        .ok_or_else(|| "rilettura senza righe".to_owned())
}

/// Il conteggio di un confronto; `None` se il provider lo rifiuta prima
/// dell'invio, che per un valore distinto vale quanto nessuna riga: il valore
/// non e stato confuso con quello conservato.
async fn count(
    scope: &mut dyn TransactionScope,
    statement: Statement,
) -> std::result::Result<Option<u64>, String> {
    match scope.query(&statement, &CancellationToken::new()).await {
        Ok(rows) => count_of(&rows).map(Some),
        Err(error) if error.phase == ErrorPhase::Prepare => Ok(None),
        Err(error) => Err(format!("confronto: {}", describe(&error))),
    }
}

/// Il confronto torna come atteso: il valore stesso ritrova la riga, uno
/// distinto non la ritrova o e rifiutato prima dell'invio.
fn as_expected(matched: Option<u64>, expected: usize) -> bool {
    matched.map_or(expected == 0, |matched| {
        usize::try_from(matched).ok() == Some(expected)
    })
}

async fn native(target: &Target<'_>, case: &Case, id: i32) -> Probe {
    let mut scope = begin(target).await?;
    let probe = native_steps(target, scope.as_mut(), case, id).await;
    let rollback = scope.rollback(&CancellationToken::new()).await;
    probe?;
    rollback.map_err(|error| format!("rollback: {}", describe(&error)))
}

async fn native_steps(
    target: &Target<'_>,
    scope: &mut dyn TransactionScope,
    case: &Case,
    id: i32,
) -> Probe {
    let sql = |kind| (target.native)(kind, case.column);
    let inserted = scope
        .execute(
            &Statement::new(sql(NativeSql::Insert))
                .with_params(vec![ParameterValue::I32(id), case.value.clone()]),
            &CancellationToken::new(),
        )
        .await;
    let empty = Vec::new();
    let (read, raw, distinct) = match &case.expect {
        Expect::Refused(category) => return refused(&inserted, *category),
        Expect::Assigned { read } => (read, &None, &empty),
        Expect::Exact {
            read,
            raw,
            distinct,
        } => (read, raw, distinct),
    };
    let assigned = matches!(case.expect, Expect::Assigned { .. });
    inserted.map_err(|error| format!("scrittura: {}", describe(&error)))?;
    let stored = query_first(
        scope,
        Statement::new(sql(NativeSql::Select)).with_params(vec![ParameterValue::I32(id)]),
    )
    .await?;
    if !same(&stored, read) {
        return Err(format!("riletto {stored:?}, atteso {read:?}"));
    }
    if let Some(raw) = raw {
        let stored = query_first(
            scope,
            Statement::new(sql(NativeSql::Raw)).with_params(vec![ParameterValue::I32(id)]),
        )
        .await?;
        if !same(&stored, raw) {
            return Err(format!("conservato {stored:?}, atteso {raw:?}"));
        }
    }
    for (value, expected) in comparisons(&case.value, distinct).filter(|_| !assigned) {
        let matched = count(
            scope,
            Statement::new(sql(NativeSql::Count))
                .with_params(vec![ParameterValue::I32(id), value.clone()]),
        )
        .await?;
        if !as_expected(matched, expected) {
            return Err(format!(
                "col = ? con {value:?}: {matched:?} righe, attese {expected}"
            ));
        }
    }
    Ok(())
}

fn portable_table(target: &Target<'_>) -> TableRef {
    TableRef {
        schema: target.table.schema.clone(),
        name: target.table.object.clone(),
    }
}

async fn portable(target: &Target<'_>, case: &Case, id: i32) -> Probe {
    let mut scope = begin(target).await?;
    let probe = portable_steps(target, scope.as_mut(), case, id).await;
    let rollback = scope.rollback(&CancellationToken::new()).await;
    probe?;
    rollback.map_err(|error| format!("rollback: {}", describe(&error)))
}

async fn portable_steps(
    target: &Target<'_>,
    scope: &mut dyn TransactionScope,
    case: &Case,
    id: i32,
) -> Probe {
    let kind = target.provider.kind();
    let insert = PortableStatement::Insert(InsertStatement {
        table: portable_table(target),
        columns: vec![target.id_column.to_owned(), case.column.to_owned()],
        values: vec![vec![
            Expression::literal(ParameterValue::I32(id)),
            Expression::literal(case.value.clone()),
        ]],
        returning: Vec::new(),
    });
    let inserted = match compile_portable(kind, &insert) {
        Ok(statement) => scope.execute(&statement, &CancellationToken::new()).await,
        Err(error) => Err(error),
    };
    let distinct = match &case.expect {
        Expect::Refused(category) => return refused(&inserted, *category),
        Expect::Exact { distinct, .. } => distinct,
        Expect::Assigned { .. } => return Err("Assigned vale soltanto sul percorso nativo".into()),
    };
    inserted.map_err(|error| format!("scrittura portable: {}", describe(&error)))?;
    for (value, expected) in comparisons(&case.value, distinct) {
        let select = PortableStatement::Select(SelectStatement {
            table: portable_table(target),
            projection: Projection::Columns(vec![target.id_column.to_owned()]),
            filter: Some(and(vec![
                eq(target.id_column, ParameterValue::I32(id)),
                eq(case.column, value.clone()),
            ])),
            order_by: Vec::new(),
            limit: None,
        });
        let statement = compile_portable(kind, &select)
            .map_err(|error| format!("select portable: {}", describe(&error)))?;
        let matched = match scope.query(&statement, &CancellationToken::new()).await {
            Ok(rows) => Some(u64::try_from(rows.len()).unwrap_or(u64::MAX)),
            Err(error) if error.phase == ErrorPhase::Prepare => None,
            Err(error) => return Err(format!("confronto portable: {}", describe(&error))),
        };
        if !as_expected(matched, expected) {
            return Err(format!(
                "eq portable con {value:?}: {matched:?} righe, attese {expected}"
            ));
        }
    }
    Ok(())
}

async fn read(target: &Target<'_>, case: &Case, id: i32) -> Probe {
    let distinct = match &case.expect {
        Expect::Exact { distinct, .. } => distinct,
        Expect::Refused(_) => return read_refused(target, case).await,
        Expect::Assigned { .. } => return Err("Assigned vale soltanto sul percorso nativo".into()),
    };
    let mut scope = begin(target).await?;
    let inserted = scope
        .execute(
            &Statement::new((target.native)(NativeSql::Insert, case.column))
                .with_params(vec![ParameterValue::I32(id), case.value.clone()]),
            &CancellationToken::new(),
        )
        .await;
    if let Err(error) = inserted {
        let _ = scope.rollback(&CancellationToken::new()).await;
        return Err(format!("scrittura: {}", describe(&error)));
    }
    scope
        .commit(&CancellationToken::new())
        .await
        .map_err(|error| format!("commit: {}", describe(&error)))?;
    let mut probe = Ok(());
    for (value, expected) in comparisons(&case.value, distinct) {
        match read_count(target, case.column, id, value).await {
            Ok(matched) if matched == expected => {}
            Err(reason) if expected == 0 && reason.contains("/Prepare:") => {}
            Ok(matched) => {
                probe = Err(format!(
                    "filtro di lettura con {value:?}: {matched} righe, attese {expected}"
                ));
                break;
            }
            Err(reason) => {
                probe = Err(reason);
                break;
            }
        }
    }
    let cleanup = delete(target, case.column, id).await;
    probe.and(cleanup)
}

async fn read_refused(target: &Target<'_>, case: &Case) -> Probe {
    let Expect::Refused(category) = &case.expect else {
        return Ok(());
    };
    // La lettura rifiuta prima di aprire lo stream; la categoria puo essere
    // quella del proprio validatore, la fase no.
    match read_count(target, case.column, 0, &case.value).await {
        Err(reason) if reason.contains("/Prepare:") || reason.contains("/Validate:") => Ok(()),
        Err(reason) => Err(format!(
            "rifiuto atteso {category:?} in lettura, ottenuto {reason}"
        )),
        Ok(_) => Err(format!(
            "rifiuto atteso {category:?} in lettura, filtro eseguito"
        )),
    }
}

async fn read_count(
    target: &Target<'_>,
    column: &str,
    id: i32,
    value: &ParameterValue,
) -> std::result::Result<usize, String> {
    let operation = ReadOperation {
        source: target.table.clone(),
        projection: vec![target.id_column.to_owned()],
        order_by: Vec::new(),
        row_limit: None,
        row_offset: None,
        filter: Some(FilterExpression::And {
            args: vec![
                FilterExpression::Eq {
                    field: target.id_column.to_owned(),
                    parameter: "id".to_owned(),
                },
                FilterExpression::Eq {
                    field: column.to_owned(),
                    parameter: "value".to_owned(),
                },
            ],
        }),
        declared_crs: Vec::new(),
    };
    let parameters = ParameterBag::new(BTreeMap::from([
        ("id".to_owned(), ParameterValue::I32(id)),
        ("value".to_owned(), value.clone()),
    ]));
    let cancellation = CancellationToken::new();
    let budget = budget(target)?;
    let mut stream = target
        .provider
        .read(
            target.secret,
            &operation,
            &parameters,
            &budget,
            &cancellation,
        )
        .await
        .map_err(|error| describe(&error))?;
    let mut rows = 0_usize;
    while let Some(batch) = stream
        .next_batch(&cancellation)
        .await
        .map_err(|error| describe(&error))?
    {
        rows = rows.saturating_add(batch.num_rows());
    }
    Ok(rows)
}

async fn delete(target: &Target<'_>, column: &str, id: i32) -> Probe {
    let mut scope = begin(target).await?;
    let deleted = scope
        .execute(
            &Statement::new((target.native)(NativeSql::Delete, column))
                .with_params(vec![ParameterValue::I32(id)]),
            &CancellationToken::new(),
        )
        .await;
    if let Err(error) = deleted {
        let _ = scope.rollback(&CancellationToken::new()).await;
        return Err(format!("pulizia: {}", describe(&error)));
    }
    scope
        .commit(&CancellationToken::new())
        .await
        .map(|_| ())
        .map_err(|error| format!("commit pulizia: {}", describe(&error)))
}
