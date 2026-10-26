//! Il cancello unico dell'ammissione dei parametri.
//!
//! Ogni percorso che esegue uno statement compilato dal repository passa da
//! qui, e da nessun'altra parte: la regola e una sola
//! ([`crate::operand_admission`]), e questo modulo e il solo che la applica
//! a uno statement eseguibile. Il vincolo e strutturale, non una
//! convenzione:
//!
//! - lo SQL compilato da un AST portable ([`CompiledPortable`]) non espone i
//!   valori legati: uno [`Statement`] eseguibile ne esce soltanto da
//!   [`portable`] e [`mutation`], dopo l'ammissione;
//! - le funzioni della regola usate dai provider (filtro della lettura,
//!   parametri tipati della query relazionale, segnaposto descritto) si
//!   chiamano da qui ([`read_filter`], [`query_parameters`], [`described`]);
//!   la prova
//!   `tests/admission_gate_inventory.rs` rifiuta una chiamata diretta fuori
//!   dal core.
//!
//! # I punti d'ingresso
//!
//! | ingresso | cancello |
//! | --- | --- |
//! | facade `execute_portable*` (SDK Python `execute_portable_*`, CLI `portable-execute`, `testing`) | [`portable`] |
//! | DML relazionale dello SDK (`execute` / `execute_returning_rows` con piano `mutation`) | [`mutation`] |
//! | query relazionale dello SDK (piano `query`) | [`query`], eseguita con `facade::query_admitted` |
//! | `facade::execute_judged` / `query_judged` con un piano | [`portable`], [`mutation`]; una query con parametri si rifiuta |
//! | `Provider::query` (runtime) | [`query_parameters`] |
//! | lettura plan-based (`Provider::read`, filtro) | [`read_filter`] |
//! | parameter array e SQL del chiamante Db2 | [`described`], sul tipo che `SQLDescribeParam` da al segnaposto |
//! | scrittura Arrow (`Provider::write`) | la regola delle scritture della #134 (`store_rewrite`) |
//! | SQL del chiamante (`TransactionScope::execute`/`query`) su PostgreSQL, `MySQL`, `MariaDB`, SQL Server, Oracle | limite dichiarato in [`crate::operand_admission`] |
//!
//! [`CompiledPortable`]: crate::portable::CompiledPortable

use crate::plan::ProviderKind;
use crate::portable::{compile_portable, PortableStatement};
use crate::provider::ParameterValue;
use crate::relational::MutationOperation;
use crate::spatial_operand::ParameterFamilies;
use crate::transaction::{Statement, TransactionScope};
use crate::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};

pub use crate::operand_admission::{
    admit as described, admit_filter as read_filter, admit_typed_parameters as query_parameters,
};

/// Lo statement eseguibile di un AST portable: ogni letterale ammesso sul
/// tipo della colonna che incontra, letto dal catalogo della transazione
/// (`TransactionScope::column_types`), poi la compilazione.
///
/// # Errors
///
/// Quelli della lettura del catalogo, dell'ammissione e della compilazione.
pub async fn portable(
    tx: &mut dyn TransactionScope,
    statement: &PortableStatement,
    cancellation: &CancellationToken,
) -> Result<Statement> {
    let provider = tx.provider_kind();
    let admitted = admit_on_catalog(tx, statement, cancellation).await?;
    Ok(compile_portable(provider, &admitted)?.into_admitted_statement())
}

/// Uno statement relazionale passato dal cancello: si costruisce soltanto
/// qui ([`mutation`], [`query`]) e si esegue con
/// [`crate::facade::query_admitted`] e [`crate::facade::execute_admitted`].
///
/// Tiene anche le famiglie dei valori **del chiamante**
/// ([`ParameterFamilies`]): le regole F, T e C (`spatial_operand`,
/// `cast_table`) giudicano i valori come il chiamante li ha dati, e
/// l'ammissione li riscrive nel tipo dell'operando; valgono tutte e due.
#[derive(Debug, Clone, PartialEq)]
pub struct Admitted {
    statement: Statement,
    families: ParameterFamilies,
}

impl Admitted {
    /// Lo statement ammesso, da leggere.
    #[must_use]
    pub const fn statement(&self) -> &Statement {
        &self.statement
    }

    /// Le famiglie dei valori del chiamante, per `facade::verify_operands_bound`.
    #[must_use]
    pub const fn families(&self) -> &ParameterFamilies {
        &self.families
    }

    pub(crate) fn into_statement(self) -> Statement {
        self.statement
    }
}

/// Una query relazionale con i valori del chiamante.
///
/// I valori sono nell'ordine dei segnaposto che il renderer ha reso (`bind_names`, un nome per
/// segnaposto): i parametri tipizzati si ammettono sul tipo che il renderer
/// scrive nel `CAST` ([`query_parameters`]), e lo statement porta i valori
/// ammessi. Un nome legato due volte con valori diversi si rifiuta.
///
/// # Errors
///
/// `InvalidPlan` se i valori non sono quanti i segnaposto
/// (`BIND_LAYOUT_MISMATCH`) o fuori dal tipo dichiarato (regola T); quelli
/// di [`query_parameters`].
pub fn query(
    provider: ProviderKind,
    operation: &crate::relational::QueryOperation,
    sql: String,
    bind_names: &[String],
    values: &[ParameterValue],
) -> Result<Admitted> {
    // Le famiglie dei valori del chiamante e la regola T prima della
    // riscrittura: un valore fuori dal tipo dichiarato si rifiuta come tale,
    // e non come valore inesatto nel tipo del `CAST`.
    let families = ParameterFamilies::from_positional(provider, bind_names, values)?;
    families.judge_typed(
        provider,
        &crate::spatial_operand::OperandPlan::Query(operation),
    )?;
    let mut named = std::collections::BTreeMap::new();
    for (name, value) in bind_names.iter().zip(values) {
        if named
            .insert(name.clone(), value.clone())
            .is_some_and(|previous| &previous != value)
        {
            return Err(DatabaseError::invalid_plan(
                "lo stesso parametro della query con valori diversi",
            ));
        }
    }
    let admitted = query_parameters(
        provider,
        operation,
        &crate::provider::ParameterBag::new(named),
    )?;
    let params = bind_names
        .iter()
        .map(|name| {
            admitted
                .get(name)
                .cloned()
                .ok_or_else(|| DatabaseError::invalid_plan("parametro della query senza valore"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Admitted {
        statement: Statement::new(sql).with_params(params),
        families,
    })
}

/// Lo statement eseguibile di un DML relazionale con i valori del chiamante.
///
/// `values` e nell'ordine dei bind del piano: il piano si abbassa
/// nell'AST portable con quei valori, si ammette come [`portable`] e si
/// compila. I valori spatial restano quelli del chiamante: li giudica la
/// regola degli operandi geometrici (`facade::verify_operands`). Un
/// parametro tipizzato confrontato si ammette sul tipo del suo `CAST`
/// ([`crate::cast_table::rendered_target`]).
///
/// # Errors
///
/// `InvalidPlan` se i valori non sono quanti i bind del piano; quelli di
/// [`portable`].
pub async fn mutation(
    tx: &mut dyn TransactionScope,
    operation: &MutationOperation,
    values: &[ParameterValue],
    cancellation: &CancellationToken,
) -> Result<Admitted> {
    let provider = tx.provider_kind();
    let mut binds = crate::relational_mutation::Binds::values(values);
    let (statement, _) = crate::relational_mutation::lower(operation, &mut binds)?;
    if binds.names.len() != values.len() {
        return Err(DatabaseError::invalid_plan(
            "valori DML diversi dai segnaposto del piano",
        ));
    }
    let families = ParameterFamilies::from_positional(provider, &binds.names, values)?;
    let admitted = admit_on_catalog(tx, &statement, cancellation).await?;
    let mut compiled = compile_portable(provider, &admitted)?.into_admitted_statement();
    if compiled.params.len() != values.len() {
        return Err(layout_error(provider));
    }
    for position in binds.spatial {
        compiled.params[position] = values[position].clone();
    }
    // Un parametro tipizzato confrontato si rende `CAST(? AS tipo)` con una
    // sentinella (regola T): il valore del chiamante si ammette sul tipo che
    // il renderer scrive nel `CAST`, come nella query relazionale.
    for (position, parameter_type) in binds.typed {
        compiled.params[position] = described(
            provider,
            &values[position],
            &crate::cast_table::rendered_target(provider, parameter_type),
        )?;
    }
    Ok(Admitted {
        statement: compiled,
        families,
    })
}

async fn admit_on_catalog(
    tx: &mut dyn TransactionScope,
    statement: &PortableStatement,
    cancellation: &CancellationToken,
) -> Result<PortableStatement> {
    let provider = tx.provider_kind();
    let relation = crate::operand_admission::statement_relation(statement);
    let types = tx
        .column_types(&relation, cancellation)
        .await?
        .unwrap_or_default();
    crate::operand_admission::admit_portable(provider, statement, &types)
}

fn layout_error(provider: ProviderKind) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Internal,
        ErrorPhase::Prepare,
        Some(provider),
        "layout bind DML incoerente dopo il lowering",
    )
}
