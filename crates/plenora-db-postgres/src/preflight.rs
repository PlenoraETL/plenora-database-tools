use crate::error::{classify_error, public_error};
#[cfg(test)]
use crate::geometry_properties::crs_changes;
use crate::write::LeafLoss;
use crate::{PostgresProvider, PostgresSchemaEvolution};
use arrow_schema::SchemaRef;
use plenora_database_core::loss::{LossReport, MappingPolicy};
use plenora_database_core::loss_registry::{RuleId, RuleInstance, RuleParameters};
use plenora_database_core::plan::{ProviderKind, WriteMode, WriteOperation};
use plenora_database_core::primary_key::validate_create_primary_key;

use plenora_database_core::provider::SecretString;
use plenora_database_core::store_rewrite::ColumnRewrites;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};

#[allow(clippy::too_many_lines)]
pub async fn write(
    provider: &PostgresProvider,
    secret: &SecretString,
    operation: &WriteOperation,
    input_schema: &SchemaRef,
    metadata_plan: &crate::geometry_properties::SchemaPlan,
) -> Result<(
    LossReport,
    Vec<ColumnRewrites>,
    Vec<RuleInstance>,
    Option<crate::catalog::CatalogSchemaToken>,
)> {
    let client = provider.connect_session(secret).await?;
    crate::geometry_properties::validate_resolved_crs_against_postgis(&client, input_schema)
        .await?;
    let schema_name = crate::target_schema::schema_of(&operation.target)?;
    let exists: bool = client
        .client()?
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )",
            &[&schema_name, &operation.target.object],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?
        .get(0);
    if operation.mode == WriteMode::Create && exists {
        return Err(public_error(
            ErrorCategory::Conflict,
            ErrorPhase::Prepare,
            false,
            "target PostgreSQL già esistente",
        ));
    }
    // Solo Create crea la tabella: ogni altra mode — Replace inclusa —
    // scrive dentro un target che deve gia esistere. Replace svuota e
    // riempie, non ricrea, quindi un target assente e NotFound e non un
    // invito a fabbricarne uno con lo schema parziale del piano.
    if operation.mode != WriteMode::Create && !exists {
        return Err(public_error(
            ErrorCategory::NotFound,
            ErrorPhase::Prepare,
            false,
            "target PostgreSQL non esistente",
        ));
    }
    // `create_spatial_index` appartiene alla sola mode che costruisce la
    // tabella. Per le altre il target esiste gia con i suoi indici, e
    // `create_spatial_indexes` emette `CREATE INDEX` senza `IF NOT EXISTS`:
    // onorare il flag su un Append fallirebbe "already exists" alla seconda
    // esecuzione, ignorarlo in silenzio e peggio. Il piano viene rifiutato.
    if operation.create_spatial_index && operation.mode != WriteMode::Create {
        return Err(public_error(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Prepare,
            false,
            "create_spatial_index PostgreSQL e applicabile solo a WriteMode::Create: \
             le altre mode scrivono in un target che ha gia i propri indici",
        ));
    }
    // Le chiavi di Create diventano PRIMARY KEY. Il core verifica presenza,
    // assenza di duplicati e non-nullability prima della DDL, evitando che
    // il server modifichi implicitamente lo schema dichiarato. I vincoli di
    // tipo restano specifici del provider.
    if operation.mode == WriteMode::Create {
        if let Err(violation) = validate_create_primary_key(input_schema, &operation.keys) {
            return Err(public_error(
                ErrorCategory::InvalidPlan,
                ErrorPhase::Prepare,
                false,
                &violation.message("PostgreSQL"),
            ));
        }
    }
    if operation.mode == WriteMode::Replace {
        // Replace conserva la definizione del target: dichiarare chiavi
        // significa descrivere una tabella che Replace non costruisce.
        // Accettarle e ignorarle lascerebbe credere che il target venga
        // creato o riconciliato con quelle chiavi.
        if !operation.keys.is_empty() || !operation.update_columns.is_empty() {
            return Err(public_error(
                ErrorCategory::InvalidPlan,
                ErrorPhase::Prepare,
                false,
                "Replace PostgreSQL non ha semantica di chiave: svuota e riempie il \
                 target esistente, quindi keys e update_columns non sono \
                 applicabili (per creare la tabella con una PRIMARY KEY \
                 usare WriteMode::Create)",
            ));
        }
    }
    let snapshot = if exists {
        Some(crate::catalog::schema_token(client.client()?, &operation.target).await?)
    } else {
        None
    };
    // La regola A (`write::admission`): ogni colonna cerca la chiave
    // d'ammissione della propria foglia nel fixture della major del server;
    // una coppia non misurata rifiuta la scrittura con ogni politica.
    let admitted =
        crate::write::admission::preflight(client.client()?, operation, input_schema).await?;
    // La regola K sotto collation: una chiave con una collation non
    // deterministica si rifiuta prima di ogni effetto.
    if exists && operation.mode != WriteMode::Create {
        crate::write::visibility::refuse_nondeterministic_keys(
            client.client()?,
            operation,
            ErrorPhase::Prepare,
        )
        .await?;
    }
    // Le perdite delle foglie del tipo di ogni colonna (domini, array, range,
    // multirange, composti), lo stesso cammino con cui `Strict` rifiuta i
    // valori nella scrittura (`write::leaf_losses`): con le altre politiche
    // il valore passa e la perdita si dichiara qui.
    let leaves = crate::write::leaf_losses(client.client()?, operation, input_schema).await?;
    let leaf = |name: &str, loss: LeafLoss| {
        leaves
            .get(name)
            .is_some_and(|losses| losses.contains(&loss))
    };
    // In una chiave un valore riscritto si rifiuta con ogni politica (regola
    // S): non e una perdita da dichiarare.
    let is_key = |name: &str| operation.keys.iter().any(|key| key == name);
    let mut losses = crate::geometry_properties::representation_rules(input_schema)?;
    if operation.mode != WriteMode::DeleteByKeys {
        losses.extend(metadata_plan.rules()?);
    }
    if exists && operation.mode != WriteMode::Create {
        let (target_columns, _schema_token) =
            provider.cached_columns(&client, &operation.target).await?;
        for (field_id, source) in input_schema.fields().iter().enumerate() {
            let Some(target) = target_columns
                .iter()
                .find(|column| column.name == source.name().as_str())
            else {
                if provider.schema_evolution == PostgresSchemaEvolution::AddNullableColumns
                    && supports_additive_evolution(operation.mode)
                {
                    let mut create = operation.clone();
                    create.mode = WriteMode::Create;
                    let added_leaves =
                        crate::write::leaf_losses(client.client()?, &create, input_schema).await?;
                    if let Some(leaves) = added_leaves.get(source.name()) {
                        for rule in select_leaf_rules(
                            operation.mapping_policy,
                            is_key(source.name()),
                            leaves.iter().map(|leaf| (*leaf, true)),
                        ) {
                            losses.push(instance(
                                rule,
                                field_number(field_id)?,
                                Some(source.data_type().to_string()),
                                None,
                            ));
                        }
                    }
                    losses.push(instance(
                        RuleId::PgAddNullable,
                        field_number(field_id)?,
                        Some(source.data_type().to_string()),
                        Some("nuova colonna nullable".to_owned()),
                    ));
                } else {
                    losses.push(mapping_loss(
                        field_id,
                        RuleId::PgMissingColumn,
                        Some(source.data_type().to_string()),
                        None,
                    )?);
                }
                continue;
            };
            let target_field = target.arrow_field();
            if source.data_type() != target_field.data_type() {
                losses.push(mapping_loss(
                    field_id,
                    RuleId::PgArrowMismatch,
                    Some(source.data_type().to_string()),
                    Some(target_field.data_type().to_string()),
                )?);
            }
            // La regola P: una colonna temporale con precisione ridotta
            // (`timestamp(0)`, `time(3)`, `interval day`, anche dentro un
            // dominio, un array o un composto) arrotonda o tronca un valore
            // al microsecondo. Fino al 25/10 10:00:00.999999 diventava
            // 10:00:01 in ogni profilo, con `Strict`, dove la scala di un
            // `numeric` si rifiutava. Lo stesso criterio di MySQL
            // (`DATETIME(0)`), e la scrittura lo rifiuta di nuovo nella sua
            // transazione (`write::canonical::refuse_lossy`).
            let declarations = std::iter::once(target.native_declaration.as_deref())
                .chain(std::iter::once(target.domain_base_type.as_deref()))
                .flatten()
                .chain(
                    target
                        .composite_fields
                        .iter()
                        .map(|field| field.declaration.as_str()),
                );
            let reduced = declarations
                .into_iter()
                .find(|declaration| crate::write::reduced_temporal(declaration))
                .map(str::to_owned);
            // Una lunghezza massima (`varchar(n)`, `char(n)`): il server tronca
            // in silenzio gli spazi finali oltre la lunghezza. Con `Strict` la
            // scrittura rifiuta quei valori uno per uno
            // (`write::canonical::check`); con le altre politiche la perdita
            // possibile si dichiara qui.
            let bounded = target
                .native_declaration
                .as_deref()
                .or(target.domain_base_type.as_deref())
                .is_some_and(|declaration| {
                    let base = declaration.trim_end_matches("[]");
                    (base.starts_with("character varying(") || base.starts_with("character("))
                        && base.ends_with(')')
                })
                || leaf(source.name(), LeafLoss::Bounded)
                || leaf(source.name(), LeafLoss::Padded);
            // La regola S: `char(n)` completa con spazi un testo piu corto.
            // Con `Strict` la scrittura lo rifiuta valore per valore (e in una
            // chiave con ogni politica); con le altre politiche si dichiara.
            let padded = target
                .native_declaration
                .as_deref()
                .or(target.domain_base_type.as_deref())
                .is_some_and(|declaration| {
                    let base = declaration.trim_end_matches("[]");
                    base.starts_with("character(") && base.ends_with(')')
                })
                || leaf(source.name(), LeafLoss::Padded);
            // Un numero che arrotonda il testo (`numeric(p,s)`, `real`, `double
            // precision`), a qualunque profondita del tipo: con `Strict` la
            // scrittura rifiuta il valore testuale, con le altre politiche si
            // dichiara qui (`'1.005'` verso un dominio su `numeric(5,2)`
            // diventava `1.01` con il `LossReport` vuoto).
            // La regola J: `jsonb` normalizza l'oggetto (ordine delle chiavi,
            // spazi) e di una chiave ripetuta tiene l'ultimo valore. Con
            // `Strict` la scrittura rifiuta le chiavi ripetute valore per
            // valore (`write::canonical`), e l'ordine e gli spazi sono la
            // semantica del tipo; con le altre politiche la perdita possibile
            // si dichiara qui.
            // Il tipo base di un dominio, o la colonna, e i campi di un
            // composto: `native_declaration` di un dominio e il suo nome.
            let jsonb = target
                .domain_base_type
                .as_deref()
                .or(target.native_declaration.as_deref())
                .into_iter()
                .chain(
                    target
                        .composite_fields
                        .iter()
                        .map(|field| field.declaration.as_str()),
                )
                .any(|declaration| declaration.trim_end_matches("[]") == "jsonb")
                || leaf(source.name(), LeafLoss::Jsonb);
            let selected = select_leaf_rules(
                operation.mapping_policy,
                is_key(source.name()),
                [
                    (
                        LeafLoss::ReducedTemporal,
                        reduced.is_some() || leaf(source.name(), LeafLoss::ReducedTemporal),
                    ),
                    (LeafLoss::Bounded, bounded),
                    (LeafLoss::Padded, padded),
                    (LeafLoss::Rounding, leaf(source.name(), LeafLoss::Rounding)),
                    (LeafLoss::Jsonb, jsonb),
                ],
            );
            for rule in selected {
                let target_type = if rule == RuleId::PgTemporal {
                    reduced
                        .clone()
                        .or_else(|| target.native_declaration.clone())
                } else {
                    target.native_declaration.clone()
                };
                losses.push(instance(
                    rule,
                    field_number(field_id)?,
                    Some(source.data_type().to_string()),
                    target_type,
                ));
            }
            if source.is_nullable() && !target.nullable {
                losses.push(nullability_loss(field_id, operation.mapping_policy)?);
            }
            losses.extend(crate::geometry_properties::target_rules(
                source,
                &target_field,
                field_number(field_id)?,
                operation.mapping_policy,
            )?);
        }
        for key in &operation.keys {
            if !target_columns.iter().any(|column| &column.name == key) {
                return Err(DatabaseError::invalid_plan(
                    "chiave write assente nel target PostgreSQL",
                ));
            }
        }
    }
    // Le regole J e P per Create: la colonna che il campo dichiara, e per un
    // tipo dell'utente le foglie del tipo nel catalogo (`write::leaf_losses`).
    if operation.mode == WriteMode::Create {
        for (field_id, source) in input_schema.fields().iter().enumerate() {
            let contract = crate::field_contract::FieldContract::parse(source)?;
            let declared = contract
                .native_declaration
                .or(contract.native_type)
                .map(str::to_owned);
            let name = source.name().as_str();
            let jsonb = declared
                .as_deref()
                .is_some_and(|declaration| declaration.trim_end_matches("[]") == "jsonb");
            for rule in select_leaf_rules(
                operation.mapping_policy,
                is_key(name),
                [
                    (
                        LeafLoss::ReducedTemporal,
                        leaf(name, LeafLoss::ReducedTemporal),
                    ),
                    (
                        LeafLoss::Bounded,
                        leaf(name, LeafLoss::Bounded) || leaf(name, LeafLoss::Padded),
                    ),
                    (LeafLoss::Padded, leaf(name, LeafLoss::Padded)),
                    (LeafLoss::Rounding, leaf(name, LeafLoss::Rounding)),
                    (LeafLoss::Jsonb, jsonb || leaf(name, LeafLoss::Jsonb)),
                ],
            ) {
                losses.push(instance(
                    rule,
                    field_number(field_id)?,
                    Some(source.data_type().to_string()),
                    declared.clone(),
                ));
            }
        }
    }
    // La regola S con le riscritture misurate: con le politiche non strette
    // la perdita di ogni colonna che non e chiave (con `Strict` il valore
    // riscritto si rifiuta nella scrittura, in una chiave con ogni politica).
    if operation.mapping_policy != MappingPolicy::Strict {
        for column in &admitted {
            if !is_key(input_schema.field(column.field).name()) {
                for rewrite in &column.rewrites {
                    losses.push(instance(
                        rewrite.rule_id().canonical(ProviderKind::Postgres),
                        field_number(column.field)?,
                        None,
                        None,
                    ));
                }
            }
        }
    }
    let (instances, execution) = plenora_database_core::loss_registry::compile_instances(
        ProviderKind::Postgres,
        operation.mapping_policy,
        losses,
    )?;
    let report = LossReport {
        schema_version: 2,
        policy: operation.mapping_policy,
        losses: execution
            .risk
            .iter()
            .filter(|risk| risk.subject.is_none())
            .filter(|risk| {
                operation.mapping_policy != MappingPolicy::Strict
                    || risk.rule_id != RuleId::PgGeometryRepresentation
            })
            .map(|risk| {
                let mut loss = risk.rule_id.legacy(
                    risk.field_id,
                    risk.source_type.clone(),
                    risk.target_type.clone(),
                );
                loss.severity = risk.severity;
                loss
            })
            .collect(),
    };
    // Un report si accumula una perdita per campo, e i nomi di tipo arrivano
    // dallo schema sorgente: entrambe le dimensioni dipendono dal target, non
    // da noi. Il contratto ha dei tetti, e questo e il punto in cui si sa
    // ancora quale operazione stava producendo il documento.
    report.validate()?;
    if !report.permits_execution() {
        return Err(public_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Prepare,
            false,
            "preflight PostgreSQL rileva conversioni non ammesse",
        ));
    }
    if let Some(expected) = &snapshot {
        if &crate::catalog::schema_token(client.client()?, &operation.target).await? != expected {
            return Err(DatabaseError::invalid_plan(
                "contratto PostgreSQL cambiato durante il preflight",
            ));
        }
    }
    drop(client);
    Ok((report, admitted, instances, snapshot))
}

// Un authority ID letto dal catalogo arricchisce uno SRID gia dichiarato:
// non cambia le coordinate o il riferimento della geometria. Due identita
// esplicite discordanti restano invece un rischio, anche con SRID uguale.
fn field_number(field_id: usize) -> Result<u32> {
    u32::try_from(field_id)
        .map_err(|_| DatabaseError::invalid_plan("numero colonne oltre il contratto LossReport"))
}

/// Selector puro usato sia per Create sia per target esistente.
pub fn select_leaf_rules(
    policy: MappingPolicy,
    key: bool,
    conditions: impl IntoIterator<Item = (LeafLoss, bool)>,
) -> Vec<RuleId> {
    conditions
        .into_iter()
        .filter_map(|(leaf, present)| {
            if !present {
                return None;
            }
            let rule = match leaf {
                LeafLoss::ReducedTemporal => RuleId::PgTemporal,
                LeafLoss::Bounded => RuleId::PgTextLimit,
                LeafLoss::Padded => RuleId::PgPadText,
                LeafLoss::Rounding => RuleId::PgNumeric,
                LeafLoss::Jsonb => RuleId::PgJsonb,
            };
            if (policy == MappingPolicy::Strict && rule != RuleId::PgTemporal)
                || (key && rule == RuleId::PgPadText)
            {
                None
            } else {
                Some(rule)
            }
        })
        .collect()
}

// NOT NULL rifiuta il valore nullo; non converte un valore valido. Strict
// conserva il precedente rifiuto strutturale, le altre policy non richiedono
// consenso per un rischio di rifiuto che non altera dati.
fn nullability_loss(field_id: usize, _policy: MappingPolicy) -> Result<RuleInstance> {
    mapping_loss(field_id, RuleId::PgNullability, None, None)
}

fn mapping_loss(
    field_id: usize,
    rule: RuleId,
    source_type: Option<String>,
    target_type: Option<String>,
) -> Result<RuleInstance> {
    Ok(instance(
        rule,
        field_number(field_id)?,
        source_type,
        target_type,
    ))
}
fn instance(
    rule_id: RuleId,
    field_id: u32,
    source_type: Option<String>,
    target_type: Option<String>,
) -> RuleInstance {
    RuleInstance {
        subject: None,
        rule_id,
        field_id,
        source_type,
        target_type,
        parameters: RuleParameters::default(),
    }
}

const fn supports_additive_evolution(mode: WriteMode) -> bool {
    matches!(
        mode,
        WriteMode::Append
            | WriteMode::Replace
            | WriteMode::TruncateInsert
            | WriteMode::Update
            | WriteMode::Upsert
    )
}

#[cfg(test)]
#[path = "preflight_length_tests.rs"]
mod length_tests;
