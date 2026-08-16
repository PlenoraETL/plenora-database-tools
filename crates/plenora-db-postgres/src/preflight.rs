use crate::error::{classify_error, public_error};
use crate::pool::PooledClient;
use crate::{PostgresProvider, PostgresSchemaEvolution};
use arrow_schema::SchemaRef;
use plenora_database_core::loss::{LossCategory, LossReport, LossSeverity, MappingLoss};
use plenora_database_core::plan::{WriteMode, WriteOperation};
use plenora_database_core::protocol;
use plenora_database_core::provider::SecretString;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};

#[allow(clippy::too_many_lines)]
pub async fn write(
    provider: &PostgresProvider,
    secret: &SecretString,
    operation: &WriteOperation,
    input_schema: &SchemaRef,
) -> Result<LossReport> {
    let client = provider.connect_session(secret).await?;
    validate_resolved_crs_against_postgis(&client, input_schema).await?;
    let schema_name = operation.target.schema.as_deref().unwrap_or("public");
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
    let mut losses = Vec::new();
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
                    losses.push(MappingLoss {
                        field_id: u32::try_from(field_id).map_err(|_| {
                            DatabaseError::invalid_plan(
                                "numero colonne oltre il contratto LossReport",
                            )
                        })?,
                        category: LossCategory::NativeType,
                        severity: LossSeverity::Information,
                        reason: "colonna aggiunta al target come nullable".to_owned(),
                        source_type: Some(source.data_type().to_string()),
                        target_type: Some("nuova colonna nullable".to_owned()),
                    });
                } else {
                    losses.push(mapping_loss(
                        field_id,
                        LossCategory::NativeType,
                        "colonna sorgente assente nel target",
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
                    LossCategory::NativeType,
                    "tipo Arrow non equivalente al target PostgreSQL",
                    Some(source.data_type().to_string()),
                    Some(target_field.data_type().to_string()),
                )?);
            }
            if source.is_nullable() && !target.nullable {
                losses.push(mapping_loss(
                    field_id,
                    LossCategory::Nullability,
                    "sorgente nullable verso colonna NOT NULL",
                    None,
                    None,
                )?);
            }
            let source_srid = source
                .metadata()
                .get(protocol::GEOMETRY_SRID)
                .or_else(|| source.metadata().get("plenora.srid"));
            let target_srid = target_field
                .metadata()
                .get(protocol::GEOMETRY_SRID)
                .or_else(|| target_field.metadata().get("plenora.srid"));
            if source_srid != target_srid && (source_srid.is_some() || target_srid.is_some()) {
                losses.push(mapping_loss(
                    field_id,
                    LossCategory::Srid,
                    "SRID sorgente e target differenti",
                    source_srid.cloned(),
                    target_srid.cloned(),
                )?);
            }
            let source_crs = source.metadata().get(protocol::GEOMETRY_CRS_ID);
            let target_crs = target_field.metadata().get(protocol::GEOMETRY_CRS_ID);
            if source_crs != target_crs && (source_crs.is_some() || target_crs.is_some()) {
                losses.push(mapping_loss(
                    field_id,
                    LossCategory::Crs,
                    "identificatore CRS sorgente e target differente",
                    source_crs.cloned(),
                    target_crs.cloned(),
                )?);
            }
        }
        for key in &operation.keys {
            if !target_columns.iter().any(|column| &column.name == key) {
                return Err(DatabaseError::invalid_plan(
                    "chiave write assente nel target PostgreSQL",
                ));
            }
        }
    }
    let report = LossReport {
        schema_version: 1,
        policy: operation.mapping_policy,
        losses,
    };
    if !report.permits_execution() {
        return Err(public_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Prepare,
            false,
            "preflight PostgreSQL rileva conversioni non ammesse",
        ));
    }
    drop(client);
    Ok(report)
}

async fn validate_resolved_crs_against_postgis(
    client: &PooledClient,
    input_schema: &SchemaRef,
) -> Result<()> {
    for field in input_schema.fields() {
        if field
            .metadata()
            .get(protocol::GEOMETRY_CRS_RESOLUTION)
            .is_none_or(|value| value != "resolved")
        {
            continue;
        }
        let srid = field
            .metadata()
            .get(protocol::GEOMETRY_SRID)
            .and_then(|value| value.parse::<i32>().ok())
            .ok_or_else(|| DatabaseError::invalid_plan("CRS resolved senza SRID valido"))?;
        let declared_id = field
            .metadata()
            .get(protocol::GEOMETRY_CRS_ID)
            .ok_or_else(|| DatabaseError::invalid_plan("CRS resolved senza identificatore"))?;
        let row = client
            .client()?
            .query_opt(
                "SELECT auth_name, auth_srid
                 FROM spatial_ref_sys
                 WHERE srid = $1",
                &[&srid],
            )
            .await
            .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?
            .ok_or_else(|| {
                public_error(
                    ErrorCategory::Crs,
                    ErrorPhase::Prepare,
                    false,
                    "SRID resolved assente da spatial_ref_sys",
                )
            })?;
        let authority: Option<String> = row.get(0);
        let authority_code: Option<i32> = row.get(1);
        let observed_id = authority
            .filter(|value| !value.is_empty())
            .zip(authority_code)
            .map(|(name, code)| format!("{name}:{code}"))
            .ok_or_else(|| {
                public_error(
                    ErrorCategory::Crs,
                    ErrorPhase::Prepare,
                    false,
                    "spatial_ref_sys non risolve un authority ID",
                )
            })?;
        if !declared_id.eq_ignore_ascii_case(&observed_id) {
            return Err(public_error(
                ErrorCategory::Crs,
                ErrorPhase::Prepare,
                false,
                "identificatore CRS non coerente con spatial_ref_sys",
            ));
        }
    }
    Ok(())
}

fn mapping_loss(
    field_id: usize,
    category: LossCategory,
    reason: &str,
    source_type: Option<String>,
    target_type: Option<String>,
) -> Result<MappingLoss> {
    Ok(MappingLoss {
        field_id: u32::try_from(field_id).map_err(|_| {
            DatabaseError::invalid_plan("numero colonne oltre il contratto LossReport")
        })?,
        category,
        severity: LossSeverity::DataLoss,
        reason: reason.to_owned(),
        source_type,
        target_type,
    })
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
