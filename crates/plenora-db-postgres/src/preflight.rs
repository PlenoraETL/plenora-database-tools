use crate::error::{classify_error, public_error};
use crate::pool::PooledClient;
use crate::write::LeafLoss;
use crate::{PostgresProvider, PostgresSchemaEvolution};
use arrow_schema::SchemaRef;
use plenora_database_core::loss::{
    LossCategory, LossReport, LossSeverity, MappingLoss, MappingPolicy,
};
use plenora_database_core::plan::{ProviderKind, WriteMode, WriteOperation};
use plenora_database_core::primary_key::validate_create_primary_key;
use plenora_database_core::protocol;
use plenora_database_core::provider::SecretString;
use plenora_database_core::store_rewrite::{ColumnRewrites, RewriteGuard};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};

#[allow(clippy::too_many_lines)]
pub async fn write(
    provider: &PostgresProvider,
    secret: &SecretString,
    operation: &WriteOperation,
    input_schema: &SchemaRef,
) -> Result<(LossReport, Vec<ColumnRewrites>)> {
    let client = provider.connect_session(secret).await?;
    validate_resolved_crs_against_postgis(&client, input_schema).await?;
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
    let strict = operation.mapping_policy == MappingPolicy::Strict;
    // In una chiave un valore riscritto si rifiuta con ogni politica (regola
    // S): non e una perdita da dichiarare.
    let is_key = |name: &str| operation.keys.iter().any(|key| key == name);
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
            if reduced.is_some() || leaf(source.name(), LeafLoss::ReducedTemporal) {
                losses.push(reduced_temporal_loss(
                    field_id,
                    source,
                    reduced.or_else(|| target.native_declaration.clone()),
                )?);
            }
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
                || leaf(source.name(), LeafLoss::Bounded);
            if bounded && !strict {
                losses.push(bounded_loss(
                    field_id,
                    source,
                    target.native_declaration.clone(),
                )?);
            }
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
            if padded && !strict && !is_key(source.name()) {
                losses.push(padded_loss(
                    field_id,
                    source,
                    target.native_declaration.clone(),
                )?);
            }
            // Un numero che arrotonda il testo (`numeric(p,s)`, `real`, `double
            // precision`), a qualunque profondita del tipo: con `Strict` la
            // scrittura rifiuta il valore testuale, con le altre politiche si
            // dichiara qui (`'1.005'` verso un dominio su `numeric(5,2)`
            // diventava `1.01` con il `LossReport` vuoto).
            if !strict && leaf(source.name(), LeafLoss::Rounding) {
                losses.push(rounding_loss(
                    field_id,
                    source,
                    target.native_declaration.clone(),
                )?);
            }
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
            if jsonb && !strict {
                losses.push(jsonb_loss(
                    field_id,
                    source,
                    target.native_declaration.clone(),
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
            // Lo SRID che la colonna riceverebbe, dichiarato o derivato da
            // `EPSG:<n>` (GEO-008): un campo senza `srid` ma con `EPSG:4326`
            // verso una colonna 4326 non perde niente.
            let derived_srid = plenora_database_core::field_contract::FieldContract::parse(source)?
                .target_srid()?
                .map(|srid| srid.to_string());
            let source_srid = source
                .metadata()
                .get(protocol::GEOMETRY_SRID)
                .or_else(|| source.metadata().get("plenora.srid"))
                .or(derived_srid.as_ref());
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
            if leaf(name, LeafLoss::ReducedTemporal) {
                losses.push(reduced_temporal_loss(field_id, source, declared.clone())?);
            }
            if strict {
                continue;
            }
            if leaf(name, LeafLoss::Bounded) {
                losses.push(bounded_loss(field_id, source, declared.clone())?);
            }
            if leaf(name, LeafLoss::Padded) && !is_key(name) {
                losses.push(padded_loss(field_id, source, declared.clone())?);
            }
            if leaf(name, LeafLoss::Rounding) {
                losses.push(rounding_loss(field_id, source, declared.clone())?);
            }
            let jsonb = declared
                .as_deref()
                .is_some_and(|declaration| declaration.trim_end_matches("[]") == "jsonb");
            if jsonb || leaf(name, LeafLoss::Jsonb) {
                losses.push(jsonb_loss(field_id, source, declared)?);
            }
        }
    }
    // La regola S con le riscritture misurate: con le politiche non strette
    // la perdita di ogni colonna che non e chiave (con `Strict` il valore
    // riscritto si rifiuta nella scrittura, in una chiave con ogni politica).
    losses.extend(
        RewriteGuard::new(
            ProviderKind::Postgres,
            input_schema,
            operation,
            admitted.clone(),
        )?
        .losses(),
    );
    let report = LossReport {
        schema_version: 2,
        policy: operation.mapping_policy,
        losses,
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
    drop(client);
    Ok((report, admitted))
}

// Deviazione dichiarata (REJ-003, contracts/adoption-source.json): i rifiuti decisi
// leggendo il bersaglio escono con fase `prepare`, non `probe`.
/// Il CRS di ogni campo geometrico contro `spatial_ref_sys` del bersaglio,
/// prima di ogni effetto.
///
/// - uno SRID **derivato** da `crs_id` `EPSG:<n>` (GEO-008) deve essere
///   registrato come `EPSG:<n>`, qualunque sia lo stato di risoluzione: lo
///   stesso numero puo nominare un altro CRS (`54009` e `ESRI:54009` in
///   `PostGIS`) o nessuno;
/// - un CRS `resolved` con `crs_id` deve coincidere con l'autorita
///   registrata per il suo SRID;
/// - una colonna `geography` vuole uno SRID geografico: uno proiettato e un
///   limite del bersaglio (`unsupported`), uno sconosciuto un CRS che il
///   bersaglio non conosce (`crs`). Senza questa lettura il rifiuto arrivava
///   dalla DDL come `protocol`.
async fn validate_resolved_crs_against_postgis(
    client: &PooledClient,
    input_schema: &SchemaRef,
) -> Result<()> {
    let crs_error =
        |message: &str| public_error(ErrorCategory::Crs, ErrorPhase::Prepare, false, message);
    // `spatial_ref_sys` dello schema di PostGIS (regola Q), letto una volta.
    let mut postgis: Option<String> = None;
    for field in input_schema.fields() {
        let contract = plenora_database_core::field_contract::FieldContract::parse(field)?;
        if !contract.spatial {
            continue;
        }
        let resolved = contract.crs_resolution == Some("resolved") && contract.crs_id.is_some();
        let geography = contract.is_geography();
        // Lo SRID della colonna, dichiarato o derivato (GEO-008); lo schema e
        // gia stato verificato da `write::prepare_state`.
        let Some(target) = contract.target_srid_source()? else {
            continue;
        };
        let Ok(srid) = i32::try_from(target.srid) else {
            continue;
        };
        if srid == 0 || !(target.derived || resolved || geography) {
            continue;
        }
        let qualifier = if let Some(qualifier) = &postgis {
            qualifier.clone()
        } else {
            let qualifier =
                crate::postgis_schema::qualifier(client.client()?, ErrorPhase::Prepare).await?;
            postgis = Some(qualifier.clone());
            qualifier
        };
        let row = client
            .client()?
            .query_opt(
                &crate::postgis_schema::qualify(
                    "SELECT auth_name, auth_srid, \
                        (srtext::pg_catalog.text LIKE 'GEOGCS%' \
                         OR srtext::pg_catalog.text LIKE 'GEOGCRS%' \
                         OR proj4text::pg_catalog.text LIKE '%+proj=longlat%' \
                         OR proj4text::pg_catalog.text LIKE '%+proj=latlong%') \
                     FROM spatial_ref_sys WHERE srid = $1",
                    &qualifier,
                ),
                &[&srid],
            )
            .await
            .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?
            .ok_or_else(|| crs_error("SRID del campo assente da spatial_ref_sys"))?;
        let authority: Option<String> = row.get(0);
        let authority_code: Option<i32> = row.get(1);
        let geographic: Option<bool> = row.get(2);
        if target.derived
            && !(authority
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case("EPSG"))
                && authority_code == Some(srid))
        {
            return Err(crs_error(
                "SRID derivato da crs_id EPSG registrato in spatial_ref_sys con un'altra identita",
            ));
        }
        // Una definizione senza identificatore non si confronta con il
        // catalogo: il campo registra lo SRID che dichiara (GEO-004), e
        // `resolved` qui vuol dire `resolved` con `crs_id`.
        if let (true, Some(declared_id)) = (resolved, contract.crs_id) {
            let observed_id = authority
                .as_deref()
                .filter(|value| !value.is_empty())
                .zip(authority_code)
                .map(|(name, code)| format!("{name}:{code}"))
                .ok_or_else(|| crs_error("spatial_ref_sys non risolve un authority ID"))?;
            if !declared_id.eq_ignore_ascii_case(&observed_id) {
                return Err(crs_error(
                    "identificatore CRS non coerente con spatial_ref_sys",
                ));
            }
        }
        if geography && geographic != Some(true) {
            return Err(public_error(
                ErrorCategory::Unsupported,
                ErrorPhase::Prepare,
                false,
                "geography PostgreSQL richiede uno SRID geografico in spatial_ref_sys",
            ));
        }
    }
    Ok(())
}

fn field_number(field_id: usize) -> Result<u32> {
    u32::try_from(field_id)
        .map_err(|_| DatabaseError::invalid_plan("numero colonne oltre il contratto LossReport"))
}

/// La regola P: una precisione temporale sotto il microsecondo.
fn reduced_temporal_loss(
    field_id: usize,
    source: &arrow_schema::Field,
    target_type: Option<String>,
) -> Result<MappingLoss> {
    mapping_loss(
        field_id,
        LossCategory::Precision,
        "precisione temporale della colonna sotto il microsecondo: il valore si \
         arrotonderebbe o troncherebbe",
        Some(source.data_type().to_string()),
        target_type,
    )
}

/// Una lunghezza massima (`varchar(n)`, `char(n)`), anche dentro un dominio,
/// un array o un composto.
fn bounded_loss(
    field_id: usize,
    source: &arrow_schema::Field,
    target_type: Option<String>,
) -> Result<MappingLoss> {
    Ok(MappingLoss {
        field_id: field_number(field_id)?,
        category: LossCategory::Precision,
        severity: LossSeverity::Warning,
        reason: "gli spazi finali oltre la lunghezza della colonna si troncherebbero".to_owned(),
        source_type: Some(source.data_type().to_string()),
        target_type,
    })
}

/// `char(n)` completa con spazi un testo piu corto (regola S,
/// `plenora_database_core::store_rewrite`), anche dentro un dominio, un array
/// o un composto.
fn padded_loss(
    field_id: usize,
    source: &arrow_schema::Field,
    target_type: Option<String>,
) -> Result<MappingLoss> {
    Ok(MappingLoss {
        field_id: field_number(field_id)?,
        category: LossCategory::NativeType,
        severity: LossSeverity::DataLoss,
        reason: plenora_database_core::store_rewrite::StoreRewrite::PadText { chars: 0 }
            .reason()
            .to_owned(),
        source_type: Some(source.data_type().to_string()),
        target_type,
    })
}

/// Un numero che arrotonda il testo, anche dentro un dominio, un array, un
/// range o un composto.
fn rounding_loss(
    field_id: usize,
    source: &arrow_schema::Field,
    target_type: Option<String>,
) -> Result<MappingLoss> {
    Ok(MappingLoss {
        field_id: field_number(field_id)?,
        category: LossCategory::Precision,
        severity: LossSeverity::DataLoss,
        reason: "un numero del tipo della colonna (numeric con scala, real, double precision) \
                 arrotonderebbe il valore testuale"
            .to_owned(),
        source_type: Some(source.data_type().to_string()),
        target_type,
    })
}

/// La regola J: `jsonb`, anche dentro un dominio, un array o un composto.
fn jsonb_loss(
    field_id: usize,
    source: &arrow_schema::Field,
    target_type: Option<String>,
) -> Result<MappingLoss> {
    Ok(MappingLoss {
        field_id: field_number(field_id)?,
        category: LossCategory::NativeType,
        severity: LossSeverity::Warning,
        reason: "jsonb tiene l'ultimo valore di una chiave ripetuta e normalizza ordine delle \
                 chiavi e spazi"
            .to_owned(),
        source_type: Some(source.data_type().to_string()),
        target_type,
    })
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
