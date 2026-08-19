//! Path write `MySQL`. v1.2 estende Append (unica mode originale) con
//! Create (Blocco A). Upsert/Update/Replace/DeleteByKeys
//! pianificati per tranches future.

#![allow(
    clippy::doc_markdown,
    clippy::too_many_lines,
    clippy::option_if_let_else,
    clippy::redundant_pub_crate
)]

use crate::types::{mysql_identifier, mysql_renderer};
use crate::{MysqlColumnKind, MysqlColumnSpec, MysqlObjectDescription};
use chrono::{Datelike, NaiveDate, Timelike};
use mysql_async::{Params, Value};
use plenora_database_core::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::field_contract::FieldContract;
use plenora_database_core::loss::{LossReport, MappingPolicy};
use plenora_database_core::outcome::{
    CertainPhase, Recovery, RowCounts, WriteOutcome, WriteStatus,
};
use plenora_database_core::plan::{TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::primary_key::validate_create_primary_key;
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition,
};
use plenora_database_sql::ObjectName;

#[derive(Debug, Clone)]
struct MysqlWriteColumn {
    name: String,
    kind: MysqlColumnKind,
    nullable: bool,
    quoted: String,
    spatial_srid: Option<u32>,
    exact_geometry_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MysqlWritePlan {
    /// Mode dell'operazione, replicata dal piano per il rendering e il
    /// preflight (es. la policy fail-closed degli indici Upsert).
    mode: WriteMode,
    quoted_target: String,
    /// Nome del target senza quoting (per staging table naming).
    target_object_raw: String,
    columns: Vec<MysqlWriteColumn>,
    /// Colonne (quoted) da aggiornare nel `ON DUPLICATE KEY UPDATE` di
    /// una Upsert. Vuoto per Append/Create/TruncateInsert.
    upsert_update_columns: Vec<String>,
    /// Colonne key dell'Upsert (nomi grezzi) per la verifica fail-closed
    /// contro gli unique index del target. Vuoto per non-Upsert.
    upsert_keys: Vec<String>,
    /// Colonne key dell'Upsert (quoted) per la clausola `ON DUPLICATE KEY
    /// UPDATE` no-op degli Upsert keys-only. Vuoto per non-Upsert.
    upsert_keys_quoted: Vec<String>,
    /// Colonne (quoted) da usare come JOIN keys per Update via staging.
    /// Vuoto per non-Update modes.
    update_key_columns: Vec<String>,
    /// Colonne (quoted) da aggiornare in UPDATE (default: tutte non-key).
    update_set_columns: Vec<String>,
}

fn compile_write_column(
    field: &Field,
    renderer: &plenora_database_sql::Renderer,
    profile: &dyn crate::profile::ProductProfile,
) -> Result<MysqlWriteColumn> {
    let product = profile.product();
    let contract = FieldContract::parse(field)?;
    let (kind, spatial_srid, exact_geometry_type) = if contract.spatial {
        // Il primo cancello e il prodotto: leggere geometrie non dice nulla
        // su cosa il server accetti in ingresso, e senza quella prova il
        // piano si chiude qui invece che al primo INSERT.
        if !profile.write_spatial_is_qualified() {
            return Err(unsupported(
                "write spatial non qualificata per il prodotto servito",
            ));
        }
        if !contract.is_geometry()
            || contract.encoding != Some("wkb")
            || field.data_type() != &DataType::Binary
        {
            return Err(unsupported(format!(
                "write spatial {product} richiede geometry GeoArrow WKB Binary"
            )));
        }
        if contract.dimensions != Some("xy") {
            return Err(unsupported(format!(
                "write spatial {product} qualifica soltanto geometrie XY"
            )));
        }
        let srid = contract.srid.ok_or_else(|| {
            crs_error(format!("write spatial {product} richiede SRID dichiarato"))
        })?;
        let exact = match contract.types_declaration {
            Some("mixed") => None,
            Some("exact") => {
                let geometry_type = contract
                    .geometry_types
                    .ok_or_else(|| mapping_error("tipo geometrico exact assente dal contratto"))?;
                if geometry_type.contains(',') || !profile.writable_geometry_type(geometry_type) {
                    return Err(unsupported(format!(
                        "insieme di tipi geometrici non qualificato per {product}"
                    )));
                }
                Some(geometry_type.to_ascii_lowercase())
            }
            _ => {
                return Err(unsupported(format!(
                    "dichiarazione tipi geometrici non qualificata per {product}"
                )));
            }
        };
        (MysqlColumnKind::Geometry, Some(srid), exact)
    } else {
        (write_column_kind(field)?, None, None)
    };
    Ok(MysqlWriteColumn {
        name: field.name().clone(),
        kind,
        nullable: field.is_nullable(),
        quoted: renderer.quote_identifier(&mysql_identifier(field.name())?)?,
        spatial_srid,
        exact_geometry_type,
    })
}

fn validate_spatial_policy(operation: &WriteOperation, columns: &[MysqlWriteColumn]) -> Result<()> {
    let spatial = columns
        .iter()
        .any(|column| column.kind == MysqlColumnKind::Geometry);
    match (spatial, operation.srid_policy) {
        (true, Some(plenora_database_core::plan::SridPolicy::RequireMatch)) | (false, None) => {
            Ok(())
        }
        (true, _) => Err(unsupported(
            "write spatial richiede SridPolicy::RequireMatch",
        )),
        (false, Some(_)) => Err(unsupported(
            "srid_policy non appartiene a una append scalare",
        )),
    }
}

/// I tipi geometrici scrivibili come dichiarazione `exact`.
///
/// Vive qui perche qui e usata la lista in scrittura; il profilo la espone
/// come decisione, cosi un secondo prodotto puo restringerla senza duplicarla.
#[allow(clippy::redundant_pub_crate)]
pub(crate) fn geometry_type_is_writable(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "point"
            | "linestring"
            | "polygon"
            | "multipoint"
            | "multilinestring"
            | "multipolygon"
            | "geometrycollection"
    )
}

impl MysqlWritePlan {
    /// Compila il piano di scrittura di una `WriteOperation`.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso fuori dal sottoinsieme qualificato.
    /// Il piano di scrittura, con il profilo che decide la parte spatial.
    ///
    /// # Errors
    ///
    /// Come `compile`.
    pub(super) fn compile_with_profile(
        schema: &SchemaRef,
        operation: &WriteOperation,
        database: &str,
        profile: &dyn crate::profile::ProductProfile,
    ) -> Result<Self> {
        // Chi ha il profilo attribuisce da se: dipendere dal bordo del
        // provider funziona in produzione e lascia scoperto ogni chiamante
        // interno, che e poi il modo in cui questi errori si osservano nei
        // test.
        crate::profile::attributed(
            profile,
            Self::compile_unattributed(schema, operation, database, profile),
        )
    }

    fn compile_unattributed(
        schema: &SchemaRef,
        operation: &WriteOperation,
        database: &str,
        profile: &dyn crate::profile::ProductProfile,
    ) -> Result<Self> {
        let product = profile.product();
        plenora_database_core::field_contract::validate_schema_contract(schema.as_ref())?;
        validate_operation(operation, database)?;
        if schema.fields().is_empty() {
            return Err(prepare_error(
                ErrorCategory::Schema,
                format!("schema Arrow vuoto per append {product}"),
            ));
        }
        let renderer = mysql_renderer();
        let target_schema = operation.target.schema.as_deref().unwrap_or(database);
        let target = ObjectName {
            catalog: None,
            schema: Some(mysql_identifier(target_schema)?),
            object: mysql_identifier(&operation.target.object)?,
        };
        let columns = schema
            .fields()
            .iter()
            .map(|field| compile_write_column(field, &renderer, profile))
            .collect::<Result<Vec<_>>>()?;
        validate_spatial_policy(operation, &columns)?;
        if operation.mode == WriteMode::Create {
            // Le chiavi di `Create` diventano la PRIMARY KEY. Presenza,
            // nullability e ripetizioni sono strutturali e valgono su ogni
            // provider: le verifica il core, una volta sola. Prima la
            // presenza veniva controllata anche qui, con un secondo
            // messaggio per lo stesso difetto: quale dei due arrivasse al
            // chiamante dipendeva dall'ordine dei controlli, non dal piano.
            if let Err(violation) = validate_create_primary_key(schema, &operation.keys) {
                return Err(prepare_error(
                    ErrorCategory::InvalidPlan,
                    violation.message(product),
                ));
            }
            validate_primary_key_parts(&operation.keys)?;
            for key in &operation.keys {
                let Some(column) = columns.iter().find(|c| c.name == *key) else {
                    // Irraggiungibile: `columns` deriva 1:1 dai campi dello
                    // stesso schema su cui il core ha appena provato la
                    // presenza. Se accadesse sarebbe un difetto nostro, non
                    // un piano invalido, e la categoria lo dice.
                    return Err(prepare_error(
                        ErrorCategory::Internal,
                        format!(
                            "colonna '{key}' presente nello schema Arrow ma \
                             assente dalle colonne compilate"
                        ),
                    ));
                };
                validate_primary_key_column(key, column)?;
            }
        } else if matches!(operation.mode, WriteMode::Upsert | WriteMode::DeleteByKeys) {
            // Qui le chiavi non costruiscono una PRIMARY KEY: servono a
            // identificare le righe, e l'unico requisito e che esistano.
            // Per DeleteByKeys lo schema porta solo le colonne chiave, che
            // formano il filtro del DELETE.
            for key in &operation.keys {
                if !columns.iter().any(|column| column.name == *key) {
                    return Err(prepare_error(
                        ErrorCategory::InvalidPlan,
                        format!(
                            "chiave {:?} '{key}' assente dallo schema Arrow",
                            operation.mode
                        ),
                    ));
                }
            }
        }
        if operation.mode == WriteMode::DeleteByKeys {
            for col in &columns {
                if !operation.keys.contains(&col.name) {
                    return Err(prepare_error(
                        ErrorCategory::InvalidPlan,
                        format!(
                            "DeleteByKeys: colonna '{}' non è una key — schema Arrow \
 deve contenere solo le colonne key",
                            col.name
                        ),
                    ));
                }
            }
        }
        let upsert_update_columns = if operation.mode == WriteMode::Upsert {
            columns
                .iter()
                .filter(|c| !operation.keys.contains(&c.name))
                .map(|c| c.quoted.clone())
                .collect()
        } else {
            Vec::new()
        };
        // Key columns dell'Upsert: nomi grezzi (per il match con gli unique
        // index del target) e quoted (per la clausola ON DUPLICATE no-op dei
        // keys-only). La presenza di ogni key nello schema è già verificata
        // sopra per Upsert/DeleteByKeys.
        let (upsert_keys, upsert_keys_quoted) = if operation.mode == WriteMode::Upsert {
            let quoted: Vec<String> = operation
                .keys
                .iter()
                .map(|k| {
                    columns
                        .iter()
                        .find(|c| &c.name == k)
                        .map(|c| c.quoted.clone())
                        .ok_or_else(|| {
                            prepare_error(
                                ErrorCategory::InvalidPlan,
                                format!("chiave Upsert '{k}' assente dallo schema Arrow"),
                            )
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            (operation.keys.clone(), quoted)
        } else {
            (Vec::new(), Vec::new())
        };
        // Update-specific: mappa keys + update columns quoted.
        let (update_key_columns, update_set_columns) = if operation.mode == WriteMode::Update {
            let key_quoted: Vec<String> = operation
                .keys
                .iter()
                .map(|k| {
                    columns
                        .iter()
                        .find(|c| &c.name == k)
                        .map(|c| c.quoted.clone())
                        .ok_or_else(|| {
                            prepare_error(
                                ErrorCategory::InvalidPlan,
                                format!("chiave Update '{k}' assente dallo schema Arrow"),
                            )
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            // Default: tutte le colonne non-key. Se update_columns esplicite, uso quelle.
            let set_names: Vec<&String> = if operation.update_columns.is_empty() {
                columns
                    .iter()
                    .filter(|c| !operation.keys.contains(&c.name))
                    .map(|c| &c.name)
                    .collect()
            } else {
                operation.update_columns.iter().collect()
            };
            if set_names.is_empty() {
                return Err(prepare_error(
                    ErrorCategory::InvalidPlan,
                    "Update: nessuna colonna da aggiornare (schema = keys only)",
                ));
            }
            let set_quoted: Vec<String> = set_names
                .iter()
                .map(|name| {
                    columns
                        .iter()
                        .find(|c| &c.name == *name)
                        .map(|c| c.quoted.clone())
                        .ok_or_else(|| {
                            prepare_error(
                                ErrorCategory::InvalidPlan,
                                format!(
                                    "Update: colonna '{name}' \
 non presente nello schema Arrow"
                                ),
                            )
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            (key_quoted, set_quoted)
        } else {
            (Vec::new(), Vec::new())
        };
        Ok(Self {
            mode: operation.mode,
            quoted_target: renderer.quote_object(&target)?,
            target_object_raw: operation.target.object.clone(),
            columns,
            upsert_update_columns,
            upsert_keys,
            upsert_keys_quoted,
            update_key_columns,
            update_set_columns,
        })
    }

    /// Nome staging table temporary per Update (deterministico per target).
    ///
    /// TEMPORARY table MySQL sono session-scoped; il naming deve solo
    /// evitare collision col target reale. Prefix `__pln_stg_` +
    /// short target hash è unica per sessione.
    #[must_use]
    pub(super) fn staging_temp_name(&self, execution_id: &str) -> String {
        format!(
            "__pln_stg_{}_{}",
            self.target_object_raw.chars().take(24).collect::<String>(),
            execution_id.replace('-', "_"),
        )
    }

    /// Renderizza `UPDATE target JOIN staging ON keys SET updates` per
    /// WriteMode::Update.
    #[must_use]
    pub(super) fn render_update_from_staging(&self, staging_quoted: &str) -> String {
        let on_clause = self
            .update_key_columns
            .iter()
            .map(|k| format!("t.{k} = s.{k}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let set_clause = self
            .update_set_columns
            .iter()
            .map(|c| format!("t.{c} = s.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "UPDATE {} AS t JOIN {staging_quoted} AS s ON {on_clause} SET {set_clause};",
            self.quoted_target
        )
    }

    /// Renderizza INSERT verso una staging table (target diverso dal target
    /// reale, senza clausola ON DUPLICATE KEY). Usato per Update via staging.
    ///
    /// # Errors
    ///
    /// Come `render_insert`.
    pub(super) fn render_insert_into_staging(
        &self,
        staging_quoted: &str,
        rows: usize,
    ) -> Result<String> {
        // Staging INSERT: mai ON DUPLICATE KEY (la staging è vuota e senza
        // vincoli in conflitto; il merge avviene dopo con UPDATE JOIN).
        self.render_insert_generic(staging_quoted, rows, None)
    }

    /// Corpo della clausola `ON DUPLICATE KEY UPDATE` per un Upsert verso il
    /// target reale, oppure `None` per le altre mode.
    ///
    /// - Upsert con colonne non-key: `col=VALUES(col), ...`.
    /// - Upsert **keys-only** (schema di sole key): `k0=k0` — un update no-op
    ///   che rende l'INSERT idempotente (insert-or-ignore) invece di fallire
    ///   con duplicate key. Senza questa clausola un Upsert keys-only
    ///   diventerebbe un INSERT nudo che erra sul primo conflitto.
    fn upsert_on_duplicate_clause(&self) -> Option<String> {
        if self.mode != WriteMode::Upsert {
            return None;
        }
        if self.upsert_update_columns.is_empty() {
            self.upsert_keys_quoted
                .first()
                .map(|key| format!("{key}={key}"))
        } else {
            Some(
                self.upsert_update_columns
                    .iter()
                    .map(|c| format!("{c}=VALUES({c})"))
                    .collect::<Vec<_>>()
                    .join(", "),
            )
        }
    }

    /// Renderizza un INSERT multi-riga con soli placeholder.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso fuori dai limiti di binding di `MySQL`.
    pub(super) fn render_insert(&self, rows: usize) -> Result<String> {
        let on_duplicate = self.upsert_on_duplicate_clause();
        self.render_insert_generic(&self.quoted_target, rows, on_duplicate.as_deref())
    }

    fn render_insert_generic(
        &self,
        target: &str,
        rows: usize,
        on_duplicate: Option<&str>,
    ) -> Result<String> {
        if rows == 0 {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "INSERT richiede almeno una riga",
            ));
        }
        let placeholder_count = rows.checked_mul(self.columns.len()).ok_or_else(|| {
            prepare_error(
                ErrorCategory::ResourceLimit,
                "overflow nel conteggio dei placeholder",
            )
        })?;
        if placeholder_count > crate::MAX_BIND_PARAMETERS {
            return Err(prepare_error(
                ErrorCategory::ResourceLimit,
                format!(
                    "INSERT con {placeholder_count} placeholder oltre il limite di {}",
                    crate::MAX_BIND_PARAMETERS
                ),
            ));
        }

        let quoted_columns = self
            .columns
            .iter()
            .map(|column| column.quoted.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let row_placeholders = self
            .columns
            .iter()
            .map(|column| {
                column.spatial_srid.map_or_else(
                    || "?".to_owned(),
                    |srid| format!("ST_GeomFromWKB(?, {srid})"),
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!("INSERT INTO {target} ({quoted_columns}) VALUES ");
        for row in 0..rows {
            if row > 0 {
                sql.push_str(", ");
            }
            sql.push('(');
            sql.push_str(&row_placeholders);
            sql.push(')');
        }
        // Upsert v1.2: ON DUPLICATE KEY UPDATE (corpo precalcolato da
        // `upsert_on_duplicate_clause`). Uso `VALUES(col)` (deprecato in
        // 8.0.20+ ma ancora funzionante in 8.4 LTS). Migrare a alias
        // `AS new / new.col` in un giro futuro.
        if let Some(updates) = on_duplicate {
            sql.push_str(" ON DUPLICATE KEY UPDATE ");
            sql.push_str(updates);
        }
        sql.push(';');
        Ok(sql)
    }

    #[must_use]
    pub(super) const fn rows_per_statement(&self) -> usize {
        crate::MAX_BIND_PARAMETERS / self.columns.len()
    }

    /// Renderizza `DELETE FROM target WHERE (k1, k2, ...) IN ((?, ?, ...), ...)`
    /// per WriteMode::DeleteByKeys. Il numero di colonne dello schema
    /// coincide con quello delle keys (`MysqlWritePlan::compile` fa la check).
    ///
    /// # Errors
    ///
    /// Fallisce fuori dai limiti di binding di `MySQL`.
    pub(super) fn render_delete_by_keys(&self, rows: usize) -> Result<String> {
        if rows == 0 {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "DELETE richiede almeno una riga di keys",
            ));
        }
        let placeholder_count = rows.checked_mul(self.columns.len()).ok_or_else(|| {
            prepare_error(
                ErrorCategory::ResourceLimit,
                "overflow nel conteggio dei placeholder",
            )
        })?;
        if placeholder_count > crate::MAX_BIND_PARAMETERS {
            return Err(prepare_error(
                ErrorCategory::ResourceLimit,
                format!(
                    "DELETE con {placeholder_count} placeholder oltre il limite di {}",
                    crate::MAX_BIND_PARAMETERS
                ),
            ));
        }
        let keys_tuple = self
            .columns
            .iter()
            .map(|c| c.quoted.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let row_placeholders = self
            .columns
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!(
            "DELETE FROM {} WHERE ({keys_tuple}) IN (",
            self.quoted_target
        );
        for row in 0..rows {
            if row > 0 {
                sql.push_str(", ");
            }
            sql.push('(');
            sql.push_str(&row_placeholders);
            sql.push(')');
        }
        sql.push_str(");");
        Ok(sql)
    }

    pub(super) fn bind_chunk(
        &self,
        batch: &RecordBatch,
        start: usize,
        rows: usize,
    ) -> Result<Params> {
        if rows == 0
            || start
                .checked_add(rows)
                .is_none_or(|end| end > batch.num_rows())
            || batch.num_columns() != self.columns.len()
        {
            return Err(write_error(
                ErrorCategory::InvalidPlan,
                "intervallo batch non valido",
            ));
        }
        let capacity = rows.checked_mul(self.columns.len()).ok_or_else(|| {
            write_error(
                ErrorCategory::ResourceLimit,
                "overflow nel conteggio dei bind",
            )
        })?;
        let mut values = Vec::with_capacity(capacity);
        for row in start..start + rows {
            for (index, column) in self.columns.iter().enumerate() {
                let array = batch.column(index);
                if array.is_null(row) {
                    if !column.nullable {
                        return Err(prepare_error(
                            ErrorCategory::DataMapping,
                            format!("NULL nella colonna non nullable `{}`", column.name),
                        ));
                    }
                    values.push(Value::NULL);
                } else {
                    values.push(bind_value(array.as_ref(), row, &column.kind)?);
                }
            }
        }
        Ok(Params::Positional(values))
    }

    pub(super) fn validate_spatial_batch(
        &self,
        batch: &RecordBatch,
        budget: &ResourceBudget,
    ) -> Result<plenora_database_core::ewkb::EwkbStats> {
        let mut components = 0_u64;
        let mut max_depth = 0_u64;
        for (index, column) in self.columns.iter().enumerate() {
            if column.kind != MysqlColumnKind::Geometry {
                continue;
            }
            let values = batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    write_error(
                        ErrorCategory::DataMapping,
                        "array geometry incoerente con il piano",
                    )
                })?;
            for row in 0..batch.num_rows() {
                if values.is_null(row) {
                    continue;
                }
                let remaining = budget.remaining(ResourceKind::GeometryComponents);
                let inspection = plenora_database_core::ewkb::inspect_ewkb_detailed(
                    values.value(row),
                    remaining,
                    budget.limits().nesting_depth,
                )
                .map_err(|mut error| {
                    error.phase = ErrorPhase::Write;
                    error.provider = Some(crate::profile::PROVISIONAL_KIND);
                    error
                })?;
                if inspection.has_any_z || inspection.has_any_m {
                    return Err(write_error(
                        ErrorCategory::DataMapping,
                        "il provider qualifica soltanto payload WKB XY",
                    ));
                }
                if inspection.has_any_embedded_srid {
                    return Err(write_error(
                        ErrorCategory::DataMapping,
                        "SRID embedded nel payload EWKB non qualificato",
                    ));
                }
                let geometry_type = inspection
                    .root
                    .geometry_type_name()
                    .filter(|value| geometry_type_is_writable(value))
                    .ok_or_else(|| {
                        write_error(
                            ErrorCategory::DataMapping,
                            "tipo geometry WKB non qualificato dal provider",
                        )
                    })?;
                if column
                    .exact_geometry_type
                    .as_deref()
                    .is_some_and(|expected| !geometry_type.eq_ignore_ascii_case(expected))
                {
                    return Err(write_error(
                        ErrorCategory::DataMapping,
                        "tipo geometry WKB diverso dal contratto Arrow",
                    ));
                }
                components = components
                    .checked_add(inspection.stats.components)
                    .ok_or_else(|| {
                        write_error(ErrorCategory::ResourceLimit, "overflow componenti geometry")
                    })?;
                max_depth = max_depth.max(inspection.stats.max_depth);
            }
        }
        if components > 0 {
            budget
                .try_lease(ResourceKind::GeometryComponents, components)?
                .commit(components)?;
        }
        Ok(plenora_database_core::ewkb::EwkbStats {
            components,
            max_depth,
        })
    }

    /// Policy fail-closed per gli indici di un Upsert.
    ///
    /// `INSERT ... ON DUPLICATE KEY UPDATE` scatta su **qualsiasi** PRIMARY
    /// KEY o UNIQUE index in conflitto, non solo sulle `keys` dichiarate. Se
    /// il target ha un unique index diverso dalle keys, una riga in ingresso
    /// che non collide sulle keys ma collide su quell'altro indice
    /// aggiornerebbe la **riga sbagliata** (silenziosamente). Quindi
    /// richiediamo, prima di aprire la transazione:
    ///
    /// 1. esiste un PK/UNIQUE index le cui colonne coincidono (come insieme)
    ///    con le keys — l'ancora che rende deterministico il match;
    /// 2. **nessun altro** PK/UNIQUE index con un insieme di colonne diverso;
    /// 3. nessun unique index funzionale (espressione) non confrontabile.
    ///
    /// Senza (1) l'`ON DUPLICATE KEY UPDATE` non troverebbe mai un conflitto
    /// sulle keys e inserirebbe duplicati invece di aggiornare.
    fn validate_upsert_target_indexes(&self, target: &MysqlObjectDescription) -> Result<()> {
        use std::collections::BTreeSet;
        let key_set: BTreeSet<&str> = self.upsert_keys.iter().map(String::as_str).collect();
        if key_set.is_empty() {
            // validate_operation garantisce keys non vuote; difesa in profondità.
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "Upsert richiede almeno una key column",
            ));
        }
        let mut anchor_found = false;
        for index in &target.indexes {
            if !index.unique {
                continue;
            }
            if !index.column_backed {
                return Err(unsupported(format!(
                    "Upsert non qualificato: la tabella '{}' ha un unique \
 index funzionale/espressione ('{}') non confrontabile con \
 le keys — ON DUPLICATE KEY UPDATE potrebbe aggiornare la \
 riga sbagliata",
                    target.name, index.name
                )));
            }
            let index_set: BTreeSet<&str> = index.columns.iter().map(String::as_str).collect();
            if index_set == key_set {
                anchor_found = true;
            } else {
                return Err(unsupported(format!(
                    "Upsert non sicuro: keys={:?} ma la tabella '{}' ha un \
 altro PK/UNIQUE index ('{}' su {:?}) — ON DUPLICATE KEY \
 UPDATE potrebbe collidere su quell'indice e aggiornare la \
 riga sbagliata. Rimuovere l'indice in conflitto o usare \
 WriteMode::Update esplicito.",
                    self.upsert_keys, target.name, index.name, index.columns
                )));
            }
        }
        if !anchor_found {
            return Err(unsupported(format!(
                "Upsert non sicuro: nessun PRIMARY KEY o UNIQUE index della \
 tabella '{}' corrisponde a keys={:?}. Senza un unique index sulle \
 keys, ON DUPLICATE KEY UPDATE non rileverebbe i conflitti e \
 inserirebbe duplicati invece di aggiornare.",
                target.name, self.upsert_keys
            )));
        }
        Ok(())
    }

    pub(super) fn preflight(
        &self,
        target: &MysqlObjectDescription,
        profile: &dyn crate::profile::ProductProfile,
    ) -> Result<LossReport> {
        let product = profile.product();
        if target.kind != "BASE TABLE" {
            return Err(unsupported(format!(
                "append {product} richiede una BASE TABLE"
            )));
        }
        if target.engine.as_deref() != Some("InnoDB") {
            return Err(unsupported(format!(
                "append SingleTransaction {product} richiede una tabella InnoDB"
            )));
        }
        if self.mode == WriteMode::Upsert {
            self.validate_upsert_target_indexes(target)?;
        }
        for column in &self.columns {
            let server = target
                .columns
                .iter()
                .find(|candidate| candidate.name == column.name)
                .ok_or_else(|| mapping_error(format!("colonna target {product} mancante")))?;
            if !server.generation_expression.is_empty() {
                return Err(mapping_error(format!(
                    "append {product} non puo scrivere una colonna generata"
                )));
            }
            let spec = MysqlColumnSpec::from_catalog_with_profile(server, profile)?;
            if column.kind == MysqlColumnKind::Geometry {
                if spec.kind != MysqlColumnKind::Geometry {
                    return Err(mapping_error(format!(
                        "campo geometry Arrow diretto a una colonna {product} non spatial"
                    )));
                }
                if server.spatial_srid != column.spatial_srid {
                    return Err(crs_error(format!(
                        "SRID target {product} diverso dal contratto Arrow"
                    )));
                }
                let native = server.data_type.to_ascii_lowercase();
                if native != "geometry"
                    && column.exact_geometry_type.as_deref() != Some(native.as_str())
                {
                    return Err(mapping_error(format!(
                        "tipo geometry target {product} incompatibile col contratto Arrow"
                    )));
                }
                if column.exact_geometry_type.is_none() && native != "geometry" {
                    return Err(mapping_error(format!(
                        "geometrie mixed richiedono una colonna {product} GEOMETRY"
                    )));
                }
            } else if spec.kind != column.kind
                || !write_native_type_is_qualified(server, &column.kind)
            {
                return Err(mapping_error(format!(
                    "schema Arrow incompatibile con la colonna target {product}"
                )));
            }
            if column.nullable && !server.nullable {
                return Err(mapping_error(format!(
                    "nullability Arrow incompatibile con la colonna target {product}"
                )));
            }
        }
        for server in &target.columns {
            if self.columns.iter().any(|column| column.name == server.name) {
                continue;
            }
            let generated = !server.generation_expression.is_empty();
            let automatic = server
                .extra
                .split_ascii_whitespace()
                .any(|part| part.eq_ignore_ascii_case("auto_increment"));
            if !server.nullable && server.default_expression.is_none() && !generated && !automatic {
                return Err(mapping_error(format!(
                    "colonna target {product} obbligatoria assente dallo schema Arrow"
                )));
            }
        }
        Ok(LossReport {
            schema_version: 1,
            policy: MappingPolicy::Strict,
            losses: Vec::new(),
        })
    }
}

pub fn validate_batch_schema(batch: &RecordBatch, declared: &SchemaRef) -> Result<()> {
    if batch.schema().as_ref() == declared.as_ref() {
        Ok(())
    } else {
        Err(write_error(
            ErrorCategory::InvalidPlan,
            "schema del batch diverso dallo schema dichiarato",
        ))
    }
}

pub fn committed_outcome(
    execution_id: String,
    received: u64,
    inserted: u64,
) -> Result<WriteOutcome> {
    committed_outcome_for_mode(execution_id, received, inserted, WriteMode::Append)
}

/// Version mode-aware di `committed_outcome`.
///
/// Per Upsert: MySQL restituisce `affected_rows` misto (1 per insert, 2 per
/// update); non abbiamo il breakdown esatto senza query aggiuntive. Usiamo
/// `confirmed = received` (tutte processate) e `inserted/updated = None`.
///
/// # Errors
///
/// Fallisce se il RowCounts non valida contro il contratto core.
pub fn committed_outcome_for_mode(
    execution_id: String,
    received: u64,
    affected_or_inserted: u64,
    mode: WriteMode,
) -> Result<WriteOutcome> {
    let rows = match mode {
        WriteMode::Replace => RowCounts {
            // Replace: il target e stato svuotato dal DELETE nella stessa
            // transazione, quindi le righe input sono esattamente le righe
            // finali. affected_or_inserted = righe inserite = received.
            received,
            confirmed: received,
            inserted: Some(received),
            updated: Some(0),
            deleted: Some(0),
            failed: 0,
            skipped: 0,
        },
        WriteMode::Upsert => RowCounts {
            received,
            confirmed: received,
            inserted: None,
            updated: None,
            deleted: Some(0),
            failed: 0,
            skipped: 0,
        },
        WriteMode::Update => RowCounts {
            received,
            // affected_or_inserted = # righe target aggiornate dopo l'UPDATE
            // JOIN. Le righe input che non trovano match in target sono
            // no-op (idempotent), tracciate come skipped.
            confirmed: affected_or_inserted,
            inserted: Some(0),
            updated: Some(affected_or_inserted),
            deleted: Some(0),
            failed: 0,
            skipped: received.saturating_sub(affected_or_inserted),
        },
        WriteMode::DeleteByKeys => RowCounts {
            received,
            // affected_or_inserted = # righe cancellate (0 <= affected <= received).
            // Le keys non trovate non sono un errore (idempotency).
            confirmed: affected_or_inserted,
            inserted: Some(0),
            updated: Some(0),
            deleted: Some(affected_or_inserted),
            failed: 0,
            skipped: received.saturating_sub(affected_or_inserted),
        },
        _ => RowCounts {
            received,
            confirmed: affected_or_inserted,
            inserted: Some(affected_or_inserted),
            updated: Some(0),
            deleted: Some(0),
            failed: 0,
            skipped: 0,
        },
    };
    let outcome = WriteOutcome {
        schema_version: 1,
        status: WriteStatus::Committed,
        execution_id,
        provider: crate::profile::PROVISIONAL_KIND,
        rows,
        layer_outcomes: Vec::new(),
        recovery: None,
    };
    outcome.validate().map_err(|mut error| {
        error.category = ErrorCategory::Internal;
        error.phase = ErrorPhase::Write;
        error.provider = Some(crate::profile::PROVISIONAL_KIND);
        error.execution_id = Some(outcome.execution_id.clone());
        // Il COMMIT e gia riuscito: a essere incoerente e la nostra
        // contabilita, non lo stato del server. Dichiararlo `None` o lasciarlo
        // degradare a `RolledBack` invita a un retry che raddoppierebbe le
        // righe gia scritte.
        error.remote_effect = RemoteEffect::Committed;
        error.retry = RetryDisposition::Never;
        error.message = format!(
            "{} [le righe sono committed: l'incoerenza e nel conteggio \
             pubblicato, non nello stato remoto]",
            error.message
        );
        error
    })?;
    Ok(outcome)
}

pub fn commit_failure(
    mut error: DatabaseError,
    execution_id: String,
    received: u64,
) -> Result<WriteOutcome> {
    error.execution_id = Some(execution_id.clone());
    if error.remote_effect == RemoteEffect::RolledBack {
        return Err(error);
    }
    let outcome = WriteOutcome {
        schema_version: 1,
        status: WriteStatus::OutcomeUnknown,
        execution_id,
        provider: crate::profile::PROVISIONAL_KIND,
        rows: RowCounts {
            received,
            confirmed: 0,
            inserted: None,
            updated: None,
            deleted: None,
            failed: 0,
            skipped: 0,
        },
        layer_outcomes: Vec::new(),
        recovery: Some(Recovery {
            last_certain_phase: CertainPhase::CommitOrEditRequested,
            automatic_retry_allowed: false,
            idempotency_key: None,
            staging_object: None,
            verification_action: Some(
                "verificare la tabella target prima di qualsiasi retry".to_owned(),
            ),
        }),
    };
    // Anche qui l'esito del server e ignoto: se la validazione del documento
    // fallisce, l'errore che ne esce non deve suggerire che non sia successo
    // nulla.
    outcome.validate().map_err(|mut error| {
        error.execution_id = Some(outcome.execution_id.clone());
        error.remote_effect = RemoteEffect::Unknown;
        error.retry = RetryDisposition::RequiresRecovery;
        error
    })?;
    Ok(outcome)
}

/// Cosa la scrittura ha lasciato sul server prima di aprire la transazione.
///
/// Su `MySQL` il DDL fa commit implicito: una `CREATE TABLE` eseguita nella
/// fase di preparazione non appartiene alla transazione che segue e nessun
/// `ROLLBACK` la annulla.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlResidue {
    /// Nessun DDL eseguito: il rollback della transazione annulla tutto.
    None,
    /// Una tabella e stata creata e sopravvive al rollback delle righe.
    CreatedTable,
}

/// Esito di un fallimento prima del commit.
///
/// Il residuo **non** viene applicato qui: lo stampa [`stamp_ddl_residue`],
/// che e l'unico punto attraversato da ogni uscita successiva alla DDL.
/// Applicarlo anche qui lo duplicherebbe sui percorsi che passano da entrambi.
#[must_use]
pub fn rolled_back_error(
    mut error: DatabaseError,
    rollback_confirmed: bool,
    execution_id: &str,
) -> DatabaseError {
    error.execution_id = Some(execution_id.to_owned());
    if rollback_confirmed || error.remote_effect == RemoteEffect::RolledBack {
        error.remote_effect = RemoteEffect::RolledBack;
    } else {
        error.remote_effect = RemoteEffect::Unknown;
        if error.retry != RetryDisposition::Quarantine {
            error.retry = RetryDisposition::RequiresRecovery;
        }
    }
    error
}

/// Marca l'esito con cio che il rollback non ha potuto annullare.
///
/// **Ogni** uscita successiva a una DDL passa da qui: il fallimento di
/// `describe_object`, del preflight, dell'apertura della transazione, della
/// scrittura, e l'esito ambiguo del commit. Un punto solo, perche i percorsi
/// di uscita sono molti e uno dimenticato direbbe "il server e come prima"
/// mentre la tabella e li.
///
/// Senza residuo il valore passa invariato.
///
/// Su errore: `RolledBack` e `None` diventano `Partial` — righe annullate o
/// mai scritte, schema persistito — con recupero richiesto. `Unknown` resta
/// `Unknown`: non sapere se le righe siano sparite e piu grave che sapere che
/// lo schema e rimasto. Una sessione gia quarantinata non viene declassata.
///
/// Su esito `OutcomeUnknown` (commit ambiguo) la nota di verifica dice che la
/// tabella esiste comunque, cosi chi recupera sa che trovarla non prova nulla
/// sulle righe.
///
/// # Errors
///
/// Propaga l'esito in ingresso; l'`OutcomeUnknown` riscritto viene rivalidato
/// contro il contratto.
pub fn stamp_ddl_residue(
    result: Result<WriteOutcome>,
    residue: DdlResidue,
) -> Result<WriteOutcome> {
    if residue == DdlResidue::None {
        return result;
    }
    match result {
        Err(mut error) => {
            // Un errore che dichiara dati committed non ha residui da
            // annunciare: la tabella c'e per forza, e declassarlo a `Partial`
            // o chiedere recupero suggerirebbe un retry che raddoppia le
            // righe.
            if error.remote_effect == RemoteEffect::Committed {
                return Err(error);
            }
            if matches!(
                error.remote_effect,
                RemoteEffect::RolledBack | RemoteEffect::None
            ) {
                error.remote_effect = RemoteEffect::Partial;
            }
            if error.retry != RetryDisposition::Quarantine {
                error.retry = RetryDisposition::RequiresRecovery;
            }
            error.message = format!(
                "{} [la tabella creata da mode='create' e rimasta: il DDL \
 fa commit implicito e non e annullato dal rollback]",
                error.message
            );
            Err(error)
        }
        Ok(mut outcome) => {
            if outcome.status != WriteStatus::OutcomeUnknown {
                return Ok(outcome);
            }
            if let Some(recovery) = outcome.recovery.as_mut() {
                recovery.verification_action = Some(
                    "la tabella creata da mode='create' esiste comunque (DDL \
 autocommit): verificarne le righe, non la presenza, prima di \
 qualsiasi retry"
                        .to_owned(),
                );
            }
            outcome.validate()?;
            Ok(outcome)
        }
    }
}

fn bind_value(array: &dyn Array, row: usize, kind: &MysqlColumnKind) -> Result<Value> {
    macro_rules! primitive {
        ($array:ty, $value:expr) => {{
            let values = array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| mapping_error("array Arrow incoerente con il piano"))?;
            $value(values.value(row))
        }};
    }
    Ok(match kind {
        MysqlColumnKind::Bool => primitive!(BooleanArray, |value| Value::Int(i64::from(value))),
        MysqlColumnKind::I8 => primitive!(Int8Array, |value| Value::Int(i64::from(value))),
        MysqlColumnKind::U8 => primitive!(UInt8Array, |value| Value::UInt(u64::from(value))),
        MysqlColumnKind::I16 => primitive!(Int16Array, |value| Value::Int(i64::from(value))),
        MysqlColumnKind::U16 => primitive!(UInt16Array, |value| Value::UInt(u64::from(value))),
        MysqlColumnKind::I32 => primitive!(Int32Array, |value| Value::Int(i64::from(value))),
        MysqlColumnKind::U32 => primitive!(UInt32Array, |value| Value::UInt(u64::from(value))),
        MysqlColumnKind::I64 => primitive!(Int64Array, Value::Int),
        MysqlColumnKind::U64 => primitive!(UInt64Array, Value::UInt),
        MysqlColumnKind::F32 => primitive!(Float32Array, Value::Float),
        MysqlColumnKind::F64 => primitive!(Float64Array, Value::Double),
        MysqlColumnKind::Utf8 => primitive!(StringArray, |value: &str| Value::Bytes(
            value.as_bytes().to_vec()
        )),
        MysqlColumnKind::Binary | MysqlColumnKind::Geometry => {
            primitive!(BinaryArray, |value: &[u8]| Value::Bytes(value.to_vec()))
        }
        MysqlColumnKind::Date => {
            let days = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| mapping_error("array Date32 incoerente con il piano"))?
                .value(row);
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(i64::from(days))))
                .ok_or_else(|| mapping_error("Date32 fuori intervallo"))?;
            Value::Date(
                u16::try_from(date.year()).map_err(|_| mapping_error("anno fuori intervallo"))?,
                u8::try_from(date.month())
                    .map_err(|_| mapping_error("mese data fuori intervallo"))?,
                u8::try_from(date.day())
                    .map_err(|_| mapping_error("giorno data fuori intervallo"))?,
                0,
                0,
                0,
                0,
            )
        }
        MysqlColumnKind::Timestamp => {
            let micros = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| mapping_error("array timestamp incoerente con il piano"))?
                .value(row);
            let instant = chrono::DateTime::from_timestamp_micros(micros)
                .ok_or_else(|| mapping_error("timestamp fuori intervallo"))?
                .naive_utc();
            Value::Date(
                u16::try_from(instant.year())
                    .map_err(|_| mapping_error("anno timestamp fuori intervallo"))?,
                u8::try_from(instant.month())
                    .map_err(|_| mapping_error("mese timestamp fuori intervallo"))?,
                u8::try_from(instant.day())
                    .map_err(|_| mapping_error("giorno timestamp fuori intervallo"))?,
                u8::try_from(instant.hour())
                    .map_err(|_| mapping_error("ora timestamp fuori intervallo"))?,
                u8::try_from(instant.minute())
                    .map_err(|_| mapping_error("minuto timestamp fuori intervallo"))?,
                u8::try_from(instant.second())
                    .map_err(|_| mapping_error("secondo timestamp fuori intervallo"))?,
                instant.nanosecond() / 1_000,
            )
        }
        MysqlColumnKind::Decimal { scale, .. } => {
            let value = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| mapping_error("array decimal incoerente con il piano"))?
                .value(row);
            Value::Bytes(decimal_text(value, *scale)?.into_bytes())
        }
        MysqlColumnKind::Time => {
            return Err(mapping_error("tipo non qualificato per append"));
        }
    })
}

fn decimal_text(value: i128, scale: i8) -> Result<String> {
    let scale = usize::try_from(scale).map_err(|_| mapping_error("scala decimal negativa"))?;
    if scale == 0 {
        return Ok(value.to_string());
    }
    let text = value.to_string();
    let (sign, digits) = text
        .strip_prefix('-')
        .map_or(("", text.as_str()), |digits| ("-", digits));
    let body = if digits.len() <= scale {
        format!("0.{}{digits}", "0".repeat(scale - digits.len()))
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    Ok(format!("{sign}{body}"))
}

fn write_native_type_is_qualified(column: &crate::MysqlColumn, kind: &MysqlColumnKind) -> bool {
    let native = column.data_type.to_ascii_lowercase();
    match kind {
        MysqlColumnKind::Bool | MysqlColumnKind::I8 | MysqlColumnKind::U8 => native == "tinyint",
        MysqlColumnKind::I16 | MysqlColumnKind::U16 => native == "smallint",
        MysqlColumnKind::I32 | MysqlColumnKind::U32 => {
            matches!(native.as_str(), "mediumint" | "int" | "integer")
        }
        MysqlColumnKind::I64 | MysqlColumnKind::U64 => native == "bigint",
        MysqlColumnKind::F32 => native == "float",
        MysqlColumnKind::F64 => matches!(native.as_str(), "double" | "real"),
        MysqlColumnKind::Utf8 => matches!(
            native.as_str(),
            "varchar" | "tinytext" | "text" | "mediumtext" | "longtext"
        ),
        MysqlColumnKind::Binary => matches!(
            native.as_str(),
            "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob"
        ),
        MysqlColumnKind::Date => native == "date",
        MysqlColumnKind::Timestamp => {
            matches!(native.as_str(), "datetime" | "timestamp")
                && column.datetime_precision == Some(6)
        }
        MysqlColumnKind::Decimal { .. } => matches!(native.as_str(), "decimal" | "numeric"),
        MysqlColumnKind::Time | MysqlColumnKind::Geometry => false,
    }
}

fn mapping_error(message: impl Into<String>) -> DatabaseError {
    prepare_error(ErrorCategory::DataMapping, message)
}

fn crs_error(message: impl Into<String>) -> DatabaseError {
    prepare_error(ErrorCategory::Crs, message)
}

fn write_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    let mut error = prepare_error(category, message);
    error.phase = ErrorPhase::Write;
    error
}

fn validate_operation(operation: &WriteOperation, database: &str) -> Result<()> {
    // `Replace` e qualificata come DELETE FROM + bulk insert nella stessa
    // transazione InnoDB: nessun DDL, quindi nessuna perdita di indici, FK,
    // trigger, check, default, grant o AUTO_INCREMENT, e rollback pieno se
    // qualcosa fallisce dopo il DELETE.
    //
    // `TruncateInsert` resta fail-closed. Su MySQL `TRUNCATE TABLE` e DDL con
    // commit implicito: le righe sparirebbero prima dell'INSERT e nessun
    // rollback le riporterebbe indietro. Emularla con `DELETE FROM` sarebbe
    // peggio di rifiutarla — il consumer chiederebbe TRUNCATE (reset di
    // AUTO_INCREMENT, nessun trigger, nessun log riga per riga) e ne
    // otterrebbe un'altra cosa con lo stesso nome. Chi vuole svuotare e
    // riempire in transazione ha `Replace`, che dichiara esattamente questo.
    match operation.mode {
        WriteMode::Append
        | WriteMode::Create
        | WriteMode::Replace
        | WriteMode::Upsert
        | WriteMode::DeleteByKeys
        | WriteMode::Update => {}
        WriteMode::TruncateInsert => {
            return Err(unsupported(
                "WriteMode::TruncateInsert non qualificata in questo dialetto: TRUNCATE e \
 DDL con commit implicito, quindi non rollback-safe se l'INSERT \
 successivo fallisce, e non viene emulata con DELETE perche \
 avrebbe semantica diversa (AUTO_INCREMENT non azzerato, trigger \
 e log riga per riga attivi). Usare WriteMode::Replace, che \
 dichiara DELETE FROM + insert nella stessa transazione.",
            ));
        }
    }
    if operation.transaction_profile != TransactionProfile::SingleTransaction {
        return Err(unsupported("write richiede il profilo SingleTransaction"));
    }
    if operation.mapping_policy != MappingPolicy::Strict {
        return Err(unsupported(
            "write richiede MappingPolicy::Strict finche il loss preflight non e qualificato",
        ));
    }
    if operation.allow_partial {
        return Err(unsupported("write parziale non qualificata"));
    }
    // `Create` accetta keys opzionali e le rende PRIMARY KEY della tabella che
    // costruisce, come su PostgreSQL. Non le richiede: una tabella senza
    // chiave primaria e legittima.
    if operation.mode == WriteMode::Create && !operation.update_columns.is_empty() {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "Create non ha colonne da aggiornare: update_columns non e \
 applicabile",
        ));
    }
    // Upsert, DeleteByKeys e Update richiedono keys; Append e Replace le
    // rifiutano.
    if matches!(
        operation.mode,
        WriteMode::Upsert | WriteMode::DeleteByKeys | WriteMode::Update
    ) {
        if operation.keys.is_empty() {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                format!("mode '{:?}' richiede almeno una key column", operation.mode),
            ));
        }
        // Upsert/DeleteByKeys: update_columns esplicite non permesse (v1.2).
        // Update: update_columns esplicite opzionali (default = tutte non-key).
        if matches!(operation.mode, WriteMode::Upsert | WriteMode::DeleteByKeys)
            && !operation.update_columns.is_empty()
        {
            return Err(unsupported(
                "update_columns esplicite valide solo per WriteMode::Update; \
                 Upsert aggiorna tutte le non-key, DeleteByKeys non usa update_columns",
            ));
        }
    } else if operation.mode != WriteMode::Create
        && (!operation.keys.is_empty() || !operation.update_columns.is_empty())
    {
        // Restano Append e Replace: nessuna delle due ha semantica di chiave.
        // Il messaggio nomina la mode effettiva, e la categoria e
        // `InvalidPlan` — il piano descrive qualcosa che la mode non
        // significa, non una funzione che il provider non implementa.
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            format!(
                "mode '{:?}' non ha semantica di chiave: keys e \
 update_columns non sono applicabili",
                operation.mode
            ),
        ));
    }
    if operation.create_spatial_index {
        return Err(unsupported(
            "creazione indice spatial non ancora qualificata",
        ));
    }
    if operation.target.layer_id.is_some() {
        return Err(unsupported("layer_id non appartiene al provider"));
    }
    if operation
        .target
        .catalog
        .as_deref()
        .is_some_and(|catalog| catalog != database)
        || operation
            .target
            .schema
            .as_deref()
            .is_some_and(|schema| schema != database)
    {
        return Err(unsupported(
            "target cross-database non supportato dal provider",
        ));
    }
    Ok(())
}

fn write_column_kind(field: &Field) -> Result<MysqlColumnKind> {
    let kind = match field.data_type() {
        DataType::Boolean => MysqlColumnKind::Bool,
        DataType::Int8 => MysqlColumnKind::I8,
        DataType::UInt8 => MysqlColumnKind::U8,
        DataType::Int16 => MysqlColumnKind::I16,
        DataType::UInt16 => MysqlColumnKind::U16,
        DataType::Int32 => MysqlColumnKind::I32,
        DataType::UInt32 => MysqlColumnKind::U32,
        DataType::Int64 => MysqlColumnKind::I64,
        DataType::UInt64 => MysqlColumnKind::U64,
        DataType::Float32 => MysqlColumnKind::F32,
        DataType::Float64 => MysqlColumnKind::F64,
        DataType::Utf8 => MysqlColumnKind::Utf8,
        DataType::Binary => MysqlColumnKind::Binary,
        DataType::Date32 => MysqlColumnKind::Date,
        DataType::Timestamp(TimeUnit::Microsecond, None) => MysqlColumnKind::Timestamp,
        DataType::Decimal128(precision, scale)
            if *precision > 0
                && *precision <= 38
                && *scale >= 0
                && *scale <= precision.cast_signed() =>
        {
            MysqlColumnKind::Decimal {
                precision: *precision,
                scale: *scale,
            }
        }
        other => {
            return Err(unsupported(format!(
                "tipo Arrow non qualificato per append: {other:?}"
            )));
        }
    };
    Ok(kind)
}

/// Numero massimo di colonne in una chiave `InnoDB`.
///
/// Il server risponde 1070 "Too many key parts specified; max 16 parts
/// allowed", ma solo dopo aver ricevuto la DDL: il piano si ferma prima.
const MAX_PRIMARY_KEY_PARTS: usize = 16;

/// Numero di colonne che il motore accetta in una chiave.
///
/// E un vincolo del motore, non della nozione di chiave primaria: per questo
/// resta qui invece di salire nel core insieme alle tre regole strutturali.
///
/// # Errors
///
/// `InvalidPlan` oltre [`MAX_PRIMARY_KEY_PARTS`] colonne.
fn validate_primary_key_parts(keys: &[String]) -> Result<()> {
    if keys.len() > MAX_PRIMARY_KEY_PARTS {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            format!(
                "chiave primaria con {} colonne: il motore ne accetta al \
 massimo {MAX_PRIMARY_KEY_PARTS}",
                keys.len()
            ),
        ));
    }
    Ok(())
}

/// Vincoli di **tipo** che una colonna deve rispettare per stare in una
/// PRIMARY KEY `MySQL`.
///
/// La nullability non e qui: e strutturale, vale su ogni provider e la
/// verifica `validate_create_primary_key` nel core.
///
/// Sono provider-specifici: dipendono da come `mysql_column_ddl` traduce il
/// tipo Arrow e da cosa il motore accetta come colonna di chiave. Ogni caso e
/// stato verificato contro il riferimento, e ciascuno oggi fallirebbe **al
/// server**, dopo che la scrittura e partita:
///
/// * `Utf8` -> `TEXT` e `Binary` -> `BLOB`: errore 1170, "BLOB/TEXT column
///   used in key specification without a key length". Lo schema Arrow non
///   porta una lunghezza massima, quindi il piano non puo generare il
///   prefisso che li renderebbe indicizzabili: la mode non ha una semantica
///   qualificata per queste colonne, e dirlo prima e meglio che scoprirlo
///   dopo;
/// * `Geometry`: errore 3728, "Spatial indexes can't be primary or unique
///   indexes";
/// * `F32`/`F64`: accettati dal motore, ma una chiave primaria su virgola
///   mobile identifica righe per un valore la cui uguaglianza dipende
///   dall'arrotondamento. Resta chiusa finche non esiste una semantica
///   dichiarata.
///
/// # Errors
///
/// `InvalidPlan` per un tipo che non puo essere chiave, con il motivo.
fn validate_primary_key_column(key: &str, column: &MysqlWriteColumn) -> Result<()> {
    let refusal = match column.kind {
        MysqlColumnKind::Utf8 => Some(
            "il tipo Arrow Utf8 diventa TEXT, e il motore rifiuta TEXT in chiave \
 senza una lunghezza di prefisso, che lo schema Arrow non dichiara",
        ),
        MysqlColumnKind::Binary => Some(
            "il tipo Arrow Binary diventa BLOB, e il motore rifiuta BLOB in chiave \
 senza una lunghezza di prefisso, che lo schema Arrow non dichiara",
        ),
        MysqlColumnKind::Geometry => {
            Some("il motore non ammette colonne spatial come chiave primaria o unique")
        }
        MysqlColumnKind::F32 | MysqlColumnKind::F64 => Some(
            "una chiave primaria in virgola mobile identifica le righe con un \
             valore la cui uguaglianza dipende dall'arrotondamento",
        ),
        _ => None,
    };
    if let Some(reason) = refusal {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            format!("chiave primaria '{key}' non qualificata: {reason}"),
        ));
    }
    Ok(())
}

fn unsupported(message: impl Into<String>) -> DatabaseError {
    prepare_error(ErrorCategory::Unsupported, message)
}

// ============================ v1.2 — DDL support (Create/TruncateInsert) =======

/// Genera la dichiarazione MySQL per un tipo di colonna.
///
/// Tipi geometrici: `GEOMETRY [NOT NULL] SRID <srid>` (MySQL 8.0+ dichiara
/// SRID come constraint di colonna quando noto).
fn mysql_column_ddl(kind: &MysqlColumnKind, spatial_srid: Option<u32>) -> String {
    match kind {
        MysqlColumnKind::Bool => "TINYINT(1)".to_owned(),
        MysqlColumnKind::I8 => "TINYINT".to_owned(),
        MysqlColumnKind::U8 => "TINYINT UNSIGNED".to_owned(),
        MysqlColumnKind::I16 => "SMALLINT".to_owned(),
        MysqlColumnKind::U16 => "SMALLINT UNSIGNED".to_owned(),
        MysqlColumnKind::I32 => "INT".to_owned(),
        MysqlColumnKind::U32 => "INT UNSIGNED".to_owned(),
        MysqlColumnKind::I64 => "BIGINT".to_owned(),
        MysqlColumnKind::U64 => "BIGINT UNSIGNED".to_owned(),
        MysqlColumnKind::F32 => "FLOAT".to_owned(),
        MysqlColumnKind::F64 => "DOUBLE".to_owned(),
        // TEXT senza length hint: general-purpose (no VARCHAR(N) perché
        // il Arrow schema non porta max length). Consumer può ALTER
        // dopo il create se serve VARCHAR indexed.
        MysqlColumnKind::Utf8 => "TEXT".to_owned(),
        MysqlColumnKind::Binary => "BLOB".to_owned(),
        MysqlColumnKind::Date => "DATE".to_owned(),
        MysqlColumnKind::Time => "TIME".to_owned(),
        // DATETIME(6) = microseconds precision, allineato con
        // Timestamp(Microsecond) del Arrow schema.
        MysqlColumnKind::Timestamp => "DATETIME(6)".to_owned(),
        MysqlColumnKind::Decimal { precision, scale } => {
            format!("DECIMAL({precision},{scale})")
        }
        MysqlColumnKind::Geometry => match spatial_srid {
            Some(srid) => format!("GEOMETRY SRID {srid}"),
            None => "GEOMETRY".to_owned(),
        },
    }
}

/// Costruisce `CREATE TABLE` MySQL da un `SchemaRef` Arrow.
///
/// - Colonne: derivate dallo schema (nome + tipo MySQL + NOT NULL/NULL).
/// - PRIMARY KEY: dal parametro `operation.keys` se non vuoto (WriteMode::Create
///   di solito non ha keys, ma se dato si applica).
/// - Engine: InnoDB (transactional, richiesto per il pattern OLTP OLTP).
/// - Charset: utf8mb4 (default moderno MySQL 8.4).
///
/// # Errors
///
/// `Unsupported` se un tipo Arrow non ha mapping MySQL (delega a `compile_write_column`).
pub(crate) fn build_create_table_sql(
    schema: &SchemaRef,
    operation: &WriteOperation,
    database: &str,
    profile: &dyn crate::profile::ProductProfile,
) -> Result<String> {
    let renderer = mysql_renderer();
    let target_schema = operation.target.schema.as_deref().unwrap_or(database);
    let object_name = ObjectName {
        catalog: None,
        schema: Some(mysql_identifier(target_schema)?),
        object: mysql_identifier(&operation.target.object)?,
    };
    let quoted_target = renderer.quote_object(&object_name)?;

    let columns: Vec<MysqlWriteColumn> = schema
        .fields()
        .iter()
        .map(|field| compile_write_column(field, &renderer, profile))
        .collect::<Result<Vec<_>>>()?;

    let mut lines = Vec::with_capacity(columns.len() + 1);
    for col in &columns {
        let type_decl = mysql_column_ddl(&col.kind, col.spatial_srid);
        let null_decl = if col.nullable { "NULL" } else { "NOT NULL" };
        lines.push(format!("    {} {} {}", col.quoted, type_decl, null_decl));
    }
    if !operation.keys.is_empty() {
        let pk_cols: Vec<String> = operation
            .keys
            .iter()
            .map(|k| {
                let id = mysql_identifier(k)?;
                renderer.quote_identifier(&id)
            })
            .collect::<Result<Vec<_>>>()?;
        lines.push(format!("    PRIMARY KEY ({})", pk_cols.join(", ")));
    }

    Ok(format!(
        "CREATE TABLE {quoted_target} (\n{}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        lines.join(",\n")
    ))
}

/// Genera `CREATE TEMPORARY TABLE staging_name (...)` con la stessa
/// struttura dello schema Arrow.
///
/// Serve a `WriteMode::Update`, che accumula le righe in staging e poi
/// esegue un UPDATE JOIN. Il commento diceva anche `Replace`, con RENAME
/// atomico: non e piu vero da quando Replace usa DELETE + INSERT nella
/// stessa transazione, e una descrizione che nomina un meccanismo
/// inesistente e peggio di nessuna descrizione.
///
/// # Errors
///
/// Come `build_create_table_sql`.
pub(crate) fn build_temp_staging_sql(
    schema: &SchemaRef,
    staging_name: &str,
    database: &str,
    profile: &dyn crate::profile::ProductProfile,
) -> Result<String> {
    let renderer = mysql_renderer();
    let staging_ident = mysql_identifier(staging_name)?;
    let db_ident = mysql_identifier(database)?;
    let staging_object = ObjectName {
        catalog: None,
        schema: Some(db_ident),
        object: staging_ident,
    };
    let quoted_staging = renderer.quote_object(&staging_object)?;
    let columns: Vec<MysqlWriteColumn> = schema
        .fields()
        .iter()
        .map(|field| compile_write_column(field, &renderer, profile))
        .collect::<Result<Vec<_>>>()?;
    let lines: Vec<String> = columns
        .iter()
        .map(|c| {
            let ty = mysql_column_ddl(&c.kind, c.spatial_srid);
            let null = if c.nullable { "NULL" } else { "NOT NULL" };
            format!("    {} {} {}", c.quoted, ty, null)
        })
        .collect();
    Ok(format!(
        "CREATE TEMPORARY TABLE {quoted_staging} (\n{}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        lines.join(",\n")
    ))
}

/// Genera nome quoted per staging table (usato dopo `build_temp_staging_sql`).
///
/// # Errors
///
/// Se `staging_name` o `database` non sono identifier MySQL validi.
pub(crate) fn quote_staging_name(staging_name: &str, database: &str) -> Result<String> {
    let renderer = mysql_renderer();
    let obj = ObjectName {
        catalog: None,
        schema: Some(mysql_identifier(database)?),
        object: mysql_identifier(staging_name)?,
    };
    renderer.quote_object(&obj)
}

/// Genera `DELETE FROM db.table` per `WriteMode::Replace`.
///
/// DML, non DDL: sta dentro la transazione del bulk insert, quindi un
/// fallimento successivo lo annulla insieme alle righe gia scritte. Non tocca
/// la definizione della tabella, che e esattamente cio che Replace promette di
/// conservare.
///
/// # Errors
///
/// Se schema o oggetto del target non sono identificatori `MySQL` validi.
pub(crate) fn build_delete_all_sql(operation: &WriteOperation, database: &str) -> Result<String> {
    let renderer = mysql_renderer();
    let target_schema = operation.target.schema.as_deref().unwrap_or(database);
    let object_name = ObjectName {
        catalog: None,
        schema: Some(mysql_identifier(target_schema)?),
        object: mysql_identifier(&operation.target.object)?,
    };
    Ok(format!(
        "DELETE FROM {}",
        renderer.quote_object(&object_name)?
    ))
}

fn prepare_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category,
        phase: ErrorPhase::Prepare,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(crate::profile::PROVISIONAL_KIND),
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MysqlColumn, MysqlObjectDescription, MysqlSchemaToken, MAX_BIND_PARAMETERS};
    use chrono::NaiveDate;
    use mysql_async::{Params, Value};
    use plenora_database_core::arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array,
        Int64Array, StringArray, TimestampMicrosecondArray, UInt32Array,
    };
    use plenora_database_core::arrow::schema::{DataType, Field, Schema};
    use plenora_database_core::arrow::RecordBatch;
    use plenora_database_core::loss::MappingPolicy;
    use plenora_database_core::outcome::{CertainPhase, WriteStatus};
    use plenora_database_core::plan::{ObjectRef, ProviderKind, TransactionProfile, WriteMode};
    use plenora_database_core::protocol;
    use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new_with_metadata(
            fields,
            HashMap::from([(
                protocol::CONTRACT_VERSION_KEY.to_owned(),
                protocol::CONTRACT_VERSION.to_owned(),
            )]),
        ))
    }

    fn append_operation() -> WriteOperation {
        WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("warehouse".to_owned()),
                object: "events".to_owned(),
                layer_id: None,
            },
            mode: WriteMode::Append,
            mapping_policy: MappingPolicy::Strict,
            transaction_profile: TransactionProfile::SingleTransaction,
            keys: Vec::new(),
            update_columns: Vec::new(),
            srid_policy: None,
            create_spatial_index: false,
            allow_partial: false,
        }
    }

    fn append_plan(fields: Vec<Field>) -> MysqlWritePlan {
        MysqlWritePlan::compile_with_profile(
            &schema(fields),
            &append_operation(),
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect("piano append qualificato")
    }

    /// L'ordine delle colonne e quello dello schema Arrow, non un ordine
    /// ricavato dal nome: e l'unico che resta allineato ai buffer di riga.
    #[test]
    fn insert_renders_qualified_quoted_columns_in_schema_order() {
        let plan = append_plan(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]);
        assert_eq!(
            plan.render_insert(2).expect("insert di due righe"),
            "INSERT INTO `warehouse`.`events` (`id`, `label`) VALUES (?, ?), (?, ?);"
        );

        let escaped = append_plan(vec![
            Field::new("zeta", DataType::Int64, false),
            Field::new("al`pha", DataType::Utf8, false),
        ]);
        assert_eq!(
            escaped.render_insert(1).expect("insert di una riga"),
            "INSERT INTO `warehouse`.`events` (`zeta`, `al``pha`) VALUES (?, ?);"
        );
    }

    /// Un INSERT senza righe non e una scrittura vuota: e una VALUES list
    /// sintatticamente invalida che il server rifiuterebbe dopo la rete.
    #[test]
    fn insert_requires_at_least_one_row() {
        let error = append_plan(vec![Field::new("id", DataType::Int64, false)])
            .render_insert(0)
            .expect_err("insert senza righe");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    /// Il tetto di 65.535 placeholder e del protocollo: superarlo va visto
    /// prima del `COM_STMT_PREPARE`, non nell'errore del server.
    #[test]
    fn insert_stops_at_the_placeholder_ceiling_before_the_network() {
        let plan = append_plan(vec![Field::new("id", DataType::Int64, false)]);
        let sql = plan
            .render_insert(MAX_BIND_PARAMETERS)
            .expect("insert al tetto dei placeholder");
        assert_eq!(sql.matches('?').count(), MAX_BIND_PARAMETERS);
        let error = plan
            .render_insert(MAX_BIND_PARAMETERS + 1)
            .expect_err("insert oltre il tetto dei placeholder");
        assert_eq!(error.category, ErrorCategory::ResourceLimit);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    /// Il conteggio dei placeholder e un prodotto: senza controllo esplicito
    /// un overflow lo riporterebbe dentro il tetto invece di rifiutarlo.
    #[test]
    fn insert_row_count_overflow_is_checked() {
        let plan = append_plan(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]);
        let error = plan
            .render_insert(usize::MAX / 2 + 1)
            .expect_err("prodotto righe per colonne in overflow");
        assert_eq!(error.category, ErrorCategory::ResourceLimit);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    #[test]
    fn compile_accepts_supported_arrow_types_in_schema_order() {
        let plan = append_plan(vec![
            Field::new("flag", DataType::Boolean, false),
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(12, 2), true),
            Field::new(
                "created_at",
                DataType::Timestamp(
                    plenora_database_core::arrow::schema::TimeUnit::Microsecond,
                    None,
                ),
                false,
            ),
        ]);
        assert_eq!(plan.columns[0].kind, MysqlColumnKind::Bool);
        assert_eq!(plan.columns[1].kind, MysqlColumnKind::I64);
        assert_eq!(
            plan.columns[2].kind,
            MysqlColumnKind::Decimal {
                precision: 12,
                scale: 2,
            }
        );
        assert_eq!(plan.columns[3].kind, MysqlColumnKind::Timestamp);
        assert_eq!(plan.columns[0].name, "flag");
        assert!(!plan.columns[0].nullable);
        assert_eq!(plan.columns[2].quoted, "`amount`");
    }

    #[test]
    fn compile_rejects_unqualified_operation_shapes_before_the_network() {
        let input = schema(vec![Field::new("id", DataType::Int64, false)]);
        let mut cases = Vec::new();

        // Ogni forma porta con se la categoria che le spetta, invece di
        // essere schiacciata su una sola: `Unsupported` significa "il
        // provider non lo fa", `InvalidPlan` significa "il piano descrive
        // qualcosa che la mode non significa". Sono risposte diverse e il
        // consumer le tratta diversamente.
        let mut operation = append_operation();
        operation.transaction_profile = TransactionProfile::ChunkCommitted;
        cases.push((operation, ErrorCategory::Unsupported));

        let mut operation = append_operation();
        operation.allow_partial = true;
        cases.push((operation, ErrorCategory::Unsupported));

        // Append non ha semantica di chiave: keys e update_columns non sono
        // una funzione mancante, sono un piano incoerente.
        let mut operation = append_operation();
        operation.keys.push("id".to_owned());
        cases.push((operation, ErrorCategory::InvalidPlan));

        let mut operation = append_operation();
        operation.update_columns.push("label".to_owned());
        cases.push((operation, ErrorCategory::InvalidPlan));

        let mut operation = append_operation();
        operation.create_spatial_index = true;
        cases.push((operation, ErrorCategory::Unsupported));

        let mut operation = append_operation();
        operation.mapping_policy = MappingPolicy::Lossy;
        cases.push((operation, ErrorCategory::Unsupported));

        for (operation, expected) in cases {
            let error = MysqlWritePlan::compile_with_profile(
                &input,
                &operation,
                "warehouse",
                &crate::profile::MYSQL_PROFILE,
            )
            .expect_err("forma write non qualificata");
            assert_eq!(error.category, expected, "{operation:?}");
            assert_eq!(error.phase, ErrorPhase::Prepare);
        }
    }

    /// Le mode senza semantica di chiave rifiutano keys e update_columns, e
    /// il messaggio nomina la mode vera: prima diceva sempre "Append" anche
    /// per Create e Replace.
    /// `Create` accetta keys opzionali e le rende PRIMARY KEY, come su
    /// PostgreSQL. Prima le rifiutava, il che rendeva irraggiungibile il ramo
    /// `PRIMARY KEY` di `build_create_table_sql`: codice che non poteva
    /// essere eseguito da nessun piano valido.
    #[test]
    fn create_accepts_keys_and_renders_them_as_a_primary_key() {
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("tenant", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ];
        let mut operation = append_operation();
        operation.mode = WriteMode::Create;
        operation.keys = vec!["id".to_owned(), "tenant".to_owned()];
        let input = schema(fields);

        MysqlWritePlan::compile_with_profile(
            &input,
            &operation,
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect("Create con keys");
        let ddl = build_create_table_sql(
            &input,
            &operation,
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect("DDL");
        assert!(
            ddl.contains("PRIMARY KEY (`id`, `tenant`)"),
            "PRIMARY KEY assente dalla DDL: {ddl}"
        );

        // Senza keys la tabella nasce senza chiave primaria: legittimo.
        let mut without = append_operation();
        without.mode = WriteMode::Create;
        let plain = build_create_table_sql(
            &input,
            &without,
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect("DDL");
        assert!(!plain.contains("PRIMARY KEY"), "{plain}");

        // Una key che non e nello schema Arrow non puo diventare PRIMARY KEY.
        let mut absent = append_operation();
        absent.mode = WriteMode::Create;
        absent.keys = vec!["mai_dichiarata".to_owned()];
        assert_eq!(
            MysqlWritePlan::compile_with_profile(
                &input,
                &absent,
                "warehouse",
                &crate::profile::MYSQL_PROFILE
            )
            .expect_err("key assente accettata")
            .category,
            ErrorCategory::InvalidPlan
        );

        // Una PRIMARY KEY nullable non esiste. MySQL la rifiuterebbe con
        // l'errore 1171, ma al server: il piano va fermato prima della rete.
        let nullable = schema(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
        ]);
        let mut on_nullable = append_operation();
        on_nullable.mode = WriteMode::Create;
        on_nullable.keys = vec!["id".to_owned()];
        let error = MysqlWritePlan::compile_with_profile(
            &nullable,
            &on_nullable,
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect_err("PRIMARY KEY nullable accettata");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Prepare);
        assert!(error.message.contains("nullable"), "{}", error.message);

        // Una chiave ripetuta produrrebbe `PRIMARY KEY (id, id)`.
        let mut repeated = append_operation();
        repeated.mode = WriteMode::Create;
        repeated.keys = vec!["id".to_owned(), "id".to_owned()];
        assert_eq!(
            MysqlWritePlan::compile_with_profile(
                &input,
                &repeated,
                "warehouse",
                &crate::profile::MYSQL_PROFILE
            )
            .expect_err("chiave ripetuta accettata")
            .category,
            ErrorCategory::InvalidPlan
        );

        // `update_columns` non ha senso su Create: non aggiorna nulla.
        let mut updating = append_operation();
        updating.mode = WriteMode::Create;
        updating.update_columns = vec!["label".to_owned()];
        assert_eq!(
            MysqlWritePlan::compile_with_profile(
                &input,
                &updating,
                "warehouse",
                &crate::profile::MYSQL_PROFILE
            )
            .expect_err("update_columns accettate")
            .category,
            ErrorCategory::InvalidPlan
        );
    }

    /// I tipi che non possono stare in una PRIMARY KEY `MySQL` sono rifiutati
    /// dal piano, non dal server.
    ///
    /// Ciascuno di questi casi e stato verificato contro il riferimento e
    /// produce un errore lato server: 1170 per TEXT/BLOB, 3728 per le colonne
    /// spatial, 1070 oltre 16 parti. Arrivarci significa aver gia aperto la
    /// sessione ed eseguito la DDL.
    #[test]
    fn primary_key_types_and_limits_are_refused_before_the_server() {
        let cases: [(&str, DataType, &str); 4] = [
            ("utf8", DataType::Utf8, "TEXT"),
            ("binary", DataType::Binary, "BLOB"),
            ("float32", DataType::Float32, "virgola mobile"),
            ("float64", DataType::Float64, "virgola mobile"),
        ];
        for (name, data_type, expected) in cases {
            let input = schema(vec![
                Field::new(name, data_type, false),
                Field::new("payload", DataType::Int64, false),
            ]);
            let mut operation = append_operation();
            operation.mode = WriteMode::Create;
            operation.keys = vec![name.to_owned()];
            let Err(error) = MysqlWritePlan::compile_with_profile(
                &input,
                &operation,
                "warehouse",
                &crate::profile::MYSQL_PROFILE,
            ) else {
                panic!("{name}: chiave accettata");
            };
            assert_eq!(error.category, ErrorCategory::InvalidPlan, "{name}");
            assert_eq!(error.phase, ErrorPhase::Prepare, "{name}");
            assert!(
                error.message.contains(expected),
                "{name}: messaggio senza la ragione: {}",
                error.message
            );
        }

        // Spatial: il motore la rifiuta con 3728.
        // La fixture spatial e nullable, e il controllo sulla nullability
        // scatterebbe per primo nascondendo quello sul tipo: qui serve una
        // colonna spatial **non** nullable, cosi la sola ragione del rifiuto e
        // che MySQL non ammette indici spatial come chiave.
        let nullable_spatial = spatial_field("point", 4_326);
        let spatial = schema(vec![
            Field::new(
                nullable_spatial.name(),
                nullable_spatial.data_type().clone(),
                false,
            )
            .with_metadata(nullable_spatial.metadata().clone()),
            Field::new("payload", DataType::Int64, false),
        ]);
        let mut operation = spatial_operation();
        operation.mode = WriteMode::Create;
        operation.keys = vec!["geom".to_owned()];
        let error = MysqlWritePlan::compile_with_profile(
            &spatial,
            &operation,
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect_err("chiave spatial accettata");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert!(error.message.contains("spatial"), "{}", error.message);

        // Oltre 16 parti: il motore risponde 1070.
        let wide_fields = (0..17)
            .map(|index| Field::new(format!("k{index}"), DataType::Int64, false))
            .collect::<Vec<_>>();
        let wide = schema(wide_fields);
        let mut too_many = append_operation();
        too_many.mode = WriteMode::Create;
        too_many.keys = (0..17).map(|index| format!("k{index}")).collect();
        let error = MysqlWritePlan::compile_with_profile(
            &wide,
            &too_many,
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect_err("17 parti accettate");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert!(error.message.contains("16"), "{}", error.message);

        // 16 parti esatte restano ammesse: il limite e un confine, non un veto.
        let bounded_fields = (0..16)
            .map(|index| Field::new(format!("k{index}"), DataType::Int64, false))
            .collect::<Vec<_>>();
        let bounded = schema(bounded_fields);
        let mut exact = append_operation();
        exact.mode = WriteMode::Create;
        exact.keys = (0..16).map(|index| format!("k{index}")).collect();
        MysqlWritePlan::compile_with_profile(
            &bounded,
            &exact,
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect("16 parti rifiutate");
    }

    #[test]
    fn modes_without_key_semantics_reject_keys_and_update_columns() {
        for mode in [WriteMode::Append, WriteMode::Replace] {
            let mut operation = append_operation();
            operation.mode = mode;
            operation.keys = vec!["id".to_owned()];
            let error = MysqlWritePlan::compile_with_profile(
                &schema(vec![Field::new("id", DataType::Int64, false)]),
                &operation,
                "warehouse",
                &crate::profile::MYSQL_PROFILE,
            )
            .expect_err("keys accettate");
            assert_eq!(error.category, ErrorCategory::InvalidPlan);
            assert!(
                error.message.contains(&format!("{mode:?}")),
                "il messaggio deve nominare la mode: {}",
                error.message
            );

            let mut operation = append_operation();
            operation.mode = mode;
            operation.update_columns = vec!["id".to_owned()];
            assert_eq!(
                MysqlWritePlan::compile_with_profile(
                    &schema(vec![Field::new("id", DataType::Int64, false)]),
                    &operation,
                    "warehouse",
                    &crate::profile::MYSQL_PROFILE,
                )
                .expect_err("update_columns accettate")
                .category,
                ErrorCategory::InvalidPlan
            );
        }
    }

    #[test]
    fn compile_rejects_cross_database_and_layer_targets() {
        let input = schema(vec![Field::new("id", DataType::Int64, false)]);

        let mut cross_database = append_operation();
        cross_database.target.schema = Some("other_database".to_owned());
        let error = MysqlWritePlan::compile_with_profile(
            &input,
            &cross_database,
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect_err("target cross-database");
        assert_eq!(error.category, ErrorCategory::Unsupported);

        let mut layer = append_operation();
        layer.target.layer_id = Some(plenora_database_core::plan::LayerId::Number(1));
        let error = MysqlWritePlan::compile_with_profile(
            &input,
            &layer,
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect_err("layer MySQL");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }

    #[test]
    fn compile_rejects_empty_or_unqualified_arrow_schemas() {
        let error = MysqlWritePlan::compile_with_profile(
            &schema(Vec::new()),
            &append_operation(),
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect_err("schema vuoto");
        assert_eq!(error.category, ErrorCategory::Schema);

        let unsupported = schema(vec![Field::new(
            "created_at",
            DataType::Timestamp(
                plenora_database_core::arrow::schema::TimeUnit::Nanosecond,
                Some("UTC".into()),
            ),
            false,
        )]);
        let error = MysqlWritePlan::compile_with_profile(
            &unsupported,
            &append_operation(),
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect_err("timestamp con timezone non qualificato");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }

    /// Il contratto Arrow e parte del piano: una versione estranea non puo
    /// essere interpretata e non deve arrivare al server.
    #[test]
    fn compile_rejects_a_foreign_contract_version() {
        let foreign = Arc::new(Schema::new_with_metadata(
            vec![Field::new("id", DataType::Int64, false)],
            HashMap::from([(
                protocol::CONTRACT_VERSION_KEY.to_owned(),
                "999.0".to_owned(),
            )]),
        ));
        let error = MysqlWritePlan::compile_with_profile(
            &foreign,
            &append_operation(),
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect_err("contratto Arrow estraneo");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }

    fn server_column(
        name: &str,
        data_type: &str,
        declaration: &str,
        nullable: bool,
    ) -> MysqlColumn {
        MysqlColumn {
            name: name.to_owned(),
            ordinal: 1,
            data_type: data_type.to_owned(),
            native_declaration: declaration.to_owned(),
            nullable,
            default_expression: None,
            character_set: None,
            collation: None,
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
            spatial_srid: None,
            extra: String::new(),
            generation_expression: String::new(),
        }
    }

    fn base_table(columns: Vec<MysqlColumn>) -> MysqlObjectDescription {
        base_table_with_indexes(columns, Vec::new())
    }

    fn base_table_with_indexes(
        columns: Vec<MysqlColumn>,
        indexes: Vec<crate::MysqlIndex>,
    ) -> MysqlObjectDescription {
        MysqlObjectDescription {
            schema: "warehouse".to_owned(),
            name: "events".to_owned(),
            kind: "BASE TABLE".to_owned(),
            engine: Some("InnoDB".to_owned()),
            columns,
            indexes,
            token: MysqlSchemaToken("token".to_owned()),
        }
    }

    fn unique_index(name: &str, columns: &[&str]) -> crate::MysqlIndex {
        crate::MysqlIndex {
            name: name.to_owned(),
            unique: true,
            column_backed: true,
            columns: columns.iter().map(|c| (*c).to_owned()).collect(),
        }
    }

    fn identity_plan() -> MysqlWritePlan {
        append_plan(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ])
    }

    fn identity_target() -> Vec<MysqlColumn> {
        vec![
            server_column("id", "bigint", "bigint", false),
            server_column("label", "varchar", "varchar(32)", true),
        ]
    }

    fn server_error(code: u16, message: &str) -> mysql_async::Error {
        mysql_async::Error::Server(mysql_async::ServerError {
            code,
            message: message.to_owned(),
            state: "HY000".to_owned(),
        })
    }

    /// Il chunk non dipende dai dati ma dal numero di colonne: due esecuzioni
    /// della stessa append devono produrre esattamente gli stessi INSERT.
    #[test]
    fn chunk_size_is_deterministic_and_fits_the_placeholder_ceiling() {
        let single = append_plan(vec![Field::new("id", DataType::Int64, false)]);
        assert_eq!(single.rows_per_statement(), MAX_BIND_PARAMETERS);
        let pair = identity_plan();
        assert_eq!(pair.rows_per_statement(), MAX_BIND_PARAMETERS / 2);
        assert_eq!(
            pair.rows_per_statement(),
            identity_plan().rows_per_statement()
        );
        assert_eq!(
            pair.render_insert(pair.rows_per_statement())
                .expect("chunk al tetto")
                .matches('?')
                .count(),
            pair.rows_per_statement() * 2
        );
    }

    /// I valori viaggiano come bind del protocollo binario: il testo SQL resta
    /// fatto di soli placeholder anche per testo, decimal e NULL.
    #[test]
    fn chunk_binding_is_positional_and_never_interpolates_values() {
        let fields = vec![
            Field::new("flag", DataType::Boolean, false),
            Field::new("id", DataType::Int64, false),
            Field::new("count", DataType::UInt32, false),
            Field::new("ratio", DataType::Float64, false),
            Field::new("label", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
            Field::new("day", DataType::Date32, false),
            Field::new(
                "moment",
                DataType::Timestamp(
                    plenora_database_core::arrow::schema::TimeUnit::Microsecond,
                    None,
                ),
                false,
            ),
            Field::new("amount", DataType::Decimal128(12, 2), true),
        ];
        let plan = append_plan(fields.clone());
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
        let day = NaiveDate::from_ymd_opt(2026, 1, 2).expect("giorno");
        let days = i32::try_from(day.signed_duration_since(epoch).num_days()).expect("date32");
        let micros = day
            .and_hms_micro_opt(3, 4, 5, 123_456)
            .expect("istante")
            .and_utc()
            .timestamp_micros();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(Int64Array::from(vec![7, -7])),
            Arc::new(UInt32Array::from(vec![4_000_000_000, 0])),
            Arc::new(Float64Array::from(vec![1.5, -2.25])),
            Arc::new(StringArray::from(vec![Some("reference"), None])),
            Arc::new(BinaryArray::from_opt_vec(vec![Some(&[1_u8, 2][..]), None])),
            Arc::new(Date32Array::from(vec![days, days])),
            Arc::new(TimestampMicrosecondArray::from(vec![micros, micros])),
            Arc::new(
                Decimal128Array::from(vec![Some(-105_i128), None])
                    .with_precision_and_scale(12, 2)
                    .expect("decimal"),
            ),
        ];
        let batch = RecordBatch::try_new(schema(fields), columns).expect("batch append");
        let Params::Positional(values) = plan.bind_chunk(&batch, 0, 2).expect("bind del chunk")
        else {
            panic!("bind MySQL non posizionale");
        };
        assert_eq!(values.len(), 18);
        assert_eq!(values[0], Value::Int(1));
        assert_eq!(values[1], Value::Int(7));
        assert_eq!(values[2], Value::UInt(4_000_000_000));
        assert_eq!(values[3], Value::Double(1.5));
        assert_eq!(values[4], Value::Bytes(b"reference".to_vec()));
        assert_eq!(values[5], Value::Bytes(vec![1, 2]));
        assert_eq!(values[6], Value::Date(2026, 1, 2, 0, 0, 0, 0));
        assert_eq!(values[7], Value::Date(2026, 1, 2, 3, 4, 5, 123_456));
        assert_eq!(values[8], Value::Bytes(b"-1.05".to_vec()));
        assert_eq!(values[9], Value::Int(0));
        assert_eq!(values[13], Value::NULL);
        assert_eq!(values[14], Value::NULL);
        assert_eq!(values[17], Value::NULL);

        let sql = plan.render_insert(2).expect("insert del chunk");
        assert!(!sql.contains("reference"), "{sql}");
        assert!(!sql.contains("1.05"), "{sql}");
        assert!(!sql.to_ascii_uppercase().contains("INFILE"), "{sql}");
    }

    /// Una cella NULL in una colonna dichiarata non nullable e un errore di
    /// mapping locale: va vista prima di aprire la transazione.
    #[test]
    fn null_cells_in_non_nullable_columns_fail_before_the_network() {
        let plan = append_plan(vec![Field::new("id", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            schema(vec![Field::new("id", DataType::Int64, true)]),
            vec![Arc::new(Int64Array::from(vec![None, Some(2)])) as ArrayRef],
        )
        .expect("batch con NULL");
        let error = plan
            .bind_chunk(&batch, 0, 2)
            .expect_err("NULL in colonna non nullable");
        assert_eq!(error.category, ErrorCategory::DataMapping);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    /// Il chunk deve restare dentro il batch: un intervallo fuori misura e un
    /// errore esplicito, non una lettura oltre la fine dell'array.
    #[test]
    fn chunk_bounds_are_checked_against_the_batch() {
        let fields = vec![Field::new("id", DataType::Int64, false)];
        let plan = append_plan(fields.clone());
        let batch = RecordBatch::try_new(
            schema(fields),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef],
        )
        .expect("batch");
        assert_eq!(
            plan.bind_chunk(&batch, 1, 2)
                .expect_err("chunk oltre il batch")
                .category,
            ErrorCategory::InvalidPlan
        );
        assert_eq!(
            plan.bind_chunk(&batch, 0, 0)
                .expect_err("chunk vuoto")
                .category,
            ErrorCategory::InvalidPlan
        );
    }

    /// Lo schema del batch e quello dichiarato dallo stream: una deriva va
    /// vista prima di convertire i valori.
    #[test]
    fn batch_schema_drift_is_rejected_before_binding() {
        let declared = schema(vec![Field::new("id", DataType::Int64, false)]);
        let stable = RecordBatch::try_new(
            Arc::clone(&declared),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .expect("batch stabile");
        validate_batch_schema(&stable, &declared).expect("schema stabile");

        let drifted = RecordBatch::try_new(
            schema(vec![Field::new("renamed", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .expect("batch deviato");
        let error = validate_batch_schema(&drifted, &declared).expect_err("schema deviato");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Write);
    }

    /// Strict puo dichiarare zero perdite solo dopo aver visto lo schema del
    /// server: e il preflight, non il piano offline, a stabilirlo.
    #[test]
    fn server_preflight_reports_no_losses_only_for_a_compatible_table() {
        let report = identity_plan()
            .preflight(
                &base_table(vec![
                    server_column("id", "bigint", "bigint", false),
                    server_column("label", "varchar", "varchar(32)", true),
                    server_column("noted_at", "datetime", "datetime(6)", true),
                ]),
                &crate::profile::MYSQL_PROFILE,
            )
            .expect("preflight compatibile");
        assert_eq!(report.schema_version, 1);
        assert_eq!(report.policy, MappingPolicy::Strict);
        assert!(report.losses.is_empty());
        assert!(report.permits_execution());
    }

    /// Ogni divergenza fra schema Arrow e schema server e una perdita che
    /// Strict non ammette: nessuna transazione deve essere aperta.
    #[test]
    fn server_preflight_rejects_targets_that_strict_cannot_write() {
        let plan = identity_plan();
        let cases = vec![
            vec![server_column("id", "bigint", "bigint", false)],
            vec![
                server_column("id", "bigint", "bigint", false),
                server_column("label", "int", "int", true),
            ],
            vec![
                server_column("id", "bigint", "bigint", false),
                server_column("label", "varchar", "varchar(32)", false),
            ],
            vec![
                server_column("id", "bigint", "bigint", false),
                server_column("label", "varchar", "varchar(32)", true),
                server_column("mandatory", "int", "int", false),
            ],
        ];
        for columns in cases {
            let error = plan
                .preflight(&base_table(columns), &crate::profile::MYSQL_PROFILE)
                .expect_err("target incompatibile");
            assert_eq!(error.category, ErrorCategory::DataMapping);
            assert_eq!(error.phase, ErrorPhase::Prepare);
        }

        let mut generated = base_table(identity_target());
        generated.columns[1].generation_expression = "concat('x')".to_owned();
        assert_eq!(
            plan.preflight(&generated, &crate::profile::MYSQL_PROFILE)
                .expect_err("colonna generata")
                .category,
            ErrorCategory::DataMapping
        );

        let mut view = base_table(identity_target());
        view.kind = "VIEW".to_owned();
        assert_eq!(
            plan.preflight(&view, &crate::profile::MYSQL_PROFILE)
                .expect_err("target non tabella")
                .category,
            ErrorCategory::Unsupported
        );
    }

    /// Una colonna JSON, ENUM o BIT non e ancora un target di scrittura
    /// qualificato anche se in lettura collassa su Utf8 o Binary.
    #[test]
    fn server_preflight_keeps_unqualified_write_targets_closed() {
        let plan = identity_plan();
        for (data_type, declaration) in [
            ("json", "json"),
            ("enum", "enum('alpha','beta')"),
            ("set", "set('read','write')"),
            ("char", "char(8)"),
        ] {
            let error = plan
                .preflight(
                    &base_table(vec![
                        server_column("id", "bigint", "bigint", false),
                        server_column("label", data_type, declaration, true),
                    ]),
                    &crate::profile::MYSQL_PROFILE,
                )
                .expect_err("target non qualificato");
            assert_eq!(error.category, ErrorCategory::DataMapping);
        }

        let year = append_plan(vec![Field::new("year_value", DataType::Int16, false)]);
        assert_eq!(
            year.preflight(
                &base_table(vec![server_column("year_value", "year", "year", false,)]),
                &crate::profile::MYSQL_PROFILE
            )
            .expect_err("YEAR reinterpreta Int16")
            .category,
            ErrorCategory::DataMapping
        );

        let binary = append_plan(vec![Field::new("payload", DataType::Binary, true)]);
        for (data_type, declaration) in [("bit", "bit(16)"), ("binary", "binary(16)")] {
            assert_eq!(
                binary
                    .preflight(
                        &base_table(vec![
                            server_column("payload", data_type, declaration, true,)
                        ]),
                        &crate::profile::MYSQL_PROFILE
                    )
                    .expect_err("target binary non qualificato")
                    .category,
                ErrorCategory::DataMapping
            );
        }
    }

    #[test]
    fn server_preflight_requires_microsecond_temporal_precision() {
        let plan = append_plan(vec![Field::new(
            "moment",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);
        for precision in [None, Some(0), Some(3)] {
            let mut column = server_column("moment", "datetime", "datetime", true);
            column.datetime_precision = precision;
            assert_eq!(
                plan.preflight(&base_table(vec![column]), &crate::profile::MYSQL_PROFILE)
                    .expect_err("precisione temporale lossy")
                    .category,
                ErrorCategory::DataMapping
            );
        }
        let mut exact = server_column("moment", "datetime", "datetime(6)", true);
        exact.datetime_precision = Some(6);
        assert!(plan
            .preflight(&base_table(vec![exact]), &crate::profile::MYSQL_PROFILE)
            .is_ok());
    }

    /// Un COMMIT interrotto non e un rollback: l'esito resta ignoto e non
    /// autorizza retry automatico.
    #[test]
    fn commit_interruption_produces_an_unknown_outcome_without_automatic_retry() {
        let interrupted = crate::error::timeout_error(
            &crate::profile::MYSQL_PROFILE,
            ErrorPhase::Commit,
            RemoteEffect::None,
        );
        let outcome = commit_failure(interrupted, "mysql-test-1".to_owned(), 7)
            .expect("esito ignoto pubblicabile");
        outcome.validate().expect("outcome valido");
        assert_eq!(outcome.status, WriteStatus::OutcomeUnknown);
        assert_eq!(outcome.provider, ProviderKind::Mysql);
        assert_eq!(outcome.rows.received, 7);
        assert_eq!(outcome.rows.confirmed, 0);
        let recovery = outcome.recovery.expect("recovery obbligatoria");
        assert!(!recovery.automatic_retry_allowed);
        assert_eq!(
            recovery.last_certain_phase,
            CertainPhase::CommitOrEditRequested
        );
    }

    /// Il deadlock e l'unico esito che il server dichiara annullato: resta
    /// `RolledBack` anche quando emerge al commit o senza rollback confermato.
    #[test]
    fn a_declared_deadlock_stays_rolled_back_instead_of_unknown() {
        let deadlock = crate::error::driver_error(
            &crate::profile::MYSQL_PROFILE,
            &server_error(1_213, "Deadlock found when trying to get lock"),
            ErrorPhase::Write,
            RemoteEffect::None,
        );
        assert_eq!(deadlock.remote_effect, RemoteEffect::RolledBack);

        let error = commit_failure(deadlock.clone(), "mysql-test-2".to_owned(), 3)
            .expect_err("deadlock dichiarato dal server");
        assert_eq!(error.remote_effect, RemoteEffect::RolledBack);
        assert_eq!(error.execution_id.as_deref(), Some("mysql-test-2"));

        let shaped = rolled_back_error(deadlock, false, "mysql-test-2");
        assert_eq!(shaped.remote_effect, RemoteEffect::RolledBack);
        assert_eq!(shaped.execution_id.as_deref(), Some("mysql-test-2"));
    }

    /// Un errore pre-commit puo dichiarare `RolledBack` solo dopo un ROLLBACK
    /// confermato: altrimenti l'effetto remoto resta ignoto.
    #[test]
    fn pre_commit_errors_claim_rollback_only_when_it_is_confirmed() {
        let failure = crate::error::driver_error(
            &crate::profile::MYSQL_PROFILE,
            &server_error(1_062, "Duplicate entry"),
            ErrorPhase::Write,
            RemoteEffect::None,
        );
        let confirmed = rolled_back_error(failure.clone(), true, "mysql-test-3");
        assert_eq!(confirmed.category, ErrorCategory::Conflict);
        assert_eq!(confirmed.remote_effect, RemoteEffect::RolledBack);
        assert_eq!(confirmed.retry, RetryDisposition::Never);

        let ambiguous = rolled_back_error(failure, false, "mysql-test-3");
        assert_eq!(ambiguous.remote_effect, RemoteEffect::Unknown);
        assert_eq!(ambiguous.retry, RetryDisposition::RequiresRecovery);
    }

    #[test]
    fn an_already_quarantined_error_stays_non_retryable_when_rollback_is_unobservable() {
        let quarantined = DatabaseError {
            category: ErrorCategory::Protocol,
            phase: ErrorPhase::Write,
            remote_effect: RemoteEffect::Unknown,
            retry: RetryDisposition::Quarantine,
            provider: Some(ProviderKind::Mysql),
            execution_id: None,
            message: "conteggio righe MySQL incoerente".to_owned(),
            diagnostics: None,
        };

        let shaped = rolled_back_error(quarantined, false, "mysql-test-quarantine");
        assert_eq!(shaped.remote_effect, RemoteEffect::Unknown);
        assert_eq!(shaped.retry, RetryDisposition::Quarantine);
        assert!(!shaped.is_retryable());
        assert_eq!(
            shaped.execution_id.as_deref(),
            Some("mysql-test-quarantine")
        );
    }

    /// Il conteggio pubblicato deve superare la validazione del contratto e
    /// non puo confermare piu righe di quante ne siano state ricevute.
    /// Un `Create` fallito non e "come prima": la tabella creata dalla DDL
    /// sopravvive al rollback, perche su MySQL il DDL fa commit implicito.
    /// L'esito deve dirlo su **ogni** uscita, altrimenti un retry cieco
    /// sbatte contro `Conflict` su un target che il chiamante crede assente.
    #[test]
    fn a_created_table_survives_the_rollback_and_every_outcome_says_so() {
        let failure = write_error(ErrorCategory::Protocol, "insert fallita");

        // Senza residuo l'esito passa invariato.
        let clean = stamp_ddl_residue(
            Err(rolled_back_error(failure.clone(), true, "mysql-create-1")),
            DdlResidue::None,
        )
        .expect_err("errore propagato");
        assert_eq!(clean.remote_effect, RemoteEffect::RolledBack);
        assert!(!clean.message.contains("commit implicito"));

        // Rollback confermato: righe annullate, schema no.
        let residual = stamp_ddl_residue(
            Err(rolled_back_error(failure.clone(), true, "mysql-create-2")),
            DdlResidue::CreatedTable,
        )
        .expect_err("errore propagato");
        assert_eq!(residual.remote_effect, RemoteEffect::Partial);
        assert_eq!(residual.retry, RetryDisposition::RequiresRecovery);
        assert!(residual.message.contains("commit implicito"));

        // Uscita che non ha mai aperto la transazione (`describe_object`,
        // preflight, START TRANSACTION): `None` e altrettanto falso.
        let untouched = stamp_ddl_residue(
            Err(write_error(ErrorCategory::Schema, "preflight cambiato")),
            DdlResidue::CreatedTable,
        )
        .expect_err("errore propagato");
        assert_eq!(untouched.remote_effect, RemoteEffect::Partial);
        assert_eq!(untouched.retry, RetryDisposition::RequiresRecovery);

        // Rollback non confermato: l'incertezza sulle righe resta.
        let ambiguous = stamp_ddl_residue(
            Err(rolled_back_error(failure, false, "mysql-create-3")),
            DdlResidue::CreatedTable,
        )
        .expect_err("errore propagato");
        assert_eq!(ambiguous.remote_effect, RemoteEffect::Unknown);

        // La quarantena e la disposizione piu forte e non viene declassata.
        let mut quarantined = write_error(ErrorCategory::Timeout, "timeout");
        quarantined.retry = RetryDisposition::Quarantine;
        let shaped = stamp_ddl_residue(Err(quarantined), DdlResidue::CreatedTable)
            .expect_err("errore propagato");
        assert_eq!(shaped.retry, RetryDisposition::Quarantine);

        // Commit ambiguo: l'esito resta `OutcomeUnknown`, ma la nota di
        // verifica dice che trovare la tabella non prova nulla sulle righe.
        let unknown = commit_failure(
            write_error(ErrorCategory::Timeout, "commit ambiguo"),
            "mysql-create-4".to_owned(),
            3,
        )
        .expect("outcome unknown");
        let stamped =
            stamp_ddl_residue(Ok(unknown), DdlResidue::CreatedTable).expect("outcome propagato");
        assert_eq!(stamped.status, WriteStatus::OutcomeUnknown);
        assert!(stamped
            .recovery
            .as_ref()
            .and_then(|recovery| recovery.verification_action.as_deref())
            .is_some_and(|action| action.contains("esiste comunque")));

        // Un commit riuscito non viene toccato: la tabella doveva esserci.
        let committed =
            committed_outcome_for_mode("mysql-create-5".to_owned(), 3, 3, WriteMode::Create)
                .expect("outcome committed");
        let stamped = stamp_ddl_residue(Ok(committed.clone()), DdlResidue::CreatedTable)
            .expect("outcome propagato");
        assert_eq!(stamped, committed);
    }

    /// Dopo un COMMIT riuscito nessun errore puo dire che il server e come
    /// prima.
    ///
    /// Se la validazione del documento fallisce, a essere incoerente e il
    /// conteggio pubblicato, non lo stato remoto: le righe sono scritte. Un
    /// esito `None`, `RolledBack` o `Partial` inviterebbe a un retry che le
    /// raddoppierebbe.
    #[test]
    fn an_error_after_a_successful_commit_declares_the_rows_committed() {
        // `confirmed > received` e incoerente per contratto: il documento non
        // valida, ma il COMMIT e gia avvenuto.
        let error = committed_outcome_for_mode("mysql-post-1".to_owned(), 1, 5, WriteMode::Append)
            .expect_err("documento incoerente accettato");
        assert_eq!(error.remote_effect, RemoteEffect::Committed);
        assert_eq!(error.retry, RetryDisposition::Never);
        assert_eq!(error.category, ErrorCategory::Internal);
        assert!(error.message.contains("committed"));

        // Il residuo della DDL non lo tocca: la tabella c'e per forza, e
        // chiedere recupero suggerirebbe il retry che va evitato.
        let stamped = stamp_ddl_residue(Err(error.clone()), DdlResidue::CreatedTable)
            .expect_err("errore propagato");
        assert_eq!(stamped.remote_effect, RemoteEffect::Committed);
        assert_eq!(stamped.retry, RetryDisposition::Never);
        assert_eq!(
            stamped.message, error.message,
            "il residuo ha aggiunto rumore a un esito gia committed"
        );
    }

    #[test]
    fn committed_outcome_row_counts_are_contract_valid() {
        let outcome =
            committed_outcome("mysql-test-4".to_owned(), 5, 5).expect("outcome committed");
        outcome.validate().expect("outcome valido");
        assert_eq!(outcome.status, WriteStatus::Committed);
        assert_eq!(outcome.provider, ProviderKind::Mysql);
        assert_eq!(outcome.rows.inserted, Some(5));
        assert_eq!(outcome.rows.updated, Some(0));
        assert_eq!(outcome.rows.deleted, Some(0));
        assert_eq!(outcome.rows.failed, 0);
        assert_eq!(outcome.rows.skipped, 0);
        assert!(outcome.recovery.is_none());

        assert_eq!(
            committed_outcome("mysql-test-5".to_owned(), 2, 3)
                .expect_err("conferme oltre le righe ricevute")
                .category,
            ErrorCategory::Internal
        );
    }

    fn spatial_column(native_type: &str, srid: u32) -> MysqlColumn {
        let mut column = server_column("geom", native_type, native_type, true);
        column.spatial_srid = Some(srid);
        column
    }

    fn spatial_field(native_type: &str, srid: u32) -> Field {
        MysqlColumnSpec::from_catalog(&spatial_column(native_type, srid))
            .expect("colonna spatial qualificata")
            .arrow_field()
    }

    fn spatial_operation() -> WriteOperation {
        let mut operation = append_operation();
        operation.srid_policy = Some(plenora_database_core::plan::SridPolicy::RequireMatch);
        operation
    }

    fn point_wkb(type_word: u32, srid: Option<u32>, ordinates: &[f64]) -> Vec<u8> {
        let mut bytes = vec![1_u8];
        bytes.extend_from_slice(&type_word.to_le_bytes());
        if let Some(srid) = srid {
            bytes.extend_from_slice(&srid.to_le_bytes());
        }
        for ordinate in ordinates {
            bytes.extend_from_slice(&ordinate.to_le_bytes());
        }
        bytes
    }

    #[test]
    fn compile_and_preflight_qualify_only_xy_wkb_with_matching_srid() {
        let input = schema(vec![spatial_field("geometry", 4_326)]);
        let plan = MysqlWritePlan::compile_with_profile(
            &input,
            &spatial_operation(),
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect("piano spatial XY");
        assert_eq!(plan.columns[0].kind, MysqlColumnKind::Geometry);
        assert_eq!(plan.columns[0].spatial_srid, Some(4_326));
        assert_eq!(
            plan.render_insert(1).expect("insert geometry"),
            "INSERT INTO `warehouse`.`events` (`geom`) VALUES (ST_GeomFromWKB(?, 4326));"
        );
        assert!(plan
            .preflight(
                &base_table(vec![spatial_column("geometry", 4_326)]),
                &crate::profile::MYSQL_PROFILE
            )
            .is_ok());
        assert_eq!(
            plan.preflight(
                &base_table(vec![spatial_column("geometry", 3_857)]),
                &crate::profile::MYSQL_PROFILE
            )
            .expect_err("SRID target diverso")
            .category,
            ErrorCategory::Crs
        );
    }

    #[test]
    fn compile_rejects_dimensions_the_mysql_server_cannot_represent() {
        for dimensions in ["xyz", "xym", "xyzm"] {
            let mut metadata = spatial_field("geometry", 4_326).metadata().clone();
            metadata.insert(
                protocol::GEOMETRY_DIMENSIONS.to_owned(),
                dimensions.to_owned(),
            );
            let field = Field::new("geom", DataType::Binary, true).with_metadata(metadata);
            let error = MysqlWritePlan::compile_with_profile(
                &schema(vec![field]),
                &spatial_operation(),
                "warehouse",
                &crate::profile::MYSQL_PROFILE,
            )
            .expect_err("dimensione non rappresentabile da MySQL");
            assert_eq!(error.category, ErrorCategory::Unsupported);
        }
    }

    #[test]
    fn spatial_batch_rejects_ewkb_srid_and_z_before_binding() {
        let input = schema(vec![spatial_field("geometry", 4_326)]);
        let plan = MysqlWritePlan::compile_with_profile(
            &input,
            &spatial_operation(),
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect("piano spatial XY");
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget spatial");
        for payload in [
            point_wkb(0x2000_0001, Some(4_326), &[1.0, 2.0]),
            point_wkb(1_001, None, &[1.0, 2.0, 3.0]),
            point_wkb(2_001, None, &[1.0, 2.0, 3.0]),
            point_wkb(3_001, None, &[1.0, 2.0, 3.0, 4.0]),
        ] {
            let batch = RecordBatch::try_new(
                Arc::clone(&input),
                vec![Arc::new(BinaryArray::from(vec![payload.as_slice()])) as ArrayRef],
            )
            .expect("batch spatial non qualificato");
            assert_eq!(
                plan.validate_spatial_batch(&batch, &budget)
                    .expect_err("payload spatial non qualificato")
                    .category,
                ErrorCategory::DataMapping
            );
        }

        let xy = point_wkb(1, None, &[1.0, 2.0]);
        let batch = RecordBatch::try_new(
            input,
            vec![Arc::new(BinaryArray::from(vec![xy.as_slice()])) as ArrayRef],
        )
        .expect("batch spatial XY");
        assert_eq!(
            plan.validate_spatial_batch(&batch, &budget)
                .expect("WKB XY")
                .components,
            2
        );
        let Params::Positional(values) = plan
            .bind_chunk(&batch, 0, 1)
            .expect("bind WKB XY posizionale")
        else {
            panic!("bind MySQL non posizionale");
        };
        assert_eq!(values, vec![Value::Bytes(xy)]);
    }

    #[test]
    fn spatial_batch_enforces_exact_type_and_cumulative_component_budget() {
        let input = schema(vec![spatial_field("linestring", 4_326)]);
        let plan = MysqlWritePlan::compile_with_profile(
            &input,
            &spatial_operation(),
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect("piano spatial exact");
        let point = point_wkb(1, None, &[1.0, 2.0]);
        let wrong_type = RecordBatch::try_new(
            Arc::clone(&input),
            vec![Arc::new(BinaryArray::from(vec![point.as_slice()])) as ArrayRef],
        )
        .expect("batch con tipo geometry errato");
        assert_eq!(
            plan.validate_spatial_batch(
                &wrong_type,
                &ResourceBudget::new(ResourceLimits::default()).expect("budget exact"),
            )
            .expect_err("tipo geometry diverso dal contratto")
            .category,
            ErrorCategory::DataMapping
        );

        let input = schema(vec![spatial_field("point", 4_326)]);
        let plan = MysqlWritePlan::compile_with_profile(
            &input,
            &spatial_operation(),
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect("piano point");
        let limits = ResourceLimits {
            geometry_components: 3,
            ..ResourceLimits::default()
        };
        let budget = ResourceBudget::new(limits).expect("budget componenti");
        let two_points = RecordBatch::try_new(
            Arc::clone(&input),
            vec![Arc::new(BinaryArray::from(vec![point.as_slice(), point.as_slice()])) as ArrayRef],
        )
        .expect("batch due point");
        assert_eq!(
            plan.validate_spatial_batch(&two_points, &budget)
                .expect_err("quattro componenti oltre il budget tre")
                .category,
            ErrorCategory::ResourceLimit
        );
        assert_eq!(budget.remaining(ResourceKind::GeometryComponents), 3);

        let one_point = RecordBatch::try_new(
            input,
            vec![Arc::new(BinaryArray::from(vec![point.as_slice()])) as ArrayRef],
        )
        .expect("batch un point");
        plan.validate_spatial_batch(&one_point, &budget)
            .expect("due componenti consumati");
        assert_eq!(budget.remaining(ResourceKind::GeometryComponents), 1);
        assert_eq!(
            plan.validate_spatial_batch(&one_point, &budget)
                .expect_err("budget cumulativo esaurito")
                .category,
            ErrorCategory::ResourceLimit
        );
        assert_eq!(budget.remaining(ResourceKind::GeometryComponents), 1);
    }

    // ============================ Upsert: rendering + policy indici ============

    fn upsert_operation(keys: Vec<String>) -> WriteOperation {
        WriteOperation {
            mode: WriteMode::Upsert,
            keys,
            ..append_operation()
        }
    }

    fn upsert_plan(fields: Vec<Field>, keys: Vec<String>) -> MysqlWritePlan {
        MysqlWritePlan::compile_with_profile(
            &schema(fields),
            &upsert_operation(keys),
            "warehouse",
            &crate::profile::MYSQL_PROFILE,
        )
        .expect("piano upsert qualificato")
    }

    /// Un Upsert con colonne non-key rende `ON DUPLICATE KEY UPDATE` che
    /// aggiorna esattamente le non-key con i VALUES della riga.
    #[test]
    fn upsert_renders_on_duplicate_update_for_non_key_columns() {
        let plan = upsert_plan(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("label", DataType::Utf8, true),
            ],
            vec!["id".to_owned()],
        );
        assert_eq!(
            plan.render_insert(1).expect("insert upsert"),
            "INSERT INTO `warehouse`.`events` (`id`, `label`) VALUES (?, ?) \
             ON DUPLICATE KEY UPDATE `label`=VALUES(`label`);"
        );
    }

    /// Un Upsert **keys-only** (schema di sole key) non deve degradare a un
    /// INSERT nudo che erra sul primo conflitto: rende una clausola no-op
    /// `k=k` per ottenere semantica insert-or-ignore idempotente.
    #[test]
    fn upsert_keys_only_renders_noop_on_duplicate_clause() {
        let plan = upsert_plan(
            vec![Field::new("id", DataType::Int64, false)],
            vec!["id".to_owned()],
        );
        assert_eq!(
            plan.render_insert(2).expect("insert upsert keys-only"),
            "INSERT INTO `warehouse`.`events` (`id`) VALUES (?), (?) \
             ON DUPLICATE KEY UPDATE `id`=`id`;"
        );
    }

    /// Le keys devono corrispondere a un PK/UNIQUE index reale.
    #[test]
    fn upsert_preflight_accepts_keys_matching_a_unique_index() {
        let plan = upsert_plan(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("label", DataType::Utf8, true),
            ],
            vec!["id".to_owned()],
        );
        let target =
            base_table_with_indexes(identity_target(), vec![unique_index("PRIMARY", &["id"])]);
        assert!(plan
            .preflight(&target, &crate::profile::MYSQL_PROFILE)
            .is_ok());
    }

    /// Un unique index **aggiuntivo** diverso dalle keys rende l'Upsert
    /// non sicuro: ON DUPLICATE KEY potrebbe colpire la riga sbagliata.
    #[test]
    fn upsert_preflight_rejects_a_conflicting_extra_unique_index() {
        let plan = upsert_plan(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("label", DataType::Utf8, true),
            ],
            vec!["id".to_owned()],
        );
        let target = base_table_with_indexes(
            identity_target(),
            vec![
                unique_index("PRIMARY", &["id"]),
                unique_index("uq_label", &["label"]),
            ],
        );
        let error = plan
            .preflight(&target, &crate::profile::MYSQL_PROFILE)
            .expect_err("unique index in conflitto");
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    /// Nessun unique index sulle keys → l'Upsert inserirebbe duplicati.
    #[test]
    fn upsert_preflight_rejects_keys_without_a_backing_unique_index() {
        let plan = upsert_plan(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("label", DataType::Utf8, true),
            ],
            vec!["id".to_owned()],
        );
        // Solo un indice non-unique su id: non ancora l'ancora richiesta.
        let non_unique = crate::MysqlIndex {
            name: "idx_id".to_owned(),
            unique: false,
            column_backed: true,
            columns: vec!["id".to_owned()],
        };
        let target = base_table_with_indexes(identity_target(), vec![non_unique]);
        let error = plan
            .preflight(&target, &crate::profile::MYSQL_PROFILE)
            .expect_err("nessun unique index sulle keys");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }

    /// Un unique index funzionale (espressione) non è confrontabile con le
    /// keys → fail-closed.
    #[test]
    fn upsert_preflight_rejects_a_functional_unique_index() {
        let plan = upsert_plan(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("label", DataType::Utf8, true),
            ],
            vec!["id".to_owned()],
        );
        let functional = crate::MysqlIndex {
            name: "uq_expr".to_owned(),
            unique: true,
            column_backed: false,
            columns: Vec::new(),
        };
        let target = base_table_with_indexes(
            identity_target(),
            vec![unique_index("PRIMARY", &["id"]), functional],
        );
        let error = plan
            .preflight(&target, &crate::profile::MYSQL_PROFILE)
            .expect_err("unique index funzionale");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }

    /// Un unique index composito ridondante sulle **stesse** colonne delle
    /// keys (stesso insieme) è ammesso: colpisce sempre la stessa riga.
    #[test]
    fn upsert_preflight_accepts_a_redundant_unique_index_on_the_same_keys() {
        let plan = upsert_plan(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("label", DataType::Utf8, true),
            ],
            vec!["id".to_owned()],
        );
        let target = base_table_with_indexes(
            identity_target(),
            vec![
                unique_index("PRIMARY", &["id"]),
                unique_index("uq_id_dup", &["id"]),
            ],
        );
        assert!(plan
            .preflight(&target, &crate::profile::MYSQL_PROFILE)
            .is_ok());
    }
}
