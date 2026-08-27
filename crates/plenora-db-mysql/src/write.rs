//! Preparazione ed esecuzione delle modalita di scrittura `MySQL`.
//!
//! Il percorso valida il piano prima di costruire DDL, staging e statement
//! DML specifici della modalita richiesta.

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
        // XY è l'unico profilo dimensionale misurato per questi motori:
        // `raw.geometry_dimensions` ha chiesto al
        // **parser** `POINT Z(1 2 3)` nelle due sintassi WKT, e `MySQL` risponde
        // 3037 — WKT non valido — mentre `MariaDB` lo parsa a `NULL`. Anche
        // `ST_Z` e `ST_M` sono assenti da entrambi.
        //
        // La chiusura smette percio di essere «non qualificata» e diventa un
        // fatto del prodotto: non c'e una Z da scrivere, non che non sia stata
        // provata.
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
                    // `CAST(? AS BINARY)`, e il cast non e prudenza: e la sola
                    // forma legata che entrambi i prodotti accettano.
                    //
                    // Un segnaposto non e un'espressione tipata, e la
                    // differenza si vede solo su uno dei due. `MariaDB`
                    // risponde 4079 —
                    // `ER_ILLEGAL_PARAMETER_DATA_TYPE_FOR_OPERATION` — a
                    // `ST_GeomFromWKB(?, <n>)` mentre accetta la stessa
                    // funzione su un valore tipato; `MySQL` accetta entrambe.
                    // Misurato da `raw.spatial_write_forms` sulle tre varianti:
                    // nudo, con cast, e senza SRID.
                    //
                    // Il cast e condiviso invece di essere una decisione del
                    // profilo perche una forma sola che vale su tutti e due e
                    // meglio di due che divergono senza doverlo: l'SRID resta
                    // memorizzato — 4326 su entrambi — e non c'e niente da
                    // guadagnare tenendo due rendering.
                    |srid| format!("ST_GeomFromWKB(CAST(? AS BINARY), {srid})"),
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
        // Il corpo di ON DUPLICATE KEY UPDATE e gia validato e costruito da
        // `upsert_on_duplicate_clause`.
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
            // L'SRID del contratto Arrow **e** la dichiarazione, su questo
            // percorso. Costruire la spec senza passarlo faceva rifiutare ogni
            // colonna geometrica di un prodotto il cui catalogo tace: il
            // rifiuto arrivava dalla regola della lettura — «il catalogo tace e
            // il piano non lo dichiara» — applicata a un piano che lo dichiara
            // eccome, solo altrove.
            let spec =
                MysqlColumnSpec::from_catalog_declaring(server, profile, column.spatial_srid)?;
            if column.kind == MysqlColumnKind::Geometry {
                if spec.kind != MysqlColumnKind::Geometry {
                    return Err(mapping_error(format!(
                        "campo geometry Arrow diretto a una colonna {product} non spatial"
                    )));
                }
                // La compatibilita la decide il profilo, e non e la stessa
                // domanda sui due prodotti: dove la colonna e vincolata il
                // catalogo deve portare **quell'**SRID, dove non puo esserlo il
                // catalogo tace e non c'e niente da confrontare. La
                // compatibilità non può quindi essere un semplice confronto
                // tra `Option`.
                let declared = column.spatial_srid.ok_or_else(|| {
                    crs_error(format!("write spatial {product} senza SRID dichiarato"))
                })?;
                if !profile.geometry_target_srid_is_compatible(server.spatial_srid, declared) {
                    return Err(crs_error(format!(
                        "SRID target {product} incompatibile col contratto Arrow"
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
            schema_version: 2,
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
        schema_version: 2,
        status: WriteStatus::Committed,
        execution_id,
        provider: crate::profile::PROVISIONAL_KIND,
        rows,
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
        schema_version: 2,
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
        recovery: Some(Recovery {
            last_certain_phase: CertainPhase::CommitRequested,
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
        // Upsert/DeleteByKeys non ammettono `update_columns` esplicite.
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
    if operation.create_spatial_index && operation.mode != WriteMode::Create {
        // L'indice si crea con la tabella. Su una mode che non emette DDL non
        // c'e un `CREATE TABLE` in cui metterlo, e aggiungerlo con un `ALTER`
        // separato sarebbe una seconda istruzione con un secondo commit
        // implicito: un fallimento a meta lascerebbe la tabella con l'indice e
        // senza le righe, o il contrario, e l'esito non saprebbe dirlo.
        return Err(unsupported(format!(
            "create_spatial_index appartiene alla mode Create, non a {:?}",
            operation.mode
        )));
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
/// stato verificato contro il riferimento, e ciascuno fallirebbe al
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

// ============================ DDL per Create/TruncateInsert ===================

/// Genera la dichiarazione MySQL per un tipo di colonna.
///
/// La forma della colonna geometrica la decide il **profilo**: `MySQL` la
/// vincola a un SRID, `MariaDB` non puo — e il CRS gli viaggia dentro i valori.
/// Sceglierla qui avrebbe messo una divergenza di prodotto dentro una tabella
/// di tipi, che e l'ultimo posto dove qualcuno la cercherebbe.
fn mysql_column_ddl(
    kind: &MysqlColumnKind,
    spatial_srid: Option<u32>,
    exact_geometry_type: Option<&str>,
    profile: &dyn crate::profile::ProductProfile,
) -> String {
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
        // Il tipo **esatto** quando il contratto lo dichiara. La DDL emetteva
        // `GEOMETRY` anche per un contratto `exact`, cioe creava una colonna
        // che accetta qualunque geometria per dati che ne contengono una sola:
        // il contratto diceva una cosa piu forte di quella che la tabella
        // faceva rispettare, e il primo a scriverci un poligono dentro non
        // avrebbe trovato nessuno a fermarlo.
        //
        // `raw.exact_geometry_column` ha misurato che entrambi i prodotti
        // reggono la colonna tipata e **rifiutano** il tipo sbagliato — 1366 su
        // `MariaDB`, 1416 su `MySQL`.
        MysqlColumnKind::Geometry => {
            let base = exact_geometry_type.map_or("GEOMETRY", |exact| match exact {
                "point" => "POINT",
                "linestring" => "LINESTRING",
                "polygon" => "POLYGON",
                "multipoint" => "MULTIPOINT",
                "multilinestring" => "MULTILINESTRING",
                "multipolygon" => "MULTIPOLYGON",
                "geometrycollection" => "GEOMETRYCOLLECTION",
                // Il piano ammette solo i sette di sopra — lo decide
                // `writable_geometry_type` — e un nome fuori da quell'insieme
                // non arriva qui. Se ci arrivasse, la colonna generica e
                // l'unica scelta che non inventa un tipo SQL.
                _ => "GEOMETRY",
            });
            match spatial_srid {
                Some(srid) => profile
                    .geometry_column_ddl(srid)
                    .replacen("GEOMETRY", base, 1),
                None => base.to_owned(),
            }
        }
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
    let product = profile.product();
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
        let type_decl = mysql_column_ddl(
            &col.kind,
            col.spatial_srid,
            col.exact_geometry_type.as_deref(),
            profile,
        );
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
    if operation.create_spatial_index {
        let spatial: Vec<&MysqlWriteColumn> = columns
            .iter()
            .filter(|column| column.kind == MysqlColumnKind::Geometry)
            .collect();
        if spatial.is_empty() {
            return Err(unsupported(format!(
                "create_spatial_index su uno schema {product} senza colonne geometriche"
            )));
        }
        for column in spatial {
            // `NOT NULL` non e una raccomandazione: entrambi i motori
            // rifiutano un indice spaziale su una colonna nullable, e
            // scoprirlo dal server significherebbe averlo scoperto dopo aver
            // creato la tabella — che qui fa commit implicito e non torna
            // indietro.
            if column.nullable {
                return Err(unsupported(format!(
                    "indice spatial {product} su una colonna nullable"
                )));
            }
            lines.push(format!("    SPATIAL INDEX ({})", column.quoted));
        }
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
            let ty = mysql_column_ddl(
                &c.kind,
                c.spatial_srid,
                c.exact_geometry_type.as_deref(),
                profile,
            );
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
    DatabaseError::new(
        category,
        ErrorPhase::Prepare,
        Some(crate::profile::PROVISIONAL_KIND),
        message,
    )
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
