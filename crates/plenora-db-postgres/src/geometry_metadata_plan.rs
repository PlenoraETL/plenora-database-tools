//! The registry's bounded, immutable comment plan. No policy or SQL lives here.
use super::{property_for, storage_for, PhysicalGeometry, Storage};
use crate::column_metadata::{
    digest, invalid, limit, ColumnMetadata, COLUMN_LIMIT, OPERATION_LIMIT,
};
use arrow_schema::Schema;
use plenora_database_core::loss_registry::{RuleInstance, RuleParameters};
use plenora_database_core::metadata_subject::{
    MetadataHop, MetadataReason, MetadataScope, MetadataSubject,
};
use plenora_database_core::protocol;
use plenora_database_core::Result;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct SchemaPlan {
    pub columns: Vec<ColumnMetadata>,
}

pub struct Registration {
    pub column: String,
    pub input_position: Option<usize>,
    pub comment: Option<String>,
}

impl SchemaPlan {
    fn unique_registered_ids(
        &self,
        schema: &Schema,
        comments: &[(String, Option<String>)],
    ) -> Result<()> {
        let mut identities = std::collections::BTreeSet::new();
        for column in &self.columns {
            if !identities.insert(column.id.clone()) {
                return Err(invalid());
            }
        }
        for (name, text) in comments {
            if schema.field_with_name(name).is_err() {
                if let Some(id) = crate::column_metadata::split_identity(text.as_deref())?.0 {
                    if !identities.insert(id) {
                        return Err(invalid());
                    }
                }
            }
        }
        Ok(())
    }
    /// Final immutable registration set, including catalog history and new columns.
    pub fn registrations(
        &self,
        schema: &Schema,
        comments: &[(String, Option<String>)],
    ) -> Result<Vec<Registration>> {
        use crate::column_metadata::{
            decode, merge_omissions, plan_registration, registration_bytes,
        };
        let existing = comments
            .iter()
            .map(|(name, text)| Ok((name, text, decode(text.as_deref())?)))
            .collect::<Result<Vec<_>>>()?;
        self.unique_registered_ids(schema, comments)?;
        // Legacy scalar input has no metadata to persist. In particular, a
        // synthesized name ID is not a request to change catalog comments.
        // Decode above still rejects corrupt registered metadata on the target.
        if self.columns.iter().all(|column| {
            column.physical.is_none()
                && column.field.is_empty()
                && column.schema.is_empty()
                && column.omitted.is_empty()
        }) {
            return Ok(Vec::new());
        }
        let mut history = Vec::new();
        let mut first = None;
        for (_, _, saved) in &existing {
            if let Some(saved) = saved {
                if self
                    .columns
                    .first()
                    .is_some_and(|c| c.schema != saved.schema)
                {
                    return Err(invalid());
                }
                if first
                    .is_some_and(|f| super::schema_omissions(f) != super::schema_omissions(saved))
                {
                    return Err(invalid());
                }
                if first.is_none() {
                    history.extend(
                        saved
                            .omitted
                            .iter()
                            .filter(|s| s.scope == MetadataScope::Schema)
                            .cloned(),
                    );
                    first = Some(saved);
                }
            }
        }
        if let Some(column) = self.columns.first() {
            history.extend(
                column
                    .omitted
                    .iter()
                    .filter(|s| s.scope == MetadataScope::Schema)
                    .cloned(),
            );
        }
        let mut registrations = Vec::new();
        let mut total = 0_usize;
        let mut add = |name: &str,
                       position,
                       text: Option<&str>,
                       mut incoming: ColumnMetadata|
         -> Result<()> {
            // Catalog provenance wins uniformly across old and newly added columns.
            // The execution report still describes this operation's own reason.
            incoming
                .omitted
                .retain(|s| s.scope != MetadataScope::Schema);
            merge_omissions(&mut incoming, &history)?;
            let comment = plan_registration(text, &incoming)?;
            if let Some(comment) = &comment {
                total = total
                    .checked_add(registration_bytes(comment)?)
                    .ok_or_else(limit)?;
                if total > OPERATION_LIMIT {
                    return Err(limit());
                }
            }
            registrations.push(Registration {
                column: name.to_owned(),
                input_position: position,
                comment,
            });
            Ok(())
        };
        for (position, (field, incoming)) in schema.fields().iter().zip(&self.columns).enumerate() {
            let comment = existing
                .iter()
                .find(|(name, _, _)| *name == field.name())
                .and_then(|(_, text, _)| text.as_deref());
            add(field.name(), Some(position), comment, incoming.clone())?;
        }
        // Schema history is table-wide, including registered columns not in an append projection.
        for (name, comment, saved) in existing {
            if schema.field_with_name(name).is_ok() {
                continue;
            }
            if let Some(saved) = saved {
                add(name, None, comment.as_deref(), saved)?;
            }
        }
        Ok(registrations)
    }
}

#[derive(Serialize)]
struct BorrowedColumn<'a> {
    physical: Option<PhysicalGeometry>,
    version: u8,
    id: String,
    data_type: String,
    field: BTreeMap<&'a str, &'a str>,
    #[serde(skip_serializing)]
    schema: std::sync::Arc<BTreeMap<&'a str, &'a str>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    omitted: Vec<MetadataSubject>,
}

#[derive(Default)]
struct Size(usize);
impl std::io::Write for Size {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or(std::io::ErrorKind::OutOfMemory)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encoded_size(column: &BorrowedColumn<'_>, schema_bytes: usize) -> Result<usize> {
    #[derive(Serialize)]
    struct Envelope<'a, 'b> {
        metadata: &'a BorrowedColumn<'b>,
        sha256: &'static str,
    }
    let mut counter = Size::default();
    serde_json::to_writer(
        &mut counter,
        &Envelope {
            metadata: column,
            sha256: "0000000000000000000000000000000000000000000000000000000000000000",
        },
    )
    .map_err(|_| invalid())?;
    Ok(counter.0 + ",\"schema\":".len() + schema_bytes)
}

impl SchemaPlan {
    pub fn compile(schema: &Schema) -> Result<Self> {
        super::validate_registry(&super::PROPERTY_REGISTRY)?;
        plenora_database_core::field_contract::validate_schema_contract(schema)?;
        let keys = schema
            .fields()
            .iter()
            .try_fold(schema.metadata().len(), |n, f| {
                n.checked_add(f.metadata().len()).ok_or_else(invalid)
            })?;
        // Bound borrowed-key bookkeeping before allocating tree nodes.
        if keys
            .checked_mul(std::mem::size_of::<(&str, &str)>())
            .is_none_or(|bytes| bytes > OPERATION_LIMIT)
        {
            return Err(limit());
        }
        let schema_map: BTreeMap<_, _> = schema
            .metadata()
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let mut schema_counter = Size::default();
        serde_json::to_writer(&mut schema_counter, &schema_map).map_err(|_| invalid())?;
        let mut schema_bytes = schema_counter.0;
        let mut schema_map = std::sync::Arc::new(schema_map);
        let mut columns = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            for (key, value) in field.metadata().iter().chain(schema.metadata()) {
                if key.contains('\0') || value.contains('\0') {
                    return Err(invalid());
                }
            }
            columns.push(BorrowedColumn {
                physical: PhysicalGeometry::plan(field)?,
                version: 1,
                id: field
                    .metadata()
                    .get(protocol::FIELD_ID)
                    .cloned()
                    .unwrap_or_else(|| protocol::name_field_id(field.name()).to_string()),
                data_type: field.data_type().to_string(),
                field: field
                    .metadata()
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect(),
                schema: schema_map.clone(),
                omitted: Vec::new(),
            });
        }
        reduce(&mut columns, &mut schema_map, &mut schema_bytes)?;
        let columns: Vec<ColumnMetadata> = columns
            .into_iter()
            .map(|c| ColumnMetadata {
                physical: c.physical,
                version: c.version,
                id: c.id,
                data_type: c.data_type,
                field: c
                    .field
                    .into_iter()
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect(),
                schema: c
                    .schema
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect(),
                omitted: c.omitted,
            })
            .collect();
        let retained = Schema::new_with_metadata(
            schema
                .fields()
                .iter()
                .zip(&columns)
                .map(|(field, column)| {
                    field.as_ref().clone().with_metadata(
                        column
                            .field
                            .clone()
                            .into_iter()
                            .collect::<std::collections::HashMap<_, _>>(),
                    )
                })
                .collect::<Vec<_>>(),
            columns.first().map_or_else(Default::default, |c| {
                c.schema
                    .clone()
                    .into_iter()
                    .collect::<std::collections::HashMap<_, _>>()
            }),
        );
        plenora_database_core::field_contract::validate_schema_contract(&retained)?;
        Ok(Self { columns })
    }

    pub fn rules(&self) -> Result<Vec<RuleInstance>> {
        let mut rules = Vec::new();
        for (position, column) in self.columns.iter().enumerate() {
            for subject in &column.omitted {
                if subject.scope == MetadataScope::Schema && position != 0 {
                    continue;
                }
                rules.push(RuleInstance {
                    subject: Some(subject.clone()),
                    field_id: u32::try_from(position).map_err(|_| invalid())?,
                    rule_id: super::PROPERTY_REGISTRY
                        .iter()
                        .find(|p| p.name == subject.property)
                        .and_then(|p| p.metadata_rule)
                        .ok_or_else(invalid)?,
                    parameters: RuleParameters::default(),
                    source_type: None,
                    target_type: None,
                });
            }
        }
        if rules.len() > plenora_database_core::loss::MAX_LOSSES {
            return Err(limit());
        }
        Ok(rules)
    }
}

type SharedMetadata<'a> = std::sync::Arc<BTreeMap<&'a str, &'a str>>;
fn reduce<'a>(
    columns: &mut [BorrowedColumn<'a>],
    schema_map: &mut SharedMetadata<'a>,
    schema_bytes: &mut usize,
) -> Result<()> {
    // Select removals deterministically, using exact escaped UTF-8 sizes.
    // Schema keys are removed from every replica together. CRS pairs and
    // their GeoArrow alias are removed as a unit; declarations remain valid.
    loop {
        let sizes = columns
            .iter()
            .map(|column| encoded_size(column, *schema_bytes))
            .collect::<Result<Vec<_>>>()?;
        let total = sizes
            .iter()
            .try_fold(0_usize, |n, s| n.checked_add(*s).ok_or_else(invalid))?;
        let over = sizes.iter().position(|s| *s > COLUMN_LIMIT);
        if over.is_none() && total <= OPERATION_LIMIT {
            break;
        }
        // Omitted records cannot themselves be omitted. Bound their replica
        // allocation before another reduction, even if no key can now fit.
        let annotation_bytes = columns.iter().try_fold(0_usize, |n, c| {
            let mut size = Size::default();
            serde_json::to_writer(&mut size, &c.omitted).map_err(|_| invalid())?;
            n.checked_add(size.0).ok_or_else(invalid)
        })?;
        if annotation_bytes > OPERATION_LIMIT {
            return Err(limit());
        }
        let (position, scope, key) = select_removal(columns, over)?;
        let reason = if over.is_some() {
            MetadataReason::ColumnLimit
        } else {
            MetadataReason::OperationLimit
        };
        remove(columns, schema_map, position, scope, key, reason)?;
        let mut counter = Size::default();
        serde_json::to_writer(&mut counter, schema_map.as_ref()).map_err(|_| invalid())?;
        *schema_bytes = counter.0;
    }
    Ok(())
}
fn select_removal<'a>(
    columns: &[BorrowedColumn<'a>],
    over: Option<usize>,
) -> Result<(usize, MetadataScope, &'a str)> {
    let mut candidates = Vec::new();
    for (position, column) in columns.iter().enumerate() {
        if over.is_some_and(|p| p != position) {
            continue;
        }
        for (scope, map) in [
            (MetadataScope::Schema, column.schema.as_ref()),
            (MetadataScope::Field, &column.field),
        ] {
            if scope == MetadataScope::Schema && position != over.unwrap_or(0) {
                continue;
            }
            for (&key, &value) in map {
                if storage_for(key) != Storage::Required {
                    candidates.push((key.len() + value.len(), position, scope, key));
                }
            }
        }
    }
    candidates.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)).then(a.3.cmp(b.3)));
    let (_, position, scope, key) = candidates.first().copied().ok_or_else(limit)?;
    Ok((position, scope, key))
}
fn remove<'a>(
    columns: &mut [BorrowedColumn<'a>],
    schema_map: &mut SharedMetadata<'a>,
    position: usize,
    scope: MetadataScope,
    key: &str,
    reason: MetadataReason,
) -> Result<()> {
    let group = property_for(key).removal_group;
    let schema_keys: Vec<_> = if scope == MetadataScope::Schema {
        schema_map
            .keys()
            .copied()
            .filter(|k| *k == key || group.contains(k))
            .collect()
    } else {
        Vec::new()
    };
    if scope == MetadataScope::Schema {
        // One shared map; no per-column node replication while planning.
        let mut reduced = (**schema_map).clone();
        for key in &schema_keys {
            reduced.remove(key);
        }
        *schema_map = std::sync::Arc::new(reduced);
    }
    for (index, column) in columns.iter_mut().enumerate() {
        if scope == MetadataScope::Field && index != position {
            continue;
        }
        let removed: Vec<_> = if scope == MetadataScope::Schema {
            column.schema = schema_map.clone();
            schema_keys.clone()
        } else {
            column
                .field
                .keys()
                .copied()
                .filter(|k| *k == key || group.contains(k))
                .collect()
        };
        for removed_key in removed {
            if scope == MetadataScope::Field {
                column.field.remove(removed_key);
            }
            column.omitted.push(MetadataSubject {
                index: 0,
                scope,
                field_identity: (scope == MetadataScope::Field).then(|| column.id.clone()),
                property: property_for(removed_key).name.to_owned(),
                key_sha256: digest(removed_key.as_bytes()),
                hop: MetadataHop::ColumnComment,
                reason,
            });
        }
        column
            .omitted
            .sort_by(|a, b| (a.scope as u8, &a.key_sha256).cmp(&(b.scope as u8, &b.key_sha256)));
        for (i, subject) in column.omitted.iter_mut().enumerate() {
            subject.index = u32::try_from(i).map_err(|_| invalid())?;
        }
    }
    Ok(())
}
