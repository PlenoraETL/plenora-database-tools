mod capabilities;
mod listing;
mod schema;

pub use capabilities::capability_document;
pub use listing::{list_catalogs, list_objects, list_schemas};
pub use schema::{load_columns_and_token, schema_token};

use crate::error::{classify_error, public_error};
use crate::PostgresSchemaToken;
use plenora_database_core::plan::ObjectRef;
use plenora_database_core::{ErrorCategory, ErrorPhase, Result};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use tokio_postgres::types::FromSql;
use tokio_postgres::{Client, Row};
#[derive(Clone)]
pub struct CatalogSchemaToken {
    pub public: PostgresSchemaToken,
    pub exact_signature: String,
}

impl CatalogSchemaToken {
    pub fn from_catalog_row(row: &Row) -> Result<Self> {
        let database_oid = catalog_oid(catalog_field(row, "database_oid")?)?;
        let namespace_oid = catalog_oid(catalog_field(row, "namespace_oid")?)?;
        let relation_oid = catalog_oid(catalog_field(row, "relation_oid")?)?;
        let exact_signature: String = catalog_field(row, "structural_signature")?;
        let digest = Sha256::digest(exact_signature.as_bytes());
        let mut structural_fingerprint = String::with_capacity(digest.len() * 2);
        for byte in digest {
            write!(structural_fingerprint, "{byte:02x}").map_err(|_| {
                public_error(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Prepare,
                    false,
                    "impossibile codificare il fingerprint dello schema PostgreSQL",
                )
            })?;
        }
        Ok(Self {
            public: PostgresSchemaToken {
                schema_version: 1,
                database_oid,
                namespace_oid,
                relation_oid,
                structural_fingerprint,
            },
            exact_signature,
        })
    }

    pub fn structurally_equals(&self, other: &Self) -> bool {
        self.public.database_oid == other.public.database_oid
            && self.public.namespace_oid == other.public.namespace_oid
            && self.public.relation_oid == other.public.relation_oid
            && self.exact_signature == other.exact_signature
    }
}

pub fn catalog_field<'a, T>(row: &'a Row, name: &'static str) -> Result<T>
where
    T: FromSql<'a>,
{
    row.try_get(name).map_err(|_| {
        let message =
            format!("campo catalogo PostgreSQL '{name}' incompatibile con il contratto interno");
        public_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Probe,
            false,
            &message,
        )
    })
}

pub fn catalog_json_list<T>(row: &Row, name: &'static str) -> Result<Vec<T>>
where
    T: serde::de::DeserializeOwned,
{
    let encoded: Option<String> = catalog_field(row, name)?;
    encoded.map_or_else(
        || Ok(Vec::new()),
        |value| {
            serde_json::from_str(&value).map_err(|_| {
                let message = format!(
                    "JSON del campo catalogo PostgreSQL '{name}' incompatibile con il contratto interno"
                );
                public_error(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Probe,
                    false,
                    &message,
                )
            })
        },
    )
}

pub fn catalog_oid(value: i64) -> Result<u32> {
    u32::try_from(value).map_err(|_| {
        public_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Probe,
            false,
            "OID catalogo PostgreSQL fuori intervallo",
        )
    })
}

pub struct ObjectMetadata {
    pub relation: serde_json::Value,
    pub constraints: Vec<serde_json::Value>,
    pub indexes: Vec<serde_json::Value>,
    pub policies: Vec<serde_json::Value>,
    pub privileges: Vec<serde_json::Value>,
}

#[allow(clippy::too_many_lines)]
pub async fn describe_object_metadata(
    client: &Client,
    source: &ObjectRef,
) -> Result<ObjectMetadata> {
    let schema = source.schema.as_deref().unwrap_or("public");
    let relation = client
        .query_one(
            r"
            SELECT
                c.relkind::text,
                c.relispartition,
                pg_get_partkeydef(c.oid),
                CASE WHEN c.relkind IN ('v', 'm') THEN pg_get_viewdef(c.oid, true) END,
                obj_description(c.oid, 'pg_class'),
                c.relrowsecurity,
                c.relforcerowsecurity,
                c.relreplident::text,
                c.relpersistence::text,
                c.relispopulated,
                pg_get_expr(c.relpartbound, c.oid, true),
                pg_get_userbyid(c.relowner),
                COALESCE(ts.spcname, current_setting('default_tablespace')),
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'schema', pn.nspname,
                            'name', pc.relname
                        )
                        ORDER BY pn.nspname, pc.relname
                    )::text
                    FROM pg_catalog.pg_inherits inh
                    JOIN pg_catalog.pg_class pc ON pc.oid = inh.inhparent
                    JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
                    WHERE inh.inhrelid = c.oid
                ),
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'schema', cn.nspname,
                            'name', cc.relname,
                            'bound', pg_get_expr(cc.relpartbound, cc.oid, true)
                        )
                        ORDER BY cn.nspname, cc.relname
                    )::text
                    FROM pg_catalog.pg_inherits inh
                    JOIN pg_catalog.pg_class cc ON cc.oid = inh.inhrelid
                    JOIN pg_catalog.pg_namespace cn ON cn.oid = cc.relnamespace
                    WHERE inh.inhparent = c.oid
                )
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_tablespace ts ON ts.oid = c.reltablespace
            WHERE n.nspname = $1 AND c.relname = $2
            ",
            &[&schema, &source.object],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    let relation_document = json!({
        "kind": relation_kind(&relation.get::<_, String>(0)),
        "is_partition": relation.get::<_, bool>(1),
        "partition_key": relation.get::<_, Option<String>>(2),
        "view_definition": relation.get::<_, Option<String>>(3),
        "comment": relation.get::<_, Option<String>>(4),
        "row_security": relation.get::<_, bool>(5),
        "force_row_security": relation.get::<_, bool>(6),
        "replica_identity": replica_identity(&relation.get::<_, String>(7)),
        "persistence": relation_persistence(&relation.get::<_, String>(8)),
        "is_populated": relation.get::<_, bool>(9),
        "partition_bound": relation.get::<_, Option<String>>(10),
        "owner": relation.get::<_, String>(11),
        "tablespace": relation.get::<_, String>(12),
        "parents": parse_json_array(relation.get::<_, Option<String>>(13))?,
        "partitions": parse_json_array(relation.get::<_, Option<String>>(14))?
    });
    let constraint_rows = client
        .query(
            r"
            SELECT
                con.conname,
                con.contype::text,
                pg_get_constraintdef(con.oid, true),
                con.convalidated,
                con.condeferrable,
                con.condeferred,
                (
                    SELECT jsonb_agg(a.attname ORDER BY key.ordinality)::text
                    FROM unnest(con.conkey) WITH ORDINALITY AS key(attnum, ordinality)
                    JOIN pg_catalog.pg_attribute a
                      ON a.attrelid = con.conrelid AND a.attnum = key.attnum
                ),
                rn.nspname,
                rc.relname,
                (
                    SELECT jsonb_agg(a.attname ORDER BY key.ordinality)::text
                    FROM unnest(con.confkey) WITH ORDINALITY AS key(attnum, ordinality)
                    JOIN pg_catalog.pg_attribute a
                      ON a.attrelid = con.confrelid AND a.attnum = key.attnum
                ),
                con.confupdtype::text,
                con.confdeltype::text,
                con.confmatchtype::text
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_class rc ON rc.oid = con.confrelid
            LEFT JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
            WHERE n.nspname = $1 AND c.relname = $2
            ORDER BY con.conname
            ",
            &[&schema, &source.object],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    let constraints = constraint_rows
        .iter()
        .map(|row| {
            Ok(json!({
                "name": row.get::<_, String>(0),
                "kind": constraint_kind(&row.get::<_, String>(1)),
                "definition": row.get::<_, String>(2),
                "validated": row.get::<_, bool>(3),
                "deferrable": row.get::<_, bool>(4),
                "initially_deferred": row.get::<_, bool>(5),
                "columns": parse_json_array(row.get::<_, Option<String>>(6))?,
                "referenced_schema": row.get::<_, Option<String>>(7),
                "referenced_object": row.get::<_, Option<String>>(8),
                "referenced_columns": parse_json_array(row.get::<_, Option<String>>(9))?,
                "on_update": foreign_key_action(&row.get::<_, String>(10)),
                "on_delete": foreign_key_action(&row.get::<_, String>(11)),
                "match": foreign_key_match(&row.get::<_, String>(12))
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let index_rows = client
        .query(
            r"
            SELECT
                i.relname,
                ix.indisprimary,
                ix.indisunique,
                ix.indisvalid,
                am.amname,
                pg_get_indexdef(i.oid),
                ix.indisready,
                ix.indisclustered,
                pg_get_expr(ix.indpred, ix.indrelid, true),
                pg_relation_size(i.oid)::bigint,
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'position', keys.ordinality,
                            'expression', pg_get_indexdef(
                                ix.indexrelid,
                                keys.ordinality::integer,
                                true
                            ),
                            'opclass', opc.opcname,
                            'included', keys.ordinality > ix.indnkeyatts
                        )
                        ORDER BY keys.ordinality
                    )::text
                    FROM unnest(ix.indclass) WITH ORDINALITY
                        AS keys(opclass_oid, ordinality)
                    JOIN pg_catalog.pg_opclass opc ON opc.oid = keys.opclass_oid
                ),
                EXISTS (
                    SELECT 1
                    FROM unnest(ix.indclass) AS opclass_oid
                    JOIN pg_catalog.pg_opclass opc ON opc.oid = opclass_oid
                    WHERE opc.opcname ILIKE ANY (
                        ARRAY['%geometry%', '%geography%', '%box2d%', '%box3d%']
                    )
                )
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_am am ON am.oid = i.relam
            WHERE n.nspname = $1 AND t.relname = $2
            ORDER BY i.relname
            ",
            &[&schema, &source.object],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    let indexes = index_rows
        .iter()
        .map(|row| {
            Ok(json!({
                "name": row.get::<_, String>(0),
                "primary": row.get::<_, bool>(1),
                "unique": row.get::<_, bool>(2),
                "valid": row.get::<_, bool>(3),
                "method": row.get::<_, String>(4),
                "definition": row.get::<_, String>(5),
                "ready": row.get::<_, bool>(6),
                "clustered": row.get::<_, bool>(7),
                "predicate": row.get::<_, Option<String>>(8),
                "size_bytes": row.get::<_, i64>(9),
                "keys": parse_json_array(row.get::<_, Option<String>>(10))?,
                "spatial": row.get::<_, bool>(11)
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let policy_rows = client
        .query(
            r"
            SELECT
                p.polname,
                p.polpermissive,
                p.polcmd::text,
                (
                    SELECT jsonb_agg(
                        CASE WHEN role_oid = 0 THEN 'PUBLIC' ELSE r.rolname END
                        ORDER BY CASE
                            WHEN role_oid = 0 THEN 'PUBLIC'
                            ELSE r.rolname
                        END
                    )::text
                    FROM unnest(p.polroles) AS roles(role_oid)
                    LEFT JOIN pg_catalog.pg_roles r ON r.oid = roles.role_oid
                ),
                pg_get_expr(p.polqual, p.polrelid, true),
                pg_get_expr(p.polwithcheck, p.polrelid, true)
            FROM pg_catalog.pg_policy p
            JOIN pg_catalog.pg_class c ON c.oid = p.polrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relname = $2
            ORDER BY p.polname
            ",
            &[&schema, &source.object],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    let policies = policy_rows
        .iter()
        .map(|row| {
            Ok(json!({
                "name": row.get::<_, String>(0),
                "permissive": row.get::<_, bool>(1),
                "command": policy_command(&row.get::<_, String>(2)),
                "roles": parse_json_array(row.get::<_, Option<String>>(3))?,
                "using": row.get::<_, Option<String>>(4),
                "check": row.get::<_, Option<String>>(5)
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let privilege_rows = client
        .query(
            r"
            SELECT
                COALESCE(grantee.rolname, 'PUBLIC'),
                upper(acl.privilege_type),
                acl.is_grantable
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            CROSS JOIN LATERAL aclexplode(
                COALESCE(c.relacl, acldefault('r', c.relowner))
            ) acl
            LEFT JOIN pg_catalog.pg_roles grantee ON grantee.oid = acl.grantee
            WHERE n.nspname = $1 AND c.relname = $2
            ORDER BY 1, 2
            ",
            &[&schema, &source.object],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    let privileges = privilege_rows
        .iter()
        .map(|row| {
            json!({
                "grantee": row.get::<_, String>(0),
                "privilege": row.get::<_, String>(1),
                "grantable": row.get::<_, bool>(2)
            })
        })
        .collect();
    Ok(ObjectMetadata {
        relation: relation_document,
        constraints,
        indexes,
        policies,
        privileges,
    })
}

pub fn relation_kind(kind: &str) -> &'static str {
    match kind {
        "r" => "table",
        "p" => "partitioned_table",
        "v" => "view",
        "m" => "materialized_view",
        "f" => "foreign_table",
        _ => "other",
    }
}

fn parse_json_array(value: Option<String>) -> Result<serde_json::Value> {
    value.map_or_else(
        || Ok(json!([])),
        |document| {
            serde_json::from_str(&document).map_err(|_| {
                public_error(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Probe,
                    false,
                    "metadato catalogo PostgreSQL non convertibile",
                )
            })
        },
    )
}

fn replica_identity(value: &str) -> &'static str {
    match value {
        "d" => "default",
        "n" => "nothing",
        "f" => "full",
        "i" => "index",
        _ => "unknown",
    }
}

fn relation_persistence(value: &str) -> &'static str {
    match value {
        "p" => "permanent",
        "u" => "unlogged",
        "t" => "temporary",
        _ => "unknown",
    }
}

fn foreign_key_action(value: &str) -> &'static str {
    match value {
        "a" => "no_action",
        "r" => "restrict",
        "c" => "cascade",
        "n" => "set_null",
        "d" => "set_default",
        _ => "unknown",
    }
}

fn foreign_key_match(value: &str) -> &'static str {
    match value {
        "f" => "full",
        "p" => "partial",
        "s" => "simple",
        _ => "unknown",
    }
}

fn policy_command(value: &str) -> &'static str {
    match value {
        "r" => "select",
        "a" => "insert",
        "w" => "update",
        "d" => "delete",
        "*" => "all",
        _ => "unknown",
    }
}

fn constraint_kind(kind: &str) -> &'static str {
    match kind {
        "p" => "primary_key",
        "u" => "unique",
        "f" => "foreign_key",
        "c" => "check",
        "x" => "exclusion",
        _ => "other",
    }
}
