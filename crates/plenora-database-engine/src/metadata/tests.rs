use super::*;
use plenora_database_core::provider::Inspection;
use serde_json::json;

fn source() -> ObjectRef {
    ObjectRef {
        catalog: None,
        schema: Some("app".to_owned()),
        object: "items".to_owned(),
    }
}

fn inspection(document: serde_json::Value) -> Inspection {
    Inspection {
        operation: "database.describe_object".to_owned(),
        document,
    }
}

#[test]
fn postgres_preserves_common_foreign_keys_spatial_and_native_metadata() {
    let document = json!({
        "columns": [{
            "name": "shape", "native_type": "geometry", "nullable": false,
            "numeric_precision": null, "numeric_scale": null, "spatial_srid": 4326,
            "spatial_dimensions": "XY", "spatial_type": "POINT",
            "spatial_crs_id": "EPSG:4326", "default_expression": null,
            "identity_kind": null, "generated_kind": null,
            "native_declaration": "geometry(Point,4326)", "type_kind": "b",
            "composite_fields": [], "enum_labels": [], "domain_base_type": null,
            "domain_constraints": [], "collation": null
        }],
        "schema_token": {
            "schema_version": 1, "database_oid": 10, "namespace_oid": 20,
            "relation_oid": 30, "structural_fingerprint": "pg-token"
        },
        "relation": {
            "kind": "table", "is_partition": false, "partition_key": null,
            "view_definition": null, "comment": "catalog comment",
            "row_security": true, "force_row_security": false,
            "replica_identity": "default", "persistence": "permanent",
            "is_populated": true, "partition_bound": null, "owner": "owner",
            "tablespace": "pg_default", "parents": [], "partitions": []
        },
        "constraints": [{
            "name": "items_parent_fk", "kind": "foreign_key",
            "definition": "FOREIGN KEY (parent_id) REFERENCES parent(id)",
            "validated": true, "deferrable": false, "initially_deferred": false,
            "columns": ["parent_id"], "referenced_schema": "app",
            "referenced_object": "parent", "referenced_columns": ["id"],
            "on_update": "no_action", "on_delete": "cascade", "match": "simple"
        }],
        "indexes": [{
            "name": "items_shape_idx", "primary": false, "unique": false,
            "valid": true, "method": "gist", "definition": "CREATE INDEX",
            "ready": true, "clustered": false, "predicate": null,
            "size_bytes": 8192, "spatial": true,
            "keys": [{"position": 1, "expression": "shape", "opclass": "gist_geometry_ops_2d", "included": false}]
        }],
        "policies": [{
            "name": "tenant", "permissive": true, "command": "select",
            "roles": ["app"], "using": "tenant_id = current_user", "check": null
        }],
        "privileges": [{"grantee": "app", "privilege": "SELECT", "grantable": false}]
    });
    let metadata =
        MetaData::from_inspection(ProviderKind::Postgres, &source(), inspection(document))
            .expect("metadata PostgreSQL");
    let table = metadata.one_table().expect("una tabella");
    assert_eq!(table.name(), "items");
    assert_eq!(
        table.columns()[0]
            .spatial()
            .and_then(SpatialColumnMetadata::srid),
        Some(4326)
    );
    let Observation::Observed(foreign_keys) = table.foreign_keys() else {
        panic!("foreign key misurate")
    };
    assert_eq!(foreign_keys[0].referenced_object(), "parent");
    let NativeTableMetadata::Postgres(native) = table.native() else {
        panic!("metadata nativi PostgreSQL")
    };
    assert_eq!(native.policies.len(), 1);
    assert_eq!(native.privileges.len(), 1);
}

#[test]
fn mysql_and_mariadb_keep_distinct_tokens_and_unmeasured_constraints() {
    let make_document = || {
        json!({
            "schema": "app", "name": "items", "kind": "BASE TABLE", "engine": "InnoDB",
            "columns": [{
                "name": "id", "ordinal": 1, "data_type": "bigint",
                "native_declaration": "bigint unsigned", "nullable": false,
                "default_expression": null, "character_set": null, "collation": null,
                "numeric_precision": 20, "numeric_scale": 0, "datetime_precision": null,
                "spatial_srid": null, "extra": "auto_increment", "generation_expression": ""
            }],
            "indexes": [{"name": "PRIMARY", "unique": true, "column_backed": true, "columns": ["id"]}],
            "token": "mysql-token"
        })
    };
    for provider in [ProviderKind::Mysql, ProviderKind::Mariadb] {
        let metadata = MetaData::from_inspection(provider, &source(), inspection(make_document()))
            .expect("metadata mysql-family");
        let table = metadata.one_table().expect("una tabella");
        assert_eq!(table.columns()[0].identity(), Some(true));
        assert_eq!(table.constraints(), Observation::NotMeasured);
        match (provider, table.schema_token()) {
            (ProviderKind::Mysql, SchemaToken::Mysql(value))
            | (ProviderKind::Mariadb, SchemaToken::Mariadb(value)) => {
                assert_eq!(value, "mysql-token");
            }
            _ => panic!("token attribuito al provider sbagliato"),
        }
    }
}

#[test]
fn sqlserver_preserves_typed_constraints_without_guessing_column_lists() {
    let document = json!({
        "columns": [{
            "ordinal": 1, "name": "id", "type_schema": "sys", "native_type": "int",
            "max_length": 4, "precision": 10, "scale": 0, "nullable": false,
            "identity": true, "computed": false, "generated_always_type": 0,
            "collation": null, "default_definition": null,
            "computed_definition": null, "computed_persisted": false
        }],
        "schema_token": {
            "schema_version": 1, "database_id": 4, "object_id": 8,
            "structural_fingerprint": "sqlserver-token"
        },
        "relation": {
            "catalog": "db", "schema": "app", "name": "items", "kind": "USER_TABLE",
            "temporal_type": 0, "memory_optimized": false, "durability": null
        },
        "constraints": [{
            "name": "pk_items", "kind": "PRIMARY_KEY_CONSTRAINT", "definition": null,
            "columns": "id", "referenced_object": null, "disabled": false, "not_trusted": false
        }],
        "indexes": [{
            "index_id": 1, "name": "pk_items", "kind": "CLUSTERED", "unique": true,
            "primary_key": true, "unique_constraint": true, "disabled": false,
            "filtered": false, "filter_definition": null, "columns": "id ASC", "spatial": null
        }]
    });
    let metadata =
        MetaData::from_inspection(ProviderKind::Sqlserver, &source(), inspection(document))
            .expect("metadata SQL Server");
    let table = metadata.one_table().expect("una tabella");
    assert_eq!(table.catalog(), Some("db"));
    let Observation::Observed(constraints) = table.constraints() else {
        panic!("vincoli misurati")
    };
    assert_eq!(constraints[0].columns(), Observation::NotMeasured);
    let Observation::Observed(indexes) = table.indexes() else {
        panic!("indici misurati")
    };
    assert_eq!(indexes[0].elements(), Observation::NotMeasured);
}

#[test]
fn db2_maps_ordered_index_elements_and_leaves_foreign_keys_unmeasured() {
    let document = json!({
        "schema": "APP", "name": "ITEMS", "kind": "TABLE",
        "columns": [{
            "name": "ID", "ordinal": 1, "data_type": "BIGINT", "length": 8,
            "scale": 0, "nullable": false, "default_expression": null,
            "generated": false, "identity": true
        }],
        "indexes": [{
            "name": "PK_ITEMS", "unique": true, "primary": true,
            "columns": ["ID"], "descending": [true]
        }],
        "schema_token": "db2-token"
    });
    let metadata = MetaData::from_inspection(ProviderKind::Db2, &source(), inspection(document))
        .expect("metadata Db2");
    let table = metadata.one_table().expect("una tabella");
    let Observation::Observed(indexes) = table.indexes() else {
        panic!("indici misurati")
    };
    let Observation::Observed(elements) = indexes[0].elements() else {
        panic!("elementi misurati")
    };
    assert_eq!(elements[0].descending(), Some(true));
    assert_eq!(table.foreign_keys(), Observation::NotMeasured);
}

#[test]
fn malformed_documents_fail_without_echoing_payloads() {
    let error = MetaData::from_inspection(
        ProviderKind::Db2,
        &source(),
        inspection(json!({"schema": "private-schema-value"})),
    )
    .expect_err("documento incompleto");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::DataMapping
    );
    assert!(!error.message.contains("private-schema-value"));
}
