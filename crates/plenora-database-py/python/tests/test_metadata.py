"""Metadata Core v3: identita tipizzata, cache e invalidazione."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

import plenora_database as p

from ._harness import LOCAL_TLS_MODE, postgres_dsn_or_skip


def _document() -> dict:
    return {
        "provider": "postgres",
        "tables": [
            {
                "catalog": None,
                "schema": "app",
                "name": "users",
                "kind": "table",
                "schema_token": {
                    "Postgres": {
                        "schema_version": 2,
                        "database_oid": 1,
                        "namespace_oid": 2,
                        "relation_oid": 3,
                        "structural_fingerprint": "sha256:fixture",
                    }
                },
                "columns": [
                    {
                        "name": "id",
                        "ordinal": 1,
                        "native_type": "int8",
                        "native_declaration": "bigint",
                        "nullable": False,
                        "default_expression": None,
                        "identity": True,
                        "generated": False,
                        "numeric_precision": 64,
                        "numeric_scale": 0,
                        "spatial": None,
                        "native": {
                            "Postgres": {
                                "identity_kind": "always",
                                "generated_kind": None,
                                "type_kind": "base",
                                "composite_fields": [],
                                "enum_labels": [],
                                "domain_base_type": None,
                                "domain_constraints": [],
                                "collation": None,
                            }
                        },
                    }
                ],
                "indexes": {"Observed": []},
                "constraints": {"Observed": []},
                "foreign_keys": "NotMeasured",
                "native": {
                    "Postgres": {
                        "is_partition": False,
                        "partition_key": None,
                        "view_definition": None,
                        "comment": None,
                        "row_security": False,
                        "force_row_security": False,
                        "replica_identity": "default",
                        "persistence": "permanent",
                        "is_populated": True,
                        "partition_bound": None,
                        "owner": "owner",
                        "tablespace": "",
                        "parents": [],
                        "partitions": [],
                        "policies": [],
                        "privileges": [],
                    }
                },
            }
        ],
    }


def test_typed_metadata_is_immutable_and_reuses_expression_objects() -> None:
    metadata = p.MetaData.from_document(_document())
    users = metadata.one_table()

    assert users.metadata.schema_token.fingerprint == "sha256:fixture"
    assert users.c.id.metadata.native_type == "int8"
    assert users.metadata.indexes.measured is True
    assert users.metadata.foreign_keys.measured is False
    assert p.select(users.c.id).to_ast()["source"]["object"]["object"] == "users"
    assert users.alias("u").c.id.metadata is users.c.id.metadata
    with pytest.raises(FrozenInstanceError):
        users.c.id.metadata.nullable = True


def test_engine_reflection_cache_refresh_and_invalidation_are_observable() -> None:
    name = "_sdk_typed_metadata"
    with p.create_engine(postgres_dsn_or_skip(), LOCAL_TLS_MODE) as engine:
        with engine.session() as session:
            session.execute_ddl(f'DROP TABLE IF EXISTS "{name}"')
            session.execute_ddl(f'CREATE TABLE "{name}" (id BIGINT PRIMARY KEY)')
            try:
                first = engine.reflect_table("public", name).one_table()
                cached = engine.reflect_table("public", name).one_table()
                assert first.metadata.schema_token == cached.metadata.schema_token
                assert engine.metadata_cache_entries == 1

                session.execute_ddl(f'ALTER TABLE "{name}" ADD COLUMN label TEXT')
                refreshed = engine.reflect_table(
                    "public", name, refresh=True
                ).one_table()
                assert [column.name for column in refreshed.columns] == ["id", "label"]
                assert (
                    refreshed.metadata.schema_token.fingerprint
                    != first.metadata.schema_token.fingerprint
                )
                assert engine.invalidate_metadata("public", name) == 1
                assert engine.metadata_cache_entries == 0
            finally:
                session.execute_ddl(f'DROP TABLE IF EXISTS "{name}"')
