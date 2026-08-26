"""Superficie CLI provider-neutral sullo stesso artefatto della suite SDK."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pyarrow
import pyarrow.ipc
import pytest

from ._harness import (
    MARIADB_CA_ENV,
    MYSQL_CA_ENV,
    POSTGRES_DSN_ENV,
    SQLSERVER_CA_ENV,
    mariadb_config_or_skip,
    mysql_config_or_skip,
    postgres_dsn_or_skip,
    sqlserver_config_or_skip,
)

CLI_BIN_ENV = "PLENORA_CLI_BIN"
CLI_SECRET_ENV = "PLENORA_CLI_TEST_SECRET"
CLI_CA_ENV = "PLENORA_CLI_TEST_CA"
CLI_INSECURE_TLS_ENV = "PLENORA_TLS_INSECURE_LOCAL"


@dataclass(frozen=True)
class ProviderSpec:
    provider: str
    schema: str
    secret: str
    arguments: list[str]
    environment: dict[str, str]
    qualified: str
    create_sql: str


def _spec(provider: str) -> ProviderSpec:
    table = "_cli_common_surface"
    if provider == "postgres":
        dsn = postgres_dsn_or_skip()
        return ProviderSpec(
            provider=provider,
            schema="public",
            secret=dsn,
            arguments=[],
            environment={CLI_INSECURE_TLS_ENV: "1"},
            qualified=f'"public"."{table}"',
            create_sql=f'CREATE TABLE "public"."{table}" '
            '("id" bigint NOT NULL PRIMARY KEY, "label" varchar(32) NOT NULL)',
        )

    if provider == "mysql":
        host, database, user, password, _ = mysql_config_or_skip()
        ca = os.environ[MYSQL_CA_ENV]
    elif provider == "mariadb":
        host, database, user, password, _ = mariadb_config_or_skip()
        ca = os.environ[MARIADB_CA_ENV]
    elif provider == "sqlserver":
        host, database, user, password, _ = sqlserver_config_or_skip()
        ca = os.environ[SQLSERVER_CA_ENV]
        schema = "plenora_test"
        qualified = f"[{schema}].[{table}]"
        return ProviderSpec(
            provider=provider,
            schema=schema,
            secret=password,
            arguments=[
                host,
                database,
                user,
                "--tls-ca-path-env",
                CLI_CA_ENV,
            ],
            environment={CLI_CA_ENV: ca},
            qualified=qualified,
            create_sql=f"CREATE TABLE {qualified} "
            "([id] bigint NOT NULL PRIMARY KEY, [label] nvarchar(32) NOT NULL)",
        )
    else:  # pragma: no cover - elenco parametrico chiuso sotto
        raise AssertionError(provider)

    qualified = f"`{database}`.`{table}`"
    return ProviderSpec(
        provider=provider,
        schema=database,
        secret=password,
        arguments=[
            host,
            database,
            user,
            "--tls-ca-path-env",
            CLI_CA_ENV,
        ],
        environment={CLI_CA_ENV: ca},
        qualified=qualified,
        create_sql=f"CREATE TABLE {qualified} "
        "(`id` bigint NOT NULL PRIMARY KEY, `label` varchar(32) NOT NULL)",
    )


def _run(spec: ProviderSpec, command: str, *arguments: str) -> dict:
    binary = os.environ.get(CLI_BIN_ENV)
    if not binary:
        pytest.fail(f"il gate live deve passare {CLI_BIN_ENV}")
    environment = {
        **os.environ,
        **spec.environment,
        CLI_SECRET_ENV: spec.secret,
    }
    completed = subprocess.run(
        [
            binary,
            command,
            spec.provider,
            CLI_SECRET_ENV,
            *arguments,
            *spec.arguments,
        ],
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    assert spec.secret not in completed.stdout
    assert spec.secret not in completed.stderr
    assert completed.returncode == 0, (
        f"{spec.provider}/{command}: stdout={completed.stdout!r}, "
        f"stderr={completed.stderr!r}"
    )
    assert completed.stderr == ""
    return json.loads(completed.stdout)


def _write_json(path: Path, value: object) -> str:
    path.write_text(json.dumps(value), encoding="utf-8")
    return str(path)


@pytest.mark.parametrize("provider", ["postgres", "mysql", "mariadb", "sqlserver"])
def test_cli_common_surface_roundtrips_arrow(provider: str, tmp_path: Path) -> None:
    """Ogni adapter attraversa la stessa API CLI, non quattro wrapper diversi."""

    spec = _spec(provider)
    table = "_cli_common_surface"
    object_ref = {"catalog": None, "schema": spec.schema, "object": table}
    input_path = tmp_path / "input.arrow"
    output_path = tmp_path / "output.arrow"
    schema = pyarrow.schema(
        [
            pyarrow.field("id", pyarrow.int64(), nullable=False),
            pyarrow.field("label", pyarrow.string(), nullable=False),
        ]
    )
    batch = pyarrow.record_batch([[1, 2], ["one", "two"]], schema=schema)
    with pyarrow.ipc.new_file(input_path, schema) as writer:
        writer.write_batch(batch)

    write_path = _write_json(
        tmp_path / "write.json",
        {
            "target": object_ref,
            "mode": "append",
            "mapping_policy": "strict",
            "transaction_profile": "single_transaction",
            "keys": [],
            "update_columns": [],
            "srid_policy": None,
            "create_spatial_index": False,
            "allow_partial": False,
        },
    )
    read_path = _write_json(
        tmp_path / "read.json",
        {
            "source": object_ref,
            "projection": ["id", "label"],
            "order_by": [{"field": "id", "direction": "asc"}],
            "row_limit": 10,
            "row_offset": None,
            "filter": None,
            "declared_crs": [],
        },
    )
    column = lambda field: {  # noqa: E731 - rende leggibile l'AST fixture
        "kind": "column",
        "column": {"relation": None, "field": field},
    }
    query_path = _write_json(
        tmp_path / "query.json",
        {
            "common_table_expressions": [],
            "source": {"object": object_ref, "alias": None},
            "projection": [
                {"expression": column("id"), "alias": None},
                {"expression": column("label"), "alias": None},
            ],
            "joins": [],
            "filter": None,
            "group_by": [],
            "having": None,
            "order_by": [{"expression": column("id"), "direction": "asc"}],
            "distinct": False,
            "row_limit": 10,
            "declared_crs": [],
        },
    )

    created = False
    try:
        _run(spec, "database-execute-ddl", f"DROP TABLE IF EXISTS {spec.qualified}")
        _run(spec, "database-execute-ddl", spec.create_sql)
        created = True

        written = _run(spec, "database-write-ipc", write_path, str(input_path))
        assert written["rows"]["received"] == 2
        assert written["rows"]["confirmed"] == 2

        summary = _run(spec, "database-read-summary", read_path, "-")
        assert summary["provider"] == provider
        assert summary["rows"] == 2

        materialized = _run(
            spec,
            "database-read-ipc",
            read_path,
            "-",
            str(output_path),
        )
        assert materialized["provider"] == provider
        with pyarrow.ipc.open_file(output_path) as reader:
            assert reader.read_all().num_rows == 2

        query = _run(spec, "database-query-summary", query_path, "-")
        assert query["provider"] == provider
        assert query["rows"] == 2

        inspected = _run(spec, "database-inspect-schemas")
        assert inspected["provider"] == provider
        assert isinstance(inspected["schemas"], list)

        scalar = _run(spec, "database-execute-scalar", "SELECT COUNT(*) FROM " + spec.qualified)
        assert scalar["status"] == "committed"
        assert scalar["value"] == 2
    finally:
        if created:
            _run(spec, "database-execute-ddl", f"DROP TABLE IF EXISTS {spec.qualified}")
