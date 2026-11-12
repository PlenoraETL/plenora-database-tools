"""Rapporto e consenso sullo stesso fixture live delle sessioni SDK."""
import warnings
import uuid
import json
import os
import subprocess

import pyarrow as pa
import pytest
import plenora_database as p

from ._harness import (
    connect_postgres, aconnect_postgres,
    connect_sqlserver_reference, aconnect_sqlserver_reference,
)


def input_table():
    return pa.table({"value": pa.array(["ABCDE"], type=pa.string())})


def check_outcome(outcome):
    assert outcome["schema_version"] == 3
    assert outcome["status"] == "committed"
    assert any(loss["severity"] == "data_loss" for loss in outcome["loss_report"]["losses"])


@pytest.mark.parametrize("provider", ["postgres", "sqlserver"])
def test_live_loss_consent_sync(provider):
    session = connect_postgres() if provider == "postgres" else connect_sqlserver_reference()
    schema = "public" if provider == "postgres" else "dbo"
    table = "loss_" + uuid.uuid4().hex
    target = f"{schema}.{table}"
    dtype = "varchar(4)" if provider == "postgres" else "nvarchar(4)"
    session.execute_ddl(f"CREATE TABLE {target} (value {dtype} NULL)")
    try:
        for policy in ["strict", "compatible"]:
            with pytest.raises(p.PlenoraDataMappingError) as caught:
                session.copy_from(schema, table, input_table(), mapping_policy=policy)
            assert caught.value.remote_effect in ("none", "rolled_back")
            assert "write_outcome" not in (caught.value.details or {})
            if policy == "compatible":
                assert caught.value.remote_effect == "none"
                assert caught.value.phase == "prepare"
            assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
        if provider == "postgres":
            with warnings.catch_warnings(record=True) as emitted:
                warnings.simplefilter("always", p.MappingLossWarning)
                with pytest.raises(p.PlenoraDataMappingError) as caught:
                    session.copy_from(schema, table, input_table(), mapping_policy="compatible", accept_loss=True)
            assert caught.value.remote_effect == "rolled_back"
            assert caught.value.loss_report is not None
            assert not emitted
            assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
        else:
            with pytest.warns(p.MappingLossWarning):
                check_outcome(session.copy_from(schema, table, input_table(), mapping_policy="compatible", accept_loss=True))
            assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 1
            with warnings.catch_warnings():
                warnings.simplefilter("error", p.MappingLossWarning)
                with pytest.raises(p.MappingLossWarning) as caught:
                    session.copy_from(schema, table, input_table(), mapping_policy="compatible", accept_loss=True)
                check_outcome(caught.value.write_outcome)
            assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 2
        session.execute_ddl(f"DROP TABLE {target}")
        session.execute_ddl(f"CREATE TABLE {target} (value bigint NULL)")
        exact = session.copy_from(schema, table, pa.table({"value": pa.array([1], type=pa.int64())}), mapping_policy="strict")
        assert exact["loss_report"]["losses"] == []
    finally:
        session.execute_ddl(f"DROP TABLE {target}")
        session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["postgres", "sqlserver"])
async def test_live_loss_consent_async(provider):
    session = await (aconnect_postgres() if provider == "postgres" else aconnect_sqlserver_reference())
    schema = "public" if provider == "postgres" else "dbo"
    table = "loss_" + uuid.uuid4().hex
    target = f"{schema}.{table}"
    dtype = "varchar(4)" if provider == "postgres" else "nvarchar(4)"
    await session.execute_ddl(f"CREATE TABLE {target} (value {dtype} NULL)")
    try:
        for policy in ["strict", "compatible"]:
            with pytest.raises(p.PlenoraDataMappingError) as caught:
                await session.acopy_from(schema, table, input_table(), mapping_policy=policy)
            assert "write_outcome" not in (caught.value.details or {})
            if policy == "compatible":
                assert caught.value.remote_effect == "none"
                assert caught.value.phase == "prepare"
            assert await session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
        if provider == "postgres":
            with warnings.catch_warnings(record=True) as emitted:
                warnings.simplefilter("always", p.MappingLossWarning)
                with pytest.raises(p.PlenoraDataMappingError) as caught:
                    await session.acopy_from(schema, table, input_table(), mapping_policy="compatible", accept_loss=True)
            assert caught.value.remote_effect == "rolled_back"
            assert caught.value.loss_report is not None
            assert not emitted
            assert await session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
        else:
            with pytest.warns(p.MappingLossWarning):
                check_outcome(await session.acopy_from(schema, table, input_table(), mapping_policy="compatible", accept_loss=True))
            assert await session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 1
        await session.execute_ddl(f"DROP TABLE {target}")
        await session.execute_ddl(f"CREATE TABLE {target} (value bigint NULL)")
        exact = await session.acopy_from(schema, table, pa.table({"value": pa.array([1], type=pa.int64())}), mapping_policy="strict")
        assert exact["loss_report"]["losses"] == []
    finally:
        await session.execute_ddl(f"DROP TABLE {target}")
        session.close()


@pytest.mark.parametrize("provider,command", [
    ("postgres", "database-write-ipc"), ("postgres", "write"),
    ("postgres", "bulk-write"), ("postgres", "postgres-write-ipc"),
    ("sqlserver", "database-write-ipc"), ("sqlserver", "write"),
])
def test_live_cli_loss_consent(provider, command, tmp_path):
    from .test_cli_common_surface import _spec, CLI_BIN_ENV, CLI_SECRET_ENV
    spec = _spec(provider)
    binary = os.environ.get(CLI_BIN_ENV)
    assert binary, "il gate live deve fornire il binario CLI"
    session = connect_postgres() if provider == "postgres" else connect_sqlserver_reference()
    table = "loss_cli_" + uuid.uuid4().hex
    target = f"{spec.schema}.{table}"
    strict_only = command == "postgres-write-ipc"
    dtype = "text" if strict_only else ("varchar(4)" if provider == "postgres" else "nvarchar(4)")
    session.execute_ddl(f"CREATE TABLE {target} (value {dtype} NULL)")
    source = input_table()
    arrow_path = tmp_path / "input.arrow"
    with pa.ipc.new_file(arrow_path, source.schema) as writer:
        writer.write_table(source)
    operation_path = tmp_path / "operation.json"
    request_path = tmp_path / "request.json"
    environment = {**os.environ, **spec.environment, CLI_SECRET_ENV: spec.secret}

    def run(policy, accept, output="json"):
        operation_path.write_text(json.dumps({
            "target": {"schema": spec.schema, "object": table}, "mode": "append",
            "mapping_policy": policy, "transaction_profile": "single_transaction",
            "keys": [], "update_columns": [], "create_spatial_index": False, "allow_partial": False,
        }))
        request_path.write_text(json.dumps({
            "provider": provider, "secret_environment": CLI_SECRET_ENV,
            "provider_arguments": spec.arguments, "operation_path": str(operation_path),
        }))
        if command == "write":
            args = ["--input", str(request_path), "--data", str(arrow_path)]
        elif command == "database-write-ipc":
            args = [provider, CLI_SECRET_ENV, str(operation_path), str(arrow_path), *spec.arguments]
        elif command == "bulk-write":
            args = [CLI_SECRET_ENV, str(operation_path), str(arrow_path)]
        else:
            args = [CLI_SECRET_ENV, spec.schema, table, str(arrow_path)]
        if accept:
            args += ["--accept-loss"]
        return subprocess.run([binary, command, *args, "--format", output], env=environment,
                              capture_output=True, text=True, check=False)
    try:
        if not strict_only:
            for policy in ["strict", "compatible"]:
                result = run(policy, False)
                assert result.returncode != 0
                if policy == "compatible":
                    assert "loss_report" in result.stdout + result.stderr
                assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
        for count, output in enumerate(["json", "markdown", "junit"], 1):
            result = run("strict" if strict_only else "compatible", not strict_only, output)
            if provider == "postgres" and not strict_only:
                assert result.returncode != 0
                assert "loss_report" in result.stdout + result.stderr
                assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
                continue
            assert result.returncode == 0, "scrittura CLI rifiutata"
            assert "loss_report" in result.stdout
            if not strict_only:
                assert "data_loss" in result.stdout
            assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == count
        session.execute_ddl(f"DROP TABLE {target}")
        session.execute_ddl(f"CREATE TABLE {target} (value bigint NULL)")
        exact = pa.table({"value": pa.array([1], type=pa.int64())})
        with pa.ipc.new_file(arrow_path, exact.schema) as writer:
            writer.write_table(exact)
        result = run("strict", False)
        assert result.returncode == 0
        document = json.loads(result.stdout)
        outcome = document.get("result", document)
        assert outcome["loss_report"]["losses"] == []
    finally:
        session.execute_ddl(f"DROP TABLE {target}")
        session.close()
