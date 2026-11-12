"""Parita del consenso effettivo sulle superfici live della campagna SDK."""
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


def input_table(values=("ABCDE",)):
    return pa.table({"value": pa.array(values, type=pa.string())})


def loss_type(provider):
    # La misura PG timestamp(us)->timestamp(3) rifiuta tutti i campioni.
    # UTF8->jsonb e ammessa: -0 perde realmente il segno (regola numerica S).
    return "jsonb" if provider == "postgres" else "datetime2(3)"


def stored_loss_expression(provider):
    return "CAST(value AS text)" if provider == "postgres" else "DATEPART(microsecond, value)"


def loss_table(provider, values):
    if provider == "postgres":
        field = pa.field("value", pa.string(), metadata={
            "plenora.postgres.native_type": "jsonb",
            "plenora.postgres.native_declaration": "jsonb",
        })
        samples = {1000: "0", 1001: "-0", 1002: "1.5", None: None}
        return pa.Table.from_arrays([pa.array([samples[v] for v in values], type=pa.string())],
                                   schema=pa.schema([field]))
    declaration = "datetime2(3)"
    field = pa.field("value", pa.timestamp("us"), metadata={
        f"plenora.{provider}.native_declaration": declaration,
    })
    return pa.Table.from_arrays([pa.array(values, type=pa.timestamp("us"))], schema=pa.schema([field]))


def loss_stream(provider):
    first, second = loss_table(provider, [1000]), loss_table(provider, [1001])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, first.schema) as writer:
        writer.write_table(first)
        writer.write_table(second)
    return sink.getvalue().to_pybytes()


def empty_sources(provider):
    # Anche reader e IPC vengono contati dal binding, gia nella 9.0.3.
    table = loss_table(provider, [])
    yield table
    yield pa.RecordBatch.from_arrays([pa.array([], type=table.schema[0].type)], schema=table.schema)
    yield pa.RecordBatchReader.from_batches(table.schema, [])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema):
        pass
    yield sink.getvalue().to_pybytes()


def check_empty_error(error):
    assert error.category == "invalid_plan"
    assert error.remote_effect == "none"
    check_empty_report(error.loss_report)


def check_empty_report(report):
    assert report["schema_version"] == 3
    assert report["evaluation"]["complete"] is False
    assert report["effective"] == []


def check_outcome(outcome, changed=False):
    assert outcome["schema_version"] == 3
    assert outcome["status"] == "committed"
    report = outcome["loss_report"]
    assert report["schema_version"] == 3
    assert report["evaluation"]["complete"]
    assert bool(report["effective"]) == changed
    if changed:
        assert any(entry["committed"] > 0 for entry in report["effective"])
        assert all(entry["uncertain"] == 0 for entry in report["effective"])
    return report


@pytest.mark.parametrize("provider", ["postgres", "sqlserver"])
def test_live_loss_consent_sync(provider):
    session = connect_postgres() if provider == "postgres" else connect_sqlserver_reference()
    schema = "public" if provider == "postgres" else "dbo"
    table = "loss_" + uuid.uuid4().hex
    target = f"{schema}.{table}"
    dtype = "varchar(4)" if provider == "postgres" else "nvarchar(4)"
    session.execute_ddl(f"CREATE TABLE {target} (value {dtype} NULL)")
    try:
        for policy in ["compatible", "lossy", "native"]:
            # Rischio senza trasformazione: nessun consenso e nessun warning.
            with warnings.catch_warnings(record=True) as emitted:
                warnings.simplefilter("always", p.MappingLossWarning)
                report = check_outcome(session.copy_from(schema, table, input_table(["ABCD", None]), mapping_policy=policy))
                assert report["risk"]
                assert not emitted
            session.execute_ddl(f"TRUNCATE TABLE {target}")
            # SDK seleziona Prepared: SQL Server tronca solo con consenso.
            for accept in ([False, True] if provider == "postgres" else [False]):
                with warnings.catch_warnings(record=True) as emitted:
                    warnings.simplefilter("always", p.MappingLossWarning)
                    with pytest.raises(p.PlenoraDataMappingError) as caught:
                        session.copy_from(schema, table, input_table(), mapping_policy=policy, accept_loss=accept)
                assert caught.value.remote_effect == "rolled_back"
                assert caught.value.loss_report["schema_version"] == 3
                assert caught.value.loss_report["effective"] == []
                assert not emitted
                assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
            if provider == "sqlserver":
                with pytest.warns(p.MappingLossWarning):
                    report = check_outcome(session.copy_from(schema, table, input_table(), mapping_policy=policy, accept_loss=True), changed=True)
                assert any(e["rule_id"] == "ss.text_limit" and e["committed"] == 1 for e in report["effective"])
                assert session.execute_scalar(f"SELECT value FROM {target}") == "ABCD"
                assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 1
                session.execute_ddl(f"TRUNCATE TABLE {target}")
        session.execute_ddl(f"DROP TABLE {target}")
        dtype = loss_type(provider)
        session.execute_ddl(f"CREATE TABLE {target} (value {dtype} NULL)")
        for policy in ["compatible", "lossy", "native"]:
            for values in ([1000, None], [None], [1000], *([[1002]] if provider == "postgres" else [])):
                with warnings.catch_warnings(record=True) as emitted:
                    warnings.simplefilter("always", p.MappingLossWarning)
                    check_outcome(session.copy_from(schema, table, loss_table(provider, values), mapping_policy=policy))
                    assert not emitted
                    if values == [1002]:
                        assert session.execute_scalar(f"SELECT CAST(value AS text) FROM {target}") == "1.5"
                session.execute_ddl(f"TRUNCATE TABLE {target}")
            for source in empty_sources(provider):
                with warnings.catch_warnings(record=True) as emitted:
                    warnings.simplefilter("always", p.MappingLossWarning)
                    with pytest.raises(p.PlenoraInvalidPlanError) as caught:
                        session.copy_from(schema, table, source, mapping_policy=policy)
                    check_empty_error(caught.value)
                    assert not emitted
                assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
            for source in [loss_table(provider, [1001]), loss_stream(provider)]:
                with pytest.raises(p.PlenoraDataMappingError) as caught:
                    session.copy_from(schema, table, source, mapping_policy=policy)
                assert caught.value.remote_effect == "rolled_back"
                report = caught.value.loss_report
                assert report["effective"] == []
                assert any(e["would_change"] > 0 for e in report["evaluation"]["entries"])
                assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
            for count, filter_name in enumerate(["always", "ignore", "error"], 1):
                with warnings.catch_warnings(record=True) as emitted:
                    warnings.simplefilter(filter_name, p.MappingLossWarning)
                    if filter_name == "error":
                        with pytest.raises(p.MappingLossWarning) as caught:
                            session.copy_from(schema, table, loss_table(provider, [1001]), mapping_policy=policy, accept_loss=True)
                        check_outcome(caught.value.write_outcome, changed=True)
                        assert "commit avvenuto" in str(caught.value)
                    else:
                        check_outcome(session.copy_from(schema, table, loss_table(provider, [1001]), mapping_policy=policy, accept_loss=True), changed=True)
                        assert len(emitted) == (1 if filter_name == "always" else 0)
                assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == count
            fraction = stored_loss_expression(provider)
            assert session.execute_scalar(f"SELECT MAX({fraction}) FROM {target}") == ("0" if provider == "postgres" else 1000)
            session.execute_ddl(f"TRUNCATE TABLE {target}")
        for accept in [False, True]:
            with pytest.raises(p.PlenoraDataMappingError):
                session.copy_from(schema, table, loss_table(provider, [1001]), mapping_policy="strict", accept_loss=accept)
        session.execute_ddl(f"DROP TABLE {target}")
        session.execute_ddl(f"CREATE TABLE {target} (value bigint NULL)")
        exact = session.copy_from(schema, table, pa.table({"value": pa.array([1], type=pa.int64())}), mapping_policy="strict")
        assert check_outcome(exact)["risk"] == []
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
        for policy in ["compatible", "lossy", "native"]:
            # Rischio senza trasformazione: nessun consenso e nessun warning.
            with warnings.catch_warnings(record=True) as emitted:
                warnings.simplefilter("always", p.MappingLossWarning)
                report = check_outcome(await session.acopy_from(schema, table, input_table(["ABCD", None]), mapping_policy=policy))
                assert report["risk"]
                assert not emitted
            await session.execute_ddl(f"TRUNCATE TABLE {target}")
            # SDK seleziona Prepared: SQL Server tronca solo con consenso.
            for accept in ([False, True] if provider == "postgres" else [False]):
                with warnings.catch_warnings(record=True) as emitted:
                    warnings.simplefilter("always", p.MappingLossWarning)
                    with pytest.raises(p.PlenoraDataMappingError) as caught:
                        await session.acopy_from(schema, table, input_table(), mapping_policy=policy, accept_loss=accept)
                assert caught.value.remote_effect == "rolled_back"
                assert caught.value.loss_report["schema_version"] == 3
                assert caught.value.loss_report["effective"] == []
                assert not emitted
                assert await session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
            if provider == "sqlserver":
                with pytest.warns(p.MappingLossWarning):
                    report = check_outcome(await session.acopy_from(schema, table, input_table(), mapping_policy=policy, accept_loss=True), changed=True)
                assert any(e["rule_id"] == "ss.text_limit" and e["committed"] == 1 for e in report["effective"])
                assert await session.execute_scalar(f"SELECT value FROM {target}") == "ABCD"
                assert await session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 1
                await session.execute_ddl(f"TRUNCATE TABLE {target}")
        await session.execute_ddl(f"DROP TABLE {target}")
        dtype = loss_type(provider)
        await session.execute_ddl(f"CREATE TABLE {target} (value {dtype} NULL)")
        for policy in ["compatible", "lossy", "native"]:
            for values in ([1000, None], [None], [1000], *([[1002]] if provider == "postgres" else [])):
                with warnings.catch_warnings(record=True) as emitted:
                    warnings.simplefilter("always", p.MappingLossWarning)
                    check_outcome(await session.acopy_from(schema, table, loss_table(provider, values), mapping_policy=policy))
                    assert not emitted
                    if values == [1002]:
                        assert await session.execute_scalar(f"SELECT CAST(value AS text) FROM {target}") == "1.5"
                await session.execute_ddl(f"TRUNCATE TABLE {target}")
            for source in empty_sources(provider):
                with warnings.catch_warnings(record=True) as emitted:
                    warnings.simplefilter("always", p.MappingLossWarning)
                    with pytest.raises(p.PlenoraInvalidPlanError) as caught:
                        await session.acopy_from(schema, table, source, mapping_policy=policy)
                    check_empty_error(caught.value)
                    assert not emitted
                assert await session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
            for source in [loss_table(provider, [1001]), loss_stream(provider)]:
                with pytest.raises(p.PlenoraDataMappingError) as caught:
                    await session.acopy_from(schema, table, source, mapping_policy=policy)
                assert caught.value.remote_effect == "rolled_back"
                report = caught.value.loss_report
                assert report["effective"] == []
                assert any(e["would_change"] > 0 for e in report["evaluation"]["entries"])
                assert await session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
            for count, filter_name in enumerate(["always", "ignore", "error"], 1):
                with warnings.catch_warnings(record=True) as emitted:
                    warnings.simplefilter(filter_name, p.MappingLossWarning)
                    if filter_name == "error":
                        with pytest.raises(p.MappingLossWarning) as caught:
                            await session.acopy_from(schema, table, loss_table(provider, [1001]), mapping_policy=policy, accept_loss=True)
                        check_outcome(caught.value.write_outcome, changed=True)
                        assert "commit avvenuto" in str(caught.value)
                    else:
                        check_outcome(await session.acopy_from(schema, table, loss_table(provider, [1001]), mapping_policy=policy, accept_loss=True), changed=True)
                        assert len(emitted) == (1 if filter_name == "always" else 0)
                assert await session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == count
            fraction = stored_loss_expression(provider)
            assert await session.execute_scalar(f"SELECT MAX({fraction}) FROM {target}") == ("0" if provider == "postgres" else 1000)
            await session.execute_ddl(f"TRUNCATE TABLE {target}")
        for accept in [False, True]:
            with pytest.raises(p.PlenoraDataMappingError):
                await session.acopy_from(schema, table, loss_table(provider, [1001]), mapping_policy="strict", accept_loss=accept)
        await session.execute_ddl(f"DROP TABLE {target}")
        await session.execute_ddl(f"CREATE TABLE {target} (value bigint NULL)")
        exact = await session.acopy_from(schema, table, pa.table({"value": pa.array([1], type=pa.int64())}), mapping_policy="strict")
        assert check_outcome(exact)["risk"] == []
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
        if provider == "sqlserver":
            assert run("strict", False).returncode != 0
            for output in ["json", "markdown", "junit"]:
                result = subprocess.run(
                    [binary, "bulk-write", CLI_SECRET_ENV, str(operation_path), str(arrow_path),
                     "--provider", "sqlserver", "--format", output],
                    env=environment, capture_output=True, text=True, check=False,
                )
                assert result.returncode != 0
                assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
        if not strict_only:
            for policy in ["strict", "compatible"]:
                result = run(policy, False)
                assert result.returncode != 0
                if policy == "compatible":
                    assert "loss_report" in result.stdout + result.stderr
                assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
        for count, output in enumerate(["json", "markdown", "junit"], 1):
            result = run("strict" if strict_only else "compatible", not strict_only, output)
            if not strict_only and provider == "postgres":
                assert result.returncode != 0
                assert "loss_report" in result.stdout + result.stderr
                assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == 0
                continue
            assert result.returncode == 0, "scrittura CLI rifiutata"
            assert "loss_report" in result.stdout
            if not strict_only:
                assert "data_loss" in result.stdout
            assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == count
            if provider == "sqlserver":
                assert session.execute_scalar(f"SELECT MIN(value) FROM {target}") == "ABCD"
                assert session.execute_scalar(f"SELECT MAX(value) FROM {target}") == "ABCD"
                if output == "json":
                    document = json.loads(result.stdout)
                    report = check_outcome(document.get("result", document), changed=True)
                    assert any(e["rule_id"] == "ss.text_limit" and e["committed"] == 1 for e in report["effective"])
        if not strict_only:
            session.execute_ddl(f"DROP TABLE {target}")
            dtype = loss_type(provider)
            session.execute_ddl(f"CREATE TABLE {target} (value {dtype} NULL)")
            def write_loss_batches(batches, ipc_format):
                tables = [loss_table(provider, values) for values in batches]
                with ipc_format(arrow_path, tables[0].schema) as writer:
                    for data in tables:
                        writer.write_table(data)
            for output in ["json", "markdown", "junit"]:
                for policy in ["compatible", "lossy", "native"]:
                    for batches, accept, success, count, ipc_format in [
                        ([[1000, None]], False, True, 2, pa.ipc.new_file),
                        ([[]], False, False, 0, pa.ipc.new_file),
                        ([[]], True, False, 0, pa.ipc.new_file),
                        ([[]], False, False, 0, pa.ipc.new_stream),
                        ([[]], True, False, 0, pa.ipc.new_stream),
                        ([[1001]], False, False, 0, pa.ipc.new_file),
                        ([[1000], [1001]], False, False, 0, pa.ipc.new_file),
                        ([[1001]], True, True, 1, pa.ipc.new_file),
                    ]:
                        write_loss_batches(batches, ipc_format)
                        result = run(policy, accept, output)
                        assert (result.returncode == 0) == success, result.stdout
                        for key in ["risk", "evaluation", "effective"]:
                            assert key in result.stdout
                        if batches == [[]]:
                            assert result.returncode == 2
                            assert "invalid_plan" in result.stdout
                            assert "none" in result.stdout
                        elif not success:
                            assert "rolled_back" in result.stdout
                            assert "data_mapping" in result.stdout
                        if output == "json":
                            document = json.loads(result.stdout)
                            if batches == [[]]:
                                error = document["error"]
                                assert error["category"] == "invalid_plan"
                                assert error["remote_effect"] == "none"
                                check_empty_report(error["loss_report"])
                            elif success:
                                report = check_outcome(document.get("result", document), changed=accept)
                                assert report["risk"]
                            else:
                                # Canonical and legacy envelopes preserve their own axes.
                                assert "would_change" in result.stdout
                        assert session.execute_scalar(f"SELECT COUNT(*) FROM {target}") == count
                        if accept and success:
                            fraction = stored_loss_expression(provider)
                            assert session.execute_scalar(f"SELECT MAX({fraction}) FROM {target}") == ("0" if provider == "postgres" else 1000)
                        session.execute_ddl(f"TRUNCATE TABLE {target}")
                write_loss_batches([[1001]], pa.ipc.new_file)
                assert run("strict", False, output).returncode != 0
                assert run("strict", True, output).returncode != 0
        session.execute_ddl(f"DROP TABLE {target}")
        session.execute_ddl(f"CREATE TABLE {target} (value bigint NULL)")
        exact = pa.table({"value": pa.array([1], type=pa.int64())})
        with pa.ipc.new_file(arrow_path, exact.schema) as writer:
            writer.write_table(exact)
        result = run("strict", False)
        assert result.returncode == 0
        document = json.loads(result.stdout)
        outcome = document.get("result", document)
        assert check_outcome(outcome)["risk"] == []
    finally:
        session.execute_ddl(f"DROP TABLE {target}")
        session.close()
