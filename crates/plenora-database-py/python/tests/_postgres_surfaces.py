"""Transport adapters for DB-b. Metadata decisions belong to the Rust provider."""
import asyncio
import itertools
import hashlib
import json
import os
import subprocess
from pathlib import Path

import pyarrow as pa
import plenora_database as p
from plenora_database.errors import PlenoraError

from ._harness import connect_postgres, aconnect_postgres

SURFACES = ("rust", "cli", "sdk_sync", "sdk_async", "runtime")
POLICIES = ("strict", "compatible", "lossy", "native")
CONSENTS = (False, True)
REFUSED = {"axis_lat_lon": "unsupported", "conflicting_crs": "crs",
           "dimensions_unknown": "crs", "uint64_ordering": "data_mapping"}


def fixtures():
    path = os.environ.get("PLENORA_PG_GEOMETRY_FIXTURES")
    if path is None:
        path = Path(__file__).resolve().parents[4] / "tests/fixtures/postgres_geometry_qualification.json"
    return json.loads(Path(path).read_text(encoding="utf-8"))


def cells(source):
    return set(itertools.product(
        [(f["name"], f["variant"]) for f in source["fixtures"]],
        source["properties"], SURFACES, POLICIES, CONSENTS))


def table(fixture):
    types = {"int64": pa.int64(), "uint64": pa.uint64(), "string": pa.string(), "binary": pa.binary()}
    fields, arrays = [], []
    for field in fixture["fields"]:
        dtype = types[field["type"]]
        fields.append(pa.field(field["name"], dtype, field["nullable"], field["metadata"]))
        values = field["values"]
        if field["type"] == "binary":
            values = [None if v is None else bytes.fromhex(v) for v in values]
        arrays.append(pa.array(values, type=dtype))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields, fixture["schema"]))


def operation(name, policy):
    return {"target": {"schema": "public", "object": name}, "mode": "create",
            "mapping_policy": policy, "transaction_profile": "single_transaction",
            "keys": [], "update_columns": [], "create_spatial_index": False, "allow_partial": False}


def read_operation(name):
    return {"source": {"schema": "public", "object": name}, "projection": [],
            "order_by": [{"field": "id", "direction": "asc"}], "row_limit": None,
            "row_offset": None, "filter": None, "declared_crs": []}


def json_file(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")
    return str(path)


def error_document(error):
    for current in (error, error.__cause__, error.__context__):
        text = str(current)
        for secret in (os.environ.get("PLENORA_TEST_POSTGRES_DSN"), "SENTINELLA-DATI-7f3a"):
            assert not secret or secret not in text
    return {key: getattr(error, key) for key in
            ("category", "phase", "remote_effect", "retry", "provider", "details")}


def normalized(result):
    """Compare reports and axes verbatim; execution IDs and messages are not identities."""
    document = result["document"]
    if not result["ok"]:
        return {"ok": False, **{key: document.get(key) for key in
                ("category", "phase", "remote_effect", "retry", "provider", "details")}}
    return {"ok": True, **{key: document[key] for key in ("status", "rows", "loss_report")}}


def process(command, environment):
    completed = subprocess.run(command, env=environment, capture_output=True, text=True, check=False)
    for secret in (environment.get("PLENORA_TEST_POSTGRES_DSN"), "SENTINELLA-DATI-7f3a"):
        assert not secret or secret not in completed.stdout + completed.stderr
    return completed


class Harness:
    def __init__(self, directory, dsn):
        self.directory = directory
        self.environment = {**os.environ, "PLENORA_TEST_POSTGRES_DSN": dsn,
                            "PLENORA_TLS_INSECURE_LOCAL": "1"}
        self.cli = os.environ["PLENORA_CLI_BIN"]
        self.helper = os.environ["PLENORA_PG_SURFACE_BIN"]
        assert Path(self.cli).is_file() and Path(self.helper).is_file()
        self.serial = 0

    def path(self, suffix):
        self.serial += 1
        return self.directory / f"artifact_{self.serial}.{suffix}"

    def invoke(self, surface, action, op, source=None, accept=False):
        output = self.path("arrow")
        request = {"surface": surface, "action": action, "operation": op,
                   "accept_loss": accept, "output": str(output)}
        if source is not None:
            path = self.path("arrow")
            with pa.ipc.new_file(path, source.schema) as writer:
                writer.write_table(source)
            request["input"] = str(path)
        if surface == "cli":
            op_path = json_file(self.path("json"), op)
            if action == "preflight":
                command = [self.cli, "bulk-write", "PLENORA_TEST_POSTGRES_DSN", op_path,
                           request["input"], "--dry-run", "--offline"]
                if accept:
                    command.append("--accept-loss")
            elif action == "write":
                command = [self.cli, "database-write-ipc", "postgres", "PLENORA_TEST_POSTGRES_DSN",
                           op_path, request["input"]]
                if accept:
                    command.append("--accept-loss")
            else:
                command = [self.cli, "database-read-ipc", "postgres", "PLENORA_TEST_POSTGRES_DSN",
                           op_path, "-", str(output)]
            completed = process([*command, "--format", "json"], self.environment)
            result = {"ok": completed.returncode == 0, "document": json.loads(completed.stdout)}
            if not result["ok"]:
                result["document"] = result["document"].get("error", result["document"])
                # CLI-2.0 contract, not a second provider decision.
                expected_exit = {"invalid_plan": 2, "invalid_configuration": 2, "schema": 3,
                                 "data_mapping": 3, "crs": 3, "unsupported": 3, "resource_limit": 4}
                assert completed.returncode == expected_exit[result["document"]["category"]]
        else:
            completed = process([self.helper, json_file(self.path("json"), request)], self.environment)
            assert completed.returncode == 0, "qualification host failed, not a provider refusal"
            result = json.loads(completed.stdout)
        if action == "read" and result["ok"]:
            opener = pa.ipc.open_file if surface == "cli" else pa.ipc.open_stream
            with opener(output) as reader:
                return result["document"]["read_loss"], reader.read_all()
        return result


def sync_write(harness, op, source, accept):
    session = connect_postgres()
    try:
        try:
            result = session.copy_from("public", op["target"]["object"], source, mode=op["mode"],
                                       mapping_policy=op["mapping_policy"], accept_loss=accept)
            return {"ok": True, "document": result}
        except PlenoraError as error:
            return {"ok": False, "document": error_document(error)}
    finally:
        session.close()


async def async_write(op, source, accept):
    session = await aconnect_postgres()
    try:
        try:
            result = await session.acopy_from("public", op["target"]["object"], source, mode=op["mode"],
                                              mapping_policy=op["mapping_policy"], accept_loss=accept)
            return {"ok": True, "document": result}
        except PlenoraError as error:
            return {"ok": False, "document": error_document(error)}
    finally:
        await session.aclose()


def sync_read(harness, op):
    session = connect_postgres()
    try:
        reader = session.read("public", op["source"]["object"], order_by=[("id", "asc")])
        output = reader.read_all()
        return reader.read_loss, output
    finally:
        session.close()


async def async_read(op):
    session = await aconnect_postgres()
    try:
        reader = await session.aread("public", op["source"]["object"], order_by=[("id", "asc")])
        output = await p.AsyncArrowStreamReader(reader).read_all()
        return reader.read_loss, output
    finally:
        await session.aclose()


WRITERS = {
    "rust": lambda h, op, t, a: h.invoke("rust", "write", op, t, a),
    "cli": lambda h, op, t, a: h.invoke("cli", "write", op, t, a),
    "sdk_sync": sync_write,
    "sdk_async": lambda h, op, t, a: asyncio.run(async_write(op, t, a)),
    "runtime": lambda h, op, t, a: h.invoke("runtime", "write", op, t, a),
}
READERS = {
    "rust": lambda h, op: h.invoke("rust", "read", op),
    "cli": lambda h, op: h.invoke("cli", "read", op),
    "sdk_sync": sync_read,
    "sdk_async": lambda h, op: asyncio.run(async_read(op)),
    "runtime": lambda h, op: h.invoke("runtime", "read", op),
}


def assert_original(actual, expected):
    assert actual.schema.equals(expected.schema, check_metadata=True), "schema or metadata changed"
    assert actual.equals(expected), "values/geometry bytes changed"


def expected_subject(scope):
    return {"index": 0, "scope": "field" if scope == "field" else "schema",
            "field_identity": "1" if scope == "field" else None,
            "property": "native_metadata", "key_sha256": hashlib.sha256(b"opaque").hexdigest(),
            "hop": "column_comment", "reason": "operation_limit" if scope == "aggregate" else "column_limit"}


def retained_table(original, scope):
    fields = list(original.schema)
    metadata = dict(original.schema.metadata)
    if scope == "field":
        field_metadata = dict(fields[0].metadata)
        del field_metadata[b"opaque"]
        fields[0] = fields[0].with_metadata(field_metadata)
    else:
        del metadata[b"opaque"]
    return pa.Table.from_arrays(original.columns, schema=pa.schema(fields, metadata))


def loss_sources():
    """Boundaries exercise reports/consent, independently of the nominal fixtures."""
    for scope in ("field", "schema", "aggregate"):
        count = 20 if scope == "aggregate" else 2
        fields = [pa.field("id" if i == 0 else f"v{i}", pa.int64(),
                           metadata={"plenora.field_id": str(i + 1)}) for i in range(count)]
        metadata = {"plenora.contract.version": "1"}
        key, value = "opaque", "x" * (900_000 if scope == "aggregate" else 1_048_576)
        if scope == "field":
            fields[0] = fields[0].with_metadata({**fields[0].metadata, key.encode(): value.encode()})
        else:
            metadata[key] = value
        schema = pa.schema(fields, metadata)
        for empty in (False, True):
            yield scope, empty, pa.Table.from_arrays(
                [pa.array([] if empty else [1], type=f.type) for f in schema], schema=schema)
