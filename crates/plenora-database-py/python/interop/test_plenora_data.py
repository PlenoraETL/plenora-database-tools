"""database ↔ data-tools2 sulla strada Python, senza conversioni.

Fuori da `python/tests`: richiede il wheel di `plenora-data` installato
accanto a `plenora-database`, che la campagna SDK di questo repository non
costruisce. Si esegue a mano o dalla suite d'interoperabilita:

    PLENORA_TEST_POSTGRES_DSN=... python -m pytest python/interop

Un `BatchReader` entra in `plenora_data.run` come sorgente
(`__arrow_c_stream__`), e la `pyarrow.Table` che ne esce entra in
`copy_from` cosi com'e.
"""

from __future__ import annotations

import os

import pytest

pyarrow = pytest.importorskip("pyarrow")
plenora_data = pytest.importorskip("plenora_data")

import plenora_database as p  # noqa: E402


def _session():
    dsn = os.environ.get("PLENORA_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("manca PLENORA_TEST_POSTGRES_DSN")
    config = p.EngineConfig.from_postgres_dsn(dsn, tls_mode="insecure_local")
    return p.engine_from_url(config).session()


def test_database_to_data_to_database_without_conversions() -> None:
    session = _session()
    try:
        session.execute_sql("DROP TABLE IF EXISTS _pyx_data_source")
        session.execute_sql("DROP TABLE IF EXISTS _pyx_data_target")
        session.execute_sql(
            "CREATE TABLE _pyx_data_source (id BIGINT PRIMARY KEY, importo DOUBLE PRECISION)"
        )
        session.execute_sql(
            "INSERT INTO _pyx_data_source "
            "SELECT gs, gs * 1.5 FROM generate_series(1, 50) gs"
        )
        piano = {
            "version": 1,
            "inputs": ["t"],
            "steps": [
                {
                    "out": "alti",
                    "op": "table.filter",
                    "in": ["t"],
                    "config": {"column": "id", "operator": ">", "value": 10},
                }
            ],
            "outputs": ["alti"],
        }
        # database → data: il reader come sorgente, nessun `bytes`.
        risultato = plenora_data.run(
            piano, {"t": session.read("public", "_pyx_data_source")}
        )
        tabella = risultato.tables["alti"]
        assert isinstance(tabella, pyarrow.Table)
        assert tabella.num_rows == 40
        # data → database: la tabella cosi com'e.
        outcome = session.copy_from(
            "public",
            "_pyx_data_target",
            tabella,
            mode="create",
            mapping_policy="compatible",
        )
        assert outcome["status"] == "committed"
        assert outcome["rows"]["confirmed"] == 40
        riletta = session.read(
            "public", "_pyx_data_target", order_by=[("id", "asc")]
        ).read_all()
        assert riletta.column("id").to_pylist() == list(range(11, 51))
    finally:
        try:
            session.execute_sql("DROP TABLE IF EXISTS _pyx_data_source")
            session.execute_sql("DROP TABLE IF EXISTS _pyx_data_target")
        finally:
            session.close()
