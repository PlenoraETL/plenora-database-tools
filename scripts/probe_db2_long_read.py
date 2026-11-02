#!/usr/bin/env python3
"""Sonda senza DDL sul wheel installato: testo intero, buffer e CAST server.

Eseguire nell'ambiente del wheel storico con DB2_ENV del runner ufficiale:
    python scripts/probe_db2_long_read.py
Un risultato truncated per il CAST dimostra la perdita silenziosa (exit 1).
Un rifiuto e distinto dal successo esatto; nessun payload viene stampato.
"""

import importlib.metadata
import json
import os

import plenora_database as p


def main() -> int:
    config = p.EngineConfig(
        "db2",
        host=os.environ["PLENORA_DB2_HOST"],
        database=os.environ["PLENORA_DB2_DATABASE"],
        user=os.environ["PLENORA_DB2_USER"],
        password=os.environ["PLENORA_DB2_PASSWORD"],
        port=int(os.environ.get("PLENORA_DB2_PORT", "50000")),
        tls_mode="insecure_local",
    )
    report = {"wheel": importlib.metadata.version("plenora_database"), "cases": []}
    loss = False
    with p.engine_from_url(config) as engine:
        with engine.session() as session:
            for name, expression, expected in [
                ("varchar_32000", "REPEAT(CAST('a' AS VARCHAR(32000)), 32000)", "a" * 32000),
                ("clob_40000", "REPEAT(CAST('a' AS CLOB(100K)), 40000)", "a" * 40000),
                ("clob_70000", "REPEAT(CAST('a' AS CLOB(100K)), 70000)", "a" * 70000),
                ("server_cast_length", "LENGTH(CAST(REPEAT(CAST('a' AS CLOB(100K)), 40000) AS VARCHAR(32672)))", 40000),
                ("server_cast_text", "CAST(REPEAT(CAST('a' AS CLOB(100K)), 40000) AS VARCHAR(32672))", "a" * 40000),
            ]:
                case = {"case": name}
                try:
                    value = session.execute_scalar(f"SELECT {expression} AS V FROM SYSIBM.SYSDUMMY1")
                except p.PlenoraError as error:
                    case.update(status="refused", error_type=type(error).__name__)
                else:
                    exact = value == expected
                    case.update(status="exact" if exact else "truncated_or_changed")
                    # Solo lunghezza e conteggio della sonda sintetica.
                    case["length_or_count"] = len(value) if isinstance(value, str) else value
                    loss |= not exact
                report["cases"].append(case)
    print(json.dumps(report, indent=2))
    return int(loss)


if __name__ == "__main__":
    raise SystemExit(main())
