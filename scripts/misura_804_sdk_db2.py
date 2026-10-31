"""MISURA TEMPORANEA 8.0.4 (solo ramo misura/804): SDK Python su Db2.

Gira nel container del gate dopo i test Rust `misura_804`, che lasciano le
tabelle PLENORA_TEST.MIS804_* create. Stampa righe `MISURA SDK ...`.
"""

from __future__ import annotations

import io
import sys

sys.path.insert(0, "/tmp/mis804")

from misura_tests._harness import connect_db2_reference  # noqa: E402


def show(label, function):
    try:
        result = function()
    except Exception as error:  # noqa: BLE001 - misura
        print(f"MISURA SDK db2 {label} ERRORE {type(error).__name__}: {error}")
        return
    try:
        keys = result.keys()
        tuples = result.tuples()
        dicts = [row.as_dict() for row in result.all()]
        print(f"MISURA SDK db2 {label} keys={keys!r} tuples={tuples!r} dicts={dicts!r}")
    except Exception as error:  # noqa: BLE001
        print(f"MISURA SDK db2 {label} risultato={result!r} (ERRORE lettura {error!r})")


def main() -> None:
    session = connect_db2_reference()
    queries = {
        "B1-letterali": "SELECT 1 AS x, 2 AS x FROM sysibm.sysdummy1",
        "B2-join": "SELECT a.id, b.id FROM PLENORA_TEST.MIS804_A a JOIN PLENORA_TEST.MIS804_B b ON a.id < b.id",
        "B3-join-star": "SELECT * FROM PLENORA_TEST.MIS804_A a JOIN PLENORA_TEST.MIS804_B b ON a.id < b.id",
        "A-ts": "SELECT ID, T6, T7, T9, T12, VARCHAR(T7) AS S7, VARCHAR(T12) AS S12 FROM PLENORA_TEST.MIS804_TS ORDER BY ID",
        "A-ts-T7": "SELECT ID, T7, VARCHAR(T7) AS S FROM PLENORA_TEST.MIS804_TS ORDER BY ID",
        "A-ts-T12": "SELECT ID, T12, VARCHAR(T12) AS S FROM PLENORA_TEST.MIS804_TS ORDER BY ID",
    }
    try:
        for label, sql in queries.items():
            show(f"session.query_sql {label}", lambda sql=sql: session.query_sql(sql))
        for label, sql in queries.items():
            def in_transaction(sql=sql):
                tx = session.begin()
                try:
                    return tx.query_sql(sql)
                finally:
                    tx.rollback()
            show(f"tx.query_sql {label}", in_transaction)
        for column in ("T6", "T7", "T9", "T12"):
            label = f"session.read A-ts-{column}"
            try:
                chunks = list(
                    session.read(
                        "PLENORA_TEST",
                        "MIS804_TS",
                        projection=["ID", column],
                        order_by=[("ID", "asc")],
                        catalog="PLENORA",
                    )
                )
            except Exception as error:  # noqa: BLE001
                print(f"MISURA SDK db2 {label} ERRORE {type(error).__name__}: {error}")
                continue
            try:
                import pyarrow.ipc as ipc

                for chunk in chunks:
                    table = ipc.open_stream(io.BytesIO(chunk)).read_all()
                    print(
                        f"MISURA SDK db2 {label} schema={table.schema!r} righe={table.to_pylist()!r}"
                    )
            except ImportError:
                print(f"MISURA SDK db2 {label} pyarrow assente, chunk={len(chunks)}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
