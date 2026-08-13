"""Python SDK per plenora-database-tools.

Milestone corrente: F3-2 (Session + connect() context manager, Postgres).

Uso:

    import plenora_database

    with plenora_database.connect(dsn="host=localhost user=me dbname=app") as s:
        print(s.server_version, s.postgis_version)

Le API di query / spatial / transaction / async arrivano in F3-3..F3-8.
"""

from ._native import Session, connect, version

__all__ = ["Session", "connect", "version"]
