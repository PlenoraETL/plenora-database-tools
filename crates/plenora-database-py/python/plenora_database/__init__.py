"""Python SDK per plenora-database-tools.

Milestone corrente: F3-4 (portable AST builder Pythonic).

Uso base:

    import plenora_database

    with plenora_database.connect(dsn="host=localhost user=me dbname=app") as s:
        # SQL raw
        cnt = s.execute_scalar("SELECT COUNT(*)::BIGINT FROM users")

        # Portable AST (provider-agnostic)
        row = s.select("users").where_eq("id", 1).one()
        new = s.insert("users").values(name="Ada").returning("id").one()
        n = s.update("users").set(name="Alan").where_eq("id", 1).execute()

Le API di spatial / transaction / async arrivano in F3-5..F3-8.
"""

from ._native import version
from ._session import Session
from ._transaction import Transaction
from .query import Delete, Insert, Select, Update, Upsert
from ._native import connect as _native_connect


def connect(dsn: str) -> Session:
    """Apre una nuova sessione Postgres.

    La DSN è nel formato libpq (`host=... user=... password=... dbname=...`).
    Il probe iniziale verifica connessione + PostGIS. Fallisce con
    RuntimeError se la DSN è invalida, la rete non risponde o l'auth
    fallisce.
    """
    return Session(_native_connect(dsn))


__all__ = [
    "connect",
    "version",
    "Session",
    "Transaction",
    "Select",
    "Insert",
    "Update",
    "Delete",
    "Upsert",
]
