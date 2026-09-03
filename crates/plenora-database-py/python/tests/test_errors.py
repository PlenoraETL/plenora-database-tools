"""Test di integrazione live per la gerarchia PlenoraError."""
from __future__ import annotations

import os

import pytest

import plenora_database

from ._harness import connect_postgres, postgres_dsn_or_skip


@pytest.fixture(name="session")
def _session():
    dsn = postgres_dsn_or_skip()
    s = connect_postgres(dsn)
    try:
        yield s
    finally:
        s.close()


def test_plenora_error_is_runtime_error_subclass() -> None:
    # Retro-compat: chi filtra RuntimeError continua a intercettare.
    assert issubclass(plenora_database.PlenoraError, RuntimeError)


def test_specific_errors_inherit_from_plenora_error() -> None:
    for cls in [
        plenora_database.PlenoraSchemaError,
        plenora_database.PlenoraNotFoundError,
        plenora_database.PlenoraTimeoutError,
        plenora_database.PlenoraCancelledError,
        plenora_database.PlenoraConflictError,
        plenora_database.PlenoraProtocolError,
    ]:
        assert issubclass(cls, plenora_database.PlenoraError)


def test_result_and_mapping_errors_join_the_public_taxonomy() -> None:
    assert issubclass(
        plenora_database.NoResultFound, plenora_database.PlenoraNotFoundError
    )
    assert issubclass(
        plenora_database.MultipleResultsFound, plenora_database.PlenoraConflictError
    )
    assert issubclass(plenora_database.OrmError, plenora_database.PlenoraError)
    assert issubclass(
        plenora_database.JsonInputError,
        plenora_database.PlenoraDataMappingError,
    )


def test_not_found_error_on_nonexistent_table(session) -> None:
    with pytest.raises(plenora_database.PlenoraError) as exc_info:
        session.execute_scalar("SELECT * FROM tabella_che_non_esiste_xyz")
    exc = exc_info.value
    # Postgres classifica la tabella mancante come SQLSTATE 42P01 → NotFound
    # secondo il mapping del driver (l'oggetto SQL non esiste).
    assert isinstance(exc, plenora_database.PlenoraNotFoundError), \
        f"atteso PlenoraNotFoundError, ottenuto {type(exc).__name__}: {exc}"
    assert exc.category == "not_found"
    assert exc.phase in {"read", "prepare", "validate"}
    assert exc.provider == "postgres"
    assert exc.retry in {"never", "requires_recovery"}


def test_cancelled_error_via_statement_timeout(session) -> None:
    with pytest.raises(plenora_database.PlenoraError) as exc_info:
        with session.begin(statement_timeout_ms=100) as tx:
            tx.execute_sql("SELECT pg_sleep(2)")
    exc = exc_info.value
    # statement_timeout Postgres → SQLSTATE 57014 → Cancelled.
    assert isinstance(exc, plenora_database.PlenoraCancelledError), \
        f"atteso PlenoraCancelledError, ottenuto {type(exc).__name__}: {exc}"
    assert exc.category == "cancelled"


def test_error_carries_diagnostics_none_if_absent(session) -> None:
    with pytest.raises(plenora_database.PlenoraError) as exc_info:
        session.execute_scalar("SELECT * FROM tabella_che_non_esiste_xyz")
    exc = exc_info.value
    # `diagnostics` deve essere None o dict; mai una stringa Rust Debug.
    assert exc.diagnostics is None or isinstance(exc.diagnostics, (dict, list))


def test_invalid_dsn_raises_specific_category(session) -> None:
    # Connessione a host inesistente: Io/Connect o Timeout.
    with pytest.raises(plenora_database.PlenoraError) as exc_info:
        plenora_database.engine_from_url(
            plenora_database.EngineConfig.from_postgres_dsn(
                "host=host-inesistente.invalid user=x password=y dbname=z connect_timeout=1"
            )
        )
    exc = exc_info.value
    # Categoria attesa: Io / Timeout / Protocol / Transient / Cancelled.
    # Non deve essere Internal (bug del driver).
    assert exc.category != "internal", f"categoria Internal inaspettata: {exc}"
    # Phase = connect / probe.
    assert exc.phase in {"connect", "probe"}, f"phase inatteso: {exc.phase}"


def test_error_str_has_category_prefix(session) -> None:
    with pytest.raises(plenora_database.PlenoraError) as exc_info:
        session.execute_scalar("SELECT * FROM tabella_che_non_esiste_xyz")
    msg = str(exc_info.value)
    assert msg.startswith("not_found:"), f"atteso prefisso 'not_found:', trovato: {msg!r}"


def test_transaction_error_maps_correctly(session) -> None:
    # Errori dentro una transazione devono comunque essere PlenoraError con
    # la sottoclasse esatta (qui: NotFound per tabella inesistente).
    with pytest.raises(plenora_database.PlenoraNotFoundError):
        with session.begin() as tx:
            tx.execute_sql("SELECT * FROM tabella_che_non_esiste_xyz_2")


def test_read_only_transaction_violation_is_categorized(session) -> None:
    with pytest.raises(plenora_database.PlenoraError) as exc_info:
        with session.begin(read_only=True) as tx:
            tx.execute_sql("CREATE TABLE _pyf6_ro_test (id INT)")
    # Postgres: SQLSTATE 25006 (read_only_sql_transaction).
    # Il mapping puo' cadere in Conflict/Execution/Authorization — verifichiamo
    # solo che sia una PlenoraError specifica, NON Internal.
    assert exc_info.value.category != "internal"
