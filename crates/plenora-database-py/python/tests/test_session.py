"""Test di integrazione live per Session e connect().

Skippa gracefully se `PLENORA_TEST_POSTGRES_DSN` non è settato.

Uso:
    export PLENORA_TEST_POSTGRES_DSN="host=dataflow-postgres user=dataflow password=dataflow_test_2026 dbname=dataflow_test"
    pytest python/tests/test_session.py -v
"""
from __future__ import annotations

import os

import pytest

import plenora_database

from ._harness import connect_postgres, postgres_dsn_or_skip


def test_version_returns_semver_like_string() -> None:
    v = plenora_database.version()
    assert isinstance(v, str)
    assert len(v.split(".")) >= 2, f"attesa versione con almeno major.minor: {v}"
    # Deve iniziare con una cifra.
    assert v[0].isdigit(), f"versione non parte con cifra: {v}"


def test_connect_returns_session_with_populated_metadata() -> None:
    dsn = postgres_dsn_or_skip()
    session = connect_postgres(dsn)
    try:
        # server_version deve essere non vuoto e contenere una cifra
        # (formato tipico "16.4" o "16.4 (Debian ...)").
        assert isinstance(session.server_version, str)
        assert any(c.isdigit() for c in session.server_version), \
            f"server_version senza cifre: {session.server_version!r}"
        # postgis_version è Optional[str]: se presente deve iniziare con cifra.
        if session.postgis_version is not None:
            assert isinstance(session.postgis_version, str)
            assert session.postgis_version[0].isdigit(), \
                f"postgis_version non parte con cifra: {session.postgis_version!r}"
        assert session.is_closed is False
    finally:
        session.close()


def test_context_manager_marks_session_closed_on_exit() -> None:
    dsn = postgres_dsn_or_skip()
    with connect_postgres(dsn) as s:
        assert s.is_closed is False
        server = s.server_version
        assert server
    # Dopo l'exit dal context manager: closed.
    assert s.is_closed is True


def test_close_is_idempotent() -> None:
    dsn = postgres_dsn_or_skip()
    s = connect_postgres(dsn)
    s.close()
    assert s.is_closed is True
    # Seconda close non deve sollevare.
    s.close()
    assert s.is_closed is True


def test_invalid_dsn_raises_runtime_error_with_categorized_message() -> None:
    # Formato DSN sintatticamente OK ma host inesistente → deve fallire
    # con RuntimeError. Il messaggio deve avere il prefisso di categoria
    # dell'errore Rust (es. "Io:", "Connect:").
    with pytest.raises(RuntimeError) as exc_info:
        plenora_database.engine_from_url(
            plenora_database.EngineConfig.from_postgres_dsn(
                "host=host-che-non-esiste.invalid user=x password=y dbname=z connect_timeout=2"
            )
        )
    msg = str(exc_info.value)
    # Il messaggio contiene almeno un ":" — il prefisso categoria.
    assert ":" in msg, f"atteso messaggio con prefisso categoria: {msg}"


def test_repr_contains_key_metadata() -> None:
    dsn = postgres_dsn_or_skip()
    with connect_postgres(dsn) as s:
        r = repr(s)
        assert "Session" in r
        assert "server_version" in r
        assert "closed=false" in r or "closed=False" in r
