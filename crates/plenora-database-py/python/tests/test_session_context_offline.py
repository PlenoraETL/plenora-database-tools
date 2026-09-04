"""Contratto offline del session context tipizzato e redatto."""

from __future__ import annotations

import pytest

import plenora_database as p


def test_session_context_preserves_types_classifications_and_order() -> None:
    context = p.SessionContext()

    assert context.is_empty()
    assert len(context) == 0
    assert context.get("app.missing") is None
    assert context.classification("app.missing") is None

    context.insert_sensitive("app.token", True)
    context.insert_public("app.actor", "worker")
    context.insert_internal("app.request_id", 42)

    assert not context.is_empty()
    assert len(context) == 3
    assert context.keys() == ["app.actor", "app.request_id", "app.token"]
    assert context.get("app.actor") == "worker"
    assert context.get("app.request_id") == 42
    assert context.get("app.token") is True
    assert context.classification("app.actor") == "public"
    assert context.classification("app.request_id") == "internal"
    assert context.classification("app.token") == "sensitive"
    assert repr(context) == "<SessionContext entries=3>"


@pytest.mark.parametrize("method", ["insert_public", "insert_internal", "insert_sensitive"])
def test_session_context_rejects_values_without_exposing_them(method: str) -> None:
    secret = "session-value-that-must-not-leak-731"
    context = p.SessionContext()

    with pytest.raises(TypeError) as error:
        getattr(context, method)("app.value", {"secret": secret})

    assert secret not in str(error.value)


def test_session_context_rejects_invalid_keys_and_unrepresentable_integers() -> None:
    context = p.SessionContext()

    with pytest.raises(ValueError):
        context.insert_public("INVALID KEY", "value")
    with pytest.raises(TypeError) as error:
        context.insert_public("app.counter", 1 << 100)
    assert str(1 << 100) not in str(error.value)
