"""Contratto offline del checkpoint keyset persistente."""

from __future__ import annotations

import json

import plenora_database as p
import pytest


def test_read_checkpoint_round_trips_with_a_qualified_composite_scope() -> None:
    checkpoint = p.ReadCheckpoint(
        "db2",
        "app",
        "events",
        [("tenant_id", "asc"), ("event_id", "desc")],
        [7, 42],
        ["tenant_id", "event_id"],
        catalog="warehouse",
    )

    document = checkpoint.to_json()
    decoded = json.loads(document)
    restored = p.ReadCheckpoint.from_json(document)

    assert decoded["schema_version"] == 2
    assert decoded["scope_fingerprint"].startswith("sha256:")
    assert restored.provider == "db2"
    assert restored.catalog == "warehouse"
    assert restored.schema == "app"
    assert restored.object == "events"
    assert restored.order_by == [("tenant_id", "asc"), ("event_id", "desc")]
    assert restored.to_json() == document


def test_read_checkpoint_public_errors_and_repr_do_not_expose_values() -> None:
    secret = "private-checkpoint-value-91"
    checkpoint = p.ReadCheckpoint(
        "postgres",
        "app",
        "events",
        [("event_id", "asc")],
        [secret],
    )
    assert secret not in repr(checkpoint)

    with pytest.raises(p.PlenoraInvalidPlanError) as error:
        p.ReadCheckpoint(
            "postgres",
            "app",
            "events",
            [("tenant_id", "asc"), ("event_id", "asc")],
            [secret],
        )
    assert secret not in str(error.value)


def test_read_checkpoint_rejects_unknown_provider_and_null_key() -> None:
    with pytest.raises(ValueError):
        p.ReadCheckpoint("oracle", "app", "events", [("id", "asc")], [1])
    with pytest.raises(p.PlenoraInvalidPlanError):
        p.ReadCheckpoint("db2", "app", "events", [("id", "asc")], [None])
