"""Consumer statico: deve compilare con mypy --strict su ogni Python supportato."""

from __future__ import annotations

from typing import Any

from typing_extensions import assert_type

import plenora_database as p


def consume_result(result: p.Result) -> None:
    rows = result.all()
    assert_type(rows, list[p.Row])
    first = result.first()
    assert_type(first, p.Row | None)
    if first is not None:
        assert_type(first.as_dict(), dict[str, Any])


def build_statement() -> p.SelectStatement:
    users = p.table("users", "id", "name")
    return p.select(users.c.id, users.c.name).where(
        users.c.id == p.bind("identity", p.BindType.INTEGER)
    )


def configure() -> p.EngineConfig:
    pool = p.PoolConfig(max_connections=8, acquire_timeout_ms=5_000)
    return p.EngineConfig.from_url(
        "postgresql://user:password@localhost/application"
        f"?max_connections={pool.max_connections}"
        f"&acquire_timeout_ms={pool.acquire_timeout_ms}"
    )
