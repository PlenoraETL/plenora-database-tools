"""Piani EXPLAIN tipizzati e probe applicative senza payload nei messaggi."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ExplainPlan:
    provider: str
    rows: tuple[dict[str, Any], ...]
    analyzed: bool

    @property
    def estimated_rows(self) -> int | None:
        for row in self.rows:
            for name in ("Plan Rows", "rows", "cardinality", "estimated_rows"):
                value = row.get(name)
                if isinstance(value, int) and not isinstance(value, bool):
                    return value
        return None


@dataclass(frozen=True, slots=True)
class ProbeResult:
    name: str
    measured: bool
    passed: bool
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class ProbeReport:
    provider: str
    checks: tuple[ProbeResult, ...]

    @property
    def healthy(self) -> bool:
        return all(item.passed for item in self.checks if item.measured)


def explain(
    session: Any,
    sql: str,
    params: list[Any] | None = None,
    *,
    analyze: bool = False,
) -> ExplainPlan:
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("EXPLAIN richiede SQL non vuoto")
    provider = _provider(session)
    statement = _explain_sql(provider, sql, analyze)
    execute = getattr(session, "query_sql", None)
    if not callable(execute):
        raise TypeError("sessione priva della superficie EXPLAIN")
    rows = execute(statement, params)
    if not hasattr(rows, "all"):
        raise RuntimeError("EXPLAIN non ha restituito righe strutturate")
    return ExplainPlan(provider, tuple(row.as_dict() for row in rows), analyze)


async def explain_async(
    session: Any,
    sql: str,
    params: list[Any] | None = None,
    *,
    analyze: bool = False,
) -> ExplainPlan:
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("EXPLAIN richiede SQL non vuoto")
    provider = _provider(session)
    rows = await session.query_sql(
        _explain_sql(provider, sql, analyze), params
    )
    if not hasattr(rows, "all"):
        raise RuntimeError("EXPLAIN non ha restituito righe strutturate")
    return ExplainPlan(provider, tuple(row.as_dict() for row in rows), analyze)


def probe_engine(engine: Any) -> ProbeReport:
    provider = getattr(engine, "provider_kind", None)
    if not isinstance(provider, str) or not provider:
        raise TypeError("engine privo di provider_kind")
    checks = [
        ProbeResult("engine-active", True, not bool(engine.is_disposed)),
    ]
    try:
        statistics = engine.statistics()
        checks.append(ProbeResult("statistics", True, isinstance(statistics, dict)))
    except Exception:  # noqa: BLE001 - il report classifica senza esporre l'errore
        checks.append(ProbeResult("statistics", True, False))
    return ProbeReport(provider, tuple(checks))


def _provider(session: Any) -> str:
    capabilities = getattr(session, "capabilities", None)
    provider = capabilities.get("provider") if isinstance(capabilities, dict) else None
    if provider not in {"postgres", "mysql", "mariadb", "sqlserver", "db2"}:
        raise ValueError("provider EXPLAIN non qualificato")
    return provider


def _explain_sql(provider: str, sql: str, analyze: bool) -> str:
    if provider in {"sqlserver", "db2"}:
        raise ValueError("EXPLAIN strutturato non qualificato per il provider")
    if provider == "postgres":
        return f"EXPLAIN (FORMAT JSON, ANALYZE {'TRUE' if analyze else 'FALSE'}) {sql}"
    if analyze:
        raise ValueError("EXPLAIN ANALYZE non qualificato per il provider")
    return f"EXPLAIN {sql}"


__all__ = [
    "ExplainPlan",
    "ProbeReport",
    "ProbeResult",
    "explain",
    "explain_async",
    "probe_engine",
]
