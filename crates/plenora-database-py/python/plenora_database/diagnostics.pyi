from typing import Any

class ExplainPlan:
    provider: str
    rows: tuple[dict[str, Any], ...]
    analyzed: bool
    @property
    def estimated_rows(self) -> int | None: ...

class ProbeResult:
    name: str
    measured: bool
    passed: bool
    detail: str | None

class ProbeReport:
    provider: str
    checks: tuple[ProbeResult, ...]
    @property
    def healthy(self) -> bool: ...

def explain(session: Any, sql: str, params: list[Any] | None = None, *, analyze: bool = False) -> ExplainPlan: ...
async def explain_async(session: Any, sql: str, params: list[Any] | None = None, *, analyze: bool = False) -> ExplainPlan: ...
def probe_engine(engine: Any) -> ProbeReport: ...
