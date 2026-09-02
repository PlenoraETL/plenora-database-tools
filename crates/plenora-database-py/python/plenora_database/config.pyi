from typing import Any

class EngineConfig:
    provider: str
    host: str | None
    database: str | None
    user: str | None
    password: str | None
    port: int | None
    tls_mode: str
    tls_ca: str | None
    def __post_init__(self) -> None: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_url(cls, value: str) -> EngineConfig: ...

def engine_from_url(value: str | EngineConfig) -> Any: ...
async def async_engine_from_url(value: str | EngineConfig) -> Any: ...
