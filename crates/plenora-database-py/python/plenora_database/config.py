"""Configurazione tipizzata e factory provider-neutral degli Engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, parse_qsl, unquote, urlencode, urlsplit, urlunsplit

_PROVIDERS = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "age": "postgres",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "mssql": "sqlserver",
    "sqlserver": "sqlserver",
    "oracle": "oracle",
    "db2": "db2",
}


@dataclass(frozen=True, slots=True)
class PoolConfig:
    """Limiti di backpressure applicati dal provider prima della probe."""

    max_connections: int = 4
    acquire_timeout_ms: int = 10_000

    def __post_init__(self) -> None:
        if (
            not isinstance(self.max_connections, int)
            or isinstance(self.max_connections, bool)
            or self.max_connections < 1
        ):
            raise ValueError("max_connections deve essere un intero positivo")
        if (
            not isinstance(self.acquire_timeout_ms, int)
            or isinstance(self.acquire_timeout_ms, bool)
            or self.acquire_timeout_ms < 1
        ):
            raise ValueError("acquire_timeout_ms deve essere un intero positivo")


@dataclass(frozen=True, slots=True)
class EngineConfig:
    provider: str
    host: str | None = None
    database: str | None = None
    user: str | None = None
    password: str | None = None
    port: int | None = None
    tls_mode: str = "require"
    tls_ca: str | None = None
    pool: PoolConfig | None = None
    _raw_url: str | None = None

    def __post_init__(self) -> None:
        if self.provider not in set(_PROVIDERS.values()):
            raise ValueError("provider engine non supportato")
        if self.tls_mode not in {"require", "insecure_local"}:
            raise ValueError(
                "tls_mode engine non valido; valori: require, insecure_local"
            )
        if self.provider == "db2" and self.pool is not None:
            raise ValueError("pool configurabile non qualificato per Db2")

    def __repr__(self) -> str:
        return (
            "EngineConfig("
            f"provider={self.provider!r}, host={self.host!r}, "
            f"database={self.database!r}, port={self.port!r}, "
            f"tls_mode={self.tls_mode!r})"
        )

    @classmethod
    def from_url(cls, value: str) -> EngineConfig:
        if not isinstance(value, str) or not value:
            raise ValueError("URL database non valido")
        parsed = urlsplit(value)
        provider = _PROVIDERS.get(parsed.scheme.lower())
        if provider is None:
            raise ValueError("schema URL database non supportato")
        query = parse_qs(parsed.query, strict_parsing=False)
        tls_mode = query.get("tls_mode", ["require"])[-1]
        tls_ca = query.get("tls_ca", [None])[-1]
        try:
            has_pool = "max_connections" in query or "acquire_timeout_ms" in query
            pool = (
                PoolConfig(
                    max_connections=int(query.get("max_connections", ["4"])[-1]),
                    acquire_timeout_ms=int(
                        query.get("acquire_timeout_ms", ["10000"])[-1]
                    ),
                )
                if has_pool
                else None
            )
        except (TypeError, ValueError) as error:
            raise ValueError("configurazione pool URL non valida") from error
        database = unquote(parsed.path.lstrip("/")) or None
        host = parsed.hostname
        user = None if parsed.username is None else unquote(parsed.username)
        password = None if parsed.password is None else unquote(parsed.password)
        if provider != "postgres" and not all((host, database, user, password)):
            raise ValueError("URL database privo di credenziali o destinazione")
        retained_query = urlencode(
            [
                (name, item)
                for name, item in parse_qsl(parsed.query, keep_blank_values=True)
                if name
                not in {
                    "tls_mode",
                    "tls_ca",
                    "max_connections",
                    "acquire_timeout_ms",
                }
            ]
        )
        postgres_scheme = (
            "postgresql" if parsed.scheme.lower() == "age" else parsed.scheme
        )
        raw_url = urlunsplit(
            (
                postgres_scheme,
                parsed.netloc,
                parsed.path,
                retained_query,
                parsed.fragment,
            )
        )
        return cls(
            provider,
            host=host,
            database=database,
            user=user,
            password=password,
            port=parsed.port,
            tls_mode=tls_mode,
            tls_ca=tls_ca,
            pool=pool,
            _raw_url=raw_url,
        )

    @classmethod
    def from_postgres_dsn(
        cls,
        dsn: str,
        *,
        tls_mode: str = "require",
        pool: PoolConfig | None = None,
    ) -> EngineConfig:
        """Adatta una DSN libpq senza ricomporla o mostrarla."""
        if not isinstance(dsn, str) or not dsn:
            raise ValueError("DSN PostgreSQL non valida")
        return cls(
            "postgres", tls_mode=tls_mode, pool=pool, _raw_url=dsn
        )


def engine_from_url(value: str | EngineConfig) -> Any:
    """Crea l'Engine sync corretto senza stampare o ricomporre credenziali."""

    config = value if isinstance(value, EngineConfig) else EngineConfig.from_url(value)
    from . import (  # import lazy per evitare cicli nel package pubblico
        _create_db2_engine,
        _create_mariadb_engine,
        _create_mysql_engine,
        _create_oracle_engine,
        _create_postgres_engine,
        _create_sqlserver_engine,
    )

    if config.provider == "postgres":
        if config._raw_url is None:
            raise ValueError("configurazione PostgreSQL priva di URL")
        max_connections, acquire_timeout_ms = _pool_arguments(config)
        return _create_postgres_engine(
            config._raw_url,
            config.tls_mode,
            max_connections=max_connections,
            acquire_timeout_ms=acquire_timeout_ms,
        )
    args = _network_arguments(config)
    max_connections, acquire_timeout_ms = _pool_arguments(config)
    if config.provider == "mysql":
        return _create_mysql_engine(
            *args,
            tls_ca_pem=_ca_bytes(config),
            tls_mode="require" if config.tls_mode == "require" else "insecure_trust_server",
            max_connections=max_connections,
            acquire_timeout_ms=acquire_timeout_ms,
        )
    if config.provider == "mariadb":
        return _create_mariadb_engine(
            *args,
            tls_ca_pem=_ca_bytes(config),
            tls_mode="require" if config.tls_mode == "require" else "insecure_trust_server",
            max_connections=max_connections,
            acquire_timeout_ms=acquire_timeout_ms,
        )
    if config.provider == "sqlserver":
        return _create_sqlserver_engine(
            *args,
            tls_ca_pem=_ca_bytes(config),
            tls_mode="require" if config.tls_mode == "require" else "insecure_trust_server",
            max_connections=max_connections,
            acquire_timeout_ms=acquire_timeout_ms,
        )
    if config.provider == "oracle":
        return _create_oracle_engine(
            *args,
            tls_ca_path=config.tls_ca,
            tls_mode="require" if config.tls_mode == "require" else "disable",
            max_connections=max_connections,
            acquire_timeout_ms=acquire_timeout_ms,
        )
    return _create_db2_engine(
        *args,
        tls_ca_path=config.tls_ca,
        tls_mode="require" if config.tls_mode == "require" else "disable",
    )


async def async_engine_from_url(value: str | EngineConfig) -> Any:
    """Variante asyncio della factory provider-neutral."""

    config = value if isinstance(value, EngineConfig) else EngineConfig.from_url(value)
    from . import (
        _create_async_db2_engine,
        _create_async_mariadb_engine,
        _create_async_mysql_engine,
        _create_async_oracle_engine,
        _create_async_postgres_engine,
        _create_async_sqlserver_engine,
    )

    if config.provider == "postgres":
        if config._raw_url is None:
            raise ValueError("configurazione PostgreSQL priva di URL")
        max_connections, acquire_timeout_ms = _pool_arguments(config)
        return await _create_async_postgres_engine(
            config._raw_url,
            config.tls_mode,
            max_connections=max_connections,
            acquire_timeout_ms=acquire_timeout_ms,
        )
    args = _network_arguments(config)
    max_connections, acquire_timeout_ms = _pool_arguments(config)
    if config.provider == "mysql":
        return await _create_async_mysql_engine(
            *args,
            tls_ca_pem=_ca_bytes(config),
            tls_mode="require" if config.tls_mode == "require" else "insecure_trust_server",
            max_connections=max_connections,
            acquire_timeout_ms=acquire_timeout_ms,
        )
    if config.provider == "mariadb":
        return await _create_async_mariadb_engine(
            *args,
            tls_ca_pem=_ca_bytes(config),
            tls_mode="require" if config.tls_mode == "require" else "insecure_trust_server",
            max_connections=max_connections,
            acquire_timeout_ms=acquire_timeout_ms,
        )
    if config.provider == "sqlserver":
        return await _create_async_sqlserver_engine(
            *args,
            tls_ca_pem=_ca_bytes(config),
            tls_mode="require" if config.tls_mode == "require" else "insecure_trust_server",
            max_connections=max_connections,
            acquire_timeout_ms=acquire_timeout_ms,
        )
    if config.provider == "oracle":
        return await _create_async_oracle_engine(
            *args,
            tls_ca_path=config.tls_ca,
            tls_mode="require" if config.tls_mode == "require" else "disable",
            max_connections=max_connections,
            acquire_timeout_ms=acquire_timeout_ms,
        )
    return await _create_async_db2_engine(
        *args,
        tls_ca_path=config.tls_ca,
        tls_mode="require" if config.tls_mode == "require" else "disable",
    )


def _network_arguments(config: EngineConfig) -> tuple[str, str, str, str, int | None]:
    if not all((config.host, config.database, config.user, config.password)):
        raise ValueError("configurazione database incompleta")
    return (
        config.host or "",
        config.database or "",
        config.user or "",
        config.password or "",
        config.port,
    )


def _ca_bytes(config: EngineConfig) -> bytes | None:
    return None if config.tls_ca is None else config.tls_ca.encode()


def _pool_arguments(config: EngineConfig) -> tuple[int, int]:
    pool = config.pool or PoolConfig()
    return pool.max_connections, pool.acquire_timeout_ms


__all__ = ["EngineConfig", "PoolConfig", "async_engine_from_url", "engine_from_url"]
