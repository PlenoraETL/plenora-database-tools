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
    "db2": "db2",
}


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
    _raw_url: str | None = None

    def __post_init__(self) -> None:
        if self.provider not in set(_PROVIDERS.values()):
            raise ValueError("provider engine non supportato")
        if self.tls_mode not in {"require", "insecure_local"}:
            raise ValueError("tls_mode engine non valido")

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
                if name not in {"tls_mode", "tls_ca"}
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
            _raw_url=raw_url,
        )


def engine_from_url(value: str | EngineConfig) -> Any:
    """Crea l'Engine sync corretto senza stampare o ricomporre credenziali."""

    config = value if isinstance(value, EngineConfig) else EngineConfig.from_url(value)
    from . import (  # import lazy per evitare cicli nel package pubblico
        create_db2_engine,
        create_engine,
        create_mariadb_engine,
        create_mysql_engine,
        create_sqlserver_engine,
    )

    if config.provider == "postgres":
        if config._raw_url is None:
            raise ValueError("configurazione PostgreSQL priva di URL")
        return create_engine(config._raw_url, config.tls_mode)
    args = _network_arguments(config)
    if config.provider == "mysql":
        return create_mysql_engine(
            *args, tls_ca_pem=_ca_bytes(config), tls_mode=config.tls_mode
        )
    if config.provider == "mariadb":
        return create_mariadb_engine(
            *args, tls_ca_pem=_ca_bytes(config), tls_mode=config.tls_mode
        )
    if config.provider == "sqlserver":
        return create_sqlserver_engine(
            *args, tls_ca_pem=_ca_bytes(config), tls_mode=config.tls_mode
        )
    return create_db2_engine(*args, tls_ca_path=config.tls_ca, tls_mode=config.tls_mode)


async def async_engine_from_url(value: str | EngineConfig) -> Any:
    """Variante asyncio della factory provider-neutral."""

    config = value if isinstance(value, EngineConfig) else EngineConfig.from_url(value)
    from . import (
        create_async_db2_engine,
        create_async_engine,
        create_async_mariadb_engine,
        create_async_mysql_engine,
        create_async_sqlserver_engine,
    )

    if config.provider == "postgres":
        if config._raw_url is None:
            raise ValueError("configurazione PostgreSQL priva di URL")
        return await create_async_engine(config._raw_url, config.tls_mode)
    args = _network_arguments(config)
    if config.provider == "mysql":
        return await create_async_mysql_engine(
            *args, tls_ca_pem=_ca_bytes(config), tls_mode=config.tls_mode
        )
    if config.provider == "mariadb":
        return await create_async_mariadb_engine(
            *args, tls_ca_pem=_ca_bytes(config), tls_mode=config.tls_mode
        )
    if config.provider == "sqlserver":
        return await create_async_sqlserver_engine(
            *args, tls_ca_pem=_ca_bytes(config), tls_mode=config.tls_mode
        )
    return await create_async_db2_engine(
        *args, tls_ca_path=config.tls_ca, tls_mode=config.tls_mode
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


__all__ = ["EngineConfig", "async_engine_from_url", "engine_from_url"]
