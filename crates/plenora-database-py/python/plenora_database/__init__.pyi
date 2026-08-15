"""Type stubs top-level per plenora_database."""
from typing import Any

from . import spatial as spatial
from ._async_session import AsyncSession
from ._async_transaction import AsyncTransaction
from ._native import (
    PlenoraAuthenticationError,
    PlenoraAuthorizationError,
    PlenoraCancelledError,
    PlenoraConcurrentModificationError,
    PlenoraConflictError,
    PlenoraCrsError,
    PlenoraDataMappingError,
    PlenoraError,
    PlenoraExecutionError,
    PlenoraInternalError,
    PlenoraInvalidConfigurationError,
    PlenoraInvalidPlanError,
    PlenoraIoError,
    PlenoraNotFoundError,
    PlenoraProtocolError,
    PlenoraResourceLimitError,
    PlenoraSchemaError,
    PlenoraTimeoutError,
    PlenoraTransientError,
    PlenoraUnsupportedError,
)
from ._session import Session
from ._transaction import Transaction
from .async_query import (
    AsyncDelete,
    AsyncInsert,
    AsyncSelect,
    AsyncUpdate,
    AsyncUpsert,
)
from .query import Delete, Insert, Select, Update, Upsert
from .spatial import SpatialReference
from .types import (
    TypedValue,
    date,
    decimal,
    null,
    timestamp,
    timestamptz,
    uuid,
)


def version() -> str: ...
def connect(dsn: str, tls_mode: str = "require") -> Session: ...
async def aconnect(dsn: str, tls_mode: str = "require") -> AsyncSession: ...


__all__: list[str]
