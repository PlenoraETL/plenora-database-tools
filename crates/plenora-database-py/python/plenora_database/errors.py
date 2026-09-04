"""Gerarchia di eccezioni pubbliche esposte dall'SDK.

Tutte discendono da `PlenoraError` che a sua volta discende da
`RuntimeError`. Consumer che filtravano su `RuntimeError` continuano
a intercettare tutti gli errori del SDK.

Ogni istanza porta attributi ispezionabili:
  - `category` (str, snake_case: "schema", "not_found", ...)
  - `phase` (str: "read", "write", "commit", ...)
  - `retry` (dict con `kind` ed eventuale `delay_ms`)
  - `remote_effect` (str: "none", "rolled_back", "partial",
                     "committed", "unknown")
  - `provider` (str: "postgres" / "mysql" / "sqlserver" / None)
  - `execution_id` (str o None)
  - `message` (str redatta e bounded)
  - `details` (dict con eventuale `row_diagnostics`, oppure None)
  - `diagnostics` (alias delle diagnostiche di riga, oppure None)
  - `parameter_index`, `portable_type`, `target_type` (diagnostica bind
    sanificata, oppure None)

Uso tipico:

    from plenora_database import PlenoraSchemaError, PlenoraNotFoundError

    try:
        s.select("t").where_eq("id", 1).one()
    except PlenoraNotFoundError as e:
        print("chiave assente:", e.category, e.phase)
    except PlenoraSchemaError as e:
        print("schema out-of-sync:", e)
"""

from ._native import (
    PlenoraAuthenticationError,
    PlenoraAuthorizationError,
    PlenoraCancelledError,
    PlenoraCommitOutcomeUnknownError,  # PFM CHG-004
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

__all__ = [
    "PlenoraError",
    "PlenoraInvalidPlanError",
    "PlenoraInvalidConfigurationError",
    "PlenoraSchemaError",
    "PlenoraDataMappingError",
    "PlenoraCrsError",
    "PlenoraUnsupportedError",
    "PlenoraNotFoundError",
    "PlenoraConflictError",
    "PlenoraConcurrentModificationError",
    "PlenoraAuthenticationError",
    "PlenoraAuthorizationError",
    "PlenoraTimeoutError",
    "PlenoraCancelledError",
    "PlenoraResourceLimitError",
    "PlenoraIoError",
    "PlenoraProtocolError",
    "PlenoraTransientError",
    "PlenoraExecutionError",
    "PlenoraInternalError",
    "PlenoraCommitOutcomeUnknownError",
]
