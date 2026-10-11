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
    PlenoraCommitOutcomeUnknownError,
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


class _SdkAxes:
    """Gli assi di PYTHON-SDK §6 per un errore che solleva lo SDK Python.

    Le eccezioni native ricevono gli assi dal Rust; una classe definita in
    Python li dichiara qui, come attributi di classe, e ogni istanza li porta
    senza che chi la solleva debba ricordarsene. Va messa per prima fra le
    basi, davanti alla classe nativa della sua categoria.
    """

    _category = "internal"
    _phase = "validate"
    _remote_effect = "none"

    def __init__(self, message: str = "", *args: object, phase: str | None = None) -> None:
        super().__init__(message, *args)
        for name, value in (
            ("category", self._category),
            ("phase", phase or self._phase),
            ("remote_effect", self._remote_effect),
            ("retry", {"kind": "never"}),
            ("message", str(message)),
            ("provider", None),
            ("execution_id", None),
            ("details", None),
            ("diagnostics", None),
        ):
            setattr(self, name, value)

