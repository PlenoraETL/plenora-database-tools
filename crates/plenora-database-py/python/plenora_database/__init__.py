"""Python SDK per plenora-database-tools.

F3-1 skeleton: espone solo `version()`. Le API di primo livello (Session,
transazioni, portable AST, spatial) sono nelle milestone F3-2..F3-8.
"""

from ._native import version

__all__ = ["version"]
