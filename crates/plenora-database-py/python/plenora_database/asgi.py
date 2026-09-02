"""Integrazione ASGI/FastAPI senza dipendenza obbligatoria dal framework."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

ASGIApp = Callable[
    [dict[str, Any], Callable[[], Awaitable[dict]], Callable[[dict], Awaitable[None]]],
    Awaitable[None],
]


class DatabaseASGIMiddleware:
    """Espone una sessione per request in ``scope['state'][state_key]``."""

    def __init__(self, app: ASGIApp, engine: Any, *, state_key: str = "database"):
        if not callable(app) or not isinstance(state_key, str) or not state_key:
            raise TypeError("middleware database richiede app e state_key validi")
        self.app = app
        self.engine = engine
        self.state_key = state_key

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") == "lifespan":
            try:
                await self.app(scope, receive, send)
            finally:
                self.engine.dispose()
            return
        if scope.get("type") not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return
        state = scope.setdefault("state", {})
        if self.state_key in state:
            raise RuntimeError("scope ASGI contiene gia la sessione database")
        session = self.engine.session()
        enter = getattr(session, "__aenter__", None)
        exit_ = getattr(session, "__aexit__", None)
        if not callable(enter) or not callable(exit_):
            raise TypeError("ASGI richiede un AsyncEngine")
        entered = await enter()
        state[self.state_key] = entered
        try:
            await self.app(scope, receive, send)
        except BaseException as error:
            await exit_(type(error), error, error.__traceback__)
            raise
        else:
            await exit_(None, None, None)
        finally:
            state.pop(self.state_key, None)

def session_dependency(engine: Any) -> Callable[[], AsyncIterator[Any]]:
    """Crea una dependency FastAPI compatibile, senza importare FastAPI."""

    async def dependency() -> AsyncIterator[Any]:
        session = engine.session()
        async with session as entered:
            yield entered

    return dependency


__all__ = ["DatabaseASGIMiddleware", "session_dependency"]
