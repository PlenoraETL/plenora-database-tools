"""Repository PostgreSQL con Engine globale e unita di lavoro per request."""

from __future__ import annotations

import asyncio
import os

import plenora_database as db


class UserRepository:
    """Il repository riceve la transazione: non possiede connessioni globali."""

    @staticmethod
    def find(transaction: db.Transaction, user_id: int) -> db.Row | None:
        return (
            transaction.select("users")
            .where_eq("id", user_id)
            .one_or_none()
        )

    @staticmethod
    async def find_async(
        transaction: db.AsyncTransaction, user_id: int
    ) -> db.Row | None:
        return await (
            transaction.select("users")
            .where_eq("id", user_id)
            .one_or_none()
        )


def handle_request(engine: db.Engine, user_id: int) -> db.Row | None:
    """Ogni request apre una sessione e delimita una transazione."""
    with engine.session() as session:
        with session.begin(native_query_policy="deny") as transaction:
            return UserRepository.find(transaction, user_id)


async def handle_async_request(
    engine: db.AsyncEngine, user_id: int
) -> db.Row | None:
    """Stesso confine applicativo sulla superficie asyncio."""
    async with engine.session() as session:
        async with await session.begin(
            native_query_policy="deny"
        ) as transaction:
            return await UserRepository.find_async(transaction, user_id)


async def main() -> None:
    dsn = os.environ["PLENORA_DATABASE_DSN"]
    config = db.EngineConfig.from_postgres_dsn(dsn)
    with db.engine_from_url(config) as engine:
        async with await db.async_engine_from_url(config) as async_engine:
            print(handle_request(engine, 1))
            print(await handle_async_request(async_engine, 1))


if __name__ == "__main__":
    asyncio.run(main())
