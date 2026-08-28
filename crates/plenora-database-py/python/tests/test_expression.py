"""Expression language Core v3 e risultato uniforme, senza fixture live."""

from __future__ import annotations

import pytest

import plenora_database as p
from plenora_database.expression import (
    _compile_statement,
    _execute_statement,
    _execute_statement_async,
)
from plenora_database._native import compile_relational_query


def _joined_statement() -> p.SelectStatement:
    users = p.table("users", "id", "team_id", "name", schema="app").alias("u")
    teams = p.table("teams", "id", "name", schema="app").alias("t")
    return (
        p.select(users.c.id, users.c.name.label("user_name"), teams.c.name)
        .select_from(users)
        .join(teams, users.c.team_id == teams.c.id)
        .where((users.c.id >= p.bind("minimum")) & (users.c.id != p.bind("excluded")))
        .order_by(users.c.id.desc())
        .limit(10)
    )


def test_statement_is_immutable_and_keeps_values_out_of_the_ir() -> None:
    users = p.table("users", p.column("id"), p.column("name"))
    base = p.select(users.c.id)
    filtered = base.where(users.c.name == p.bind("wanted"))

    assert base.to_ast()["filter"] is None
    assert filtered.to_ast()["source"]["object"]["object"] == "users"
    assert "private-value-7f21" not in filtered.to_json()
    with pytest.raises(TypeError):
        base.order_by("id")
    with pytest.raises(TypeError):
        base.join(users, "invalid")


def test_alias_and_join_are_serialized_in_the_canonical_ir_shape() -> None:
    ast = _joined_statement().to_ast()

    assert ast["source"]["alias"] == "u"
    assert ast["joins"][0]["source"]["alias"] == "t"
    assert ast["joins"][0]["kind"] == "inner"
    assert ast["projection"][1]["alias"] == "user_name"


@pytest.mark.parametrize(
    ("provider", "placeholder", "quote"),
    (
        ("postgres", "$1", '"'),
        ("mysql", "?", "`"),
        ("mariadb", "?", "`"),
        ("sqlserver", "@p1", "["),
        ("db2", "?", '"'),
    ),
)
def test_canonical_compiler_covers_every_public_provider(
    provider: str, placeholder: str, quote: str
) -> None:
    sql, names = compile_relational_query(_joined_statement().to_json(), provider)

    assert placeholder in sql
    assert quote in sql
    assert "JOIN" in sql
    assert names == ["minimum", "excluded"]
    assert "minimum" not in sql
    assert "excluded" not in sql


def test_repeated_bind_name_preserves_positional_order() -> None:
    users = p.table("users", "id", "owner_id")
    statement = p.select(users.c.id).where(
        (users.c.id == p.bind("identity")) | (users.c.owner_id == p.bind("identity"))
    )

    sql, values = _compile_statement(statement, {"identity": 41}, "postgres")
    assert "$1" in sql and "$2" in sql
    assert values == [41, 41]


def test_bind_validation_is_exact_and_does_not_expose_values() -> None:
    statement = p.select(p.bind("expected").label("value"))
    secret = "private-value-9ca4"

    with pytest.raises(ValueError) as error:
        _compile_statement(statement, {"unexpected": secret}, "postgres")
    assert secret not in str(error.value)
    assert secret not in statement.to_json()


def test_result_cardinality_and_rows_are_defensive_copies() -> None:
    result = p.Result([{"value": 1}])
    first = result.one()
    first["value"] = 99

    assert result.keys() == ("value",)
    assert result.scalar_one() == 1
    assert result.all() == [{"value": 1}]
    assert p.Result([]).one_or_none() is None
    with pytest.raises(p.NoResultFound):
        p.Result([]).one()
    with pytest.raises(p.MultipleResultsFound):
        p.Result([{"value": 1}, {"value": 2}]).one_or_none()


def test_sync_execution_uses_compiled_sql_and_ordered_binds() -> None:
    class Native:
        def execute_returning_rows(self, sql, values):
            assert "$1" in sql
            assert values == [7]
            return [{"answer": values[0]}]

    statement = p.select(p.bind("answer").label("answer"))
    assert _execute_statement(Native(), statement, {"answer": 7}, "postgres").one() == {
        "answer": 7
    }


@pytest.mark.asyncio
async def test_async_execution_uses_the_same_compiler_and_result() -> None:
    class Native:
        async def execute_returning_rows(self, sql, values):
            assert "@p1" in sql
            assert values == [8]
            return [{"answer": values[0]}]

    statement = p.select(p.bind("answer").label("answer"))
    result = await _execute_statement_async(
        Native(), statement, {"answer": 8}, "sqlserver"
    )
    assert result.scalar_one() == 8
