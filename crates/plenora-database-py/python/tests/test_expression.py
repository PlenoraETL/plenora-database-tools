"""Expression language Core v3 e risultato uniforme, senza fixture live."""

from __future__ import annotations

import pytest

import plenora_database as p
from plenora_database.expression import (
    _compile_mutation_statement,
    _compile_statement,
    _execute_statement,
    _execute_statement_async,
)
from plenora_database._native import compile_relational_mutation, compile_relational_query


def _joined_statement() -> p.SelectStatement:
    users = p.table("users", "id", "team_id", "name", schema="app").alias("u")
    teams = p.table("teams", "id", "name", schema="app").alias("t")
    return (
        p.select(users.c.id, users.c.name.label("user_name"), teams.c.name)
        .select_from(users)
        .join(teams, users.c.team_id == teams.c.id)
        .where(
            (users.c.id >= p.bind("minimum", p.BindType.INTEGER))
            & (users.c.id != p.bind("excluded", p.BindType.INTEGER))
        )
        .order_by(users.c.id.desc())
        .limit(10)
    )


def test_statement_is_immutable_and_keeps_values_out_of_the_ir() -> None:
    users = p.table("users", p.column("id"), p.column("name"))
    base = p.select(users.c.id)
    filtered = base.where(users.c.name == p.bind("wanted", p.BindType.STRING))

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
        (users.c.id == p.bind("identity", p.BindType.INTEGER))
        | (users.c.owner_id == p.bind("identity", p.BindType.INTEGER))
    )

    sql, values = _compile_statement(statement, {"identity": 41}, "postgres")
    assert "$1" in sql and "$2" in sql
    assert values == [41, 41]


def test_bind_validation_is_exact_and_does_not_expose_values() -> None:
    statement = p.select(p.bind("expected", p.BindType.STRING).label("value"))
    secret = "private-value-9ca4"

    with pytest.raises(ValueError) as error:
        _compile_statement(statement, {"unexpected": secret}, "postgres")
    assert secret not in str(error.value)
    assert secret not in statement.to_json()


def test_bind_type_is_mandatory_and_the_ast_is_explicit() -> None:
    with pytest.raises(TypeError):
        p.bind("missing")  # type: ignore[call-arg]
    typed = p.bind("answer", p.BindType.INTEGER)
    assert typed._ast() == {
        "kind": "typed_parameter",
        "name": "answer",
        "parameter_type": "integer",
    }
    sql, names = compile_relational_query(p.select(typed).to_json(), "db2")
    assert sql == "SELECT CAST(? AS INTEGER) FROM SYSIBM.SYSDUMMY1"
    assert names == ["answer"]
    with pytest.raises(TypeError):
        p.bind("unsafe", "INTEGER")


@pytest.mark.parametrize("provider", ("postgres", "mysql", "mariadb", "sqlserver", "db2"))
def test_functions_grouping_having_and_predicates_use_the_canonical_ir(
    provider: str,
) -> None:
    sales = p.table("sales", "category", "amount", "name")
    total = p.func.sum(sales.c.amount)
    statement = (
        p.select(sales.c.category, total.label("total"))
        .where(
            sales.c.name.ilike(p.bind("pattern", p.BindType.STRING))
            & sales.c.amount.between(
                p.bind("lower", p.BindType.INTEGER),
                p.bind("upper", p.BindType.INTEGER),
            )
            & sales.c.category.in_(
                p.bind("first", p.BindType.STRING),
                p.bind("second", p.BindType.STRING),
            )
            & sales.c.name.is_not_null()
        )
        .group_by(sales.c.category)
        .having(total > p.bind("minimum", p.BindType.INTEGER))
    )

    sql, names = compile_relational_query(statement.to_json(), provider)

    assert "SUM(" in sql
    assert "GROUP BY" in sql
    assert "HAVING" in sql
    assert "BETWEEN" in sql
    assert "IN (" in sql
    assert "IS NOT NULL" in sql
    assert names == ["pattern", "lower", "upper", "first", "second", "minimum"]


@pytest.mark.parametrize("provider", ("postgres", "mysql", "mariadb", "sqlserver", "db2"))
def test_windows_subqueries_ctes_and_set_operations_compile_for_every_provider(
    provider: str,
) -> None:
    events = p.table("events", "tenant", "sequence")
    ranked = p.select(
        events.c.tenant,
        p.func.sum(events.c.sequence)
        .over(
            partition_by=(events.c.tenant,),
            order_by=(events.c.sequence,),
            rows=(None, 0),
        )
        .label("running_total"),
    ).subquery("ranked")
    aggregate = (
        p.select(ranked.c.tenant.label("tenant"))
        .select_from(ranked)
        .where(ranked.c.running_total > p.bind("minimum", p.BindType.INTEGER))
    )
    cte = aggregate.cte("qualified")
    alternative = p.select(events.c.tenant.label("tenant")).where(
        events.c.sequence == p.bind("exact", p.BindType.INTEGER)
    )
    statement = (
        p.select(cte.c.tenant)
        .select_from(cte)
        .where(alternative.exists())
        .union(alternative, all=True)
    )

    sql, names = compile_relational_query(statement.to_json(), provider)

    assert sql.startswith("WITH ")
    assert "OVER (" in sql
    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in sql
    assert "EXISTS (" in sql
    assert "UNION ALL" in sql
    assert names == ["minimum", "exact", "exact"]


def test_advanced_expression_validation_fails_before_native_compilation() -> None:
    users = p.table("users", "id", "name")

    with pytest.raises(TypeError):
        users.c.id.in_()
    with pytest.raises(TypeError):
        p.func.lower("name")
    with pytest.raises(ValueError):
        p.func.sum(users.c.id).over(rows=(None, 0), range_=(None, 0))
    with pytest.raises(ValueError):
        p.select(p.func.count()).subquery("counts")
    with pytest.raises(ValueError):
        p.select(users.c.id, users.c.name).scalar_subquery()
    invalid_window = p.select(
        p.func.lower(p.bind("value", p.BindType.STRING)).over()
    )
    with pytest.raises(p.PlenoraInvalidPlanError):
        compile_relational_query(invalid_window.to_json(), "postgres")


def test_canonical_mutations_keep_values_outside_the_ir_and_lower_per_provider() -> None:
    users = p.table("users", "id", "name", schema="app")
    update = p.update(users).values(name=p.bind("name", p.BindType.STRING)).where(
        users.c.id == p.bind("identity", p.BindType.INTEGER)
    )
    secret = "private-name-54e2"

    for provider in ("postgres", "mysql", "mariadb", "sqlserver", "db2"):
        sql, names, returns_rows = compile_relational_mutation(
            update.to_json(), provider
        )
        assert names == ["name", "identity"]
        assert returns_rows is False
        assert secret not in sql

    sql, values, returns_rows = _compile_mutation_statement(
        update, {"name": secret, "identity": 7}, "postgres"
    )
    assert "$1" in sql and "$2" in sql
    assert values == [secret, 7]
    assert returns_rows is False
    assert secret not in update.to_json()

    returning = p.insert(users).values(
        id=p.bind("identity", p.BindType.INTEGER),
        name=p.bind("name", p.BindType.STRING),
    ).returning(users.c.id)
    for provider in ("postgres", "mariadb", "sqlserver"):
        _, names, returns_rows = compile_relational_mutation(
            returning.to_json(), provider
        )
        assert names == ["identity", "name"]
        assert returns_rows is True
    for provider in ("mysql", "db2"):
        with pytest.raises(p.PlenoraError):
            compile_relational_mutation(returning.to_json(), provider)

    upsert = (
        p.upsert(users)
        .values(
            id=p.bind("identity", p.BindType.INTEGER),
            name=p.bind("insert_name", p.BindType.STRING),
        )
        .on_conflict(users.c.id)
        .set(name=p.bind("updated_name", p.BindType.STRING))
    )
    for provider in ("postgres", "mysql", "mariadb", "sqlserver", "db2"):
        sql, names, returns_rows = compile_relational_mutation(
            upsert.to_json(), provider
        )
        assert names == ["identity", "insert_name", "updated_name"]
        assert returns_rows is False
        assert secret not in sql

    with pytest.raises(p.PlenoraError):
        compile_relational_mutation(
            p.upsert(users)
            .values(
                id=p.bind("identity", p.BindType.INTEGER),
                name=p.bind("name", p.BindType.STRING),
            )
            .to_json(),
            "postgres",
        )


def test_result_cardinality_and_rows_are_immutable() -> None:
    users = p.table("users", "value")
    result = p.Result([{"value": 1}])
    first = result.one()
    with pytest.raises(TypeError):
        first["value"] = 99  # type: ignore[index]

    assert result.keys() == ("value",)
    assert result.scalar_one() == 1
    assert [row.as_dict() for row in result.all()] == [{"value": 1}]
    row = result.one()
    assert row[0] == row["value"] == row[users.c.value] == 1
    assert tuple(row) == (1,)
    assert row.as_dict() == {"value": 1}
    assert result.tuples() == [(1,)]
    assert p.Result([]).one_or_none() is None
    with pytest.raises(p.NoResultFound):
        p.Result([]).one()
    with pytest.raises(p.MultipleResultsFound):
        p.Result([{"value": 1}, {"value": 2}]).one_or_none()
    unknown = p.MutationResult("upsert", "sqlserver", None)
    assert unknown.count_is_known is False
    assert "unknown" in repr(unknown)
    with pytest.raises(TypeError):
        bool(unknown)


def test_sync_execution_uses_compiled_sql_and_ordered_binds() -> None:
    class Native:
        def execute_returning_rows(self, sql, values):
            assert "$1" in sql
            assert values == [7]
            return [{"answer": values[0]}]

    statement = p.select(p.bind("answer", p.BindType.INTEGER).label("answer"))
    assert _execute_statement(
        Native(), statement, {"answer": 7}, "postgres"
    ).one().as_dict() == {"answer": 7}


@pytest.mark.asyncio
async def test_async_execution_uses_the_same_compiler_and_result() -> None:
    class Native:
        async def execute_returning_rows(self, sql, values):
            assert "@p1" in sql
            assert values == [8]
            return [{"answer": values[0]}]

    statement = p.select(p.bind("answer", p.BindType.INTEGER).label("answer"))
    result = await _execute_statement_async(
        Native(), statement, {"answer": 8}, "sqlserver"
    )
    assert result.scalar_one() == 8
