"""Expression language Core relazionale e risultato uniforme, senza fixture live."""

from __future__ import annotations

import json

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
        ("oracle", ":1", '"'),
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


@pytest.mark.parametrize(
    "provider", ("postgres", "mysql", "mariadb", "sqlserver", "oracle", "db2")
)
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


@pytest.mark.parametrize(
    "provider", ("postgres", "mysql", "mariadb", "sqlserver", "oracle", "db2")
)
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


def test_python_builder_exposes_distinct_on_lateral_and_locking() -> None:
    events = p.table("events", "id").alias("e")
    details = p.table("details", "event_id").alias("d")
    latest = (
        p.select(details.c.event_id)
        .where(details.c.event_id == events.c.id)
        .limit(1)
        .subquery("latest")
    )
    distinct_statement = (
        p.select(events.c.id)
        .select_from(events)
        .join(latest, kind="cross", lateral=True)
        .distinct_on(events.c.id)
        .order_by(events.c.id)
    )
    locking_statement = (
        p.select(events.c.id)
        .select_from(events)
        .with_for_update(events, strength="share", skip_locked=True)
    )

    sql, names = compile_relational_query(distinct_statement.to_json(), "postgres")
    assert "DISTINCT ON (\"e\".\"id\")" in sql
    assert "CROSS JOIN LATERAL" in sql
    assert names == []
    locking_sql, locking_names = compile_relational_query(
        locking_statement.to_json(), "postgres"
    )
    assert locking_sql.endswith("FOR SHARE OF \"e\" SKIP LOCKED")
    assert locking_names == []

    with pytest.raises(ValueError):
        p.select(events.c.id).with_for_update(nowait=True, skip_locked=True)
    with pytest.raises(ValueError):
        p.select(events.c.id).join(events, kind="cross", lateral=True)


def test_portable_arithmetic_cast_case_and_null_ordering() -> None:
    invoices = p.table("invoices", "id", "amount", "discount")
    total = (
        (invoices.c.amount - invoices.c.discount)
        * p.bind("multiplier", p.BindType.DECIMAL)
    ).cast(p.BindType.DECIMAL)
    band = p.case(
        (
            invoices.c.amount > p.bind("threshold", p.BindType.DECIMAL),
            p.bind("high", p.BindType.STRING),
        ),
        else_=p.bind("low", p.BindType.STRING),
    )
    statement = (
        p.select(total.label("total"), band.label("band"))
        .select_from(invoices)
        .order_by(invoices.c.discount.asc().nulls_last())
    )

    postgres, names = compile_relational_query(statement.to_json(), "postgres")
    assert 'CAST((("invoices"."amount" - "invoices"."discount") * ' in postgres
    assert "CASE WHEN" in postgres
    assert '"invoices"."discount" ASC NULLS LAST' in postgres
    assert names == ["multiplier", "threshold", "high", "low"]

    sqlserver, _ = compile_relational_query(statement.to_json(), "sqlserver")
    assert "CASE WHEN [invoices].[discount] IS NULL THEN 1 ELSE 0 END ASC" in sqlserver

    with pytest.raises(TypeError):
        _ = invoices.c.amount + 1
    with pytest.raises(ValueError):
        p.case()


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

    for provider in ("postgres", "mysql", "mariadb", "sqlserver", "oracle", "db2"):
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
    for provider in ("mysql", "oracle", "db2"):
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
    for provider in ("postgres", "mysql", "mariadb", "sqlserver", "oracle", "db2"):
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
        def execute_returning_rows(self, sql, values, *, relational=None):
            assert "$1" in sql
            assert values == [7]
            # La regola S: l'IR accompagna lo SQL fino alla transazione, che
            # lo giudica sul catalogo del provider prima di eseguire.
            assert relational == ("query", statement.to_json())
            return [{"answer": values[0]}]

    statement = p.select(p.bind("answer", p.BindType.INTEGER).label("answer"))
    assert _execute_statement(
        Native(), statement, {"answer": 7}, "postgres"
    ).one().as_dict() == {"answer": 7}


def test_every_execution_passes_the_plan_to_the_transaction() -> None:
    """Regola S: nessuna esecuzione dello SDK manda lo SQL senza il piano.

    Su d11a957 `_execute_statement` chiamava `execute_returning_rows(sql,
    values)` ed `execute(sql, values)`: la transazione non riceveva l'IR, e
    una tabella non tipizzata o una colonna non mappata non si giudicavano.
    """
    calls: list[tuple[str, object]] = []

    class Native:
        def execute_returning_rows(self, sql, values, *, relational=None):
            calls.append(("rows", relational))
            return []

        def execute(self, sql, values, *, relational=None):
            calls.append(("count", relational))
            return 1

    items = p.table("items", "id", "g", "h", schema="public")
    query = p.select(items.c.id).order_by(items.c.g)
    update = p.update(items).values(id=p.bind("v", p.BindType.INTEGER)).where(
        items.c.id == p.bind("k", p.BindType.INTEGER)
    )
    _execute_statement(Native(), query, None, "postgres")
    _execute_statement(Native(), update, {"v": 1, "k": 2}, "postgres")
    assert calls == [
        ("rows", ("query", query.to_json())),
        ("count", ("mutation", update.to_json())),
    ]


@pytest.mark.asyncio
async def test_async_execution_uses_the_same_compiler_and_result() -> None:
    class Native:
        async def execute_returning_rows(self, sql, values, *, relational=None):
            assert "@p1" in sql
            assert values == [8]
            assert relational == ("query", statement.to_json())
            return [{"answer": values[0]}]

    statement = p.select(p.bind("answer", p.BindType.INTEGER).label("answer"))
    result = await _execute_statement_async(
        Native(), statement, {"answer": 8}, "sqlserver"
    )
    assert result.scalar_one() == 8


def test_an_unknown_key_in_a_window_frame_bound_is_refused() -> None:
    """Una chiave estranea in un limite di frame non viene ignorata.

    Il JSON dell'IR attraversa il confine nativo: una chiave sconosciuta
    accettata li cambierebbe la finestra senza errore.
    """
    events = p.table("events", "tenant", "sequence")
    statement = p.select(
        p.func.sum(events.c.sequence)
        .over(order_by=(events.c.sequence,), rows=(-1, 0))
        .label("running_total"),
    )
    document = json.loads(statement.to_json())
    compile_relational_query(json.dumps(document), "postgres")

    bounds: list[dict] = []

    def collect(node: object) -> None:
        if isinstance(node, dict):
            if node.get("kind") == "preceding":
                bounds.append(node)
            for child in node.values():
                collect(child)
        elif isinstance(node, list):
            for child in node:
                collect(child)

    collect(document)
    assert len(bounds) == 1
    bounds[0]["negate"] = True
    with pytest.raises(ValueError, match="IR relazionale non valido"):
        compile_relational_query(json.dumps(document), "postgres")


def test_a_probe_under_a_scalar_comparison_is_refused_by_the_compiler() -> None:
    """La regola P allo SDK: `g = ST_Centroid(sonda)` non si compila.

    `=` non confronta gli SRID e `ST_Centroid` ha una sola geometria: fino
    al 25/10 nessuno giudicava la sonda, e su una colonna 4326 una sonda 3857
    rendeva zero righe senza errore. Ora il compilatore la rifiuta su ogni
    dialetto, come `SpatialValue` e come bind nudo; la stessa sonda dentro
    `ST_Intersects` resta ammessa su PostgreSQL, con la guardia sul typmod.
    """
    from plenora_database.expression import (
        _spatial_function,
        _spatial_predicate,
        _spatial_value,
    )

    features = p.table("features", "id", "g", schema="public")

    def declared() -> p.Expression:
        return _spatial_value(p.bind("sonda", p.BindType.BINARY), 3857, "geometry")

    for probe in (declared(), p.bind("sonda", p.BindType.BINARY)):
        statement = p.select(features.c.id).where(
            features.c.g == _spatial_function("centroid", probe)
        )
        for provider in ("postgres", "mysql", "mariadb", "sqlserver", "oracle", "db2"):
            with pytest.raises(p.PlenoraUnsupportedError):
                compile_relational_query(statement.to_json(), provider)
    judged = p.select(features.c.id).where(
        _spatial_predicate(
            "intersects", features.c.g, _spatial_function("centroid", declared())
        )
    )
    sql, _ = compile_relational_query(judged.to_json(), "postgres")
    assert "postgis_typmod_srid" in sql


def test_a_probe_outside_a_spatial_predicate_of_its_block_is_refused() -> None:
    """La regola P strutturale allo SDK: le forme P1 e P2 non si compilano.

    Una sonda nella projection di una subquery `IN` o di una sorgente
    derivata nominata per alias non la confronta nessun nodo spatial: su
    d30d1d2 si compilavano su ogni dialetto, e su una colonna 4326 una sonda
    3857 rendeva zero righe senza errore. Anche la projection e l'`ORDER BY`
    della query esterna si rifiutano; la stessa sonda in un predicato spatial
    del blocco resta ammessa.
    """
    from plenora_database.expression import _spatial_function, _spatial_predicate, _spatial_value

    features = p.table("features", "id", "g", schema="public").alias("t")

    def declared() -> p.Expression:
        return _spatial_value(p.bind("sonda", p.BindType.BINARY), 3857, "geometry")

    projected = p.select(declared().label("s"))
    derived = projected.subquery("d")
    statements = {
        "g IN (SELECT sonda)": p.select(features.c.id).where(
            features.c.g.in_subquery(projected)
        ),
        "JOIN (SELECT sonda AS s) d ON t.g = d.s": p.select(features.c.id)
        .select_from(features)
        .join(derived, features.c.g == derived.c.s),
        "JOIN (SELECT sonda AS s) d ON ST_Intersects(t.g, d.s)": p.select(features.c.id)
        .select_from(features)
        .join(derived, _spatial_predicate("intersects", features.c.g, derived.c.s)),
        "SELECT ST_Distance(t.g, sonda)": p.select(
            features.c.id,
            _spatial_function("distance", features.c.g, declared()).label("m"),
        ).select_from(features),
    }
    for name, statement in statements.items():
        for provider in ("postgres", "mysql", "mariadb", "sqlserver", "oracle", "db2"):
            with pytest.raises(p.PlenoraUnsupportedError):
                compile_relational_query(statement.to_json(), provider)
                pytest.fail(f"{name} {provider}: compilato")
    judged = (
        p.select(features.c.id)
        .select_from(features)
        .where(_spatial_predicate("intersects", features.c.g, declared()))
    )
    sql, _ = compile_relational_query(judged.to_json(), "postgres")
    assert "postgis_typmod_srid" in sql


def test_a_probe_hidden_by_a_wrapper_is_refused_by_the_compiler() -> None:
    """Il mondo chiuso allo SDK: una sonda dietro CAST, COALESCE o CASE.

    La settima rilettura, dall'API pubblica: un bind nascosto da un
    involucro non spatial in posizione geometrica non era riconosciuto come
    sonda, e su 9fb015b `Geometry(srid=4326).predicate("intersects", t.c.g,
    p.bind("p", BindType.BINARY).cast(BindType.BINARY))` si compilava su
    ogni dialetto; su PostGIS una sonda 3857 lontana rendeva zero righe
    senza errore. Ora ogni argomento geometrico e una colonna, una funzione
    spatial o una sonda diretta, e il resto si rifiuta compilando.
    """
    features = p.table("features", "id", "g", schema="public")
    hidden = {
        "CAST(:p AS BINARY)": p.bind("p", p.BindType.BINARY).cast(p.BindType.BINARY),
        "CAST(:p AS STRING)": p.bind("p", p.BindType.STRING).cast(p.BindType.STRING),
        "COALESCE(:p)": p.func.coalesce(p.bind("p", p.BindType.BINARY)),
        "CASE WHEN id = :i THEN :p END": p.case(
            (features.c.id == p.bind("i", p.BindType.INTEGER), p.bind("p", p.BindType.BINARY))
        ),
    }
    for name, reference in hidden.items():
        for function in ("intersects", "within"):
            statement = p.select(features.c.id).where(
                p.Geometry(srid=4326).predicate(function, features.c.g, reference)
            )
            for provider in ("postgres", "mysql", "mariadb", "sqlserver", "oracle", "db2"):
                with pytest.raises(p.PlenoraUnsupportedError, match="argomento geometrico"):
                    compile_relational_query(statement.to_json(), provider)
                    pytest.fail(f"{name} {function} {provider}: compilato")
        within = p.select(features.c.id).where(
            p.Geometry(srid=4326).predicate(
                "d_within", features.c.g, reference, p.bind("d", p.BindType.FLOAT)
            )
        )
        with pytest.raises(p.PlenoraUnsupportedError, match="argomento geometrico"):
            compile_relational_query(within.to_json(), "postgres")
