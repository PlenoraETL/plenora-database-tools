"""Expression language immutabile sopra l'IR relazionale canonico."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from collections.abc import Iterator
from typing import Any, Iterable, Mapping


def _identifier(value: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("identificatore relazionale non valido")
    if len(value) > 256:
        raise ValueError("identificatore relazionale oltre il limite")
    return value


class Expression:
    """Nodo componibile che non contiene valori applicativi."""

    def _ast(self) -> dict[str, Any]:
        raise NotImplementedError

    def _compare(self, operator: str, other: Expression) -> Predicate:
        if not isinstance(other, Expression):
            raise TypeError("il confronto richiede column(), bind() o un'altra espressione")
        return Predicate("compare", (self, other), operator)

    def __eq__(self, other: object) -> Predicate:  # type: ignore[override]
        return self._compare("eq", other)  # type: ignore[arg-type]

    def __ne__(self, other: object) -> Predicate:  # type: ignore[override]
        return self._compare("ne", other)  # type: ignore[arg-type]

    def __lt__(self, other: Expression) -> Predicate:
        return self._compare("lt", other)

    def __le__(self, other: Expression) -> Predicate:
        return self._compare("lte", other)

    def __gt__(self, other: Expression) -> Predicate:
        return self._compare("gt", other)

    def __ge__(self, other: Expression) -> Predicate:
        return self._compare("gte", other)

    def label(self, name: str) -> Label:
        return Label(self, _identifier(name))

    def asc(self) -> Ordering:
        return Ordering(self, "asc")

    def desc(self) -> Ordering:
        return Ordering(self, "desc")

    def __bool__(self) -> bool:
        raise TypeError("le espressioni relazionali non hanno un valore booleano Python")


@dataclass(frozen=True, slots=True, eq=False)
class UnboundColumn(Expression):
    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _identifier(self.name))

    def _ast(self) -> dict[str, Any]:
        return {"kind": "column", "column": {"relation": None, "field": self.name}}


@dataclass(frozen=True, slots=True, eq=False)
class Column(Expression):
    table: Table
    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _identifier(self.name))

    def _ast(self) -> dict[str, Any]:
        return {
            "kind": "column",
            "column": {"relation": self.table.qualifier, "field": self.name},
        }


@dataclass(frozen=True, slots=True, eq=False)
class BindParameter(Expression):
    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _identifier(self.name))

    def _ast(self) -> dict[str, Any]:
        return {"kind": "parameter", "name": self.name}


@dataclass(frozen=True, slots=True, eq=False)
class Wildcard(Expression):
    table: Table | None = None

    def _ast(self) -> dict[str, Any]:
        relation = None if self.table is None else self.table.qualifier
        return {"kind": "wildcard", "relation": relation}


@dataclass(frozen=True, slots=True, eq=False)
class Label(Expression):
    expression: Expression
    name: str

    def _ast(self) -> dict[str, Any]:
        return self.expression._ast()


@dataclass(frozen=True, slots=True, eq=False)
class Predicate(Expression):
    kind: str
    arguments: tuple[Expression, ...]
    operator: str | None = None

    def __and__(self, other: Predicate) -> Predicate:
        return and_(self, other)

    def __or__(self, other: Predicate) -> Predicate:
        return or_(self, other)

    def __invert__(self) -> Predicate:
        return Predicate("not", (self,))

    def _ast(self) -> dict[str, Any]:
        if self.kind == "compare":
            left, right = self.arguments
            return {
                "kind": "compare",
                "left": left._ast(),
                "operator": self.operator,
                "right": right._ast(),
            }
        if self.kind in {"and", "or"}:
            return {"kind": self.kind, "arguments": [arg._ast() for arg in self.arguments]}
        if self.kind == "not":
            return {"kind": "not", "expression": self.arguments[0]._ast()}
        raise ValueError("predicato relazionale non supportato")


@dataclass(frozen=True, slots=True)
class Ordering:
    expression: Expression
    direction: str

    def __post_init__(self) -> None:
        if self.direction not in {"asc", "desc"}:
            raise ValueError("direzione relazionale non valida")

    def _ast(self) -> dict[str, Any]:
        return {"expression": self.expression._ast(), "direction": self.direction}


class ColumnCollection:
    """Namespace stabile `table.c.nome` delle colonne dichiarate."""

    __slots__ = ("_columns",)

    def __init__(self, columns: tuple[Column, ...]) -> None:
        object.__setattr__(self, "_columns", {item.name: item for item in columns})

    def __getattr__(self, name: str) -> Column:
        try:
            return self._columns[name]
        except KeyError as error:
            raise AttributeError("colonna non dichiarata") from error

    def __getitem__(self, name: str) -> Column:
        try:
            return self._columns[name]
        except KeyError as error:
            raise KeyError("colonna non dichiarata") from error

    def __iter__(self) -> Iterator[Column]:
        return iter(self._columns.values())


@dataclass(frozen=True, slots=True, init=False, eq=False)
class Table:
    name: str
    schema: str | None
    catalog: str | None
    alias_name: str | None
    columns: tuple[Column, ...]
    c: ColumnCollection

    def __init__(
        self,
        name: str,
        columns: Iterable[str | UnboundColumn],
        *,
        schema: str | None = None,
        catalog: str | None = None,
        alias_name: str | None = None,
    ) -> None:
        object.__setattr__(self, "name", _identifier(name))
        object.__setattr__(self, "schema", None if schema is None else _identifier(schema))
        object.__setattr__(self, "catalog", None if catalog is None else _identifier(catalog))
        object.__setattr__(
            self,
            "alias_name",
            None if alias_name is None else _identifier(alias_name),
        )
        names = tuple(
            item.name if isinstance(item, UnboundColumn) else _identifier(item)
            for item in columns
        )
        if len(set(names)) != len(names):
            raise ValueError("nomi colonna duplicati nella tabella")
        bound = tuple(Column(self, item) for item in names)
        object.__setattr__(self, "columns", bound)
        object.__setattr__(self, "c", ColumnCollection(bound))

    @property
    def qualifier(self) -> str:
        return self.alias_name or self.name

    @property
    def star(self) -> Wildcard:
        return Wildcard(self)

    def alias(self, name: str) -> Table:
        return Table(
            self.name,
            (item.name for item in self.columns),
            schema=self.schema,
            catalog=self.catalog,
            alias_name=name,
        )

    def _source_ast(self) -> dict[str, Any]:
        return {
            "object": {"catalog": self.catalog, "schema": self.schema, "object": self.name},
            "alias": self.alias_name,
        }


@dataclass(frozen=True, slots=True)
class _Join:
    table: Table
    on: Predicate | None
    kind: str

    def __post_init__(self) -> None:
        if self.kind not in {"inner", "left", "right", "full", "cross"}:
            raise ValueError("tipo di join non valido")
        if self.on is not None and not isinstance(self.on, Predicate):
            raise TypeError("la condizione ON deve essere un predicato relazionale")
        if self.kind == "cross" and self.on is not None:
            raise ValueError("cross join non accetta una condizione ON")
        if self.kind != "cross" and self.on is None:
            raise ValueError("join senza condizione ON")

    def _ast(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "source": self.table._source_ast(),
            "lateral": False,
            "on": None if self.on is None else self.on._ast(),
        }


def _projection_ast(expression: Expression) -> dict[str, Any]:
    if isinstance(expression, Label):
        return {"expression": expression._ast(), "alias": expression.name}
    return {"expression": expression._ast(), "alias": None}


def _expression_table(expression: Expression) -> Table | None:
    if isinstance(expression, (Column, Wildcard)):
        return expression.table
    if isinstance(expression, Label):
        return _expression_table(expression.expression)
    return None


@dataclass(frozen=True, slots=True)
class SelectStatement:
    projections: tuple[Expression, ...]
    source: Table | None = None
    joins: tuple[_Join, ...] = ()
    predicate: Predicate | None = None
    orderings: tuple[Ordering, ...] = ()
    row_limit: int | None = None
    row_offset: int | None = None
    is_distinct: bool = False

    def __post_init__(self) -> None:
        if not self.projections:
            raise ValueError("select senza proiezione")
        if not all(isinstance(item, Expression) for item in self.projections):
            raise TypeError("select accetta soltanto espressioni relazionali")

    def select_from(self, source: Table) -> SelectStatement:
        if not isinstance(source, Table):
            raise TypeError("select_from richiede table()")
        return replace(self, source=source)

    def join(
        self,
        right: Table,
        on: Predicate | None = None,
        *,
        kind: str = "inner",
    ) -> SelectStatement:
        if not isinstance(right, Table):
            raise TypeError("join richiede table()")
        return replace(self, joins=(*self.joins, _Join(right, on, kind)))

    def where(self, predicate: Predicate) -> SelectStatement:
        if not isinstance(predicate, Predicate):
            raise TypeError("where richiede un predicato relazionale")
        combined = predicate if self.predicate is None else and_(self.predicate, predicate)
        return replace(self, predicate=combined)

    def order_by(self, *values: Expression | Ordering) -> SelectStatement:
        if not all(isinstance(value, (Expression, Ordering)) for value in values):
            raise TypeError("order_by richiede espressioni o ordinamenti relazionali")
        additions = tuple(
            value if isinstance(value, Ordering) else value.asc() for value in values
        )
        return replace(self, orderings=(*self.orderings, *additions))

    def limit(self, value: int) -> SelectStatement:
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError("limit deve essere un intero non negativo")
        return replace(self, row_limit=value)

    def offset(self, value: int) -> SelectStatement:
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError("offset deve essere un intero non negativo")
        return replace(self, row_offset=value)

    def distinct(self) -> SelectStatement:
        return replace(self, is_distinct=True)

    def _resolved_source(self) -> Table | None:
        if self.source is not None:
            return self.source
        for projection in self.projections:
            if source := _expression_table(projection):
                return source
        return None

    def to_ast(self) -> dict[str, Any]:
        source = self._resolved_source()
        return {
            "common_table_expressions": [],
            "source": None if source is None else source._source_ast(),
            "projection": [_projection_ast(item) for item in self.projections],
            "joins": [item._ast() for item in self.joins],
            "filter": None if self.predicate is None else self.predicate._ast(),
            "group_by": [],
            "having": None,
            "order_by": [item._ast() for item in self.orderings],
            "distinct": self.is_distinct,
            "row_limit": self.row_limit,
            "row_offset": self.row_offset,
            "locking": None,
            "declared_crs": [],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_ast(), sort_keys=True, separators=(",", ":"))


def column(name: str) -> UnboundColumn:
    return UnboundColumn(name)


def table(
    name: str,
    *columns: str | UnboundColumn,
    schema: str | None = None,
    catalog: str | None = None,
) -> Table:
    return Table(name, columns, schema=schema, catalog=catalog)


def bind(name: str) -> BindParameter:
    return BindParameter(name)


def select(*expressions: Expression) -> SelectStatement:
    return SelectStatement(tuple(expressions))


def and_(*predicates: Predicate) -> Predicate:
    if len(predicates) < 2 or not all(isinstance(item, Predicate) for item in predicates):
        raise TypeError("and_ richiede almeno due predicati")
    flattened: list[Expression] = []
    for predicate in predicates:
        flattened.extend(predicate.arguments if predicate.kind == "and" else (predicate,))
    return Predicate("and", tuple(flattened))


def or_(*predicates: Predicate) -> Predicate:
    if len(predicates) < 2 or not all(isinstance(item, Predicate) for item in predicates):
        raise TypeError("or_ richiede almeno due predicati")
    flattened: list[Expression] = []
    for predicate in predicates:
        flattened.extend(predicate.arguments if predicate.kind == "or" else (predicate,))
    return Predicate("or", tuple(flattened))


def _compile_statement(
    statement: SelectStatement,
    parameters: Mapping[str, Any] | None,
    provider: str,
) -> tuple[str, list[Any]]:
    from ._native import compile_relational_query

    if not isinstance(statement, SelectStatement):
        raise TypeError("statement relazionale non supportato")
    if parameters is not None and not isinstance(parameters, Mapping):
        raise TypeError("i bind relazionali richiedono un mapping")
    values = {} if parameters is None else dict(parameters)
    if not all(isinstance(name, str) for name in values):
        raise TypeError("i nomi dei bind devono essere stringhe")
    sql, bind_names = compile_relational_query(statement.to_json(), provider)
    if set(values) != set(bind_names):
        raise ValueError("insieme dei bind incompatibile con lo statement")
    return sql, [values[name] for name in bind_names]


def _execute_statement(
    native: Any,
    statement: SelectStatement,
    parameters: Mapping[str, Any] | None,
    provider: str,
) -> Result:
    from .result import Result

    sql, values = _compile_statement(statement, parameters, provider)
    return Result(native.execute_returning_rows(sql, values))


async def _execute_statement_async(
    native: Any,
    statement: SelectStatement,
    parameters: Mapping[str, Any] | None,
    provider: str,
) -> Result:
    from .result import Result

    sql, values = _compile_statement(statement, parameters, provider)
    return Result(await native.execute_returning_rows(sql, values))
