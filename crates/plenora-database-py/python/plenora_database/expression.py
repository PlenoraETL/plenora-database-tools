"""Expression language immutabile sopra l'IR relazionale canonico."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from collections.abc import Iterator
from enum import Enum
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

    def in_(self, *values: Expression) -> Predicate:
        return _list_predicate(self, values, negated=False)

    def not_in(self, *values: Expression) -> Predicate:
        return _list_predicate(self, values, negated=True)

    def between(self, lower: Expression, upper: Expression) -> Predicate:
        return _between_predicate(self, lower, upper, negated=False)

    def not_between(self, lower: Expression, upper: Expression) -> Predicate:
        return _between_predicate(self, lower, upper, negated=True)

    def like(self, pattern: Expression) -> Predicate:
        return _like_predicate(self, pattern, case_insensitive=False, negated=False)

    def ilike(self, pattern: Expression) -> Predicate:
        return _like_predicate(self, pattern, case_insensitive=True, negated=False)

    def not_like(self, pattern: Expression) -> Predicate:
        return _like_predicate(self, pattern, case_insensitive=False, negated=True)

    def not_ilike(self, pattern: Expression) -> Predicate:
        return _like_predicate(self, pattern, case_insensitive=True, negated=True)

    def in_subquery(self, statement: SelectStatement) -> Predicate:
        return _subquery_predicate(self, statement, negated=False)

    def not_in_subquery(self, statement: SelectStatement) -> Predicate:
        return _subquery_predicate(self, statement, negated=True)

    def is_null(self) -> Predicate:
        return Predicate("is_null", (self,), negated=False)

    def is_not_null(self) -> Predicate:
        return Predicate("is_null", (self,), negated=True)

    def __bool__(self) -> bool:
        raise TypeError("le espressioni relazionali non hanno un valore booleano Python")


class ExecutableStatement:
    """Marker comune degli statement eseguibili dal lifecycle Core v3."""


class BindType(str, Enum):
    """Tipo logico portabile di un bind, tradotto dal renderer per dialect."""

    BOOLEAN = "boolean"
    INTEGER = "integer"
    BIG_INTEGER = "big_integer"
    FLOAT = "float"
    STRING = "string"
    BINARY = "binary"
    DATE = "date"
    TIMESTAMP = "timestamp"


@dataclass(frozen=True, slots=True, eq=False)
class UnboundColumn(Expression):
    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _identifier(self.name))

    def _ast(self) -> dict[str, Any]:
        return {"kind": "column", "column": {"relation": None, "field": self.name}}


@dataclass(frozen=True, slots=True, eq=False)
class Column(Expression):
    table: Relation
    name: str
    metadata: Any | None = None

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
    type_: BindType | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _identifier(self.name))
        if self.type_ is not None and not isinstance(self.type_, BindType):
            raise TypeError("type_ richiede un membro di BindType")

    def _ast(self) -> dict[str, Any]:
        if self.type_ is not None:
            return {
                "kind": "typed_parameter",
                "name": self.name,
                "parameter_type": self.type_.value,
            }
        return {"kind": "parameter", "name": self.name}


@dataclass(frozen=True, slots=True, eq=False)
class Wildcard(Expression):
    table: Relation | None = None

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
class FunctionExpression(Expression):
    function: str
    arguments: tuple[Expression, ...]

    def _ast(self) -> dict[str, Any]:
        return {
            "kind": "scalar",
            "function": self.function,
            "arguments": [argument._ast() for argument in self.arguments],
        }

    def over(
        self,
        *,
        partition_by: Iterable[Expression] = (),
        order_by: Iterable[Expression | Ordering] = (),
        rows: tuple[int | None, int | None] | None = None,
        range_: tuple[int | None, int | None] | None = None,
        groups: tuple[int | None, int | None] | None = None,
    ) -> WindowExpression:
        partitions = tuple(partition_by)
        if not all(isinstance(item, Expression) for item in partitions):
            raise TypeError("partition_by richiede espressioni relazionali")
        orderings = _orderings(tuple(order_by))
        frames = [("rows", rows), ("range", range_), ("groups", groups)]
        selected = [(unit, frame) for unit, frame in frames if frame is not None]
        if len(selected) > 1:
            raise ValueError("una window accetta un solo tipo di frame")
        frame = None if not selected else _window_frame(*selected[0])
        return WindowExpression(self.function, self.arguments, partitions, orderings, frame)


@dataclass(frozen=True, slots=True, eq=False)
class WindowExpression(Expression):
    function: str
    arguments: tuple[Expression, ...]
    partition_by: tuple[Expression, ...]
    order_by: tuple[Ordering, ...]
    frame: dict[str, Any] | None

    def _ast(self) -> dict[str, Any]:
        return {
            "kind": "window",
            "function": self.function,
            "arguments": [argument._ast() for argument in self.arguments],
            "partition_by": [expression._ast() for expression in self.partition_by],
            "order_by": [ordering._ast() for ordering in self.order_by],
            "frame": self.frame,
        }


@dataclass(frozen=True, slots=True, eq=False)
class ScalarSubquery(Expression):
    statement: SelectStatement

    def _ast(self) -> dict[str, Any]:
        return {"kind": "scalar_subquery", "query": self.statement.to_ast()}


@dataclass(frozen=True, slots=True, eq=False)
class Predicate(Expression):
    kind: str
    arguments: tuple[Expression, ...]
    operator: str | None = None
    negated: bool = False
    case_insensitive: bool = False

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
        if self.kind == "in_list":
            return {
                "kind": "in_list",
                "expression": self.arguments[0]._ast(),
                "values": [item._ast() for item in self.arguments[1:]],
                "negated": self.negated,
            }
        if self.kind == "between":
            expression, lower, upper = self.arguments
            return {
                "kind": "between",
                "expression": expression._ast(),
                "lower": lower._ast(),
                "upper": upper._ast(),
                "negated": self.negated,
            }
        if self.kind == "like":
            expression, pattern = self.arguments
            return {
                "kind": "like",
                "expression": expression._ast(),
                "pattern": pattern._ast(),
                "case_insensitive": self.case_insensitive,
                "negated": self.negated,
            }
        if self.kind == "is_null":
            return {
                "kind": "is_null",
                "expression": self.arguments[0]._ast(),
                "negated": self.negated,
            }
        if self.kind == "exists":
            statement = self.arguments[0]
            if not isinstance(statement, ScalarSubquery):
                raise TypeError("EXISTS richiede una query relazionale")
            return {
                "kind": "exists",
                "query": statement.statement.to_ast(),
                "negated": self.negated,
            }
        if self.kind == "in_subquery":
            expression, query = self.arguments
            if not isinstance(query, ScalarSubquery):
                raise TypeError("IN subquery richiede una query relazionale")
            return {
                "kind": "in_subquery",
                "expression": expression._ast(),
                "query": query.statement.to_ast(),
                "negated": self.negated,
            }
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
    metadata: Any | None

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
        object.__setattr__(self, "metadata", None)

    @property
    def qualifier(self) -> str:
        return self.alias_name or self.name

    @property
    def star(self) -> Wildcard:
        return Wildcard(self)

    def alias(self, name: str) -> Table:
        aliased = Table(
            self.name,
            (item.name for item in self.columns),
            schema=self.schema,
            catalog=self.catalog,
            alias_name=name,
        )
        object.__setattr__(aliased, "metadata", self.metadata)
        for source, destination in zip(self.columns, aliased.columns, strict=True):
            object.__setattr__(destination, "metadata", source.metadata)
        return aliased

    def _source_ast(self) -> dict[str, Any]:
        return {
            "object": {"catalog": self.catalog, "schema": self.schema, "object": self.name},
            "alias": self.alias_name,
        }


@dataclass(frozen=True, slots=True, init=False, eq=False)
class DerivedTable:
    statement: SelectStatement
    alias_name: str
    columns: tuple[Column, ...]
    c: ColumnCollection

    def __init__(
        self,
        statement: SelectStatement,
        alias_name: str,
        column_names: Iterable[str],
    ) -> None:
        object.__setattr__(self, "statement", statement)
        object.__setattr__(self, "alias_name", _identifier(alias_name))
        names = tuple(_identifier(name) for name in column_names)
        if not names or len(set(names)) != len(names):
            raise ValueError("colonne subquery mancanti o duplicate")
        bound = tuple(Column(self, name) for name in names)
        object.__setattr__(self, "columns", bound)
        object.__setattr__(self, "c", ColumnCollection(bound))

    @property
    def qualifier(self) -> str:
        return self.alias_name

    @property
    def star(self) -> Wildcard:
        return Wildcard(self)

    def _derived_ast(self) -> dict[str, Any]:
        return {"query": self.statement.to_ast(), "alias": self.alias_name}


@dataclass(frozen=True, slots=True, init=False, eq=False)
class CommonTable:
    statement: SelectStatement
    name: str
    recursive: bool
    columns: tuple[Column, ...]
    c: ColumnCollection

    def __init__(
        self,
        statement: SelectStatement,
        name: str,
        column_names: Iterable[str],
        *,
        recursive: bool = False,
    ) -> None:
        object.__setattr__(self, "statement", statement)
        object.__setattr__(self, "name", _identifier(name))
        object.__setattr__(self, "recursive", bool(recursive))
        names = tuple(_identifier(item) for item in column_names)
        if not names or len(set(names)) != len(names):
            raise ValueError("colonne CTE mancanti o duplicate")
        bound = tuple(Column(self, item) for item in names)
        object.__setattr__(self, "columns", bound)
        object.__setattr__(self, "c", ColumnCollection(bound))

    @property
    def qualifier(self) -> str:
        return self.name

    @property
    def star(self) -> Wildcard:
        return Wildcard(self)

    def _source_ast(self) -> dict[str, Any]:
        return {
            "object": {"catalog": None, "schema": None, "object": self.name},
            "alias": None,
        }

    def _cte_ast(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "recursive": self.recursive,
            "query": self.statement.to_ast(),
        }


Relation = Table | DerivedTable | CommonTable


def _relation_ast(relation: Relation) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if isinstance(relation, DerivedTable):
        return None, relation._derived_ast()
    return relation._source_ast(), None


@dataclass(frozen=True, slots=True)
class _Join:
    table: Relation
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
        source, derived_source = _relation_ast(self.table)
        return {
            "kind": self.kind,
            "source": source,
            "derived_source": derived_source,
            "lateral": False,
            "on": None if self.on is None else self.on._ast(),
        }


def _projection_ast(expression: Expression) -> dict[str, Any]:
    if isinstance(expression, Label):
        return {"expression": expression._ast(), "alias": expression.name}
    return {"expression": expression._ast(), "alias": None}


def _expression_table(expression: Expression) -> Relation | None:
    if isinstance(expression, (Column, Wildcard)):
        return expression.table
    if isinstance(expression, Label):
        return _expression_table(expression.expression)
    if isinstance(expression, (FunctionExpression, Predicate)):
        for argument in expression.arguments:
            if relation := _expression_table(argument):
                return relation
    if isinstance(expression, WindowExpression):
        nested = (*expression.arguments, *expression.partition_by)
        for argument in nested:
            if relation := _expression_table(argument):
                return relation
        for ordering in expression.order_by:
            if relation := _expression_table(ordering.expression):
                return relation
    return None


def _projection_names(
    projections: tuple[Expression, ...],
    explicit: tuple[str, ...],
) -> tuple[str, ...]:
    if explicit:
        if len(explicit) != len(projections):
            raise ValueError("numero nomi colonna incompatibile con la proiezione")
        return tuple(_identifier(name) for name in explicit)
    names: list[str] = []
    for projection in projections:
        if isinstance(projection, Label):
            names.append(projection.name)
        elif isinstance(projection, Column):
            names.append(projection.name)
        else:
            raise ValueError("la proiezione richiede label esplicite")
    return tuple(names)


@dataclass(frozen=True, slots=True)
class _SetOperation:
    operator: str
    statement: SelectStatement
    all: bool = False

    def _ast(self) -> dict[str, Any]:
        return {"operator": self.operator, "all": self.all, "query": self.statement.to_ast()}


@dataclass(frozen=True, slots=True)
class SelectStatement(ExecutableStatement):
    projections: tuple[Expression, ...]
    source: Relation | None = None
    joins: tuple[_Join, ...] = ()
    predicate: Predicate | None = None
    groupings: tuple[Expression, ...] = ()
    having_predicate: Predicate | None = None
    orderings: tuple[Ordering, ...] = ()
    set_operations: tuple[_SetOperation, ...] = ()
    row_limit: int | None = None
    row_offset: int | None = None
    is_distinct: bool = False

    def __post_init__(self) -> None:
        if not self.projections:
            raise ValueError("select senza proiezione")
        if not all(isinstance(item, Expression) for item in self.projections):
            raise TypeError("select accetta soltanto espressioni relazionali")

    def select_from(self, source: Relation) -> SelectStatement:
        if not isinstance(source, (Table, DerivedTable, CommonTable)):
            raise TypeError("select_from richiede una relazione")
        return replace(self, source=source)

    def join(
        self,
        right: Relation,
        on: Predicate | None = None,
        *,
        kind: str = "inner",
    ) -> SelectStatement:
        if not isinstance(right, (Table, DerivedTable, CommonTable)):
            raise TypeError("join richiede una relazione")
        return replace(self, joins=(*self.joins, _Join(right, on, kind)))

    def where(self, predicate: Predicate) -> SelectStatement:
        if not isinstance(predicate, Predicate):
            raise TypeError("where richiede un predicato relazionale")
        combined = predicate if self.predicate is None else and_(self.predicate, predicate)
        return replace(self, predicate=combined)

    def group_by(self, *expressions: Expression) -> SelectStatement:
        if not expressions or not all(isinstance(item, Expression) for item in expressions):
            raise TypeError("group_by richiede almeno una espressione relazionale")
        return replace(self, groupings=(*self.groupings, *expressions))

    def having(self, predicate: Predicate) -> SelectStatement:
        if not isinstance(predicate, Predicate):
            raise TypeError("having richiede un predicato relazionale")
        combined = (
            predicate
            if self.having_predicate is None
            else and_(self.having_predicate, predicate)
        )
        return replace(self, having_predicate=combined)

    def order_by(self, *values: Expression | Ordering) -> SelectStatement:
        additions = _orderings(values)
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

    def subquery(self, name: str, *column_names: str) -> DerivedTable:
        names = _projection_names(self.projections, column_names)
        return DerivedTable(self, name, names)

    def cte(
        self,
        name: str,
        *column_names: str,
        recursive: bool = False,
    ) -> CommonTable:
        names = _projection_names(self.projections, column_names)
        return CommonTable(self, name, names, recursive=recursive)

    def scalar_subquery(self) -> ScalarSubquery:
        if len(self.projections) != 1:
            raise ValueError("una subquery scalare richiede una sola proiezione")
        return ScalarSubquery(self)

    def exists(self) -> Predicate:
        return Predicate("exists", (ScalarSubquery(self),))

    def union(self, other: SelectStatement, *, all: bool = False) -> SelectStatement:
        return self._set_operation("union", other, all)

    def intersect(self, other: SelectStatement, *, all: bool = False) -> SelectStatement:
        return self._set_operation("intersect", other, all)

    def except_(self, other: SelectStatement, *, all: bool = False) -> SelectStatement:
        return self._set_operation("except", other, all)

    def _set_operation(
        self,
        operator: str,
        other: SelectStatement,
        all: bool,
    ) -> SelectStatement:
        if not isinstance(other, SelectStatement):
            raise TypeError("l'operazione insiemistica richiede select()")
        if not isinstance(all, bool):
            raise TypeError("all deve essere booleano")
        return replace(
            self,
            set_operations=(*self.set_operations, _SetOperation(operator, other, all)),
        )

    def _resolved_source(self) -> Relation | None:
        if self.source is not None:
            return self.source
        for projection in self.projections:
            if source := _expression_table(projection):
                return source
        return None

    def to_ast(self) -> dict[str, Any]:
        source = self._resolved_source()
        source_ast, derived_source_ast = (
            (None, None) if source is None else _relation_ast(source)
        )
        ctes: list[CommonTable] = []
        for relation in (source, *(join.table for join in self.joins)):
            if isinstance(relation, CommonTable) and all(
                existing.name != relation.name for existing in ctes
            ):
                ctes.append(relation)
        return {
            "common_table_expressions": [cte._cte_ast() for cte in ctes],
            "source": source_ast,
            "derived_source": derived_source_ast,
            "projection": [_projection_ast(item) for item in self.projections],
            "joins": [item._ast() for item in self.joins],
            "filter": None if self.predicate is None else self.predicate._ast(),
            "group_by": [expression._ast() for expression in self.groupings],
            "having": (
                None if self.having_predicate is None else self.having_predicate._ast()
            ),
            "order_by": [item._ast() for item in self.orderings],
            "distinct": self.is_distinct,
            "distinct_on": [],
            "set_operations": [operation._ast() for operation in self.set_operations],
            "row_limit": self.row_limit,
            "row_offset": self.row_offset,
            "locking": None,
            "declared_crs": [],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_ast(), sort_keys=True, separators=(",", ":"))


def _mutation_target(target: Table) -> dict[str, Any]:
    if not isinstance(target, Table) or target.alias_name is not None:
        raise TypeError("il target DML richiede table() senza alias")
    return {"catalog": target.catalog, "schema": target.schema, "object": target.name}


def _mutation_values(target: Table, values: Mapping[str, Expression]) -> tuple[tuple[str, Expression], ...]:
    if not values:
        raise ValueError("values richiede almeno una assegnazione")
    declared = {column.name for column in target.columns}
    if set(values) - declared:
        raise ValueError("values contiene colonne non dichiarate")
    if not all(isinstance(value, Expression) for value in values.values()):
        raise TypeError("i valori DML richiedono bind() o Column")
    return tuple(values.items())


def _returning_columns(target: Table, columns: tuple[Column, ...]) -> tuple[str, ...]:
    if not columns or not all(isinstance(column, Column) for column in columns):
        raise TypeError("returning richiede almeno una Column")
    if any(column.table is not target for column in columns):
        raise ValueError("returning contiene colonne di un'altra tabella")
    names = tuple(column.name for column in columns)
    if len(set(names)) != len(names):
        raise ValueError("returning contiene colonne duplicate")
    return names


@dataclass(frozen=True, slots=True)
class InsertStatement(ExecutableStatement):
    target: Table
    columns: tuple[str, ...] = ()
    rows: tuple[tuple[Expression, ...], ...] = ()
    returning_names: tuple[str, ...] = ()

    def values(self, **values: Expression) -> InsertStatement:
        assignments = _mutation_values(self.target, values)
        if self.columns and set(self.columns) != {name for name, _ in assignments}:
            raise ValueError("le righe INSERT richiedono lo stesso insieme di colonne")
        columns = self.columns or tuple(name for name, _ in assignments)
        by_name = dict(assignments)
        row = tuple(by_name[name] for name in columns)
        return replace(self, columns=columns, rows=(*self.rows, row))

    def returning(self, *columns: Column) -> InsertStatement:
        return replace(self, returning_names=_returning_columns(self.target, columns))

    def to_ast(self) -> dict[str, Any]:
        return {
            "type": "insert",
            "target": _mutation_target(self.target),
            "columns": list(self.columns),
            "rows": [[value._ast() for value in row] for row in self.rows],
            "returning": list(self.returning_names),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_ast(), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class UpdateStatement(ExecutableStatement):
    target: Table
    assignments: tuple[tuple[str, Expression], ...] = ()
    predicate: Predicate | None = None
    returning_names: tuple[str, ...] = ()

    def values(self, **values: Expression) -> UpdateStatement:
        additions = _mutation_values(self.target, values)
        merged = dict(self.assignments)
        merged.update(additions)
        return replace(self, assignments=tuple(merged.items()))

    def where(self, predicate: Predicate) -> UpdateStatement:
        if not isinstance(predicate, Predicate):
            raise TypeError("where richiede un predicato relazionale")
        combined = predicate if self.predicate is None else and_(self.predicate, predicate)
        return replace(self, predicate=combined)

    def returning(self, *columns: Column) -> UpdateStatement:
        return replace(self, returning_names=_returning_columns(self.target, columns))

    def to_ast(self) -> dict[str, Any]:
        return {
            "type": "update",
            "target": _mutation_target(self.target),
            "assignments": [
                {"column": name, "value": value._ast()}
                for name, value in self.assignments
            ],
            "filter": None if self.predicate is None else self.predicate._ast(),
            "returning": list(self.returning_names),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_ast(), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class DeleteStatement(ExecutableStatement):
    target: Table
    predicate: Predicate | None = None
    returning_names: tuple[str, ...] = ()

    def where(self, predicate: Predicate) -> DeleteStatement:
        if not isinstance(predicate, Predicate):
            raise TypeError("where richiede un predicato relazionale")
        combined = predicate if self.predicate is None else and_(self.predicate, predicate)
        return replace(self, predicate=combined)

    def returning(self, *columns: Column) -> DeleteStatement:
        return replace(self, returning_names=_returning_columns(self.target, columns))

    def to_ast(self) -> dict[str, Any]:
        return {
            "type": "delete",
            "target": _mutation_target(self.target),
            "filter": None if self.predicate is None else self.predicate._ast(),
            "returning": list(self.returning_names),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_ast(), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class UpsertStatement(ExecutableStatement):
    target: Table
    columns: tuple[str, ...] = ()
    rows: tuple[tuple[Expression, ...], ...] = ()
    conflict_names: tuple[str, ...] = ()
    assignments: tuple[tuple[str, Expression], ...] = ()
    returning_names: tuple[str, ...] = ()

    def values(self, **values: Expression) -> UpsertStatement:
        additions = _mutation_values(self.target, values)
        if self.columns and set(self.columns) != {name for name, _ in additions}:
            raise ValueError("le righe UPSERT richiedono lo stesso insieme di colonne")
        columns = self.columns or tuple(name for name, _ in additions)
        by_name = dict(additions)
        return replace(
            self,
            columns=columns,
            rows=(*self.rows, tuple(by_name[name] for name in columns)),
        )

    def on_conflict(self, *columns: Column) -> UpsertStatement:
        names = _returning_columns(self.target, columns)
        if self.columns and any(name not in self.columns for name in names):
            raise ValueError("conflict target non presente nei valori UPSERT")
        return replace(self, conflict_names=names)

    def set(self, **values: Expression) -> UpsertStatement:
        additions = _mutation_values(self.target, values)
        merged = dict(self.assignments)
        merged.update(additions)
        return replace(self, assignments=tuple(merged.items()))

    def returning(self, *columns: Column) -> UpsertStatement:
        return replace(self, returning_names=_returning_columns(self.target, columns))

    def to_ast(self) -> dict[str, Any]:
        return {
            "type": "upsert",
            "target": _mutation_target(self.target),
            "columns": list(self.columns),
            "rows": [[value._ast() for value in row] for row in self.rows],
            "conflict_target": list(self.conflict_names),
            "update_on_conflict": [
                {"column": name, "value": value._ast()}
                for name, value in self.assignments
            ],
            "returning": list(self.returning_names),
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


def bind(name: str, type_: BindType | None = None) -> BindParameter:
    return BindParameter(name, type_)


def select(*expressions: Expression) -> SelectStatement:
    return SelectStatement(tuple(expressions))


def insert(target: Table) -> InsertStatement:
    _mutation_target(target)
    return InsertStatement(target)


def update(target: Table) -> UpdateStatement:
    _mutation_target(target)
    return UpdateStatement(target)


def delete(target: Table) -> DeleteStatement:
    _mutation_target(target)
    return DeleteStatement(target)


def upsert(target: Table) -> UpsertStatement:
    _mutation_target(target)
    return UpsertStatement(target)


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


def _list_predicate(
    expression: Expression,
    values: tuple[Expression, ...],
    *,
    negated: bool,
) -> Predicate:
    if not values or not all(isinstance(item, Expression) for item in values):
        raise TypeError("IN richiede almeno una espressione relazionale")
    return Predicate("in_list", (expression, *values), negated=negated)


def _between_predicate(
    expression: Expression,
    lower: Expression,
    upper: Expression,
    *,
    negated: bool,
) -> Predicate:
    if not isinstance(lower, Expression) or not isinstance(upper, Expression):
        raise TypeError("BETWEEN richiede estremi relazionali")
    return Predicate("between", (expression, lower, upper), negated=negated)


def _like_predicate(
    expression: Expression,
    pattern: Expression,
    *,
    case_insensitive: bool,
    negated: bool,
) -> Predicate:
    if not isinstance(pattern, Expression):
        raise TypeError("LIKE richiede un pattern relazionale")
    return Predicate(
        "like",
        (expression, pattern),
        negated=negated,
        case_insensitive=case_insensitive,
    )


def _subquery_predicate(
    expression: Expression,
    statement: SelectStatement,
    *,
    negated: bool,
) -> Predicate:
    if not isinstance(statement, SelectStatement) or len(statement.projections) != 1:
        raise TypeError("IN subquery richiede select() con una proiezione")
    return Predicate(
        "in_subquery",
        (expression, ScalarSubquery(statement)),
        negated=negated,
    )


def _orderings(values: Iterable[Expression | Ordering]) -> tuple[Ordering, ...]:
    items = tuple(values)
    if not all(isinstance(value, (Expression, Ordering)) for value in items):
        raise TypeError("order_by richiede espressioni o ordinamenti relazionali")
    return tuple(
        value if isinstance(value, Ordering) else value.asc() for value in items
    )


def _window_bound(value: int | None, *, start: bool) -> dict[str, Any]:
    if value is None:
        return {"kind": "unbounded_preceding" if start else "unbounded_following"}
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError("il limite window deve essere intero o None")
    if value == 0:
        return {"kind": "current_row"}
    if value < 0:
        return {"kind": "preceding", "offset": abs(value)}
    return {"kind": "following", "offset": value}


def _window_frame(
    units: str,
    bounds: tuple[int | None, int | None],
) -> dict[str, Any]:
    if not isinstance(bounds, tuple) or len(bounds) != 2:
        raise TypeError("il frame window richiede una coppia (inizio, fine)")
    return {
        "units": units,
        "start": _window_bound(bounds[0], start=True),
        "end": _window_bound(bounds[1], start=False),
    }


class FunctionNamespace:
    """Catalogo chiuso delle funzioni scalar e aggregate del Core."""

    __slots__ = ()

    _FUNCTIONS = {
        "lower": "lower",
        "upper": "upper",
        "coalesce": "coalesce",
        "count": "count",
        "sum": "sum",
        "avg": "average",
        "min": "minimum",
        "max": "maximum",
        "row_number": "row_number",
        "rank": "rank",
        "dense_rank": "dense_rank",
        "lag": "lag",
        "lead": "lead",
    }

    def __getattr__(self, name: str):
        try:
            function = self._FUNCTIONS[name]
        except KeyError as error:
            raise AttributeError("funzione relazionale non supportata") from error

        def call(*arguments: Expression) -> FunctionExpression:
            if name == "count" and not arguments:
                arguments = (Wildcard(),)
            if not all(isinstance(argument, Expression) for argument in arguments):
                raise TypeError("le funzioni richiedono espressioni relazionali")
            return FunctionExpression(function, tuple(arguments))

        return call


func = FunctionNamespace()


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


def _compile_mutation_statement(
    statement: InsertStatement | UpdateStatement | DeleteStatement | UpsertStatement,
    parameters: Mapping[str, Any] | None,
    provider: str,
) -> tuple[str, list[Any], bool]:
    from ._native import compile_relational_mutation

    if not isinstance(
        statement, (InsertStatement, UpdateStatement, DeleteStatement, UpsertStatement)
    ):
        raise TypeError("mutazione relazionale non supportata")
    if parameters is not None and not isinstance(parameters, Mapping):
        raise TypeError("i bind relazionali richiedono un mapping")
    values = {} if parameters is None else dict(parameters)
    if not all(isinstance(name, str) for name in values):
        raise TypeError("i nomi dei bind devono essere stringhe")
    sql, bind_names, returns_rows = compile_relational_mutation(
        statement.to_json(), provider
    )
    if set(values) != set(bind_names):
        raise ValueError("insieme dei bind incompatibile con lo statement")
    return sql, [values[name] for name in bind_names], returns_rows


def _execute_statement(
    native: Any,
    statement: ExecutableStatement,
    parameters: Mapping[str, Any] | None,
    provider: str,
) -> Result | int | MutationResult:
    from .result import MutationResult, Result

    if isinstance(statement, SelectStatement):
        sql, values = _compile_statement(statement, parameters, provider)
        return Result(native.execute_returning_rows(sql, values))
    sql, values, returns_rows = _compile_mutation_statement(
        statement, parameters, provider
    )
    if returns_rows:
        return Result(native.execute_returning_rows(sql, values))
    affected = native.execute(sql, values)
    if isinstance(statement, UpsertStatement):
        return MutationResult(
            "upsert", provider, None if provider == "sqlserver" else affected
        )
    return affected


async def _execute_statement_async(
    native: Any,
    statement: ExecutableStatement,
    parameters: Mapping[str, Any] | None,
    provider: str,
) -> Result | int | MutationResult:
    from .result import MutationResult, Result

    if isinstance(statement, SelectStatement):
        sql, values = _compile_statement(statement, parameters, provider)
        return Result(await native.execute_returning_rows(sql, values))
    sql, values, returns_rows = _compile_mutation_statement(
        statement, parameters, provider
    )
    if returns_rows:
        return Result(await native.execute_returning_rows(sql, values))
    affected = await native.execute(sql, values)
    if isinstance(statement, UpsertStatement):
        return MutationResult(
            "upsert", provider, None if provider == "sqlserver" else affected
        )
    return affected
