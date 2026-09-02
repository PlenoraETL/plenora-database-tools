"""Diff tipizzato fra metadata ORM desiderati e reflection osservata.

Il modulo produce un piano esplicito e fail-closed. Un rename non viene mai
dedotto, le osservazioni ``NotMeasured`` non autorizzano modifiche e nessuna
operazione rischiosa viene eseguita senza approvazione del chiamante.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from typing import Any

from .metadata import MetaData
from .orm import (
    ForeignKeyConstraint,
    Migration,
    OrmMappingError,
    OrmMetadata,
    OrmStateError,
    UniqueConstraint,
    _ddl_type,
    _execute_ddl,
    _qualified_table,
    _quote_identifier,
    _render_server_default,
    _session_provider,
)


class SchemaRisk(str, Enum):
    SAFE = "safe"
    REQUIRES_LOCK = "requires-lock"
    LOSSY = "lossy"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True, slots=True)
class SchemaOperation:
    kind: str
    table: str
    risk: SchemaRisk
    statement: str | None
    reverse_statement: str | None = None
    column: str | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        if not self.kind or not self.table:
            raise ValueError("operazione schema priva di identita")
        if self.risk is SchemaRisk.UNSUPPORTED and self.statement is not None:
            raise ValueError("operazione unsupported non puo contenere DDL")
        if self.risk is not SchemaRisk.UNSUPPORTED and self.statement is None:
            raise ValueError("operazione schema eseguibile priva di DDL")


@dataclass(frozen=True, slots=True)
class SchemaDiff:
    provider: str
    operations: tuple[SchemaOperation, ...]
    fingerprint: str

    @property
    def is_empty(self) -> bool:
        return not self.operations

    @property
    def risks(self) -> frozenset[SchemaRisk]:
        return frozenset(operation.risk for operation in self.operations)

    def apply(
        self,
        session: Any,
        *,
        allow: Iterable[SchemaRisk | str] = (SchemaRisk.SAFE,),
    ) -> tuple[str, ...]:
        provider = _session_provider(session)
        if provider != self.provider:
            raise OrmStateError("piano schema destinato a un provider diverso")
        allowed = _normalize_risks(allow)
        for operation in self.operations:
            if operation.risk is SchemaRisk.UNSUPPORTED:
                raise OrmStateError("piano schema contiene un'operazione unsupported")
            if operation.risk not in allowed:
                raise OrmStateError("rischio schema non autorizzato")
            if operation.statement is None:  # protetto dal costruttore
                raise OrmStateError("operazione schema priva di DDL")
        completed: list[str] = []
        for operation in self.operations:
            assert operation.statement is not None
            _execute_ddl(session, operation.statement)
            completed.append(operation.kind)
        return tuple(completed)

    def migration(
        self,
        revision: str,
        down_revision: str | tuple[str, ...] | None,
        *,
        allow: Iterable[SchemaRisk | str] = (SchemaRisk.SAFE,),
    ) -> Migration:
        allowed = _normalize_risks(allow)

        def upgrade(transaction: Any) -> None:
            self.apply(transaction, allow=allowed)

        reversible = all(
            operation.reverse_statement is not None
            and operation.risk is not SchemaRisk.UNSUPPORTED
            for operation in self.operations
        )

        def downgrade(transaction: Any) -> None:
            provider = _session_provider(transaction)
            if provider != self.provider:
                raise OrmStateError("piano schema destinato a un provider diverso")
            for operation in reversed(self.operations):
                if operation.reverse_statement is None:
                    raise OrmStateError("migrazione generata non reversibile")
                _execute_ddl(transaction, operation.reverse_statement)

        checksum = sha256(
            json.dumps(
                [
                    {
                        "kind": operation.kind,
                        "risk": operation.risk.value,
                        "statement": operation.statement,
                        "reverse_statement": operation.reverse_statement,
                    }
                    for operation in self.operations
                ],
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

        return Migration(
            revision,
            down_revision,
            upgrade,
            downgrade if reversible else None,
            checksum,
        )


def compare_schema(desired: OrmMetadata, observed: MetaData) -> SchemaDiff:
    """Confronta mapper e reflection senza dedurre rename o fatti non misurati."""

    if not isinstance(desired, OrmMetadata) or not isinstance(observed, MetaData):
        raise TypeError("compare_schema richiede OrmMetadata e MetaData")
    provider = observed.provider
    if provider not in {"postgres", "mysql", "mariadb", "sqlserver", "db2"}:
        raise OrmMappingError("provider schema non qualificato")

    desired_by_key = {
        _table_key(mapper.table.catalog, mapper.table.schema, mapper.table.name): mapper
        for mapper in desired.mappers
    }
    observed_by_key = {
        _table_key(table.catalog, table.schema, table.name): table
        for table in observed.tables
    }
    operations: list[SchemaOperation] = []

    for key in sorted(desired_by_key.keys() - observed_by_key.keys()):
        mapper = desired_by_key[key]
        statement = OrmMetadata(desired.registry, models=(mapper.model,)).ddl(provider)[
            0
        ]
        operations.append(
            SchemaOperation(
                "create-table",
                _display_key(key),
                SchemaRisk.SAFE,
                statement,
                _drop_table(mapper.table, provider),
            )
        )

    for key in sorted(observed_by_key.keys() - desired_by_key.keys()):
        table = observed_by_key[key]
        operations.append(
            SchemaOperation(
                "drop-table",
                _display_key(key),
                SchemaRisk.LOSSY,
                _drop_table(table, provider),
                reason="la tabella osservata non e presente nei metadata desiderati",
            )
        )

    for key in sorted(desired_by_key.keys() & observed_by_key.keys()):
        mapper = desired_by_key[key]
        table = observed_by_key[key]
        target = _qualified_table(mapper.table, provider)
        mapped_columns = (
            (*mapper.primary_keys, *mapper.local_attributes)
            if mapper.inheritance == "joined"
            else mapper.attributes
        )
        desired_columns = {
            attribute.name: attribute
            for attribute in mapped_columns
            if attribute.name is not None
        }
        observed_columns = {column.name: column for column in table.columns}

        for name in sorted(desired_columns.keys() - observed_columns.keys()):
            attribute = desired_columns[name]
            column = _quote_identifier(name, provider)
            declaration = f"{column} {_ddl_type(attribute, provider)}"
            if not attribute.nullable:
                declaration += " NOT NULL"
            if attribute.server_default_spec is not None:
                declaration += " DEFAULT " + _render_server_default(
                    attribute.server_default_spec, provider
                )
            if not attribute.nullable and attribute.server_default_spec is None:
                risk = SchemaRisk.UNSUPPORTED
                statement = None
                reverse = None
                reason = "colonna non nullable senza default su tabella esistente"
            else:
                risk = (
                    SchemaRisk.REQUIRES_LOCK
                    if attribute.server_default_spec is not None
                    else SchemaRisk.SAFE
                )
                statement = f"ALTER TABLE {target} ADD {declaration}"
                reverse = f"ALTER TABLE {target} DROP COLUMN {column}"
                reason = None
            operations.append(
                SchemaOperation(
                    "add-column",
                    _display_key(key),
                    risk,
                    statement,
                    reverse,
                    name,
                    reason,
                )
            )

        for name in sorted(observed_columns.keys() - desired_columns.keys()):
            column = _quote_identifier(name, provider)
            operations.append(
                SchemaOperation(
                    "drop-column",
                    _display_key(key),
                    SchemaRisk.LOSSY,
                    f"ALTER TABLE {target} DROP COLUMN {column}",
                    column=name,
                    reason="la colonna osservata non e presente nel modello desiderato",
                )
            )

        for name in sorted(desired_columns.keys() & observed_columns.keys()):
            attribute = desired_columns[name]
            metadata = observed_columns[name].metadata
            expected = _normalized_type(_ddl_type(attribute, provider))
            actual = _normalized_type(
                metadata.native_declaration or metadata.native_type
            )
            if actual and expected != actual:
                operations.append(
                    SchemaOperation(
                        "alter-column-type",
                        _display_key(key),
                        SchemaRisk.UNSUPPORTED,
                        None,
                        column=name,
                        reason="cambio tipo richiede una migrazione esplicita",
                    )
                )
            expected_nullable = attribute.nullable
            if metadata.nullable is not None and metadata.nullable != expected_nullable:
                operations.append(
                    SchemaOperation(
                        "alter-column-nullability",
                        _display_key(key),
                        SchemaRisk.UNSUPPORTED,
                        None,
                        column=name,
                        reason="cambio nullability richiede una migrazione esplicita",
                    )
                )

        operations.extend(_constraint_diff(mapper, table, provider, key))

    payload = [
        {
            "kind": item.kind,
            "table": item.table,
            "column": item.column,
            "risk": item.risk.value,
            "statement": item.statement,
        }
        for item in operations
    ]
    fingerprint = (
        "sha256:"
        + sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )
    return SchemaDiff(provider, tuple(operations), fingerprint)


def _constraint_diff(
    mapper: Any, table: Any, provider: str, key: tuple[str, str, str]
) -> tuple[SchemaOperation, ...]:
    measured = table.metadata.constraints
    foreign_keys = table.metadata.foreign_keys
    operations: list[SchemaOperation] = []
    observed_unique = {
        tuple(item.columns.values)
        for item in measured.values
        if item.kind.lower() in {"unique", "unique_constraint"}
        and item.columns.measured
    }
    observed_fk = {
        (
            tuple(item.columns.values),
            item.referenced_object,
            tuple(item.referenced_columns.values),
        )
        for item in foreign_keys.values
        if item.columns.measured and item.referenced_columns.measured
    }
    target = _qualified_table(mapper.table, provider)
    for constraint in mapper.constraints:
        if isinstance(constraint, UniqueConstraint):
            if not measured.measured:
                continue
            if constraint.columns in observed_unique:
                continue
            columns = ", ".join(
                _quote_identifier(item, provider) for item in constraint.columns
            )
            name = (
                ""
                if constraint.name is None
                else f"CONSTRAINT {_quote_identifier(constraint.name, provider)} "
            )
            operations.append(
                SchemaOperation(
                    "add-unique",
                    _display_key(key),
                    SchemaRisk.REQUIRES_LOCK,
                    f"ALTER TABLE {target} ADD {name}UNIQUE ({columns})",
                    reason="la creazione del vincolo puo acquisire un lock",
                )
            )
        elif isinstance(constraint, ForeignKeyConstraint):
            if not foreign_keys.measured:
                continue
            target_model = mapper.model.__registry__.mapper_for(
                constraint.target
                if isinstance(constraint.target, type)
                else mapper.model.__registry__._resolve(constraint.target)
            )
            signature = (
                constraint.columns,
                target_model.table.name,
                constraint.target_columns,
            )
            if signature in observed_fk:
                continue
            local = ", ".join(
                _quote_identifier(item, provider) for item in constraint.columns
            )
            remote = ", ".join(
                _quote_identifier(item, provider) for item in constraint.target_columns
            )
            remote_table = _qualified_table(target_model.table, provider)
            name = (
                ""
                if constraint.name is None
                else f"CONSTRAINT {_quote_identifier(constraint.name, provider)} "
            )
            delete = (
                ""
                if constraint.on_delete is None
                else f" ON DELETE {constraint.on_delete}"
            )
            operations.append(
                SchemaOperation(
                    "add-foreign-key",
                    _display_key(key),
                    SchemaRisk.REQUIRES_LOCK,
                    f"ALTER TABLE {target} ADD {name}FOREIGN KEY ({local}) "
                    f"REFERENCES {remote_table} ({remote}){delete}",
                    reason="la validazione della foreign key puo acquisire un lock",
                )
            )
    return tuple(operations)


def _normalize_risks(values: Iterable[SchemaRisk | str]) -> frozenset[SchemaRisk]:
    try:
        return frozenset(
            value if isinstance(value, SchemaRisk) else SchemaRisk(value)
            for value in values
        )
    except (TypeError, ValueError) as error:
        raise ValueError("insieme rischi schema non valido") from error


def _table_key(
    catalog: str | None, schema: str | None, name: str
) -> tuple[str, str, str]:
    return catalog or "", schema or "", name


def _display_key(key: tuple[str, str, str]) -> str:
    return ".".join(item for item in key if item)


def _drop_table(table: Any, provider: str) -> str:
    return f"DROP TABLE {_qualified_table(table, provider)}"


def _normalized_type(value: str) -> str:
    aliases = {
        "int": "integer",
        "int4": "integer",
        "character varying(255)": "varchar(255)",
        "double": "double precision",
    }
    normalized = " ".join(value.strip().lower().split())
    return aliases.get(normalized, normalized)
