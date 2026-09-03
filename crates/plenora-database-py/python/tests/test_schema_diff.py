"""Schema diff applicativo: piano tipizzato, rischi e revisioni generate."""

from __future__ import annotations

from typing import ClassVar

import plenora_database as p
import pytest


def _observed(
    *, include_label: bool = False, include_legacy: bool = False
) -> p.MetaData:
    columns = [
        {
            "name": "id",
            "ordinal": 1,
            "native_type": "int4",
            "native_declaration": "integer",
            "nullable": False,
            "default_expression": None,
            "identity": False,
            "generated": False,
            "numeric_precision": 32,
            "numeric_scale": 0,
            "spatial": None,
            "native": {"Postgres": {}},
        }
    ]
    if include_label:
        columns.append(
            {
                "name": "label",
                "ordinal": 2,
                "native_type": "text",
                "native_declaration": "text",
                "nullable": True,
                "default_expression": None,
                "identity": False,
                "generated": False,
                "numeric_precision": None,
                "numeric_scale": None,
                "spatial": None,
                "native": {"Postgres": {}},
            }
        )
    if include_legacy:
        columns.append(
            {
                "name": "legacy",
                "ordinal": 3,
                "native_type": "text",
                "native_declaration": "text",
                "nullable": True,
                "default_expression": None,
                "identity": False,
                "generated": False,
                "numeric_precision": None,
                "numeric_scale": None,
                "spatial": None,
                "native": {"Postgres": {}},
            }
        )
    return p.MetaData.from_document(
        {
            "provider": "postgres",
            "tables": [
                {
                    "catalog": None,
                    "schema": None,
                    "name": "schema_items",
                    "kind": "table",
                    "schema_token": {
                        "Postgres": {
                            "schema_version": 2,
                            "structural_fingerprint": "sha256:fixture",
                        }
                    },
                    "columns": columns,
                    "indexes": {"Observed": []},
                    "constraints": {"Observed": []},
                    "foreign_keys": {"Observed": []},
                    "native": {"Postgres": {}},
                }
            ],
        }
    )


def _desired() -> p.OrmMetadata:
    registry = p.Registry()

    class Base(p.DeclarativeBase):
        __registry__ = registry

    class Item(Base):
        __tablename__ = "schema_items"

        id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
        label: p.Mapped[str | None] = p.mapped_column(str)

    return p.OrmMetadata(registry)


def _advanced_shapes(
    *,
    constraints: list[dict] | None = None,
    indexes: list[dict] | None = None,
    identity: bool | None = False,
    generated: bool | None = False,
) -> tuple[p.OrmMetadata, p.MetaData]:
    registry = p.Registry()

    class Base(p.DeclarativeBase):
        __registry__ = registry

    class Score(Base):
        __tablename__ = "schema_scores"
        __table_args__ = (
            p.CheckConstraint("score", ">=", 0, name="ck_score_nonnegative"),
            p.OrmIndex("score", name="ix_schema_scores_score"),
        )

        id: p.Mapped[int] = p.mapped_column(int, primary_key=True)
        score: p.Mapped[int] = p.mapped_column(
            int,
            nullable=False,
            server_default=p.ServerDefault.literal(0),
        )

    columns = []
    for ordinal, name in enumerate(("id", "score"), 1):
        columns.append(
            {
                "name": name,
                "ordinal": ordinal,
                "native_type": "int4",
                "native_declaration": "integer",
                "nullable": False,
                "default_expression": None,
                "identity": identity,
                "generated": generated,
                "numeric_precision": 32,
                "numeric_scale": 0,
                "spatial": None,
                "native": {"Postgres": {}},
            }
        )
    observed = p.MetaData.from_document(
        {
            "provider": "postgres",
            "tables": [
                {
                    "catalog": None,
                    "schema": None,
                    "name": "schema_scores",
                    "kind": "table",
                    "schema_token": {
                        "Postgres": {
                            "schema_version": 2,
                            "structural_fingerprint": "sha256:advanced",
                        }
                    },
                    "columns": columns,
                    "indexes": {"Observed": indexes or []},
                    "constraints": {"Observed": constraints or []},
                    "foreign_keys": {"Observed": []},
                    "native": {"Postgres": {}},
                }
            ],
        }
    )
    return p.OrmMetadata(registry), observed


def test_schema_diff_is_stable_and_generates_safe_add_column() -> None:
    first = p.compare_schema(_desired(), _observed())
    second = p.compare_schema(_desired(), _observed())

    assert first.fingerprint == second.fingerprint
    assert len(first.operations) == 1
    operation = first.operations[0]
    assert operation.kind == "add-column"
    assert operation.column == "label"
    assert operation.risk is p.SchemaRisk.SAFE
    assert operation.statement == 'ALTER TABLE "schema_items" ADD "label" TEXT'


def test_schema_diff_never_infers_rename_and_requires_loss_approval() -> None:
    diff = p.compare_schema(_desired(), _observed(include_legacy=True))
    assert [item.kind for item in diff.operations] == ["add-column", "drop-column"]
    assert diff.operations[1].risk is p.SchemaRisk.LOSSY

    class Session:
        capabilities: ClassVar[dict[str, str]] = {"provider": "postgres"}

        def __init__(self) -> None:
            self.ddl: list[str] = []

        def execute_ddl(self, statement: str) -> None:
            self.ddl.append(statement)

    session = Session()
    with pytest.raises(p.OrmStateError, match="rischio schema non autorizzato"):
        diff.apply(session)
    assert session.ddl == []

    session = Session()
    assert diff.apply(session, allow=("safe", "lossy")) == (
        "add-column",
        "drop-column",
    )


def test_schema_diff_revision_reuses_dag_runner_contract() -> None:
    diff = p.compare_schema(_desired(), _observed())
    migration = diff.migration("auto-001", None)
    assert migration.revision == "auto-001"
    assert migration.downgrade is not None


def test_schema_diff_is_empty_when_shapes_match() -> None:
    diff = p.compare_schema(_desired(), _observed(include_label=True))
    assert diff.is_empty
    assert diff.risks == frozenset()


def test_schema_diff_covers_defaults_checks_and_indexes() -> None:
    desired, observed = _advanced_shapes()
    diff = p.compare_schema(desired, observed)

    assert [operation.kind for operation in diff.operations] == [
        "add-column-default",
        "add-check",
        "create-index",
    ]
    assert diff.operations[0].statement == (
        'ALTER TABLE "schema_scores" ALTER COLUMN "score" SET DEFAULT 0'
    )
    assert 'CHECK ("score" >= 0)' in (diff.operations[1].statement or "")
    assert diff.operations[2].reverse_statement == (
        'DROP INDEX "ix_schema_scores_score"'
    )


def test_schema_diff_does_not_infer_unmeasured_index_or_column_capabilities() -> None:
    desired, observed = _advanced_shapes(
        constraints=[
            {
                "name": "ck_score_nonnegative",
                "kind": "check",
                "definition": "CHECK (score < 0)",
                "columns": {"Observed": ["score"]},
                "native": {"Postgres": {}},
            }
        ],
        indexes=[
            {
                "name": "ix_schema_scores_score",
                "unique": None,
                "primary": False,
                "elements": {
                    "Observed": [
                        {
                            "expression": "score",
                            "included": None,
                            "descending": None,
                            "native": {"Postgres": {}},
                        }
                    ]
                },
                "predicate": None,
                "spatial": None,
                "native": {"Postgres": {}},
            }
        ],
        identity=None,
        generated=None,
    )
    diff = p.compare_schema(desired, observed)

    assert [operation.kind for operation in diff.operations] == ["alter-check"]
    assert diff.operations[0].risk is p.SchemaRisk.UNSUPPORTED
