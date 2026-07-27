from __future__ import annotations

import unittest

from scripts.phase0_validate import (
    CONTRACT_ROOT,
    ValidationError,
    build_registry,
    discover_schemas,
    run_gate,
    validate_instance,
)


class Phase0ValidateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schemas = discover_schemas(CONTRACT_ROOT)
        cls.registry = build_registry(cls.schemas.values())

    def test_repository_gate_passes(self) -> None:
        report = run_gate()
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["database_connections_opened"], 0)
        self.assertTrue(
            all(check["status"] == "passed" for check in report["checks"])
        )

    def test_plan_rejects_inline_unknown_properties(self) -> None:
        schema = self.schemas[(CONTRACT_ROOT / "plan.schema.json").resolve()]
        invalid = {
            "schema_version": 1,
            "connection_ref": "env:TEST_DSN",
            "provider": "postgres",
            "password": "must-not-be-here",
            "operation": {"id": "database.test_connection"},
        }
        with self.assertRaises(ValidationError):
            validate_instance(
                invalid, schema, self.registry, "invalid plan"
            )

    def test_outcome_unknown_requires_recovery(self) -> None:
        schema = self.schemas[
            (CONTRACT_ROOT / "write-outcome.schema.json").resolve()
        ]
        invalid = {
            "schema_version": 1,
            "status": "outcome_unknown",
            "execution_id": "test",
            "provider": "arcgis",
            "rows": {
                "received": 1,
                "confirmed": 0,
                "failed": 0,
                "skipped": 0,
            },
        }
        with self.assertRaises(ValidationError):
            validate_instance(
                invalid, schema, self.registry, "invalid outcome"
            )

    def test_plan_rejects_provider_operation_mismatch(self) -> None:
        schema = self.schemas[(CONTRACT_ROOT / "plan.schema.json").resolve()]
        invalid = {
            "schema_version": 1,
            "connection_ref": "env:TEST_DSN",
            "provider": "postgres",
            "operation": {
                "id": "arcgis.read",
                "source": {"object": "layer", "layer_id": 0},
            },
        }
        with self.assertRaises(ValidationError):
            validate_instance(
                invalid, schema, self.registry, "provider mismatch"
            )

    def test_upsert_requires_keys(self) -> None:
        schema = self.schemas[(CONTRACT_ROOT / "plan.schema.json").resolve()]
        invalid = {
            "schema_version": 1,
            "connection_ref": "env:TEST_DSN",
            "provider": "postgres",
            "operation": {
                "id": "database.write",
                "target": {"schema": "public", "object": "events"},
                "mode": "upsert",
                "mapping_policy": "strict",
                "transaction_profile": "single_transaction",
            },
        }
        with self.assertRaises(ValidationError):
            validate_instance(
                invalid, schema, self.registry, "upsert without keys"
            )


if __name__ == "__main__":
    unittest.main()
