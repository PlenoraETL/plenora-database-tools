from __future__ import annotations

import unittest

from scripts.phase0_validate import (
    ACTIVE_CONTRACT_ROOT,
    ValidationError,
    build_registry,
    discover_schemas,
    run_gate,
    validate_instance,
)


class Phase0ValidateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schemas = discover_schemas(ACTIVE_CONTRACT_ROOT)
        cls.registry = build_registry(cls.schemas.values())

    def plan_schema(self) -> dict:
        return self.schemas[(ACTIVE_CONTRACT_ROOT / "plan.schema.json").resolve()]

    def test_repository_gate_passes(self) -> None:
        report = run_gate()
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["database_connections_opened"], 0)
        self.assertTrue(
            all(check["status"] == "passed" for check in report["checks"])
        )

    def test_plan_rejects_inline_unknown_properties(self) -> None:
        invalid = {
            "schema_version": 2,
            "connection_ref": "env:TEST_DSN",
            "provider": "postgres",
            "password": "must-not-be-here",
            "operation": {"id": "database.test_connection"},
        }
        with self.assertRaises(ValidationError):
            validate_instance(
                invalid, self.plan_schema(), self.registry, "invalid plan"
            )

    def test_outcome_unknown_requires_recovery(self) -> None:
        schema = self.schemas[
            (ACTIVE_CONTRACT_ROOT / "write-outcome.schema.json").resolve()
        ]
        invalid = {
            "schema_version": 2,
            "status": "outcome_unknown",
            "execution_id": "test",
            "provider": "postgres",
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

    def test_plan_rejects_an_operation_outside_the_domain(self) -> None:
        """Le operazioni non-database non sono piu una famiglia separata.

        Nella v1 `arcgis.read` era un'operazione valida, e lo schema si
        limitava a impedire di abbinarla a un provider database. Qui non e
        valida affatto: l'`id` non esiste nel contratto.
        """
        invalid = {
            "schema_version": 2,
            "connection_ref": "env:TEST_DSN",
            "provider": "postgres",
            "operation": {"id": "arcgis.read", "source": {"object": "events"}},
        }
        with self.assertRaises(ValidationError):
            validate_instance(
                invalid, self.plan_schema(), self.registry, "operazione estranea"
            )

    def test_object_ref_rejects_layer_id(self) -> None:
        """Un database non ha layer, e il riferimento non li indirizza.

        La prova sta qui e non in un documento: `object_ref` chiude le
        proprieta impreviste, quindi se `layer_id` rientrasse nello schema
        questo test tornerebbe verde da solo.
        """
        invalid = {
            "schema_version": 2,
            "connection_ref": "env:TEST_DSN",
            "provider": "postgres",
            "operation": {
                "id": "database.read",
                "source": {"object": "events", "layer_id": 0},
            },
        }
        with self.assertRaises(ValidationError):
            validate_instance(
                invalid, self.plan_schema(), self.registry, "layer_id nel piano"
            )

    def test_upsert_requires_keys(self) -> None:
        invalid = {
            "schema_version": 2,
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
                invalid, self.plan_schema(), self.registry, "upsert without keys"
            )


if __name__ == "__main__":
    unittest.main()
