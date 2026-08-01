#!/usr/bin/env python3
"""Regressioni same-SHA per i workflow di assurance Database."""

from __future__ import annotations

import unittest
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"
CANDIDATE_EXPRESSION = (
    "CANDIDATE_SHA: ${{ github.event_name == 'pull_request' "
    "&& github.event.pull_request.head.sha || github.sha }}"
)
BOUND_WORKFLOWS = (
    "ewkb-fuzz.yml",
    "mysql-assurance.yml",
    "postgres-postgis-assurance.yml",
    "postgres-postgis-matrix.yml",
    "release-manifest.yml",
    "rust-coverage.yml",
    "sqlserver-assurance.yml",
    "sqlserver-azure-assurance.yml",
    "sqlserver-version-matrix.yml",
)


class CandidateShaWorkflowTests(unittest.TestCase):
    def test_every_assurance_workflow_checks_out_and_verifies_candidate_sha(self) -> None:
        for name in BOUND_WORKFLOWS:
            with self.subTest(workflow=name):
                workflow = (WORKFLOWS / name).read_text(encoding="utf-8")
                self.assertIn(CANDIDATE_EXPRESSION, workflow)
                self.assertIn("ref: ${{ env.CANDIDATE_SHA }}", workflow)
                self.assertIn(
                    'test "$(git rev-parse HEAD)" = "$CANDIDATE_SHA"',
                    workflow,
                )

    def test_artifact_names_never_use_event_merge_sha(self) -> None:
        for name in BOUND_WORKFLOWS:
            with self.subTest(workflow=name):
                workflow = (WORKFLOWS / name).read_text(encoding="utf-8")
                self.assertNotIn("${{ github.sha }}", workflow)

    def test_coverage_executes_mysql_live_tests_against_the_pinned_reference(self) -> None:
        workflow = (WORKFLOWS / "rust-coverage.yml").read_text(encoding="utf-8")

        self.assertIn("docker compose -f docker-compose.mysql.yml up -d --wait", workflow)
        self.assertIn("--package plenora-db-mysql", workflow)
        self.assertIn("PLENORA_MYSQL_PASSWORD=\"$mysql_password\" cargo llvm-cov", workflow)
        self.assertIn("docker compose -f docker-compose.mysql.yml down --volumes", workflow)

    def test_fuzz_lock_uses_the_workspace_core_version(self) -> None:
        workspace = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
        fuzz_lock = tomllib.loads((ROOT / "fuzz" / "Cargo.lock").read_text(encoding="utf-8"))
        core = next(
            package
            for package in fuzz_lock["package"]
            if package["name"] == "plenora-database-core"
        )

        self.assertEqual(core["version"], workspace["workspace"]["package"]["version"])


if __name__ == "__main__":
    unittest.main()
