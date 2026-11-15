"""Regressioni del piano locale contro i workflow, fonte del contratto CI."""
from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from scripts import ci_locale as ci


class PlanTests(unittest.TestCase):
    def assert_coverage(self, root, plan):
        """Lettore indipendente: niente build_plan/discover per l'atteso."""
        expected = {}
        for path in sorted((root / ".github/workflows").glob("*")):
            if path.suffix not in {".yml", ".yaml"}:
                continue
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
            for name, job in document["jobs"].items():
                expected[path.relative_to(root).as_posix() + "/" + name] = job
        actual = {entry["key"]: entry["raw"] for entry in plan["jobs"] if "parent" not in entry}
        self.assertEqual(expected, actual)
        for entry in plan["jobs"]:
            if "parent" in entry:
                continue
            steps = expected[entry["key"]].get("steps", [])
            direct = [item["raw"] for item in entry["steps"]
                      if item["source"].rsplit("/steps/", 1)[0] == entry["key"]]
            self.assertEqual(steps, direct, entry["id"])

    def fixture(self, root, jobs, events=None):
        directory = root / ".github/workflows"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "test.yml"
        path.write_text(yaml.safe_dump({"on": events or {"pull_request": None}, "jobs": jobs}, sort_keys=False), encoding="utf-8")
        return path

    def job(self, steps):
        return {"runs-on": "windows-latest" if ci.host_os() == "Windows" else "ubuntu-latest", "steps": steps}

    def test_repository_coverage_and_gate_commands_are_exact(self):
        plan = ci.build_plan()
        self.assert_coverage(ci.ROOT, plan)
        steps = [s["step"].get("run") for j in plan["jobs"] for s in j["steps"]]
        self.assertIn("cargo clippy --workspace --all-targets --all-features -- -D warnings", steps)
        self.assertIn("bash .github/scripts/offline_suite.sh", steps)
        self.assertIn("cargo fmt --all -- --check", steps)
        self.assertTrue(any("scripts/check_arrow_ingress.py" == str(s).split(" ")[-1] for s in steps))

    def test_new_job_or_step_cannot_disappear(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            jobs = {"first": self.job([{"run": "cargo test"}])}
            self.fixture(root, jobs)
            old = ci.build_plan(root)
            jobs["second"] = self.job([{"run": "cargo check"}])
            self.fixture(root, jobs)
            with self.assertRaises(AssertionError):
                self.assert_coverage(root, old)
            self.assert_coverage(root, ci.build_plan(root))
            old = ci.build_plan(root)
            jobs["first"]["steps"].append({"run": "cargo fmt --all -- --check"})
            self.fixture(root, jobs)
            with self.assertRaises(AssertionError):
                self.assert_coverage(root, old)
            fresh = ci.build_plan(root)
            fresh["jobs"][0]["steps"].pop()
            with self.assertRaises(AssertionError):
                self.assert_coverage(root, fresh)

    def test_matrix_include_exclude_and_context(self):
        cells = ci.matrix_cells({"os": ["linux", "windows"], "python": ["3.14", "3.15"],
                                 "exclude": [{"os": "linux", "python": "3.15"}],
                                 "include": [{"os": "windows", "artifact": "wheel"}, {"os": "mac"}]})
        self.assertEqual(cells, [{"os": "linux", "python": "3.14"},
                                {"os": "windows", "python": "3.14", "artifact": "wheel"},
                                {"os": "windows", "python": "3.15", "artifact": "wheel"}, {"os": "mac"}])
        self.assertEqual(ci.matrix_cells({"include": [{"x": 1}, {"x": 2}]}), [{"x": 1}, {"x": 2}])
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            job = self.job([{"run": "echo ${{ matrix.x }}", "env": {"B": "step"}}])
            job.update(strategy={"matrix": {"x": [1, 2]}}, env={"A": "job", "B": "job"},
                       defaults={"run": {"shell": "bash", "working-directory": "scripts"}})
            self.fixture(root, {"matrix": job})
            entries = ci.build_plan(root)["jobs"]
            self.assertEqual([j["steps"][0]["step"]["run"] for j in entries], ["echo 1", "echo 2"])
            self.assertEqual(entries[0]["steps"][0]["env"], {"A": "job", "B": "step"})
            self.assertEqual(entries[0]["steps"][0]["working_directory"], "scripts")

    def test_local_reusable_workflow_and_composite(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.fixture(root, {"caller": {"uses": "./.github/workflows/child.yml", "with": {"value": "provided"}}})
            (root / ".github/workflows/child.yml").write_text(yaml.safe_dump({
                "on": {"workflow_call": {"inputs": {"value": {"default": "fallback"}}}},
                "jobs": {"child": self.job([{"uses": "./.github/actions/local", "with": {"value": "${{ inputs.value }}"}}])}}), encoding="utf-8")
            directory = root / ".github/actions/local"
            directory.mkdir(parents=True)
            (directory / "action.yml").write_text(yaml.safe_dump({"inputs": {"value": {"default": "default"}},
                "runs": {"using": "composite", "steps": [{"shell": "bash", "run": "echo ${{ inputs.value }}"}]}}), encoding="utf-8")
            plan = ci.build_plan(root)
            child = next(j for j in plan["jobs"] if "parent" in j)
            self.assertTrue(child["pr"])
            self.assertEqual(child["steps"][-1]["step"]["run"], "echo provided")
            self.assertEqual(len(child["steps"]), 2)

    def test_actions_mapping_is_small_and_fail_closed(self):
        self.assertEqual(set(ci.ACTIONS.values()), {"checkout", "python", "rust", "cache"})
        for use in ci.ACTIONS:
            inputs = {}
            if "setup-python" in use:
                inputs = {"python-version": f"{sys.version_info.major}.{sys.version_info.minor}"}
            if "rust-toolchain" in use:
                inputs = {"toolchain": "1.98.1"}
            argv, reason = ci.action_command({"uses": use + "@pin", "with": inputs}, ci.ROOT)
            self.assertTrue(bool(argv) != bool(reason))
        self.assertIsNone(ci.action_command({"uses": "unknown/action@pin"}, ci.ROOT)[0])
        self.assertIsNone(ci.action_command({"uses": "actions/setup-python@pin", "with": {"python-version": "0.0"}}, ci.ROOT)[0])

    def test_failed_process_is_never_pass(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            result = ci.subprocess_result([sys.executable, "-c", "raise SystemExit(7)"], root, {}, root / "failure.log", 10)
            self.assertEqual(result["outcome"], "FAIL")
            self.assertEqual(result["returncode"], 7)

    def test_receipt_keeps_failure_and_runs_always(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.fixture(root, {"test": self.job([
                {"run": "cargo test"}, {"run": "cargo check"},
                {"run": "cargo fmt", "if": "always()"}])})
            with patch.object(ci, "git_state", return_value={"sha": "abc", "dirty": True}), \
                 patch.object(ci, "subprocess_result", side_effect=[
                     {"outcome": "FAIL", "returncode": 7}, {"outcome": "PASS", "returncode": 0}]), \
                 contextlib.redirect_stdout(io.StringIO()):
                receipt, code = ci.execute(ci.build_plan(root), root, root / "target/run", [], False)
            self.assertEqual(code, 1)
            self.assertEqual([r["outcome"] for r in receipt["results"]], ["FAIL", "NON ESEGUITO", "PASS"])
            persisted = json.loads((root / "target/run/ricevuta.json").read_text(encoding="utf-8"))
            self.assertEqual(persisted["results"][0]["returncode"], 7)

    def test_release_live_and_unknown_context_never_start(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for step in ({"run": "docker compose up -d"}, {"run": "git push origin main"},
                         {"run": "echo ${{ secrets.TOKEN }}"}, {"uses": "new/action@pin"}):
                self.fixture(root, {"guard": self.job([step])})
                job = ci.build_plan(root)["jobs"][0]
                self.assertNotEqual(ci.assess(job, job["steps"][0], root)[0], "ESEGUIBILE")
            self.fixture(root, {"guard": self.job([{"run": "cargo test"}])}, {"release": None, "pull_request": None})
            job = ci.build_plan(root)["jobs"][0]
            self.assertEqual(ci.assess(job, job["steps"][0], root, True)[1], "release/pubblicazione vietata")

    def test_needs_order_and_path_confinement(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            dependent = self.job([{"run": "cargo test"}])
            dependent["needs"] = "build"
            self.fixture(root, {"test": dependent, "build": self.job([{"run": "cargo build"}])})
            ordered = ci.ordered_jobs(ci.build_plan(root)["jobs"])
            self.assertTrue(ordered[0]["key"].endswith("/build"))
            with self.assertRaises(ValueError):
                ci.local_path(root, "../outside")

    def test_empty_or_dynamic_matrix_is_visible(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for matrix in ({"x": []}, "${{ fromJSON(needs.plan.outputs.matrix) }}"):
                job = self.job([{"run": "cargo test"}])
                job["strategy"] = {"matrix": matrix}
                self.fixture(root, {"test": job})
                plan = ci.build_plan(root)
                self.assert_coverage(root, plan)
                self.assertTrue(plan["jobs"][0]["reason"])
                self.assertEqual(ci.assess(plan["jobs"][0], plan["jobs"][0]["steps"][0], root)[0], "NON ESEGUIBILE")

    def test_false_platform_condition_does_not_block_next_step(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.fixture(root, {"test": self.job([
                {"run": "sudo apt-get install example", "if": False}, {"run": "cargo test"}])})
            with patch.object(ci, "git_state", return_value={"sha": "abc", "dirty": True}), \
                 patch.object(ci, "subprocess_result", return_value={"outcome": "PASS", "returncode": 0}) as run, \
                 contextlib.redirect_stdout(io.StringIO()):
                receipt, _ = ci.execute(ci.build_plan(root), root, root / "target/run", [], False)
            run.assert_called_once()
            self.assertEqual([r["outcome"] for r in receipt["results"]], ["SALTATO", "PASS"])

    def test_portable_fixed_runner_does_not_claim_other_matrix_cell(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            job = {"runs-on": "ubuntu-latest", "steps": [{"run": "cargo fmt --all -- --check"}]}
            self.fixture(root, {"fmt": job})
            with patch.object(ci, "host_os", return_value="Windows"):
                entry = ci.build_plan(root)["jobs"][0]
                self.assertEqual(ci.assess(entry, entry["steps"][0], root)[0], "ESEGUIBILE")
                job.update({"runs-on": "${{ matrix.os }}", "strategy": {"matrix": {"os": ["ubuntu-latest"]}}})
                self.fixture(root, {"fmt": job})
                entry = ci.build_plan(root)["jobs"][0]
                self.assertEqual(ci.assess(entry, entry["steps"][0], root)[0], "SALTATO")


if __name__ == "__main__":
    unittest.main()
