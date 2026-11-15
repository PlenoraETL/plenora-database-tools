"""Regressioni del piano locale contro i workflow, fonte del contratto CI."""
from __future__ import annotations

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from scripts import ci_locale as ci


class PlanTests(unittest.TestCase):
    def setUp(self):
        (ci.ROOT / "target").mkdir(exist_ok=True)

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
        return {"runs-on": "windows-latest" if ci.host_os() == "Windows" else "ubuntu-latest",
                "defaults": {"run": {"shell": "python"}}, "steps": steps}

    def test_repository_coverage_and_gate_commands_are_exact(self):
        plan = ci.build_plan()
        self.assert_coverage(ci.ROOT, plan)
        steps = [s["step"].get("run") for j in plan["jobs"] for s in j["steps"]]
        self.assertIn("cargo clippy --workspace --all-targets --all-features -- -D warnings", steps)
        self.assertIn("bash .github/scripts/offline_suite.sh", steps)
        self.assertIn("cargo fmt --all -- --check", steps)
        self.assertTrue(any("scripts/check_arrow_ingress.py" == str(s).split(" ")[-1] for s in steps))

    def test_new_job_or_step_cannot_disappear(self):
        with tempfile.TemporaryDirectory(dir=ci.ROOT / 'target') as folder:
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
        version = (ci.ROOT / ".python-version").read_text().strip()
        alternative = version + "-fixture"
        cells = ci.matrix_cells({"os": ["linux", "windows"], "python": [version, alternative],
                                 "exclude": [{"os": "linux", "python": alternative}],
                                 "include": [{"os": "windows", "artifact": "wheel"}, {"os": "mac"}]})
        self.assertEqual(cells, [{"os": "linux", "python": version},
                                {"os": "windows", "python": version, "artifact": "wheel"},
                                {"os": "windows", "python": alternative, "artifact": "wheel"}, {"os": "mac"}])
        self.assertEqual(ci.matrix_cells({"include": [{"x": 1}, {"x": 2}]}), [{"x": 1}, {"x": 2}])
        with tempfile.TemporaryDirectory(dir=ci.ROOT / 'target') as folder:
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
        with tempfile.TemporaryDirectory(dir=ci.ROOT / 'target') as folder:
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
        with tempfile.TemporaryDirectory(dir=ci.ROOT / 'target') as folder:
            root = Path(folder)
            result = ci.subprocess_result([sys.executable, "-c", "raise SystemExit(7)"], root, {}, root / "failure.log", 10)
            self.assertEqual(result["status"], "FAIL")
            self.assertEqual(result["exit_code"], 7)

    def test_receipt_keeps_failure_and_runs_always(self):
        with tempfile.TemporaryDirectory(dir=ci.ROOT / 'target') as folder:
            root = Path(folder)
            self.fixture(root, {"test": self.job([
                {"run": "cargo test"}, {"run": "cargo check"},
                {"run": "cargo fmt", "if": "always()"}])})
            with patch.object(ci, "git_state", return_value={"sha": "abc", "dirty": True}), \
                 patch.object(ci, "subprocess_result", side_effect=[
                     {"status": "FAIL", "exit_code": 7}, {"status": "PASS", "exit_code": 0}]), \
                 contextlib.redirect_stdout(io.StringIO()):
                receipt, code = ci.execute(ci.build_plan(root), root, root / "target/run", [], False)
            self.assertEqual(code, 1)
            self.assertEqual([r["status"] for r in receipt["steps"]], ["FAIL", "NON ESEGUITO", "PASS"])
            persisted = json.loads((root / "target/run/ricevuta.json").read_text(encoding="utf-8"))
            self.assertEqual(persisted["steps"][0]["exit_code"], 7)

    def test_release_live_and_unknown_context_never_start(self):
        with tempfile.TemporaryDirectory(dir=ci.ROOT / 'target') as folder:
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
        with tempfile.TemporaryDirectory(dir=ci.ROOT / 'target') as folder:
            root = Path(folder)
            dependent = self.job([{"run": "cargo test"}])
            dependent["needs"] = "build"
            self.fixture(root, {"test": dependent, "build": self.job([{"run": "cargo build"}])})
            ordered = ci.ordered_jobs(ci.build_plan(root)["jobs"])
            self.assertTrue(ordered[0]["key"].endswith("/build"))
            with self.assertRaises(ValueError):
                ci.local_path(root, "../outside")

    def test_empty_or_dynamic_matrix_is_visible(self):
        with tempfile.TemporaryDirectory(dir=ci.ROOT / 'target') as folder:
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
        with tempfile.TemporaryDirectory(dir=ci.ROOT / 'target') as folder:
            root = Path(folder)
            self.fixture(root, {"test": self.job([
                {"run": "sudo apt-get install example", "if": False}, {"run": "cargo test"}])})
            with patch.object(ci, "git_state", return_value={"sha": "abc", "dirty": True}), \
                 patch.object(ci, "subprocess_result", return_value={"status": "PASS", "exit_code": 0}) as run, \
                 contextlib.redirect_stdout(io.StringIO()):
                receipt, _ = ci.execute(ci.build_plan(root), root, root / "target/run", [], False)
            run.assert_called_once()
            self.assertEqual([r["status"] for r in receipt["steps"]], ["NON ESEGUITO", "PASS"])

    def test_fixed_and_matrix_runner_require_declared_platform(self):
        with tempfile.TemporaryDirectory(dir=ci.ROOT / 'target') as folder:
            root = Path(folder)
            job = {"runs-on": "ubuntu-latest", "steps": [{"run": "cargo fmt --all -- --check"}]}
            self.fixture(root, {"fmt": job})
            with patch.object(ci, "host_os", return_value="Windows"):
                entry = ci.build_plan(root)["jobs"][0]
                self.assertEqual(ci.assess(entry, entry["steps"][0], root)[0], "NON ESEGUIBILE")
                job.update({"runs-on": "${{ matrix.os }}", "strategy": {"matrix": {"os": ["ubuntu-latest"]}}})
                self.fixture(root, {"fmt": job})
                entry = ci.build_plan(root)["jobs"][0]
                self.assertEqual(ci.assess(entry, entry["steps"][0], root)[0], "NON ESEGUIBILE")


class EnvironmentTests(unittest.TestCase):
    fixture = PlanTests.fixture
    job = PlanTests.job

    def setUp(self):
        (ci.ROOT / "target").mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=ci.ROOT / "target")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def run_steps(self, steps, **options):
        self.fixture(self.root, {"test": self.job(steps)})
        with patch.object(ci, "git_state", return_value={"sha": "abc", "dirty": True}), contextlib.redirect_stdout(io.StringIO()):
            return ci.execute(ci.build_plan(self.root), self.root, self.root / "target/run", [], False, **options)

    def test_bootstrap_without_yaml(self):
        result = subprocess.run([sys.executable, "-S", str(ci.ROOT / "scripts/ci_locale.py"), "--piano"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("python -m pip install -r requirements-ci-locale.txt", result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        self.assertEqual((ci.ROOT / "requirements-ci-locale.txt").read_text().strip(), "PyYAML==6.0.3")

    def test_external_checkout_pin_outputs_and_wrong_ref(self):
        source = self.root / "source"
        source.mkdir()
        def git(*args):
            return subprocess.check_output(["git", "-C", str(source), *args], text=True).strip()
        git("init")
        git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
            "commit", "--allow-empty", "-m", "fixture")
        sha = git("rev-parse", "HEAD")
        command, reason = ci.action_command({"uses": "actions/checkout@pin", "with": {
            "ref": sha, "path": "historical"}}, source, offline=True)
        self.assertEqual(reason, "")
        self.assertIn(str(source.resolve()), command)
        (self.root / "pin.txt").write_text(sha, encoding="utf-8")
        read_pin = {"id": "pin", "shell": "python", "run":
            "import os\nfrom pathlib import Path\n"
            "Path(os.environ['GITHUB_OUTPUT']).write_text('sha=' + Path('pin.txt').read_text() + '\\n')\n"}
        checkout = {"uses": "actions/checkout@pin", "with": {
            "repository": "Fixture/contracts", "ref": "${{ steps.pin.outputs.sha }}", "path": "contracts"}}
        with patch.dict(os.environ, {"CI_LOCALE_SORGENTE_CONTRACTS": str(source)}):
            for offline in (True, False):
                receipt, code = self.run_steps([read_pin, checkout], offline=offline)
                self.assertEqual(code, 0, receipt["steps"])
                self.assertEqual(ci.git(self.root / "contracts", "rev-parse", "HEAD"), sha)
                evidence = json.loads(Path(receipt["steps"][-1]["log"]).read_text().splitlines()[-1])
                self.assertEqual(evidence["actual_sha"], sha)
                self.assertEqual(evidence["expected_sha"], sha)
            checkout["with"]["ref"] = "f" * 40
            receipt, code = self.run_steps([checkout], offline=True)
            self.assertEqual((code, receipt["steps"][0]["status"]), (1, "FAIL"))
            self.assertEqual(ci.git(self.root / "contracts", "rev-parse", "HEAD"), sha)

    def test_external_checkout_mapping_and_path_guards(self):
        step = {"uses": "actions/checkout@pin", "with": {
            "repository": "Fixture/contracts", "ref": "a" * 40, "path": "contracts"}}
        with patch.dict(os.environ, {}, clear=True):
            command, reason = ci.action_command(step, self.root, offline=True)
            self.assertIsNone(command)
            self.assertIn("offline", reason)
            command, reason = ci.action_command(step, self.root)
            self.assertEqual(reason, "")
            self.assertIn("https://github.com/Fixture/contracts.git", command)
        for path in (".", "../outside"):
            step["with"]["path"] = path
            self.fixture(self.root, {"test": self.job([step])})
            entry = ci.build_plan(self.root)["jobs"][0]
            self.assertEqual(ci.assess(entry, entry["steps"][0], self.root)[0], "NON ESEGUIBILE")

    def test_external_checkout_existing_wrong_sha_fails(self):
        source = self.root / "source"
        source.mkdir()
        def git(*args):
            return subprocess.check_output(["git", "-C", str(source), *args], text=True).strip()
        git("init")
        commits = []
        for message in ("first", "second"):
            git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                "commit", "--allow-empty", "-m", message)
            commits.append(git("rev-parse", "HEAD"))
        step = {"uses": "actions/checkout@pin", "with": {
            "repository": "Fixture/contracts", "ref": commits[0], "path": "contracts"}}
        with patch.dict(os.environ, {"CI_LOCALE_SORGENTE_CONTRACTS": str(source)}):
            self.assertEqual(self.run_steps([step], offline=True)[1], 0)
            step["with"]["ref"] = commits[1]
            receipt, code = self.run_steps([step], offline=True)
            self.assertEqual((code, receipt["steps"][0]["status"]), (1, "FAIL"))

    def test_secondary_checkout_pin_output_reuse_and_wrong_ref(self):
        source = self.root / "source"
        source.mkdir()
        def git(*args):
            return subprocess.check_output(["git", "-C", str(source), *args], text=True, stderr=subprocess.PIPE).strip()
        git("init")
        git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
            "commit", "--allow-empty", "-m", "fixture")
        sha = git("rev-parse", "HEAD")
        (self.root / "pin.txt").write_text(sha)
        pin = {"id": "pin", "run": "import os,pathlib\npin=pathlib.Path('pin.txt').read_text()\npathlib.Path(os.environ['GITHUB_OUTPUT']).write_text('sha='+pin+'\\n')"}
        step = {"uses": "actions/checkout@pin", "with": {"repository": "Fixture/contracts",
                "ref": "${{ steps.pin.outputs.sha }}", "path": "dependency", "fetch-depth": 0}}
        with patch.dict(os.environ, {"CI_LOCALE_SORGENTE_CONTRACTS": str(source)}):
            for _ in range(2):
                receipt, code = self.run_steps([pin, step], offline=True)
                self.assertEqual(code, 0, receipt)
                self.assertEqual(receipt["steps"][1]["with"]["ref"], sha)
            (self.root / "pin.txt").write_text("f" * 40)
            receipt, code = self.run_steps([pin, step], offline=True)
            self.assertEqual((code, receipt["steps"][1]["status"]), (1, "FAIL"))
            # An existing, valid but different commit must also fail SHA verification.
            git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                "commit", "--allow-empty", "-m", "second")
            step["with"].update(path="second", ref=git("rev-parse", "HEAD"))
            receipt, code = self.run_steps([step], offline=True)
            self.assertEqual(code, 0, receipt)
            step["with"]["ref"] = sha
            receipt, code = self.run_steps([step], offline=True)
            self.assertEqual((code, receipt["steps"][0]["status"]), (1, "FAIL"))

    def test_secondary_checkout_mapping_offline_online_and_confinement(self):
        step = {"uses": "actions/checkout@pin", "with": {"repository": "Fixture/unconfigured",
                "ref": "a" * 40, "path": "dependency"}}
        with patch.dict(os.environ, {}, clear=True):
            command, reason = ci.action_command(step, self.root, offline=True)
            self.assertIsNone(command)
            self.assertIn("offline", reason)
            command, reason = ci.action_command(step, self.root)
            self.assertIn("https://github.com/Fixture/unconfigured.git", command)
        step["with"]["path"] = "../outside"
        with self.assertRaises(ValueError):
            ci.action_command(step, self.root)

    def test_bash_nested_python_uses_runner_interpreter(self):
        if not ci.shutil.which("bash"):
            self.skipTest("bash unavailable")
        (self.root / "probe.py").write_text(
            "import sys\nfrom pathlib import Path\nassert Path(sys.executable).resolve() == Path(" + repr(sys.executable) + ").resolve()\n")
        receipt, code = self.run_steps([{"shell": "bash", "run":
            "python probe.py\npython3 probe.py\nbash -c 'python3 probe.py'"}])
        log = receipt["steps"][0]["log"]
        self.assertEqual(code, 0, Path(log).read_text() if log else receipt["steps"])

    def test_network_and_wheelhouse_preconditions(self):
        for command in ("python -m pip install jsonschema==4.26.0", "cargo fetch --locked", "cargo test", "curl https://example.invalid", "git fetch --tags"):
            with self.subTest(command=command):
                with patch.object(ci, "subprocess_result") as run:
                    receipt, code = self.run_steps([{"run": command}], offline=True)
                run.assert_not_called()
                row = receipt["steps"][0]
                self.assertEqual((code, row["status"]), (2, "NON ESEGUITO"))
                self.assertIn("offline", row["reason"])
                self.assertIsNone(row["exit_code"])
                self.assertIsNone(row["log"])
                self.assertEqual(receipt["network_mode"], "offline")
        (self.root / "wheels").mkdir()
        command = "python -m pip install --no-index --find-links wheels demo"
        self.assertEqual(ci.preconditions({}, {"run": command}, self.root, command, True), "")
        self.assertIn("offline", ci.preconditions({}, {"run": command}, self.root, command.replace("wheels", "absent"), True))
        with patch.object(ci.shutil, "which", return_value="tool"):
            self.assertEqual(ci.preconditions({}, {"run": "cargo fmt"}, self.root, "cargo fmt", True), "")

    def test_direct_script_download_is_detected(self):
        (self.root / "scripts").mkdir()
        (self.root / "scripts/probe.py").write_text("import urllib.request\nurllib.request.urlretrieve('https://example.invalid', 'file')")
        receipt, _ = self.run_steps([{"run": "python scripts/probe.py"}], offline=True)
        self.assertIn("offline", receipt["steps"][0]["reason"])

    def test_executed_failure_and_policy_override_in_both_modes(self):
        script = self.root / "probe.py"
        for offline in (False, True):
            script.write_text("import os\nassert os.environ['PIP_NO_INDEX'] == " + repr("1" if offline else "0") + "\nassert os.environ['CARGO_BUILD_JOBS'] == '4'\nassert os.environ['CARGO_INCREMENTAL'] == '0'\nraise SystemExit(7)\n")
            with patch.dict(os.environ, {"PIP_NO_INDEX": "wrong", "CARGO_BUILD_JOBS": "99"}):
                receipt, code = self.run_steps([{"run": "python probe.py", "env": {"CARGO_INCREMENTAL": "wrong"}, "continue-on-error": True}], offline=offline)
            row = receipt["steps"][0]
            self.assertEqual((code, row["status"], row["exit_code"]), (1, "FAIL", 7))
            self.assertEqual(receipt["jobs"][0]["status"], "FAIL")
            self.assertEqual(receipt["network_mode"], "offline" if offline else "online")
            self.assertTrue(Path(row["log"]).is_file())
        self.assertEqual(ci.policy_env(False)["CARGO_NET_OFFLINE"], "false")

    def test_start_error_timeout_and_skipped_log(self):
        for error, expected in ((OSError(), "NON ESEGUITO"), (subprocess.TimeoutExpired("probe", 1), "FAIL")):
            with patch.object(ci.subprocess, "run", side_effect=error):
                row = ci.subprocess_result(["probe"], self.root, {}, self.root / "probe.log", 1)
            self.assertEqual(row["status"], expected)
            if expected == "NON ESEGUITO":
                self.assertIsNone(row["log"])
                self.assertIsNone(row["exit_code"])
        (self.root / "probe.py").write_text("print('skipping test')")
        receipt, code = self.run_steps([{"run": "python probe.py"}])
        self.assertEqual((code, receipt["steps"][0]["status"]), (2, "NON ESEGUITO"))

    def test_tools_history_and_setup(self):
        for command in ("pwsh probe.ps1", "gdal-config --version", "docker info", "cargo build", "git status"):
            with patch.object(ci.shutil, "which", return_value=None):
                self.assertIn("assente", ci.preconditions({}, {"run": command}, self.root, command, False))
        checkout = {"uses": "actions/checkout@pin", "with": {"fetch-depth": 0}}
        with patch.object(ci, "git", return_value="true"):
            self.assertIn("shallow", ci.preconditions({}, checkout, self.root, "", False))
        with patch.object(ci, "git", return_value="false"):
            self.assertEqual(ci.preconditions({}, checkout, self.root, "", False), "")
        rust = {"uses": "dtolnay/rust-toolchain@pin", "with": {"toolchain": "1.98.1", "components": "clippy"}}
        with patch.object(ci.shutil, "which", return_value="tool"), patch.object(ci.subprocess, "check_output", return_value=""):
            self.assertIn("toolchain Rust assente", ci.preconditions({}, rust, self.root, "", False))
        with patch.object(ci.shutil, "which", return_value="tool"), patch.object(ci.subprocess, "check_output", side_effect=["1.98.1-host", "rustfmt-host"]):
            self.assertIn("componente Rust assente", ci.preconditions({}, rust, self.root, "", False))
        source = self.root / "scripts/compare_historical_readers.py"
        source.parent.mkdir()
        source.write_text("BASELINE_REVISION = 'required-tag'")
        command = "python scripts/compare_historical_readers.py --old old --new new"
        with patch.object(ci, "git", side_effect=subprocess.CalledProcessError(1, "git")):
            self.assertIn("storia/tag", ci.preconditions({}, {"run": command}, self.root, command, False))
        with patch.object(ci, "git", return_value="commit"):
            self.assertIn("binario", ci.preconditions({}, {"run": command}, self.root, command, False))
            (self.root / "old").touch()
            (self.root / "new").touch()
            self.assertEqual(ci.preconditions({}, {"run": command}, self.root, command, False), "")

    def test_services_shell_platform_and_python_missing(self):
        for step, extra, reason in (
            ({"run": "echo probe", "shell": "missing-shell"}, {}, "shell"),
            ({"run": "python probe.py"}, {"services": {"db": {"image": "postgres"}}}, "servizi"),
            ({"uses": "actions/setup-python@pin", "with": {"python-version": "0.0"}}, {}, "Python"),
        ):
            self.fixture(self.root, {"probe": {**self.job([step]), **extra}})
            entry = ci.build_plan(self.root)["jobs"][0]
            status, message, _ = ci.assess(entry, entry["steps"][0], self.root)
            self.assertEqual(status, "NON ESEGUIBILE")
            self.assertIn(reason, message)
        self.fixture(self.root, {"probe": self.job([{"run": "python probe.py", "shell": "pwsh"}])})
        entry = ci.build_plan(self.root)["jobs"][0]
        with patch.object(ci.shutil, "which", return_value=None):
            status, message, _ = ci.assess(entry, entry["steps"][0], self.root)
        self.assertEqual(status, "NON ESEGUIBILE")
        self.assertIn("shell", message)

    def test_wheelhouse_failure_is_not_reclassified(self):
        (self.root / "wheels").mkdir()
        # Empty wheelhouse: pip really starts, fails, and its status remains FAIL.
        receipt, code = self.run_steps([{"run": "python -m pip install --no-index --find-links wheels ci-local-nonexistent-package"}], offline=True)
        self.assertEqual((code, receipt["steps"][0]["status"]), (1, "FAIL"))

    def test_receipt_common_fields_and_incremental_save(self):
        (self.root / "probe.py").write_text("print('ok')")
        original = ci.subprocess_result
        calls = []
        def observe(*args):
            if calls:
                saved = json.loads((self.root / "target/run/ricevuta.json").read_text(encoding="utf-8"))
                self.assertEqual(len(saved["steps"]), 1)
            calls.append(True)
            return original(*args)
        with patch.object(ci, "subprocess_result", side_effect=observe):
            receipt, code = self.run_steps([{"run": "python probe.py"}, {"run": "python probe.py"}])
        self.assertEqual(code, 0)
        self.assertEqual(receipt["schema"], 2)
        for field in "schema network_mode services_opt_in sha dirty started_at host python selected_jobs profile sources overrides jobs steps counts finished_at final_sha final_dirty exit_code".split():
            self.assertIn(field, receipt)
        for field in "job source raw name run uses with shell working_directory env if continue_on_error toolchain status reason services container duration_seconds exit_code command environment log condition_false".split():
            self.assertIn(field, receipt["steps"][0])
        self.assertEqual(receipt["counts"], {"PASS": 2})

    def test_cli_plan_selection_unknown_and_empty(self):
        self.fixture(self.root, {"test": self.job([{"run": "python probe.py"}])})
        with patch.object(ci, "ROOT", self.root), contextlib.redirect_stdout(io.StringIO()) as out:
            before = set(self.root.rglob("*"))
            self.assertEqual(ci.main(["--piano"]), 0)
            self.assertEqual(set(self.root.rglob("*")), before)
            self.assertEqual(json.loads(out.getvalue())["network_mode"], "online")
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                ci.main(["--piano", "--job", "unknown"])
            self.assertEqual(error.exception.code, 2)
        self.fixture(self.root, {})
        with patch.object(ci, "git_state", return_value={"sha": "abc", "dirty": False}):
            _, code = ci.execute(ci.build_plan(self.root), self.root, self.root / "empty", [], False)
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
