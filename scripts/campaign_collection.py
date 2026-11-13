"""Collect independent campaign stages without turning failures into evidence.

Only harness identifiers and fixed diagnostics enter the machine-readable ledger.
Raw exception messages and test payloads are deliberately never copied there.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
import json
import re
import subprocess

_active = ContextVar("campaign_stage", default=None)
_collection = ContextVar("campaign_collection", default=None)
ERRORS = (RuntimeError, OSError, ValueError, subprocess.TimeoutExpired)
RUST_RESULT = re.compile(r"^test ([\w:]+) \.\.\. (ok|FAILED|ignored)\s*$", re.MULTILINE)


def failure_message(name: str, output: str) -> str:
    """Keep operational locations and numeric server codes, never assertion values."""
    block = re.search(r"---- " + re.escape(name) + r" stdout ----\n(.*?)(?=\n---- |\nfailures:|\ntest result:|\Z)",
                      output, re.DOTALL)
    message = "test did not pass; payload redacted"
    if block:
        location = re.search(r"\b((?:crates|src)/[\w/.-]+\.rs:\d+(?::\d+)?)", block[1])
        code = re.search(r"\bcodice (\d{1,6})\b", block[1])
        if location:
            message += "; location=" + location[1]
        if code:
            message += "; server_code=" + code[1]
    return message


def observe(output: str) -> None:
    """Called before checking a command's exit status, including failed commands."""
    active = _active.get()
    if active is not None:
        active.append(output)


def complete_command(command: list[str]) -> list[str]:
    """Cargo must also finish subsequent test binaries after a binary fails."""
    command = list(command)
    for index in range(len(command) - 1):
        if command[index:index + 2] == ["cargo", "test"]:
            if "--no-fail-fast" not in command:
                command.insert(index + 2, "--no-fail-fast")
            break
    return command


class Collection:
    def __init__(self, gate: str):
        self.gate = gate
        self.results: list[dict[str, str]] = []

    @property
    def failed(self) -> bool:
        return any(row["outcome"] not in ("passed", "ignored") for row in self.results)

    @contextmanager
    def stage(self, version: str, stage: str, expected=()):
        outputs: list[str] = []
        token = _active.set(outputs)
        parent = _collection.set(self)
        error = None
        try:
            yield
        except ERRORS as caught:
            error = caught
            if isinstance(caught, subprocess.TimeoutExpired):
                partial = caught.stdout or ""
                outputs.append(partial.decode(errors="replace") if isinstance(partial, bytes) else partial)
        finally:
            _active.reset(token)
            _collection.reset(parent)
        seen = set()
        output = "\n".join(outputs)
        for name, status in dict.fromkeys(RUST_RESULT.findall(output)):
            expected_name = name if name in expected else name.rsplit("::", 1)[-1]
            # Default libtest runs also report the separately scheduled ignored family.
            if expected and expected_name not in expected and status == "ignored":
                continue
            outcome = {"ok": "passed", "FAILED": "failed", "ignored": "ignored"}[status]
            if expected and expected_name in expected and status == "ignored":
                outcome = "blocked"
            seen.add(expected_name)
            self.results.append(dict(version=version, stage=stage, test=name,
                                     outcome=outcome, message="" if outcome == "passed" else failure_message(name, output)))
        # Pytest -rA lists each case. Parameter values are payload: retain only
        # the function and a stable occurrence number, never the parameter ID.
        for index, match in enumerate(re.finditer(
                r"^(PASSED|FAILED|ERROR|SKIPPED|XFAIL|XPASS) ([\w./:-]+)(?:\[[^\n]*?\])?(?:\s|$)",
                "\n".join(outputs), re.MULTILINE)):
            status, name = match.groups()
            self.results.append(dict(version=version, stage=stage, test=f"{name}#{index}",
                                     outcome="passed" if status == "PASSED" else "ignored" if status in ("SKIPPED", "XFAIL") else "failed",
                                     message="" if status == "PASSED" else "pytest " + status.lower() + "; payload redacted"))
        for name in sorted(set(expected) - seen):
            self.results.append(dict(version=version, stage=stage, test=name,
                                     outcome="blocked", message="no test result observed"))
        self.results.append(dict(version=version, stage=stage, test=stage,
                                 outcome="failed" if error else "passed",
                                 message=("timeout; diagnostic redacted" if isinstance(error, subprocess.TimeoutExpired)
                                          else "stage failed; diagnostic redacted") if error else ""))

    def finish(self) -> None:
        parent = _collection.get()
        if parent is not None and parent is not self:
            parent.results.extend(self.results)
        print(json.dumps(dict(schema_version=1, gate=self.gate,
                              status="failed" if self.failed else "passed",
                              collection=self.results), sort_keys=True))
        if self.failed:
            raise RuntimeError("campaign collection contains failures")


def collect_all(items, operation, *, gate, key):
    """Attempt every independent item, then fail once with the complete ledger."""
    collection = Collection(gate)
    values = {}
    for item in items:
        identity = key(item)
        with collection.stage(identity, "measure"):
            values[identity] = operation(item)
            document = values[identity]
            if isinstance(document, dict):
                for observation in document.get("observations", ()):
                    collection.results.append(dict(
                        version=identity, stage="measure", test=observation["probe"],
                        outcome="passed", observed_outcome=observation["outcome"],
                        message="observation collected; detail redacted"))
    collection.finish()
    return values
