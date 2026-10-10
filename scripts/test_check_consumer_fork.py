#!/usr/bin/env python3
"""Self-test di `check_consumer_fork.py` su metadata sintetici."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "check_consumer_fork", ROOT / "scripts" / "check_consumer_fork.py"
)
assert SPEC is not None and SPEC.loader is not None
fork = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = fork
SPEC.loader.exec_module(fork)

REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"


def package(name: str, manifest: Path, source: str | None, lib: str) -> dict:
    return {
        "name": name,
        "source": source,
        "manifest_path": str(manifest),
        "targets": [{"name": lib, "kind": ["lib"]}],
    }


class ConsumerForkVerdict(unittest.TestCase):
    def test_the_fork_resolved_by_path_is_green(self) -> None:
        metadata = {
            "packages": [
                package("plenora-oracle-rs", fork.FORK_MANIFEST, None, "oracle_rs"),
                package("tokio", ROOT / "x" / "Cargo.toml", REGISTRY, "tokio"),
            ]
        }
        self.assertEqual(fork.verdict(metadata, fork.FORK_MANIFEST), [])

    def test_the_upstream_from_the_registry_is_red(self) -> None:
        upstream = Path("/registry/oracle-rs-0.1.7/Cargo.toml")
        metadata = {"packages": [package("oracle-rs", upstream, REGISTRY, "oracle_rs")]}
        problems = fork.verdict(metadata, fork.FORK_MANIFEST)
        self.assertEqual(len(problems), 2, problems)

    def test_two_drivers_or_none_are_red(self) -> None:
        both = {
            "packages": [
                package("plenora-oracle-rs", fork.FORK_MANIFEST, None, "oracle_rs"),
                package("oracle-rs", Path("/r/Cargo.toml"), REGISTRY, "oracle_rs"),
            ]
        }
        self.assertTrue(fork.verdict(both, fork.FORK_MANIFEST))
        self.assertTrue(fork.verdict({"packages": []}, fork.FORK_MANIFEST))

    def test_the_consumer_is_its_own_workspace(self) -> None:
        manifest = fork.consumer_manifest(fork.ORACLE_CRATE)
        self.assertIn("\n[workspace]\n", manifest)
        self.assertIn(fork.ORACLE_CRATE.as_posix(), manifest)
        self.assertNotIn("\n[patch", manifest)


if __name__ == "__main__":
    unittest.main()
