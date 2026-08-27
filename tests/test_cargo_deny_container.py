from __future__ import annotations

import tomllib
import unittest

from scripts import check_cargo_deny


class CargoDenyContainerTests(unittest.TestCase):
    def test_toolchain_and_tool_are_pinned(self) -> None:
        dockerfile = check_cargo_deny.DOCKERFILE.read_text(encoding="utf-8")
        self.assertRegex(dockerfile.splitlines()[0], r"^FROM rust@sha256:[0-9a-f]{64}$")
        self.assertIn("CARGO_DENY_VERSION=0.20.2", dockerfile)
        self.assertIn("cargo install --locked", dockerfile)

    def test_repository_is_mounted_read_only_and_container_is_ephemeral(self) -> None:
        _, command = check_cargo_deny.commands()
        self.assertIn("--rm", command)
        self.assertIn(f"{check_cargo_deny.ROOT}:/workspace:ro", command)
        self.assertEqual(command[-2:], ["check", "--hide-inclusion-graph"])

    def test_only_private_workspace_crates_are_excluded_from_license_scan(self) -> None:
        policy = tomllib.loads((check_cargo_deny.ROOT / "deny.toml").read_text(encoding="utf-8"))
        workspace = tomllib.loads(
            (check_cargo_deny.ROOT / "Cargo.toml").read_text(encoding="utf-8")
        )

        self.assertTrue(policy["licenses"]["private"]["ignore"])
        self.assertFalse(workspace["workspace"]["package"]["publish"])
        self.assertTrue(policy["licenses"]["allow"])


if __name__ == "__main__":
    unittest.main()
