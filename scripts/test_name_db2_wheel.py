#!/usr/bin/env python3
"""Self-test del naming dell'artefatto runtime Db2."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from name_db2_wheel import (
    db2_wheel_name,
    tag_wheel,
    tagged_wheel,
    validate_release_tag,
)


class Db2WheelNameTests(unittest.TestCase):
    def test_adds_a_distinct_pep_427_build_tag(self) -> None:
        self.assertEqual(
            db2_wheel_name(
                "plenora_database-1.0.0-cp310-abi3-linux_x86_64.whl"
            ),
            "plenora_database-1.0.0-1db2-cp310-abi3-linux_x86_64.whl",
        )

    def test_renames_and_then_validates_the_only_wheel(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            source = directory / "plenora_database-1.0.0-cp310-abi3-linux_x86_64.whl"
            source.write_bytes(b"wheel")
            result = tag_wheel(directory)
            self.assertEqual(result, tagged_wheel(directory))
            self.assertFalse(source.exists())

    def test_rejects_an_ambiguous_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for version in ("1.0.0", "1.0.1"):
                (directory / f"plenora_database-{version}-cp310-abi3-linux_x86_64.whl").touch()
            with self.assertRaisesRegex(RuntimeError, "atteso un wheel Db2"):
                tag_wheel(directory)

    def test_rejects_a_non_linux_or_already_tagged_input(self) -> None:
        for name in (
            "plenora_database-1.0.0-cp310-abi3-win_amd64.whl",
            "plenora_database-1.0.0-1db2-cp310-abi3-linux_x86_64.whl",
        ):
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                db2_wheel_name(name)

    def test_release_tag_must_match_the_wheel_version(self) -> None:
        wheel = Path(
            "plenora_database-1.0.0-1db2-cp310-abi3-linux_x86_64.whl"
        )
        validate_release_tag(
            wheel, event_name="release", ref_name="py-v1.0.0"
        )
        validate_release_tag(
            wheel, event_name="workflow_dispatch", ref_name="main"
        )
        with self.assertRaisesRegex(RuntimeError, "atteso py-v1.0.0"):
            validate_release_tag(
                wheel, event_name="release", ref_name="py-v0.14.0"
            )


if __name__ == "__main__":
    unittest.main()
