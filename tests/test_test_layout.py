from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.check_test_layout import scan


class TestLayoutGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.root = Path(self._temporary.name)
        self.source = self.root / "crates" / "demo" / "src"
        self.source.mkdir(parents=True)

    def tearDown(self) -> None:
        self._temporary.cleanup()

    def write(self, relative: str, source: str) -> None:
        path = self.source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source, encoding="utf-8")

    def test_rejects_inline_test_module_in_product_source(self) -> None:
        self.write("lib.rs", "#[cfg(test)]\nmod tests { #[test] fn works() {} }\n")

        checked, violations = scan(self.root)

        self.assertEqual(checked, 1)
        self.assertEqual(
            [(item.path.as_posix(), item.line) for item in violations],
            [("crates/demo/src/lib.rs", 1)],
        )

    def test_accepts_external_test_module(self) -> None:
        self.write(
            "lib.rs",
            '#[cfg(test)]\n#[path = "lib_tests.rs"]\nmod tests;\n',
        )
        self.write("lib_tests.rs", "use super::*;\n#[test]\nfn works() {}\n")

        checked, violations = scan(self.root)

        self.assertEqual(checked, 1)
        self.assertEqual(violations, [])

    def test_allows_nested_groups_inside_dedicated_test_file(self) -> None:
        self.write(
            "lib.rs",
            '#[cfg(test)]\n#[path = "query_tests.rs"]\nmod query_tests;\n',
        )
        self.write("query_tests.rs", "#[cfg(test)]\nmod edge_cases {}\n")

        checked, violations = scan(self.root)

        self.assertEqual(checked, 1)
        self.assertEqual(violations, [])

    def test_does_not_confuse_test_only_helpers_with_suites(self) -> None:
        self.write("lib.rs", "#[cfg(test)]\nconst TEST_LIMIT: usize = 1;\n")

        checked, violations = scan(self.root)

        self.assertEqual(checked, 1)
        self.assertEqual(violations, [])

    def test_rejects_bare_test_function_in_product_source(self) -> None:
        self.write("lib.rs", "#[test]\nfn misplaced() {}\n")

        checked, violations = scan(self.root)

        self.assertEqual(checked, 1)
        self.assertEqual(len(violations), 1)
        self.assertIn("funzione", violations[0].reason)


if __name__ == "__main__":
    unittest.main()
