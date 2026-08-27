from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.code_size import cfg_test_items, count_rust, rust_test_only_files


class CodeSizeTest(unittest.TestCase):
    def test_inline_test_module_is_not_product(self) -> None:
        source = "fn product() {}\n#[cfg(test)]\nmod checks {\n fn test_only() {}\n}\nfn after() {}\n"
        count = count_rust(source)
        # La riga vuota che separa i due item di produzione resta sorgente
        # fisico; il blocco di test, invece, sparisce per intero.
        self.assertEqual(count.physical, 3)
        self.assertEqual(count.code, 2)

    def test_braces_inside_strings_do_not_close_the_test_module(self) -> None:
        source = '#[cfg(test)]\nmod checks { const S: &str = "}"; fn x() {} }\nfn product() {}\n'
        _, spans = cfg_test_items(source)
        self.assertEqual(len(spans), 1)
        self.assertEqual(count_rust(source).code, 1)

    def test_external_cfg_test_module_is_discovered(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            library = root / "lib.rs"
            evidence = root / "evidence.rs"
            library.write_text("#[cfg(test)]\nmod evidence;\n", encoding="utf-8")
            evidence.write_text("fn only_for_tests() {}\n", encoding="utf-8")
            self.assertEqual(rust_test_only_files([library, evidence]), {evidence})

    def test_path_attribute_identifies_the_physical_test_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            library = root / "lib.rs"
            checks = root / "lib_checks.rs"
            library.write_text(
                '#[cfg(test)]\n#[path = "lib_checks.rs"]\nmod checks;\n',
                encoding="utf-8",
            )
            checks.write_text("fn only_for_tests() {}\n", encoding="utf-8")
            self.assertEqual(rust_test_only_files([library, checks]), {checks})
            self.assertEqual(count_rust(library.read_text(encoding="utf-8")).code, 0)


if __name__ == "__main__":
    unittest.main()
