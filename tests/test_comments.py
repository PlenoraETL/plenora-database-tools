"""Prove del gate sui commenti."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts import check_comments


class CommentExtractionTests(unittest.TestCase):
    def test_upstream_vendor_comments_are_not_product_documentation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "vendor").mkdir()
            (root / "vendor/upstream.rs").write_text("// TODO upstream\n", encoding="utf-8")
            (root / "product.rs").write_text("// TODO product\n", encoding="utf-8")
            checked, violations = check_comments.check_repository(root)
            self.assertEqual(checked, 1)
            self.assertEqual(len(violations), 1)

    def test_local_evidence_is_not_product_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / "assurance-results"
            evidence.mkdir()
            (evidence / "upstream.rs").write_text("// TODO upstream\n", encoding="utf-8")
            (root / "product.rs").write_text("// Current invariant.\n", encoding="utf-8")
            checked, violations = check_comments.check_repository(root)
            self.assertEqual(checked, 1)
            self.assertEqual(violations, [])

    def test_python_reads_comments_and_docstrings_but_not_values(self) -> None:
        source = '''"""TODO nel modulo"""
VALUE = "FIXME in una stringa"
# HACK nel commento
def operation():
    """XXX nella funzione."""
'''
        comments = list(check_comments.comments_for(Path("sample.py"), source))
        self.assertEqual([comment.line for comment in comments], [3, 1, 5])

    def test_rust_ignores_ordinary_and_raw_strings(self) -> None:
        source = '''
const A: &str = "// TODO non e un commento";
const B: &str = r#"/* FIXME non e un commento */"#;
// HACK reale
/* XXX reale */
'''
        comments = list(check_comments.comments_for(Path("sample.rs"), source))
        self.assertEqual([comment.line for comment in comments], [4, 5])

    def test_repository_check_reports_only_comment_violations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "valid.py").write_text('VALUE = "TODO non commento"\n', encoding="utf-8")
            (root / "invalid.rs").write_text("// prima stesura\n", encoding="utf-8")
            checked, violations = check_comments.check_repository(root)
        self.assertEqual(checked, 2)
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0].rule, "cronaca obsoleta")

    def test_roadmap_labels_are_process_history(self) -> None:
        source = """// F3-7 - vecchia fase
// === A4: session context ===
// P1.2 - read filters
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "history.rs"
            path.write_text(source, encoding="utf-8")
            violations = check_comments.check_file(path, root)
        self.assertEqual(len(violations), 3)
        self.assertTrue(all(item.rule == "cronaca obsoleta" for item in violations))

    def test_protocol_names_are_not_roadmap_labels(self) -> None:
        source = """// ParameterValue::F64 e un tipo del protocollo.
// SQL Server usa @P1; un errore puo avvenire in fase Commit.
// Il risultato dipende da cosa c'era gia nella tabella.
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "contract.rs"
            path.write_text(source, encoding="utf-8")
            violations = check_comments.check_file(path, root)
        self.assertEqual(violations, [])

    def test_historical_decision_labels_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "history.rs"
            path.write_text("// ADR 0014\n// Prima di questo fix\n", encoding="utf-8")
            violations = check_comments.check_file(path, root)
        self.assertEqual(len(violations), 2)

    def test_debt_markers_are_case_sensitive_to_avoid_italian_todo(self) -> None:
        source = "# tutto a posto, non TODO\n"
        violations = [
            check_comments.DEBT_MARKER.findall(comment.text)
            for comment in check_comments.comments_for(Path("sample.py"), source)
        ]
        self.assertEqual(violations, [["TODO"]])


if __name__ == "__main__":
    unittest.main()
