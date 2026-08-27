"""Prove del gate sui commenti."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts import check_comments


class CommentExtractionTests(unittest.TestCase):
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

    def test_debt_markers_are_case_sensitive_to_avoid_italian_todo(self) -> None:
        source = "# tutto a posto, non TODO\n"
        violations = [
            check_comments.DEBT_MARKER.findall(comment.text)
            for comment in check_comments.comments_for(Path("sample.py"), source)
        ]
        self.assertEqual(violations, [["TODO"]])


if __name__ == "__main__":
    unittest.main()
