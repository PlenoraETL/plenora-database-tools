from __future__ import annotations

import io
import tempfile
import unittest
import zipfile
from pathlib import Path

from scripts import check_docs
from scripts.generate_capabilities_docx import portable_text_bytes, render_document


class DocumentationGateTests(unittest.TestCase):
    def test_repository_documents_pass(self) -> None:
        checked, violations = check_docs.scan()
        self.assertGreaterEqual(checked, 15)
        self.assertEqual(violations, [])

    def test_missing_local_link_and_anchor_are_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "README.md").write_text(
                "# Home\n\n[missing](no.md) [anchor](target.md#missing)\n",
                encoding="utf-8",
            )
            (root / "target.md").write_text("# Present\n", encoding="utf-8")
            documents = check_docs.markdown_documents(root)
            reasons = [item.reason for item in check_docs.validate_links(root, documents)]
            self.assertTrue(any("link locale inesistente" in item for item in reasons))
            self.assertTrue(any("anchor inesistente" in item for item in reasons))

    def test_missing_python_command_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            document = root / "README.md"
            document.write_text(
                "```powershell\npython scripts\\missing.py\n```\n", encoding="utf-8"
            )
            violations = check_docs.validate_commands(root, [document])
            self.assertEqual(len(violations), 1)
            self.assertIn("missing.py", violations[0].reason)

    def test_repository_license_metadata_is_consistent(self) -> None:
        self.assertEqual(check_docs.validate_license(check_docs.ROOT), [])

    def test_generated_docx_inputs_ignore_platform_line_endings(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lf = root / "lf.txt"
            crlf = root / "crlf.txt"
            lf.write_bytes(b"first\nsecond\n")
            crlf.write_bytes(b"first\r\nsecond\r\n")
            self.assertEqual(portable_text_bytes(lf), portable_text_bytes(crlf))

    def test_generated_docx_uses_platform_independent_zip_entries(self) -> None:
        with zipfile.ZipFile(io.BytesIO(render_document())) as document:
            self.assertTrue(document.infolist())
            self.assertTrue(
                all(info.compress_type == zipfile.ZIP_STORED for info in document.infolist())
            )


if __name__ == "__main__":
    unittest.main()
