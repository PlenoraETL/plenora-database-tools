from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import check_docs


class DocumentationGateTests(unittest.TestCase):
    def test_examples_reject_removed_factories_for_any_import_alias(self) -> None:
        with tempfile.TemporaryDirectory(dir=check_docs.ROOT) as directory:
            example = Path(directory) / "example.md"
            example.write_text(
                "```python\nimport plenora_database as db\n"
                "db.create_engine(dsn)\n```\n"
                "```python\nfrom plenora_database import connect\n```\n",
                encoding="utf-8",
            )
            with patch.object(check_docs, "markdown_documents", return_value=[example]):
                violations = check_docs.validate_sdk_example_api()
            self.assertEqual(
                {item.reason for item in violations},
                {"nome SDK non pubblico: create_engine", "import SDK non pubblico: connect"},
            )

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


if __name__ == "__main__":
    unittest.main()
