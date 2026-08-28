"""Self-test della guardia che mantiene unica la rappresentazione query."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.check_relational_ir import CANONICAL, COMPATIBILITY, SQL_RENDERER, violations


RELATIONAL = """
pub enum QueryExpression { Literal }
pub struct QueryOperation;
"""
FACADE = "pub use crate::relational::*;\n"
RENDERER = """
use plenora_database_core::relational::QueryOperation;
fn render_query() {}
fn render_query_expression() {}
fn simple_select_to_relational() {}
fn simple_expression_to_relational() {}
pub fn render_select() { render_query(simple_select_to_relational()); }
pub fn render_filter() { render_query_expression(simple_expression_to_relational()); }
"""


class RelationalIrGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        for relative, content in (
            (CANONICAL, RELATIONAL),
            (COMPATIBILITY, FACADE),
            (SQL_RENDERER, RENDERER),
        ):
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_accepts_one_ir_and_two_lowering_adapters(self) -> None:
        self.assertEqual(violations(self.root), [])

    def test_rejects_a_second_query_operation(self) -> None:
        duplicate = self.root / "crates/provider/src/query.rs"
        duplicate.parent.mkdir(parents=True)
        duplicate.write_text("pub struct QueryOperation;\n", encoding="utf-8")
        self.assertTrue(any("QueryOperation" in item for item in violations(self.root)))

    def test_rejects_a_legacy_renderer_that_bypasses_the_ir(self) -> None:
        renderer = self.root / SQL_RENDERER
        renderer.write_text(
            RENDERER.replace(
                "render_query(simple_select_to_relational());",
                "let sql = SELECT_FROM_TABLE;",
            ),
            encoding="utf-8",
        )
        self.assertTrue(any("render_select" in item for item in violations(self.root)))


if __name__ == "__main__":
    unittest.main()
