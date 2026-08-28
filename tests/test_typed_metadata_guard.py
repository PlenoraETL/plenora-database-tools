"""Self-test della guardia per la reflection tipizzata."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.check_typed_metadata import ENGINE, PUBLIC_API, WIRE_ADAPTER, violations


PUBLIC = """
pub enum Observation<T> { NotMeasured, Observed(T) }
pub struct MetaData;
pub struct Table;
pub struct Column;
pub struct Index;
pub struct ForeignKey;
pub struct Constraint;
"""
WIRE = """
fn convert(provider: ProviderKind) {
    match provider {
        ProviderKind::Postgres => (),
        ProviderKind::Mysql => (),
        ProviderKind::Mariadb => (),
        ProviderKind::Sqlserver => (),
        ProviderKind::Db2 => (),
        _ => (),
    }
}
"""
ENGINE_SOURCE = """
fn rotate() { metadata.clear(); }
fn dispose() { metadata.clear(); }
pub async fn reflect_table() {}
fn metadata_cache_ttl() {}
pub fn invalidate_metadata() {}
"""


class TypedMetadataGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        for relative, content in (
            (PUBLIC_API, PUBLIC),
            (WIRE_ADAPTER, WIRE),
            (ENGINE, ENGINE_SOURCE),
        ):
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_accepts_typed_surface_and_all_current_adapters(self) -> None:
        self.assertEqual(violations(self.root), [])

    def test_rejects_an_untyped_public_document(self) -> None:
        path = self.root / PUBLIC_API
        path.write_text(PUBLIC + "pub struct Raw(serde_json::Value);\n", encoding="utf-8")
        self.assertTrue(any("serde_json::Value" in item for item in violations(self.root)))

    def test_rejects_a_provider_without_an_adapter(self) -> None:
        path = self.root / WIRE_ADAPTER
        path.write_text(WIRE.replace("ProviderKind::Db2 => (),", ""), encoding="utf-8")
        self.assertTrue(any("Db2" in item for item in violations(self.root)))

    def test_rejects_a_cache_that_survives_secret_rotation(self) -> None:
        path = self.root / ENGINE
        path.write_text(ENGINE_SOURCE.replace("metadata.clear();", "", 1), encoding="utf-8")
        self.assertTrue(any("rotazione" in item for item in violations(self.root)))


if __name__ == "__main__":
    unittest.main()
