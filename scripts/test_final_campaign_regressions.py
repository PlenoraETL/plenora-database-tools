"""Offline guards for campaign execution topology (no database required)."""
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


class FinalCampaignRegressions(unittest.TestCase):
    def test_ignored_postgres_integrations_use_campaign_configuration(self):
        tests = ROOT / "crates/plenora-db-postgres/tests"
        for source in tests.glob("*.rs"):
            text = source.read_text(encoding="utf-8")
            with self.subTest(source=source.name):
                self.assertNotRegex(text, r"const DSN(?:_MAIN)?\s*:")
                self.assertNotIn("SecretString::new(DSN", text)
        common = (tests / "common/mod.rs").read_text(encoding="utf-8")
        self.assertIn('std::env::var("PLENORA_TEST_POSTGRES_DSN")', common)
        self.assertNotIn("unwrap_or", common)

    def test_staged_wheel_suite_receives_all_surface_inputs(self):
        script = (ROOT / ".github/scripts/offline_suite.sh").read_text(encoding="utf-8")
        for variable in ("PLENORA_CLI_BIN", "PLENORA_PG_SURFACE_BIN",
                         "PLENORA_PG_GEOMETRY_FIXTURES", "PLENORA_MYSQL_SURFACE_VECTORS"):
            self.assertIn("export " + variable + "=", script)
        self.assertIn("cargo metadata", script)
        self.assertIn("--example postgres-surfaces", script)
        self.assertLess(script.index("cargo build"), script.index('cd "$staging"'))
        self.assertIn('repository="$(cygpath -m "$repository")"', script)

    def test_ubi_builder_keeps_preinstalled_curl_variant(self):
        docker = (ROOT / "docker/db2-client/Dockerfile").read_text(encoding="utf-8")
        self.assertIn("curl-minimal", docker)
        self.assertNotRegex(docker, r"\bcurl\s")
        self.assertNotIn("--skip-broken", docker)


if __name__ == "__main__":
    unittest.main()
