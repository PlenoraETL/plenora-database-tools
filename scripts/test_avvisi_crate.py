"""Regressioni della copertura Cargo e del contenuto realmente consegnato."""
from __future__ import annotations

import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import avvisi_crate as notices
import licenze_artefatto as artifacts

REPOSITORY = Path(__file__).resolve().parents[1]


class Notices(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.texts = self.root / "texts"
        self.texts.mkdir()
        (self.root / "LICENSE").write_bytes(b"Plenora proprietary product license\n")
        crate = self.texts / "example-1"
        crate.mkdir()
        (crate / "LICENSE-MIT").write_bytes(b"Copyright Example\nMIT terms\n")
        (crate / "NOTICE").write_bytes(b"Example notice\n")
        self.registry = {
            "schema_version": 1,
            "inputs": {},
            "profiles": {"test": {"crates": ["example-1"]}},
            "crates": {"example-1": {"name": "example", "version": "1", "files": [
                {"file": p.name, "sha256": notices.digest(p.read_bytes()), "origin": "cargo-package"}
                for p in sorted(crate.iterdir())
            ]}},
        }
        for name, value in (("ROOT", self.root), ("TEXTS", self.texts)):
            context = patch.object(notices, name, value)
            context.start()
            self.addCleanup(context.stop)
        context = patch.object(notices, "read_registry", return_value=self.registry)
        context.start()
        self.addCleanup(context.stop)

    def cli(self):
        binary = self.root / "example.exe"
        binary.write_bytes(b"binary")
        archive = self.root / "example.zip"
        artifacts.package(archive, "test", binary)
        return archive

    def rewrite(self, path, mutate):
        files = artifacts.read_archive(path)
        mutate(files)
        with zipfile.ZipFile(path, "w") as archive:
            for name, data in files.items():
                archive.writestr(name, data)

    def test_cli_roundtrip(self):
        path = self.cli()
        artifacts.verify(path, "test")
        self.assertIn("LICENSES/crate-rust/example-1/NOTICE", artifacts.read_archive(path))

    def test_missing_notice_rejected(self):
        path = self.cli()
        self.rewrite(path, lambda f: f.pop("LICENSES/crate-rust/example-1/NOTICE"))
        with self.assertRaises(ValueError):
            artifacts.verify(path, "test")

    def test_removed_crate_and_index_entry_rejected(self):
        path = self.cli()
        def remove(files):
            for name in list(files):
                if "/example-1/" in name:
                    del files[name]
            files["LICENSES/crate-rust/CRATE.json"] = notices.encoded({"profile": "test", "crates": {}})
        self.rewrite(path, remove)
        with self.assertRaises(ValueError):
            artifacts.verify(path, "test")

    def test_tampering_and_empty_text_fail(self):
        for data in (b"tampered", b""):
            (self.texts / "example-1/NOTICE").write_bytes(data)
            self.assertTrue(notices.validate(self.registry))
            with self.assertRaises(ValueError):
                notices.bundle("test")

    def test_missing_reason_is_not_a_waiver(self):
        self.registry["crates"]["example-1"].update(files=[], missing_reason="cache lacks license")
        with self.assertRaises(ValueError):
            notices.bundle("test")
        self.assertTrue(notices.validate(self.registry))

    def test_referral_only_notice_rejected(self):
        """Il caso siphasher: un COPYING di soli rinvii non e un avviso completo.

        Copyright e link a MIT/Apache senza il testo delle condizioni passavano
        il gate, e ogni artefatto riceveva un avviso incompleto. La regola
        pretende almeno un testo con nome LICENSE/UNLICENSE; il testo acquisito
        dall'upstream con provenienza soddisfa la regola accanto al COPYING.
        """
        crate = self.texts / "referral-1"
        crate.mkdir()
        referral = b"Copyright Referral\nSee LICENSE-MIT\n"
        (crate / "COPYING").write_bytes(referral)
        self.registry["profiles"]["test"]["crates"].append("referral-1")
        self.registry["crates"]["referral-1"] = {
            "name": "referral", "version": "1", "files": [
                {"file": "COPYING", "sha256": notices.digest(referral), "origin": "cargo-package"},
            ],
        }
        self.assertTrue(any("rinvii" in error for error in notices.validate(self.registry)))

        supplementary = b"Permission is hereby granted...\n"
        (crate / "LICENSE-MIT").write_bytes(supplementary)
        self.registry["crates"]["referral-1"]["files"].append(
            {"file": "LICENSE-MIT", "sha256": notices.digest(supplementary),
             "origin": "https://example.invalid/LICENSE-MIT"},
        )
        self.assertEqual(notices.validate(self.registry), [])

    def test_obsolete_file_and_crate_rejected(self):
        (self.texts / "extra").write_bytes(b"obsolete")
        self.assertTrue(notices.validate(self.registry))
        self.registry["profiles"]["test"]["crates"] = []
        self.assertTrue(any("obsoleti" in error for error in notices.validate(self.registry)))

    def test_nested_licenses_and_license_file(self):
        (self.texts / "legal").mkdir()
        (self.texts / "legal/terms.txt").write_bytes(b"terms")
        package = {"manifest_path": str(self.texts / "Cargo.toml"), "license_file": "legal/terms.txt"}
        self.assertEqual(len(notices.package_files(package)), 3)

    def test_graph_excludes_build_dev_and_proc_macros(self):
        metadata = {"packages": [{"name": n, "version": "1"} for n in ("runtime", "build", "dev", "macro")]}
        with patch.object(notices, "cargo", return_value="runtime v1\nruntime v1 (*)\n") as cargo:
            self.assertEqual(notices.graph(metadata, "product", [], "target"), metadata["packages"][:1])
            self.assertIn("normal,no-proc-macro", cargo.call_args.args)
            self.assertIn("--target", cargo.call_args.args)

    def test_ambiguous_package_identity_rejected(self):
        with patch.object(notices, "cargo", return_value="runtime v1\n"):
            with self.assertRaises(ValueError):
                notices.graph({"packages": [{"name": "runtime", "version": "1"}] * 2}, "p", [], "t")

    def test_wheel_record_and_payload_preserved(self):
        path = self.root / "test.whl"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("plenora_database/_native.pyd", b"native")
            archive.writestr("test.dist-info/RECORD", b"")
        artifacts.package(path, "test")
        artifacts.verify(path, "test")
        self.assertEqual(artifacts.read_archive(path)["plenora_database/_native.pyd"], b"native")
        self.rewrite(path, lambda f: f.update({"plenora_database/_native.pyd": b"changed"}))
        with self.assertRaises(ValueError):
            artifacts.verify(path, "test")

    def test_source_preserves_executable_mode_and_adds_union(self):
        path = self.root / "source.tar.gz"
        with tarfile.open(path, "w:gz") as archive:
            member = tarfile.TarInfo("source/script.sh")
            member.size, member.mode = 1, 0o755
            archive.addfile(member, io.BytesIO(b"x"))
        artifacts.package(path, "source")
        artifacts.verify(path, "source")
        with tarfile.open(path) as archive:
            self.assertEqual(archive.getmember("source/script.sh").mode, 0o755)

    def test_product_license_required(self):
        path = self.cli()
        self.rewrite(path, lambda f: f.update({"LICENSE": b"MIT"}))
        with self.assertRaises(ValueError):
            artifacts.verify(path, "test")

    def test_path_traversal_rejected(self):
        self.registry["crates"]["example-1"]["files"][0]["file"] = "../LICENSE"
        with self.assertRaises(ValueError):
            notices.bundle("test")

    def test_stale_cargo_inputs_block_packaging(self):
        self.registry["inputs"] = {"LICENSE": "0" * 64}
        with self.assertRaises(ValueError):
            notices.bundle("test")


class WorkflowWiring(unittest.TestCase):
    def test_release_cannot_bypass_graph_gate(self):
        import yaml
        workflow = yaml.safe_load((REPOSITORY / ".github/workflows/python-wheel.yml").read_text(encoding="utf-8"))
        jobs = workflow["jobs"]
        for job in ("linux", "windows", "db2-linux"):
            self.assertIn("crate-notices", jobs[job]["needs"])
        for platform in ("linux", "windows"):
            steps = jobs[platform]["steps"]
            upload = next(i for i, step in enumerate(steps) if "actions/upload-artifact@" in step.get("uses", ""))
            commands = "\n".join(step.get("run", "") for step in steps[:upload])
            self.assertIn(f"licenze_artefatto.py --profile wheel-{platform}", commands)
            self.assertIn(f"licenze_artefatto.py --profile cli-{platform}", commands)
            self.assertIn(f"check-licenze-artefatto.py --profile wheel-{platform}", commands)
            self.assertIn(".zip", steps[upload]["with"]["path"])
        collected = "\n".join(step.get("run", "") for step in jobs["release-sbom"]["steps"])
        self.assertIn("check-licenze-artefatto.py", collected)
        self.assertIn("wheel-db2-linux", collected)
        dockerfile = (REPOSITORY / "docker/db2-client/Dockerfile").read_text(encoding="utf-8")
        self.assertLess(dockerfile.index("licenze_artefatto.py"), dockerfile.index("name_db2_wheel.py"))


if __name__ == "__main__":
    unittest.main()
