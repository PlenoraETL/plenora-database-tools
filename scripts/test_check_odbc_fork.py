#!/usr/bin/env python3
"""La guardia ODBC deve rifiutare qualsiasi delta non dichiarato."""

import tempfile
import unittest
from pathlib import Path

from check_odbc_fork import inventory, verify


class ForkTests(unittest.TestCase):
    def test_exact_delta_and_mutations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            upstream, vendor = root / "upstream", root / "vendor"
            upstream.mkdir()
            vendor.mkdir()
            for base in (upstream, vendor):
                (base / "same").write_bytes(b"unchanged\n")
                (base / "delta").write_bytes(b"original\n")
                (base / "binary").write_bytes(b"\x00\xff\r\n")
            (upstream / "omitted").write_bytes(b"registry marker")
            (vendor / "delta").write_bytes(b"approved\n")
            manifest = {"upstream_files": inventory(upstream), "changes": {
                "delta": {"sha256": inventory(vendor)["delta"], "reason": "API"},
                "omitted": {"sha256": None, "reason": "registry marker"},
            }}
            self.assertEqual(verify(vendor, manifest, upstream), [])
            (vendor / "binary").write_bytes(b"\x00\xff\n")
            self.assertTrue(verify(vendor, manifest, upstream))
            (vendor / "binary").write_bytes(b"\x00\xff\r\n")
            (vendor / "same").write_bytes(b"unchanged\r\n")
            self.assertEqual(verify(vendor, manifest, upstream), [])
            for name in ("same", "delta", "new", "omitted"):
                path = vendor / name
                previous = path.read_bytes() if path.exists() else None
                path.write_bytes(b"unauthorized")
                self.assertTrue(verify(vendor, manifest, upstream), name)
                if previous is None:
                    path.unlink()
                else:
                    path.write_bytes(previous)
            (vendor / "same").unlink()
            self.assertTrue(verify(vendor, manifest, upstream))
            (vendor / "same").write_bytes(b"unchanged\n")
            (upstream / "delta").write_bytes(b"different upstream")
            self.assertTrue(verify(vendor, manifest, upstream))


if __name__ == "__main__":
    unittest.main()
