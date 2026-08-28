#!/usr/bin/env python3
"""Ogni transaction scope rende non riusabile una connessione abbandonata."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class EveryProviderQuarantinesAbandonedTransactions(unittest.TestCase):
    SOURCES = {
        "PostgresTransaction": "crates/plenora-db-postgres/src/transaction/mod.rs",
        "MysqlTransaction": "crates/plenora-db-mysql/src/transaction.rs",
        "SqlServerTransaction": "crates/plenora-db-sqlserver/src/transaction.rs",
        "Db2Transaction": "crates/plenora-db-db2/src/transaction.rs",
    }

    def test_every_transaction_has_an_explicit_drop_guard(self) -> None:
        for transaction, relative in self.SOURCES.items():
            source = (ROOT / relative).read_text(encoding="utf-8")
            with self.subTest(transaction=transaction):
                self.assertRegex(source, rf"impl Drop for {transaction}\s*\{{")

    def test_drop_guards_never_block_an_async_runtime(self) -> None:
        """Drop puo chiudere in sync, ma non annidare o attendere un runtime."""

        for transaction, relative in self.SOURCES.items():
            source = (ROOT / relative).read_text(encoding="utf-8")
            match = re.search(
                rf"impl Drop for {transaction}\s*\{{(?P<body>.*?)^\}}",
                source,
                re.MULTILINE | re.DOTALL,
            )
            self.assertIsNotNone(match, transaction)
            body = match.group("body") if match else ""
            with self.subTest(transaction=transaction):
                self.assertNotIn("block_on", body)
                self.assertNotIn(".await", body)


if __name__ == "__main__":
    unittest.main()
