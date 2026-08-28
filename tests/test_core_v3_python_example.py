#!/usr/bin/env python3
"""L'esempio Core v3 conserva il confine sessione/transazione per request."""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = (
    ROOT
    / "crates"
    / "plenora-database-py"
    / "examples"
    / "core_v3_repository.py"
)


class CoreV3PythonExampleTest(unittest.TestCase):
    def test_example_parses_and_each_handler_opens_session_and_transaction(self) -> None:
        tree = ast.parse(EXAMPLE.read_text(encoding="utf-8"), filename=str(EXAMPLE))
        handlers = {
            node.name: node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name in {"handle_request", "handle_async_request"}
        }
        self.assertEqual(set(handlers), {"handle_request", "handle_async_request"})
        for name, handler in handlers.items():
            calls = {
                node.func.attr
                for node in ast.walk(handler)
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            }
            with self.subTest(handler=name):
                self.assertIn("session", calls)
                self.assertIn("begin", calls)

    def test_repository_does_not_create_connections_or_engines(self) -> None:
        tree = ast.parse(EXAMPLE.read_text(encoding="utf-8"), filename=str(EXAMPLE))
        repository = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "UserRepository"
        )
        called = {
            node.func.attr
            for node in ast.walk(repository)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertTrue(
            {"connect", "aconnect", "create_engine", "create_async_engine"}.isdisjoint(called)
        )


if __name__ == "__main__":
    unittest.main()
