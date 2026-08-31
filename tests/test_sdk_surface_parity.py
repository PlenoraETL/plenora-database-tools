#!/usr/bin/env python3
"""Le due sessioni del SDK espongono la stessa superficie, o dicono perche no.

Il SDK ha due classi di sessione: `Session`, che serve PostgreSQL, e
`DatabaseSession`, che serve MySQL, MariaDB e SQL Server tenendo il provider
dietro `dyn Provider`.

I metodi comuni sono implementati in due classi; la guardia impedisce che una
modifica venga applicata a un solo percorso senza dichiararne la ragione.

# Perche una guardia e non una fusione

Fondere le due classi in una sarebbe la risposta pulita, e non si puo del tutto:
`metrics` poggia su `metrics_snapshot`, che e un metodo **inerente** di
`PostgresProvider` e non del trait, e `postgis_version` descrive un'estensione
che esiste su un prodotto solo. Una classe sola le perderebbe, o pretenderebbe
di allargare il trait per due metodi che riguardano un motore.

Cio che si puo pretendere e che ogni differenza sia **dichiarata**: le due
superfici coincidono, tranne cio che e elencato qui sotto con la sua ragione.
"""

from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "crates" / "plenora-database-py" / "src"
PACKAGE = ROOT / "crates" / "plenora-database-py" / "python" / "plenora_database"

# Il bordo async cambia solo il protocollo di attesa o il nome esplicito delle
# operazioni Arrow. Questa e la specifica minima della superficie, usata sia
# sui wrapper sia sullo stub del modulo nativo.
ASYNC_ALIASES = {
    "__aenter__": "__enter__",
    "__aexit__": "__exit__",
    "aread": "read",
    "acopy_from": "copy_from",
}


def class_methods(path: Path, class_name: str) -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    """Legge metodi e firme senza importare il wheel nativo."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for item in tree.body:
        if isinstance(item, ast.ClassDef) and item.name == class_name:
            return {
                method.name: method
                for method in item.body
                if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
    raise AssertionError(f"classe {class_name} assente in {path.relative_to(ROOT)}")


def public_name(name: str) -> str:
    """Normalizza soltanto le differenze di nome deliberate del bordo async."""

    return ASYNC_ALIASES.get(name, name)


def signature(method: ast.FunctionDef | ast.AsyncFunctionDef) -> tuple[object, ...]:
    """Firma chiamabile: nomi, categorie e default; annotazioni escluse."""

    args = method.args
    positional = [*args.posonlyargs, *args.args]
    if positional and positional[0].arg in {"self", "slf"}:
        positional = positional[1:]
    positional_names = tuple(argument.arg.lstrip("_") for argument in positional)
    keyword_names = tuple(argument.arg.lstrip("_") for argument in args.kwonlyargs)
    positional_defaults = tuple(ast.dump(value) for value in args.defaults)
    keyword_defaults = tuple(
        None if value is None else ast.dump(value) for value in args.kw_defaults
    )
    return (
        len(args.posonlyargs),
        positional_names,
        positional_defaults,
        args.vararg is not None,
        keyword_names,
        keyword_defaults,
        args.kwarg is not None,
    )


def assert_pair(
    case: unittest.TestCase,
    sync_path: Path,
    sync_class: str,
    async_path: Path,
    async_class: str,
) -> None:
    """Pretende stessi metodi e parametri dopo la sola normalizzazione ammessa."""

    sync = class_methods(sync_path, sync_class)
    async_raw = class_methods(async_path, async_class)
    asynchronous = {public_name(name): method for name, method in async_raw.items()}
    case.assertEqual(
        set(sync),
        set(asynchronous),
        f"superficie {sync_class}/{async_class} divergente",
    )
    for name, sync_method in sync.items():
        case.assertEqual(
            signature(sync_method),
            signature(asynchronous[name]),
            f"firma {sync_class}.{name}/{async_class} divergente",
        )


def exposed(path: Path, rust_class: str | None = None) -> set[str]:
    """I metodi che pyo3 pubblica, cioe quelli dentro `#[pymethods]`.

    Il blocco si chiude sulla graffa a colonna zero: gli helper privati che
    seguono nel file non ne fanno parte, e contarli direbbe che le due classi
    divergono dove invece condividono soltanto un'abitudine di layout.
    """

    text = path.read_text(encoding="utf-8")
    implementation = r"impl[^\n]*" if rust_class is None else rf"impl {re.escape(rust_class)}"
    blocks = re.findall(
        rf"#\[pymethods\]\n{implementation}\s*\{{(.*?)\n\}}\n",
        text,
        re.S,
    )
    names: set[str] = set()
    for block in blocks:
        names |= set(re.findall(r"^    (?:pub )?fn (\w+)", block, re.M))
    return names


class TheTwoSessionsAgree(unittest.TestCase):
    #: metodo -> perche esiste su un solo lato.
    DECLARED_DIFFERENCES = {
        "age_admin_capabilities": (
            "descrive l'amministrazione Apache AGE, estensione disponibile "
            "soltanto sulla sessione PostgreSQL"
        ),
        "age_capabilities": (
            "descrive Apache AGE, estensione disponibile soltanto sulla "
            "sessione PostgreSQL"
        ),
        "age_version": (
            "descrive Apache AGE, estensione disponibile soltanto sulla "
            "sessione PostgreSQL"
        ),
        "create_graph": (
            "amministra Apache AGE, estensione disponibile soltanto sulla "
            "sessione PostgreSQL"
        ),
        "cypher": (
            "esegue query Apache AGE, estensione disponibile soltanto sulla "
            "sessione PostgreSQL"
        ),
        "drop_graph": (
            "amministra Apache AGE, estensione disponibile soltanto sulla "
            "sessione PostgreSQL"
        ),
        "list_graphs": (
            "amministra Apache AGE, estensione disponibile soltanto sulla "
            "sessione PostgreSQL"
        ),
        "metrics": (
            "poggia su `metrics_snapshot`, metodo inerente di PostgresProvider e "
            "non del trait Provider: la sessione di famiglia tiene un "
            "`dyn Provider` e non puo chiamarlo"
        ),
        "postgis_version": (
            "descrive un'estensione che esiste su un prodotto solo; sugli altri "
            "tre non c'e niente da riportare"
        ),
    }

    def test_the_surfaces_differ_only_where_declared(self) -> None:
        postgres = exposed(SRC / "session.rs")
        family = exposed(SRC / "session_family.rs")
        self.assertTrue(postgres, "nessun metodo trovato su Session")
        self.assertTrue(family, "nessun metodo trovato su DatabaseSession")

        differenze = (postgres - family) | (family - postgres)
        non_dichiarate = sorted(differenze - set(self.DECLARED_DIFFERENCES))
        self.assertEqual(
            non_dichiarate,
            [],
            "queste differenze fra le due sessioni del SDK non sono dichiarate: "
            f"{non_dichiarate}. O si colmano, o si scrive qui perche restano.",
        )

    def test_no_declared_difference_has_quietly_been_closed(self) -> None:
        """Una differenza dichiarata ma assente e documentazione scaduta."""

        postgres = exposed(SRC / "session.rs")
        family = exposed(SRC / "session_family.rs")
        differenze = (postgres - family) | (family - postgres)
        scadute = sorted(set(self.DECLARED_DIFFERENCES) - differenze)
        self.assertEqual(
            scadute,
            [],
            f"queste differenze sono dichiarate ma non esistono piu: {scadute}",
        )


class SyncAndAsyncAreOneSurface(unittest.TestCase):
    """La specifica Core v3 copre wrapper, transazioni e modulo nativo."""

    def test_native_implementations_have_the_same_surface(self) -> None:
        for sync_file, async_file in (
            ("session.rs", "async_session.rs"),
            ("session_family.rs", "async_session_family.rs"),
            ("transaction.rs", "async_transaction.rs"),
        ):
            synchronous = exposed(SRC / sync_file)
            asynchronous = {public_name(name) for name in exposed(SRC / async_file)}
            with self.subTest(synchronous=sync_file, asynchronous=async_file):
                self.assertEqual(synchronous, asynchronous)
        self.assertEqual(
            exposed(SRC / "engine.rs", "PyEngine"),
            {public_name(name) for name in exposed(SRC / "engine.rs", "PyAsyncEngine")},
        )

    def test_python_implementations_have_the_same_surface(self) -> None:
        for sync_file, sync_class, async_file, async_class in (
            ("_engine.py", "Engine", "_engine.py", "AsyncEngine"),
            ("_session.py", "Session", "_async_session.py", "AsyncSession"),
            ("_transaction.py", "Transaction", "_async_transaction.py", "AsyncTransaction"),
            ("_session.py", "_Inspector", "_async_session.py", "_AsyncInspector"),
        ):
            with self.subTest(sync=sync_class, asynchronous=async_class):
                assert_pair(
                    self,
                    PACKAGE / sync_file,
                    sync_class,
                    PACKAGE / async_file,
                    async_class,
                )

    def test_public_stubs_have_the_same_surface(self) -> None:
        for sync_file, sync_class, async_file, async_class in (
            ("_engine.pyi", "Engine", "_engine.pyi", "AsyncEngine"),
            ("_session.pyi", "Session", "_async_session.pyi", "AsyncSession"),
            ("_transaction.pyi", "Transaction", "_async_transaction.pyi", "AsyncTransaction"),
            ("_session.pyi", "_Inspector", "_async_session.pyi", "_AsyncInspector"),
        ):
            with self.subTest(sync=sync_class, asynchronous=async_class):
                assert_pair(
                    self,
                    PACKAGE / sync_file,
                    sync_class,
                    PACKAGE / async_file,
                    async_class,
                )

    def test_native_stub_has_the_same_surface_for_both_families(self) -> None:
        native = PACKAGE / "_native.pyi"
        for sync_class, async_class in (
            ("Engine", "AsyncEngine"),
            ("Session", "AsyncSession"),
            ("DatabaseSession", "AsyncDatabaseSession"),
            ("Transaction", "AsyncTransaction"),
        ):
            with self.subTest(sync=sync_class, asynchronous=async_class):
                assert_pair(self, native, sync_class, native, async_class)

    def test_native_stub_describes_every_implemented_method(self) -> None:
        native = PACKAGE / "_native.pyi"
        for rust_file, rust_class, stub_class in (
            ("engine.rs", "PyEngine", "Engine"),
            ("engine.rs", "PyAsyncEngine", "AsyncEngine"),
            ("session.rs", "Session", "Session"),
            ("async_session.rs", "AsyncSession", "AsyncSession"),
            ("session_family.rs", "DatabaseSession", "DatabaseSession"),
            ("async_session_family.rs", "AsyncDatabaseSession", "AsyncDatabaseSession"),
            ("transaction.rs", "Transaction", "Transaction"),
            ("async_transaction.rs", "AsyncTransaction", "AsyncTransaction"),
        ):
            with self.subTest(implementation=rust_file, stub=stub_class):
                self.assertEqual(
                    exposed(SRC / rust_file, rust_class),
                    set(class_methods(native, stub_class)),
                )

    def test_async_query_contains_no_duplicate_validation_rules(self) -> None:
        path = PACKAGE / "async_query.py"
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        self.assertFalse(
            [node for node in ast.walk(tree) if isinstance(node, ast.Raise)],
            "le regole dei terminali devono vivere in query.py e non essere duplicate",
        )
        imported = {
            alias.name
            for node in tree.body
            if isinstance(node, ast.ImportFrom) and node.module == "query"
            for alias in node.names
        }
        self.assertTrue(
            {"_validate_returning", "_one_or_none", "_exactly_one"} <= imported,
            "i terminali async devono delegare alle regole condivise",
        )


if __name__ == "__main__":
    unittest.main()
