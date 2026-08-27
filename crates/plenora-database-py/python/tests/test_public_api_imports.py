"""Smoke test dell'import top-level e dei simboli pubblici dello SDK.

Il test è tollerante quando il modulo nativo non è compilato
(sviluppo puro-Python senza `maturin develop`): in quel caso
verifica solo che i moduli Python parseano senza errori sintattici.
"""
from __future__ import annotations

import ast
import importlib
import importlib.util
from pathlib import Path

import pytest


def _package_dir() -> Path:
    """La directory del package **che verrebbe importato**.

    Non quella accanto ai test: il runner del SDK installa il wheel e gira
    la suite fuori dal source tree, quindi un percorso relativo a
    `__file__` puntava a una directory inesistente — e, prima ancora, in un
    albero di sviluppo avrebbe fatto leggere i sorgenti mentre `import`
    caricava il pacchetto installato. Sono proprio le due copie che questo
    modulo esiste per non confondere.
    """

    specification = importlib.util.find_spec("plenora_database")
    if specification is None or not specification.origin:
        pytest.skip("plenora_database non installato", allow_module_level=True)
    return Path(specification.origin).resolve().parent


PACKAGE_DIR = _package_dir()


def _module_all(path: Path) -> set[str]:
    """Estrae la lista __all__ da un modulo Python via AST (no import)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "__all__":
                    if isinstance(node.value, (ast.List, ast.Tuple)):
                        return {
                            elt.value
                            for elt in node.value.elts
                            if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                        }
    return set()


def _class_members(path: Path, class_name: str) -> set[str]:
    """Nomi dichiarati da una classe in uno stub, senza importare il nativo."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return {
                item.name
                for item in node.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
    raise AssertionError(f"classe {class_name} assente in {path.name}")


def test_errors_reexports_commit_outcome_unknown() -> None:
    """errors.py deve esporre PlenoraCommitOutcomeUnknownError.

    Guard esplicito sulla regressione P0.
    """
    all_names = _module_all(PACKAGE_DIR / "errors.py")
    assert "PlenoraCommitOutcomeUnknownError" in all_names, (
        "errors.py::__all__ deve includere PlenoraCommitOutcomeUnknownError; "
        "senza il re-export l'import top-level del package fallisce."
    )


def test_init_and_errors_are_consistent() -> None:
    """Ogni Plenora*Error re-esportato da __init__.py deve esistere in errors.py.

    Previene divergenza fra le due liste (source of truth = errors.py).
    """
    init_all = _module_all(PACKAGE_DIR / "__init__.py")
    errors_all = _module_all(PACKAGE_DIR / "errors.py")
    error_names_in_init = {n for n in init_all if n.startswith("Plenora")}
    missing = error_names_in_init - errors_all
    assert not missing, (
        f"Errore names in __init__.__all__ ma non in errors.__all__: {sorted(missing)}"
    )


def test_native_stub_matches_the_common_sync_and_async_session_surface() -> None:
    """La parita pubblica non deve dipendere dal motore o dalla forma async."""
    stub = PACKAGE_DIR / "_native.pyi"
    common = {
        "capabilities",
        "execute_ddl",
        "inspect_catalogs",
        "inspect_schemas",
        "inspect_tables",
        "inspect_describe",
    }
    for class_name in ("Session", "DatabaseSession", "AsyncSession", "AsyncDatabaseSession"):
        members = _class_members(stub, class_name)
        assert common <= members, f"{class_name}: mancano {sorted(common - members)}"

    async_family = _class_members(stub, "AsyncDatabaseSession")
    assert "read" not in async_family
    assert "copy_from" not in async_family
    assert {"aread", "acopy_from"} <= async_family


def _native_importable() -> bool:
    """True se il modulo nativo è realmente importabile su questa
    piattaforma. Cerca via loader senza triggerare
    `plenora_database/__init__.py` (che è proprio ciò che vogliamo
    testare separatamente)."""
    import sys

    # Il file .so linux non è caricabile su Windows anche se presente
    # (cross-compile in Docker). Verifichiamo la piattaforma.
    if sys.platform == "linux":
        exts = (".so",)
    elif sys.platform == "win32":
        exts = (".pyd",)
    elif sys.platform == "darwin":
        exts = (".dylib", ".so")
    else:
        return False
    return any(
        p.suffix.lower() in exts for p in PACKAGE_DIR.glob("_native*.*")
    )


@pytest.mark.skipif(
    not _native_importable(),
    reason="modulo nativo non compilato per questa piattaforma (esegui `maturin develop`)",
)
def test_top_level_import_works() -> None:
    """Il modulo top-level importa e ri-esporta le eccezioni pubbliche."""
    mod = importlib.import_module("plenora_database")
    assert hasattr(mod, "PlenoraError")
    assert hasattr(mod, "PlenoraCommitOutcomeUnknownError")
    assert hasattr(mod, "SessionContext")
