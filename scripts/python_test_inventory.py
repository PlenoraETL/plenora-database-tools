"""Censimento statico dei casi pytest, senza importare il binding nativo.

Le parametrizzazioni sono lette come sintassi: nessun codice della suite e
eseguito. Forme non supportate falliscono chiuse invece di perdere casi.
"""

import ast
import itertools
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SUITE = ROOT / "crates/plenora-database-py/python/tests"


def values(node, constants):
    if isinstance(node, (ast.List, ast.Tuple)):
        return node.elts
    if isinstance(node, ast.Name) and node.id in constants:
        return values(constants[node.id], constants)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "range":
        return [ast.Constant(value) for value in range(*(ast.literal_eval(x) for x in node.args))]
    raise RuntimeError(f"parametrizzazione non inventariata: {ast.unparse(node)}")


def cases(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    constants = {target.id: node.value for node in tree.body if isinstance(node, ast.Assign)
                 for target in node.targets if isinstance(target, ast.Name)}
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
            raise RuntimeError(f"classe pytest non inventariata: {path}:{node.name}")
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "pytest_generate_tests":
            raise RuntimeError(f"generazione dinamica non inventariata: {path}")
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or not node.name.startswith("test_"):
            continue
        parameters = []
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call) and ast.unparse(decorator.func).endswith(".parametrize"):
                names = ast.literal_eval(decorator.args[0])
                names = [n.strip() for n in names.split(",")] if isinstance(names, str) else names
                parameters.append([(names, value) for value in values(decorator.args[1], constants)])
        for combination in itertools.product(*parameters):
            bindings = {}
            for names, value in combination:
                entries = [value] if len(names) == 1 else value.elts
                if len(names) != len(entries):
                    raise RuntimeError(f"parametri incoerenti: {path}:{node.name}")
                bindings.update(zip(names, entries))
            yield node, bindings


def count(targets):
    total = 0
    for target in targets:
        filename, _, name = target.partition("::")
        selected = [node for node, _ in cases(SUITE / Path(filename).name)
                    if not name or node.name == name]
        if not selected:
            raise RuntimeError(f"target pytest vuoto: {target}")
        total += len(selected)
    return total


def sdk_counts():
    """Conta ogni caso seguendo helper e fixture fino al requisito server.

    Gli unici punti terminali sono i guard del fixture; nessun nome di test
    o conteggio viene ricopiato. Una dipendenza ambigua richiede una decisione.
    """
    roots = {f"{provider}_config_or_skip": provider for provider in
             ("mysql", "mariadb", "sqlserver", "oracle", "db2")}
    roots.update(postgres_dsn_or_skip="postgres", age_dsn_or_skip="age")
    modules = {}
    for path in SUITE.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        functions, imports = {}, {}
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                functions[node.name] = node
                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Call) and ast.unparse(decorator.func).endswith(".fixture"):
                        for keyword in decorator.keywords:
                            if keyword.arg in {"params", "autouse"}:
                                raise RuntimeError(f"fixture dinamica non inventariata: {path}:{node.name}")
                            if keyword.arg == "name":
                                functions[ast.literal_eval(keyword.value)] = node
            elif isinstance(node, ast.ImportFrom) and node.level == 1:
                for alias in node.names:
                    imports[alias.asname or alias.name] = (node.module, alias.name)
        modules[path.stem] = functions, imports

    def dependencies(module, node, seen):
        functions, imports = modules[module]
        refs = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
        refs.update(n.arg for n in ast.walk(node) if isinstance(n, ast.arg))
        found = set()
        for name in refs:
            owner, original = imports.get(name, (module, name))
            if (owner, original) in seen:
                continue
            if owner == "_harness" and original in roots:
                found.add(roots[original])
            elif owner in modules and original in modules[owner][0]:
                found.update(dependencies(owner, modules[owner][0][original], seen | {(owner, original)}))
        return found

    counts = Counter()
    for path in sorted(SUITE.glob("test_*.py")):
        for node, bindings in cases(path):
            found = dependencies(path.stem, node, {(path.stem, node.name)})
            for value in bindings.values():
                found.update(dependencies(path.stem, value, set()))
            if "age" in found:
                found.discard("postgres")
            provider = bindings.get("provider")
            if isinstance(provider, ast.Constant) and provider.value in found:
                found = {provider.value}
            if path.stem == "test_benchmark_parity":
                found = {"bench"}
            if node.name.startswith("test_live_") and not found:
                raise RuntimeError(f"test live senza fixture inventariata: {path}:{node.name}")
            if len(found) > 1:
                raise RuntimeError(f"dipendenza pytest ambigua: {path.name}:{node.name}: {sorted(found)}")
            counts[next(iter(found), "offline")] += 1
    return counts
