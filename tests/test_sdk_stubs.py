"""Gli stub del SDK Python dicono cosa il SDK fa davvero.

Le factory restituiscono wrapper Python, non i tipi nativi sottostanti: anche
le firme, i parametri nominabili e i re-export devono quindi coincidere con il
runtime osservabile. Il confronto copre moduli, classi, metodi e funzioni
libere; ogni asimmetria richiede un'eccezione esplicita in `TYPING_ONLY` o
`WITHOUT_RUNTIME_MODULE`.

La regola e asimmetrica, ed e quella giusta per uno stub:

* cio che lo stub dichiara **deve** esistere a runtime, con la stessa firma —
  definito nel modulo oppure importato in esso: uno stub non puo promettere
  quello che non c'e;
* ogni membro **pubblico** del runtime deve stare nello stub — un aiutante
  privato puo restare fuori, una API no.

I controlli sono statici: leggono l'albero sintattico dei due file e non
importano il modulo nativo, quindi girano nei self-test di ogni push senza un
wheel compilato.
"""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

PACKAGE = (
    Path(__file__).resolve().parents[1]
    / "crates"
    / "plenora-database-py"
    / "python"
    / "plenora_database"
)

FACTORIES = ("_connect_mysql", "_aconnect_mysql")
#: I due wrapper Python della sessione di famiglia.
#:
#: Il nome resta indipendente dal prodotto perché gli stessi wrapper servono
#: più factory.
WRAPPERS = ("_DatabaseSessionWrapper", "_AsyncDatabaseSessionWrapper")

#: Stub senza modulo Python affiancato, con la ragione per cui non ce l'hanno.
WITHOUT_RUNTIME_MODULE = {
    "_native.pyi": "e l'estensione compilata: il suo runtime e il modulo Rust",
}

#: Nomi che vivono solo nello stub, con la ragione.
TYPING_ONLY = {
    ("query.pyi", "_PortableBackend"): (
        "protocollo strutturale per il type checker: descrive cosa i builder "
        "si aspettano dalla sessione, e non ha ne deve avere un corrispettivo "
        "a runtime"
    ),
}

Function = (ast.FunctionDef, ast.AsyncFunctionDef)


def parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


def classes(tree: ast.Module) -> dict[str, ast.ClassDef]:
    return {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}


def functions(tree: ast.Module) -> dict[str, ast.AST]:
    return {node.name: node for node in tree.body if isinstance(node, Function)}


def imported(tree: ast.Module) -> set[str]:
    """I nomi che il modulo porta dentro da altrove.

    Un modulo puo ri-esportare: `__init__.py` dichiara `version` importandola
    dal modulo nativo. Per lo stub e presente a tutti gli effetti, e pretendere
    una definizione locale renderebbe rosso un ri-export legittimo.
    """
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                names.add(alias.asname or alias.name.split(".", 1)[0])
    return names


def assigned_aliases(tree: ast.Module) -> dict[str, str]:
    """Alias semplici esportati dal modulo, per esempio ``Public = _Shared``."""
    aliases = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Name):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                aliases[target.id] = node.value.id
    return aliases


def relative_imports(tree: ast.Module, path: Path) -> dict[str, tuple[Path, str]]:
    """Origine dei simboli importati da moduli Python affiancati."""
    origins = {}
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom) or node.level != 1 or not node.module:
            continue
        source = path.parent / (node.module.replace(".", "/") + ".py")
        if not source.is_file():
            continue
        for alias in node.names:
            origins[alias.asname or alias.name] = (source, alias.name)
    return origins


def resolved_methods(
    path: Path,
    name: str,
    trail: frozenset[tuple[Path, str]] = frozenset(),
) -> dict[str, tuple] | None:
    """Superficie statica di una classe, inclusi basi locali/importate e alias.

    Il gate resta offline: segue solo file ``.py`` del pacchetto e non importa
    l'estensione nativa. Le definizioni dirette prevalgono sulle basi secondo
    l'ordine MRO rilevante per i mixin usati dal SDK.
    """
    key = (path, name)
    if key in trail:
        return None
    trail = trail | {key}
    tree = parse(path)

    aliases = assigned_aliases(tree)
    if name in aliases:
        return resolved_methods(path, aliases[name], trail)

    origins = relative_imports(tree, path)
    if name in origins:
        source, original = origins[name]
        return resolved_methods(source, original, trail)

    node = classes(tree).get(name)
    if node is None:
        return None

    methods = {}
    for base in reversed(node.bases):
        if isinstance(base, ast.Name):
            methods.update(resolved_methods(path, base.id, trail) or {})
    methods.update(declared(node))
    return methods


def signature(node: ast.AST) -> tuple:
    """La firma che un chiamante vede: nomi, posizioni, quantita di default.

    I *tipi* non si confrontano: nel `.py` sono annotazioni parziali e a volte
    forward reference scritte a mano, nel `.pyi` sono complete. Cio che rompe
    una chiamata e il nome di un argomento, la sua posizione, l'assenza di un
    default e il fatto che il risultato sia o non sia awaitable. Del `*args` si
    guarda l'esistenza e non il nome: quello il chiamante non lo scrive mai.
    """
    args = node.args
    return (
        isinstance(node, ast.AsyncFunctionDef),
        tuple(a.arg for a in args.posonlyargs),
        tuple(a.arg for a in args.args),
        tuple(a.arg for a in args.kwonlyargs),
        args.vararg is not None,
        args.kwarg is not None,
        len(args.defaults),
        tuple(default is not None for default in args.kw_defaults),
    )


def declared(node: ast.ClassDef) -> dict[str, tuple]:
    return {item.name: signature(item) for item in node.body if isinstance(item, Function)}


def dynamic_attributes(node: ast.ClassDef) -> set[str]:
    """Nomi chiusi serviti da ``__getattr__`` tramite un catalogo letterale."""

    for item in node.body:
        if not isinstance(item, ast.Assign) or not any(
            isinstance(target, ast.Name) and target.id == "_FUNCTIONS"
            for target in item.targets
        ):
            continue
        if not isinstance(item.value, ast.Dict):
            return set()
        return {
            key.value
            for key in item.value.keys
            if isinstance(key, ast.Constant) and isinstance(key.value, str)
        }
    return set()


def is_public(name: str) -> bool:
    """Pubblico: non privato, oppure un dunder che fa parte del protocollo."""
    return not name.startswith("_") or (name.startswith("__") and name.endswith("__"))


def returned_name(node: ast.AST) -> str | None:
    """Il nome dell'annotazione di ritorno, senza le virgolette del forward ref."""
    annotation = getattr(node, "returns", None)
    if isinstance(annotation, ast.Name):
        return annotation.id
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        return annotation.value
    return None


def stub_pairs() -> list[tuple[Path, Path]]:
    """Ogni stub del pacchetto, accoppiato al modulo che descrive."""
    pairs = []
    for stub in sorted(PACKAGE.glob("*.pyi")):
        runtime = stub.with_suffix(".py")
        if runtime.is_file():
            pairs.append((stub, runtime))
            continue
        assert stub.name in WITHOUT_RUNTIME_MODULE, (
            f"{stub.name} non ha un modulo Python affiancato e non e fra le "
            f"eccezioni dichiarate"
        )
    assert pairs, "nessuno stub affiancato a un modulo Python"
    return pairs


class SdkStubsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime = parse(PACKAGE / "__init__.py")
        self.stub = parse(PACKAGE / "__init__.pyi")

    def test_le_factory_interne_mysql_dichiarano_il_wrapper_restituito(self) -> None:
        """Lo stub e il runtime concordano sui due adapter interni."""
        runtime_functions = functions(self.runtime)
        stub_functions = functions(self.stub)
        for factory in FACTORIES:
            with self.subTest(factory=factory):
                self.assertIn(factory, runtime_functions)
                self.assertIn(factory, stub_functions)
                promised = returned_name(stub_functions[factory])
                self.assertIn(
                    promised,
                    WRAPPERS,
                    f"{factory} nello stub promette {promised}",
                )
                built = returned_name(runtime_functions[factory])
                if built is not None:
                    self.assertEqual(promised, built)

    def test_i_wrapper_dichiarati_esistono(self) -> None:
        runtime_classes = classes(self.runtime)
        stub_classes = classes(self.stub)
        for wrapper in WRAPPERS:
            with self.subTest(wrapper=wrapper):
                self.assertIn(wrapper, runtime_classes)
                self.assertIn(wrapper, stub_classes)

    def test_ogni_classe_dichiarata_esiste_a_runtime(self) -> None:
        """Nessuna classe promessa dallo stub manca dal modulo che descrive."""
        for stub_path, runtime_path in stub_pairs():
            runtime_tree = parse(runtime_path)
            present = (
                set(classes(runtime_tree))
                | imported(runtime_tree)
                | set(assigned_aliases(runtime_tree))
            )
            for name in classes(parse(stub_path)):
                if (stub_path.name, name) in TYPING_ONLY:
                    continue
                with self.subTest(stub=stub_path.name, cls=name):
                    self.assertIn(
                        name,
                        present,
                        "dichiarata nello stub e assente dal runtime",
                    )

    def test_ogni_classe_pubblica_del_runtime_e_dichiarata(self) -> None:
        for stub_path, runtime_path in stub_pairs():
            promised = set(classes(parse(stub_path)))
            for name in classes(parse(runtime_path)):
                if not is_public(name):
                    continue
                with self.subTest(stub=stub_path.name, cls=name):
                    self.assertIn(name, promised, "pubblica e non dichiarata")

    def test_le_funzioni_di_modulo_hanno_la_stessa_firma(self) -> None:
        """Anche le funzioni libere, non solo i metodi.

        La parità riguarda anche `connect`, `aconnect`, `version` e ogni altra
        funzione pubblica del modulo.
        """
        for stub_path, runtime_path in stub_pairs():
            runtime_tree = parse(runtime_path)
            real = functions(runtime_tree)
            reexported = imported(runtime_tree)
            for name, node in functions(parse(stub_path)).items():
                with self.subTest(stub=stub_path.name, function=name):
                    if name in reexported and name not in real:
                        # Ri-esportata: la firma e quella del modulo d'origine.
                        continue
                    self.assertIn(
                        name,
                        real,
                        "dichiarata nello stub e assente dal runtime",
                    )
                    self.assertEqual(signature(node), signature(real[name]))

    def test_nessuno_stub_promette_una_firma_che_il_runtime_non_ha(self) -> None:
        for stub_path, runtime_path in stub_pairs():
            for name, node in classes(parse(stub_path)).items():
                real = resolved_methods(runtime_path, name)
                if real is None:
                    continue
                runtime_node = classes(parse(runtime_path)).get(name)
                dynamic = set() if runtime_node is None else dynamic_attributes(runtime_node)
                for method, promised in declared(node).items():
                    with self.subTest(stub=stub_path.name, cls=name, method=method):
                        if method in dynamic:
                            continue
                        self.assertIn(
                            method,
                            real,
                            "dichiarato nello stub e assente dal runtime",
                        )
                        self.assertEqual(promised, real[method])

    def test_i_cataloghi_dinamici_corrispondono_agli_stub(self) -> None:
        """Uno ``__getattr__`` chiuso non rende invisibili funzioni aggiunte o rimosse."""

        for stub_path, runtime_path in stub_pairs():
            stub_classes = classes(parse(stub_path))
            for name, runtime_node in classes(parse(runtime_path)).items():
                dynamic = dynamic_attributes(runtime_node)
                if not dynamic or name not in stub_classes:
                    continue
                promised = set(declared(stub_classes[name])) - {"__getattr__"}
                with self.subTest(stub=stub_path.name, cls=name):
                    self.assertEqual(dynamic, promised)

    def test_il_risolutore_statico_copre_mixin_import_e_alias(self) -> None:
        """Una estrazione leggibile non deve sembrare una API rimossa."""
        query = PACKAGE / "query.py"
        init = PACKAGE / "__init__.py"
        self.assertIn("returning", resolved_methods(query, "Insert") or {})
        self.assertIn("select", resolved_methods(init, "_DatabaseSessionWrapper") or {})
        self.assertIn("catalogs", resolved_methods(init, "_DatabaseInspector") or {})

    def test_ogni_metodo_pubblico_e_dichiarato(self) -> None:
        for stub_path, runtime_path in stub_pairs():
            stub_classes = classes(parse(stub_path))
            for name, node in classes(parse(runtime_path)).items():
                if name not in stub_classes:
                    continue
                promised = declared(stub_classes[name])
                for method in declared(node):
                    if not is_public(method):
                        continue
                    with self.subTest(stub=stub_path.name, cls=name, method=method):
                        self.assertIn(
                            method,
                            promised,
                            "pubblico a runtime e non dichiarato nello stub",
                        )


if __name__ == "__main__":
    unittest.main()
