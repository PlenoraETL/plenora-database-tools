"""Gli stub del SDK Python dicono cosa il SDK fa davvero.

`__init__.pyi` dichiarava `connect_mysql(...) -> MysqlSession` e
`aconnect_mysql(...) -> AsyncMysqlSession`, cioe i tipi nativi. Le due factory
restituiscono invece i wrapper Python che gli stanno davanti, e le superfici
divergono proprio dove conta: `copy_from` nativo vuole i byte Arrow IPC in
posizione, il wrapper accetta `pyarrow`/`pandas`/`list[dict]` e argomenti per
nome. Un type checker approvava la chiamata che a runtime fallisce e rifiutava
quella che funziona.

La prima versione di questi controlli confrontava **solo i nomi** dei metodi, e
per questo non vedeva che `Session.begin` e `AsyncSession.begin` dichiaravano
quattro parametri mentre il runtime ne accetta sei — `context` e
`native_query_policy` non erano scrivibili da codice tipizzato — ne che
`__exit__` era dichiarato `(*args)` dove il runtime vuole tre argomenti
distinti. Un confronto di soli nomi trova la classe assente, non la firma
sbagliata, che e il modo piu comune in cui uno stub invecchia.

La seconda versione confrontava le firme ma **saltava in silenzio** ogni classe
presente da un lato solo: la regola scritta qui sopra e quella applicata erano
due cose diverse, e un intero modulo poteva sparire da uno stub senza che
nulla diventasse rosso. Ora ogni asimmetria e un errore, tranne quelle
elencate in `TYPING_ONLY` e `WITHOUT_RUNTIME_MODULE` — dove l'eccezione si
legge, e si legge anche perche.

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

FACTORIES = ("connect_mysql", "aconnect_mysql")
WRAPPERS = ("_MysqlSessionWrapper", "_AsyncMysqlSessionWrapper")

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

    def test_le_factory_mysql_dichiarano_il_tipo_che_restituiscono(self) -> None:
        """Lo stub e il runtime concordano su cosa esce dalla factory."""
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
            present = set(classes(runtime_tree)) | imported(runtime_tree)
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

        Il confronto precedente guardava solo il tipo di ritorno delle due
        factory MySQL: `connect`, `aconnect` e `version` non erano coperte da
        nulla.
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
            runtime_classes = classes(parse(runtime_path))
            for name, node in classes(parse(stub_path)).items():
                if name not in runtime_classes:
                    continue
                real = declared(runtime_classes[name])
                for method, promised in declared(node).items():
                    with self.subTest(stub=stub_path.name, cls=name, method=method):
                        self.assertIn(
                            method,
                            real,
                            "dichiarato nello stub e assente dal runtime",
                        )
                        self.assertEqual(promised, real[method])

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
