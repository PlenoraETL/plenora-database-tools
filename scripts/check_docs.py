#!/usr/bin/env python3
"""Controlla che le guide correnti restino risolvibili e semanticamente sane.

Non valida lo stile della prosa. Protegge invece proprietà oggettive: link e
anchor locali, comandi Python esistenti, documenti generati aggiornati e le
contraddizioni cross-file gia emerse durante l'audit.
"""

from __future__ import annotations

import ast
import re
import sys
try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SKIP_PARTS = {".git", "target", "node_modules", "__pycache__"}
LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*$", re.MULTILINE)
PYTHON_COMMAND = re.compile(
    r"\bpython(?:3)?\s+((?:scripts[\\/])[A-Za-z0-9_.\\/-]+\.py)"
)
PYTHON_FENCE = re.compile(r"```python\s*\n(.*?)```", re.DOTALL)


def method_call_blocks(text: str, method: str) -> list[str]:
    """Estrae chiamate Markdown multilinea con parentesi annidate."""

    marker = f".{method}("
    blocks: list[str] = []
    cursor = 0
    while (start := text.find(marker, cursor)) >= 0:
        depth = 1
        index = start + len(marker)
        while index < len(text) and depth:
            if text[index] == "(":
                depth += 1
            elif text[index] == ")":
                depth -= 1
            index += 1
        blocks.append(text[start:index])
        cursor = index
    return blocks


@dataclass(frozen=True)
class Violation:
    path: Path
    reason: str


def markdown_documents(root: Path = ROOT) -> list[Path]:
    return [
        path
        for path in sorted(root.rglob("*.md"))
        if not SKIP_PARTS.intersection(path.relative_to(root).parts)
    ]


def github_slug(title: str) -> str:
    """Approssimazione deterministica degli anchor Markdown usati nel repo."""

    text = re.sub(r"<[^>]+>", "", title.strip().lower())
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    return re.sub(r"[\s-]+", "-", text).strip("-")


def anchors(path: Path) -> set[str]:
    counts: dict[str, int] = {}
    found: set[str] = set()
    for heading in HEADING.findall(path.read_text(encoding="utf-8")):
        base = github_slug(heading)
        occurrence = counts.get(base, 0)
        counts[base] = occurrence + 1
        found.add(base if occurrence == 0 else f"{base}-{occurrence}")
    return found


def validate_links(root: Path, documents: list[Path]) -> list[Violation]:
    violations: list[Violation] = []
    for path in documents:
        text = path.read_text(encoding="utf-8")
        for raw in LINK.findall(text):
            target = raw.strip().strip("<>")
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            file_part, separator, anchor = target.partition("#")
            destination = path if not file_part else path.parent / unquote(file_part)
            if not destination.exists():
                violations.append(Violation(path, f"link locale inesistente: {raw}"))
                continue
            if separator and anchor and destination.suffix.lower() == ".md":
                if unquote(anchor).lower() not in anchors(destination):
                    violations.append(Violation(path, f"anchor inesistente: {raw}"))
    return violations


def validate_commands(root: Path, documents: list[Path]) -> list[Violation]:
    violations: list[Violation] = []
    for path in documents:
        text = path.read_text(encoding="utf-8")
        for command_path in PYTHON_COMMAND.findall(text):
            normalized = command_path.replace("\\", "/")
            if not (root / normalized).is_file():
                violations.append(
                    Violation(path, f"comando Python punta a un file assente: {command_path}")
                )
    return violations


def validate_python_examples(documents: list[Path]) -> list[Violation]:
    """Ogni esempio Python deve essere almeno sintatticamente eseguibile."""

    violations: list[Violation] = []
    for path in documents:
        text = path.read_text(encoding="utf-8")
        for position, source in enumerate(PYTHON_FENCE.findall(text), start=1):
            try:
                compile(
                    source,
                    f"{path}#python-{position}",
                    "exec",
                    flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT,
                )
            except SyntaxError as exc:
                violations.append(
                    Violation(
                        path,
                        f"esempio Python {position} non valido: riga {exc.lineno}",
                    )
                )
    return violations


def validate_generated(root: Path) -> list[Violation]:
    if root != ROOT:
        return []
    from scripts.render_mariadb_evidence import (
        TARGET as EVIDENCE_TARGET,
        render as render_evidence,
    )
    from scripts.render_state import TARGET as STATE_TARGET, render as render_state
    from scripts.render_offline_microbench import (
        TARGET as MICROBENCH_TARGET,
        render as render_microbench,
    )
    violations: list[Violation] = []
    for target, renderer in (
        (STATE_TARGET, render_state),
        (EVIDENCE_TARGET, render_evidence),
        (MICROBENCH_TARGET, render_microbench),
    ):
        current = target.read_text(encoding="utf-8") if target.is_file() else ""
        if current != renderer():
            violations.append(Violation(target, "documento generato non aggiornato"))
    return violations


def validate_license(root: Path) -> list[Violation]:
    """Cargo e Python dichiarano la stessa licenza proprietaria."""

    violations: list[Violation] = []
    license_path = root / "LICENSE"
    if not license_path.is_file() or "Proprietary License" not in license_path.read_text(
        encoding="utf-8"
    ):
        violations.append(Violation(license_path, "licenza proprietaria assente"))

    cargo_path = root / "Cargo.toml"
    cargo = tomllib.loads(cargo_path.read_text(encoding="utf-8"))
    if cargo.get("workspace", {}).get("package", {}).get("license-file") != "LICENSE":
        violations.append(Violation(cargo_path, "workspace Cargo non usa LICENSE"))

    for manifest in sorted((root / "crates").glob("*/Cargo.toml")):
        package = tomllib.loads(manifest.read_text(encoding="utf-8")).get("package", {})
        inherited = package.get("license-file", {}).get("workspace")
        if inherited is not True:
            violations.append(Violation(manifest, "crate non eredita license-file"))

    pyproject = root / "crates/plenora-database-py/pyproject.toml"
    project = tomllib.loads(pyproject.read_text(encoding="utf-8")).get("project", {})
    if project.get("license") != {"text": "Proprietary"}:
        violations.append(Violation(pyproject, "licenza Python non proprietaria"))
    return violations


def validate_semantics(root: Path) -> list[Violation]:
    violations: list[Violation] = []

    sdk = root / "crates/plenora-database-py/README.md"
    migration = root / "crates/plenora-database-py/docs/MIGRATION_FROM_CLI.md"
    sdk_text = sdk.read_text(encoding="utf-8")
    migration_text = migration.read_text(encoding="utf-8")
    config_text = (root / "crates/plenora-database-py/python/plenora_database/config.py").read_text(
        encoding="utf-8"
    )
    for declaration, documented in (
        ("class EngineConfig:", "`EngineConfig`"),
        ("class PoolConfig:", "`PoolConfig`"),
        ("def engine_from_url(", "`engine_from_url`"),
        ("async def async_engine_from_url(", "`async_engine_from_url`"),
    ):
        if declaration not in config_text or documented not in sdk_text:
            violations.append(
                Violation(sdk, f"lifecycle SDK non documentato: {documented}")
            )
    if "non ancora esposto al SDK Python" in sdk_text:
        violations.append(Violation(sdk, "SQL Server e dichiarato contemporaneamente assente"))
    if '"database": {' in sdk_text or '"connection_pool": {' in sdk_text:
        violations.append(Violation(sdk, "metrics() e documentato come dizionario annidato"))
    if "Non c'e equivalente SDK diretto" in migration_text:
        violations.append(Violation(migration, "copy_from esiste ma la migrazione lo nega"))
    if "server_cursor` resta `false`" not in migration_text:
        violations.append(Violation(migration, "manca la distinzione streaming/cursore riapribile"))
    unsafe_session_guidance = (
        "_session = p.connect(dsn)",
        "UNA `Session` globale",
        "più query in parallelo sullo stesso Session",
    )
    for phrase in unsafe_session_guidance:
        if phrase in migration_text:
            violations.append(
                Violation(migration, "la guida propone una Session globale condivisa")
            )
    if "Ogni request/task apre invece la propria `Session`" not in migration_text:
        violations.append(
            Violation(migration, "manca il lifecycle Engine globale e Session per request")
        )
    if "*(s.execute_scalar" in sdk_text:
        violations.append(Violation(sdk, "l'esempio async condivide una Session fra task"))

    root_readme_path = root / "README.md"
    root_readme = root_readme_path.read_text(encoding="utf-8")
    current_guides = {
        root_readme_path: root_readme,
        sdk: sdk_text,
        migration: migration_text,
        root / "docs/python-sdk-2-migration.md": (
            root / "docs/python-sdk-2-migration.md"
        ).read_text(encoding="utf-8"),
    }
    stale_sdk_forms = (
        "p.create_engine(",
        "p.create_async_engine(",
        "pip install plenora-database",
        'selectinload("',
        'joinedload("',
        "Mapping policy: `compatible` (default)",
    )
    for path, text in current_guides.items():
        for stale in stale_sdk_forms:
            if stale in text:
                violations.append(Violation(path, f"forma SDK 1.x ancora documentata: {stale}"))
        for method in ("copy_from", "acopy_from"):
            for call in method_call_blocks(text, method):
                if "mapping_policy=" not in call:
                    violations.append(
                        Violation(path, f"{method} senza mapping_policy esplicita")
                    )
    for path in (root_readme_path, sdk, migration):
        text = current_guides[path]
        if "github.com/PlenoraETL/plenora-database-tools/releases" not in text:
            violations.append(Violation(path, "distribuzione GitHub Releases non documentata"))

    for command in (
        "scripts\\sweep.py",
        "scripts\\check_docs.py",
        "scripts\\check_cargo_deny.py",
        "cargo deny check",
    ):
        if command not in root_readme:
            violations.append(Violation(root / "README.md", f"gate canonico assente: {command}"))

    benchmark = root / "benchmarks/README.md"
    benchmark_text = benchmark.read_text(encoding="utf-8")
    if "tre famiglie" not in benchmark_text or "## MySQL" not in benchmark_text:
        violations.append(Violation(benchmark, "indice benchmark incompleto"))
    sdk_benchmark = benchmark_text.partition(
        "3. **Python SDK vs subprocess CLI**"
    )[2].partition("## Harness")[0]
    if "ms/call" in sdk_benchmark or "più veloce del CLI" in sdk_benchmark:
        violations.append(
            Violation(
                benchmark,
                "un risultato SDK/CLI volatile e copiato nell'indice benchmark",
            )
        )

    evidence = root / "docs/mariadb/EVIDENCE.md"
    evidence_text = evidence.read_text(encoding="utf-8")
    if "tranche" in evidence_text.lower():
        violations.append(Violation(evidence, "il documento corrente contiene un diario storico"))
    if "non equivale a un gate live passato" not in evidence_text:
        violations.append(Violation(evidence, "inventario e verdetto live non sono distinti"))

    changelog = root / "crates/plenora-database-py/CHANGELOG.md"
    changelog_text = changelog.read_text(encoding="utf-8")
    for stale in ("docs/adr/", "docs/reviews/"):
        if stale in changelog_text:
            violations.append(Violation(changelog, f"riferimento a documentazione rimossa: {stale}"))
    return violations


def scan(root: Path = ROOT) -> tuple[int, list[Violation]]:
    documents = markdown_documents(root)
    violations: list[Violation] = []
    for path in documents:
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            violations.append(Violation(path, f"documento illeggibile: {exc}"))
            continue
        if text.count("```") % 2:
            violations.append(Violation(path, "code fence non bilanciato"))
    violations += validate_links(root, documents)
    violations += validate_commands(root, documents)
    violations += validate_python_examples(documents)
    if root == ROOT:
        violations += validate_generated(root)
        violations += validate_license(root)
        violations += validate_semantics(root)
    return len(documents), violations


def main() -> int:
    checked, violations = scan()
    if violations:
        for violation in violations:
            try:
                relative = violation.path.relative_to(ROOT)
            except ValueError:
                relative = violation.path
            print(f"{relative}: docs: {violation.reason}")
        return 1
    print(f"docs: {checked} documenti controllati, nessuna violazione")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
