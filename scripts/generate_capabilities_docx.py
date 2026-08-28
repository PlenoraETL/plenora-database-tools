#!/usr/bin/env python3
"""Genera il catalogo DOCX delle capability da fonti versionate del repository."""

from __future__ import annotations

import argparse
import ast
import hashlib
import html
import io
import re
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "docs" / "PLENORA-CAPABILITIES-CLI-SDK.docx"
STATE = ROOT / "docs" / "STATO.md"
SDK_README = ROOT / "crates" / "plenora-database-py" / "README.md"
PACKAGE = ROOT / "crates" / "plenora-database-py" / "python" / "plenora_database"
FIXED_ZIP_TIME = (2026, 8, 28, 0, 0, 0)


def markdown_table(path: Path, heading: str) -> list[list[str]]:
    """Estrae una tabella Markdown sotto un heading esatto."""

    lines = path.read_text(encoding="utf-8").splitlines()
    start = lines.index(heading) + 1
    while start < len(lines) and not lines[start].startswith("|"):
        start += 1
    rows: list[list[str]] = []
    while start < len(lines) and lines[start].startswith("|"):
        cells = [cell.strip().strip("`") for cell in lines[start].strip("|").split("|")]
        if not all(re.fullmatch(r":?-+:?", cell) for cell in cells):
            rows.append(cells)
        start += 1
    return rows


def exported_names() -> list[str]:
    tree = ast.parse((PACKAGE / "__init__.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets
        ):
            return [
                item.value
                for item in node.value.elts
                if isinstance(item, ast.Constant) and isinstance(item.value, str)
            ]
    raise RuntimeError("__all__ del package non trovato")


def public_class_inventory(exports: set[str]) -> list[tuple[str, list[str]]]:
    classes: dict[str, list[str]] = {}
    for path in sorted(PACKAGE.glob("*.pyi")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            if not isinstance(node, ast.ClassDef) or node.name not in exports:
                continue
            methods = {
                item.name
                for item in node.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                and not item.name.startswith("__")
            }
            classes.setdefault(node.name, []).extend(sorted(methods - set(classes.get(node.name, []))))
    return sorted(classes.items())


def source_fingerprint() -> str:
    """Identifica gli input del documento senza dipendere dal commit contenitore."""

    digest = hashlib.sha256()
    for path in (STATE, SDK_README, PACKAGE / "__init__.py", *sorted(PACKAGE.glob("*.pyi"))):
        digest.update(path.relative_to(ROOT).as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()[:12]


def xml_text(value: str) -> str:
    return html.escape(value, quote=False)


def paragraph(text: str = "", style: str | None = None, *, page_break: bool = False) -> str:
    properties = "" if style is None else f'<w:pPr><w:pStyle w:val="{style}"/></w:pPr>'
    if page_break:
        return '<w:p><w:r><w:br w:type="page"/></w:r></w:p>'
    lines = text.split("\n") or [""]
    runs = "<w:br/>".join(
        f'<w:r><w:t xml:space="preserve">{xml_text(line)}</w:t></w:r>' for line in lines
    )
    return f"<w:p>{properties}{runs}</w:p>"


def table(rows: list[list[str]]) -> str:
    body = []
    for row_index, row in enumerate(rows):
        cells = []
        for value in row:
            shade = '<w:shd w:fill="D9EAF7"/>' if row_index == 0 else ""
            bold = '<w:rPr><w:b/></w:rPr>' if row_index == 0 else ""
            cells.append(
                "<w:tc><w:tcPr>"
                + shade
                + "</w:tcPr><w:p><w:r>"
                + bold
                + f'<w:t xml:space="preserve">{xml_text(value)}</w:t>'
                + "</w:r></w:p></w:tc>"
            )
        body.append("<w:tr>" + "".join(cells) + "</w:tr>")
    borders = (
        '<w:tblBorders><w:top w:val="single" w:sz="4" w:color="B7C9D6"/>'
        '<w:left w:val="single" w:sz="4" w:color="B7C9D6"/>'
        '<w:bottom w:val="single" w:sz="4" w:color="B7C9D6"/>'
        '<w:right w:val="single" w:sz="4" w:color="B7C9D6"/>'
        '<w:insideH w:val="single" w:sz="4" w:color="B7C9D6"/>'
        '<w:insideV w:val="single" w:sz="4" w:color="B7C9D6"/></w:tblBorders>'
    )
    return '<w:tbl><w:tblPr><w:tblStyle w:val="TableGrid"/>' + borders + "</w:tblPr>" + "".join(body) + "</w:tbl>"


def render_document() -> bytes:
    reads = markdown_table(STATE, "### `reads`")
    writes = markdown_table(STATE, "### `writes`")
    transactions = markdown_table(STATE, "### `transactions`")
    cli_commands = markdown_table(STATE, "## Sub-comandi del CLI")
    compatibility = markdown_table(SDK_README, "## Compatibility")
    exports = exported_names()
    classes = public_class_inventory(set(exports))

    blocks = [
        paragraph("Plenora Database Tools", "Title"),
        paragraph("Catalogo completo delle capability CLI e SDK Python", "Subtitle"),
        paragraph(f"Snapshot degli input {source_fingerprint()}. Contratto attivo: v2."),
        paragraph(
            "Documento generato: matrici provider e catalogo CLI provengono da docs/STATO.md; "
            "l'inventario SDK deriva dagli stub e da __all__. Le capability runtime restano "
            "l'autorità per la specifica connessione.",
            "Quote",
        ),
        paragraph(page_break=True),
        paragraph("1. Sintesi", "Heading1"),
        paragraph(
            "La libreria offre un Core Rust comune, un CLI operativo e uno SDK Python sync/async "
            "per PostgreSQL/PostGIS, MySQL, MariaDB, SQL Server e IBM Db2 LUW. Il modello è "
            "evidence-first: una funzione resta chiusa finché non è sostenuta da una prova "
            "riproducibile; una capability assente equivale a negata."
        ),
        paragraph("Punti distintivi", "Heading2"),
        paragraph("• contratti JSON versionati e validazione fail-closed;", "ListBullet"),
        paragraph("• engine condivisibile, sessione per unità di lavoro e pool governato dal Core;", "ListBullet"),
        paragraph("• API sync e async con lo stesso modello e gli stessi errori;", "ListBullet"),
        paragraph("• streaming e bulk Arrow separati dal percorso OLTP;", "ListBullet"),
        paragraph("• supporto spatial qualificato per prodotto e semantica;", "ListBullet"),
        paragraph("• errori pubblici senza DSN, SQL bindato, token o valori di cella.", "ListBullet"),
        paragraph("2. Superfici CLI e SDK", "Heading1"),
        table(
            [
                ["Area", "CLI", "SDK Python", "Garanzia/nota"],
                ["Connessione", "probe e comandi database-*", "connect*/aconnect*, Engine", "factory provider esplicite"],
                ["Lifecycle", "processo per invocazione", "Engine, session, dispose", "una sessione per request/task"],
                ["SQL raw", "execute-sql/scalar/ddl", "execute*, execute_scalar", "bind separati dai messaggi"],
                ["Query portabili", "portable-compile/execute", "builder CRUD legacy", "adapter verso il Core"],
                ["Expression Core v3", "DSL non esposta", "table/select/bind/Result", "statement immutabili"],
                ["Metadata", "inspect/describe", "session.inspect", "cataloghi, schemi, oggetti, colonne"],
                ["Lettura Arrow", "read-ipc/read-summary", "read/aread", "streaming con backpressure"],
                ["Scrittura Arrow", "write-ipc/bulk-write", "copy_from/acopy_from", "mode governate dalle capability"],
                ["Transazioni", "transaction-test", "begin, savepoint, commit/rollback", "esito commit ambiguo esplicito"],
                ["Spatial", "test-spatial e piani", "spatial reference/predicati", "geometry/geography per provider"],
                ["Diagnostica", "doctor/diagnose/benchmark", "metrics/statistics/errors", "misure legate alla corsa"],
            ]
        ),
        paragraph("3. Capability di lettura", "Heading1"),
        table(reads),
        paragraph(
            "Streaming significa consegna incrementale a batch; non significa cursore server "
            "nominato o riprendibile. OFFSET è ammesso solo con un ordinamento deterministico."
        ),
        paragraph("4. Capability di scrittura", "Heading1"),
        table(writes),
        paragraph(
            "Il flag returning riguarda il percorso bulk WriteOutcome, non il RETURNING limitato "
            "dei builder OLTP. TruncateInsert resta chiusa su MySQL, MariaDB e Db2. Bulk Db2 è "
            "negato finché non esiste una prova riproducibile."
        ),
        paragraph("5. Capability transazionali", "Heading1"),
        table(transactions),
        paragraph(
            "MySQL e MariaDB hanno DDL con commit implicito: le righe possono essere annullate, "
            "ma uno schema creato può sopravvivere. SQL Server non offre RELEASE SAVEPOINT; il "
            "rilascio avviene al termine della transazione."
        ),
        paragraph("6. Spatial", "Heading1"),
        table(
            [
                ["Provider", "Semantiche", "WKB", "Dimensioni", "CRS dichiarato", "Indice"],
                ["PostgreSQL", "geometry + geography se PostGIS presente", "read/write sondati", "XY e profili sondati", "no", "sondato per semantica"],
                ["MySQL", "geometry", "read/write", "XY", "può essere richiesto", "sì"],
                ["MariaDB", "geometry", "read/write qualificato", "XY", "sì", "sì"],
                ["SQL Server", "geometry + geography se UDT presenti", "read/write", "XY/XYZ/XYM/XYZM", "no", "sì se entrambe le semantiche"],
                ["IBM Db2 LUW", "geometry se Spatial Extender presente", "read/write", "XY/XYZ", "sì", "no"],
            ]
        ),
        paragraph(
            "L'elenco esatto delle funzioni spatial è pubblicato a runtime in "
            "capabilities.spatial.functions_by_semantics. PostgreSQL e SQL Server distinguono "
            "geometry e geography; l'intersezione comune è disponibile in functions. Db2 "
            "qualifica SRID, dimensione, intersects, contains e within."
        ),
        paragraph("7. Query ed expression language", "Heading1"),
        paragraph(
            "Tre percorsi coesistono durante la migrazione: SQL raw; builder portable legacy per "
            "select/insert/update/delete/upsert; expression language Core v3 che serializza "
            "direttamente nell'IR relazionale canonico. Il nuovo primo incremento copre colonne, "
            "alias, join, confronti, AND/OR/NOT, distinct, order by, limit e offset."
        ),
        paragraph(
            "Result è oggi uno snapshot bufferizzato: all, first, one, one_or_none, scalar, "
            "scalar_one e scalar_one_or_none. Streaming Result, Row tipizzata, funzioni, "
            "aggregati, finestre, CTE e DML Core v3 restano passi successivi della roadmap."
        ),
        paragraph("8. Concorrenza, lifecycle e sicurezza", "Heading1"),
        paragraph("• Engine è condivisibile; sessione e transazione appartengono a una sola unità concorrente.", "ListBullet"),
        paragraph("• La sessione è esclusiva durante una transazione esplicita e torna riusabile dopo commit/rollback.", "ListBullet"),
        paragraph("• Context manager sync/async chiudono o annullano gli scope in caso di eccezione.", "ListBullet"),
        paragraph("• Timeout, cancellazione, limiti di risorse e categorie retry/remote-effect sono strutturati.", "ListBullet"),
        paragraph("• TLS è secure-by-default; le fixture non sicure richiedono una scelta esplicita.", "ListBullet"),
        paragraph("9. Compatibilità e qualificazione", "Heading1"),
        table(compatibility),
        paragraph(
            "Db2 è qualificato live su Linux x86_64 e build-only su Windows x86_64. Su macOS la "
            "feature Db2 resta fail-closed perché non esiste ancora una matrice client IBM "
            "supportata. Un gate saltato non viene contato come passato."
        ),
        paragraph("10. Catalogo completo dei comandi CLI", "Heading1"),
        table(cli_commands),
        paragraph("11. Inventario pubblico dello SDK", "Heading1"),
        paragraph("Export top-level", "Heading2"),
    ]
    for start in range(0, len(exports), 12):
        blocks.append(paragraph(", ".join(exports[start : start + 12]), "Code"))
    blocks.append(paragraph("Classi e metodi pubblici", "Heading2"))
    for class_name, methods in classes:
        blocks.append(paragraph(class_name, "Heading3"))
        blocks.append(paragraph(", ".join(methods) if methods else "Nessun metodo pubblico nello stub.", "Code"))
    blocks.extend(
        [
            paragraph("12. Confini dichiarati", "Heading1"),
            paragraph("• nessun provider offre cursori riapribili o stream resumable;", "ListBullet"),
            paragraph("• nessun provider pubblica array binding nel contratto bulk;", "ListBullet"),
            paragraph("• il percorso bulk non restituisce righe generate dal server;", "ListBullet"),
            paragraph("• la libreria non è ancora un ORM completo: identity map, unit of work e migration planner sono roadmap;", "ListBullet"),
            paragraph("• una query native resta un escape hatch governabile dalla NativeQueryPolicy.", "ListBullet"),
            paragraph("13. Fonti e interpretazione", "Heading1"),
            paragraph(
                "Fonti: docs/STATO.md (generato dal codice), contracts/v2, capability provider, "
                "stub PEP 561 dello SDK, catalogo comandi CLI, README operativo e workflow. Le "
                "capability della connessione concreta si leggono sempre da session.capabilities "
                "o dal comando database-probe: versione del server, estensioni e funzioni spatial "
                "possono dipendere dal target raggiunto."
            ),
        ]
    )

    document = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        "<w:body>"
        + "".join(blocks)
        + '<w:sectPr><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="1134" w:right="850" w:bottom="1134" w:left="850"/></w:sectPr>'
        + "</w:body></w:document>"
    )
    styles = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
<w:style w:type="paragraph" w:default="1" w:styleId="Normal"><w:name w:val="Normal"/><w:rPr><w:rFonts w:ascii="Aptos" w:hAnsi="Aptos"/><w:sz w:val="20"/><w:lang w:val="it-IT"/></w:rPr><w:pPr><w:spacing w:after="120"/></w:pPr></w:style>
<w:style w:type="paragraph" w:styleId="Title"><w:name w:val="Title"/><w:basedOn w:val="Normal"/><w:rPr><w:b/><w:color w:val="17365D"/><w:sz w:val="44"/></w:rPr><w:pPr><w:spacing w:after="220"/></w:pPr></w:style>
<w:style w:type="paragraph" w:styleId="Subtitle"><w:name w:val="Subtitle"/><w:basedOn w:val="Normal"/><w:rPr><w:color w:val="477A9E"/><w:sz w:val="28"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="Heading1"><w:name w:val="heading 1"/><w:basedOn w:val="Normal"/><w:outlineLvl w:val="0"/><w:rPr><w:b/><w:color w:val="17365D"/><w:sz w:val="30"/></w:rPr><w:pPr><w:keepNext/><w:spacing w:before="300" w:after="140"/></w:pPr></w:style>
<w:style w:type="paragraph" w:styleId="Heading2"><w:name w:val="heading 2"/><w:basedOn w:val="Normal"/><w:outlineLvl w:val="1"/><w:rPr><w:b/><w:color w:val="275F85"/><w:sz w:val="25"/></w:rPr><w:pPr><w:keepNext/><w:spacing w:before="220" w:after="100"/></w:pPr></w:style>
<w:style w:type="paragraph" w:styleId="Heading3"><w:name w:val="heading 3"/><w:basedOn w:val="Normal"/><w:outlineLvl w:val="2"/><w:rPr><w:b/><w:color w:val="477A9E"/><w:sz w:val="22"/></w:rPr><w:pPr><w:keepNext/></w:pPr></w:style>
<w:style w:type="paragraph" w:styleId="Quote"><w:name w:val="Quote"/><w:basedOn w:val="Normal"/><w:pPr><w:ind w:left="360"/><w:shd w:fill="EEF5FA"/></w:pPr><w:rPr><w:i/><w:color w:val="36596F"/></w:rPr></w:style>
<w:style w:type="paragraph" w:styleId="ListBullet"><w:name w:val="List Bullet"/><w:basedOn w:val="Normal"/><w:pPr><w:ind w:left="360" w:hanging="180"/></w:pPr></w:style>
<w:style w:type="paragraph" w:styleId="Code"><w:name w:val="Code"/><w:basedOn w:val="Normal"/><w:rPr><w:rFonts w:ascii="Consolas" w:hAnsi="Consolas"/><w:sz w:val="17"/></w:rPr><w:pPr><w:shd w:fill="F3F5F7"/><w:ind w:left="180"/></w:pPr></w:style>
<w:style w:type="table" w:styleId="TableGrid"><w:name w:val="Table Grid"/></w:style>
</w:styles>'''
    content_types = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
<Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>
<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>'''
    root_rels = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>'''
    doc_rels = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>'''
    core = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<dc:title>Plenora Database Tools — Capability CLI e SDK</dc:title><dc:creator>Plenora ETL</dc:creator><dc:subject>Catalogo capability generato</dc:subject><dc:language>it-IT</dc:language><dcterms:created xsi:type="dcterms:W3CDTF">2026-08-28T00:00:00Z</dcterms:created></cp:coreProperties>'''
    app = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"><Application>Plenora capability generator</Application></Properties>'''

    entries = {
        "[Content_Types].xml": content_types,
        "_rels/.rels": root_rels,
        "word/document.xml": document,
        "word/styles.xml": styles,
        "word/_rels/document.xml.rels": doc_rels,
        "docProps/core.xml": core,
        "docProps/app.xml": app,
    }
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name, value in entries.items():
            info = zipfile.ZipInfo(name, FIXED_ZIP_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            archive.writestr(info, value.encode("utf-8"))
    return output.getvalue()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = render_document()
    if args.check:
        if not TARGET.is_file() or TARGET.read_bytes() != rendered:
            print(f"capability-docx: {TARGET.relative_to(ROOT)} non aggiornato")
            return 1
        print("capability-docx: documento aggiornato")
        return 0
    TARGET.write_bytes(rendered)
    print(f"capability-docx: scritto {TARGET.relative_to(ROOT)} ({len(rendered)} byte)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
