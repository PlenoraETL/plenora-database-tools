#!/usr/bin/env python3
"""Le prove live della guardia del server sugli operandi scalari geometrici.

`plenora_database_core::spatial_operand::operand_server_guard` dice dove il
server rifiuta ogni operatore scalare su una colonna geometrica, cosi che
l'esecuzione non debba leggere il catalogo: sotto quella guardia le forme
arrivano al server. Una guardia cosi non vale niente se nessuno la misura:
questo script la misura, sul server vero, con lo SQL che il renderer del
repository rende per ogni forma (`tests/fixtures/operandi_server_guard.json`,
provato in `crates/plenora-database-sql/tests/operandi_geometrici.rs`), e
rifiuta il gate se un fatto cambia.

Per ogni forma si eseguono due istruzioni:

- la forma sulla colonna geometrica `G`: l'atteso e un errore del server
  (`guardia`), o l'esecuzione per le forme che la regola ammette (`IS NULL`,
  `COUNT`, `UNION ALL`);
- la stessa forma sulla colonna non geometrica che il fixture nomina
  (`controllo`): deve passare. Prova che l'errore viene dalla geometria e non
  da uno SQL che il server rifiuterebbe comunque.

I bind sono veri bind tipati: su Oracle un blocco PL/SQL apre un cursore
sullo SQL reso con `OPEN ... FOR ... USING` (`RAW`, `VARCHAR2`, `NUMBER`); su
Db2 un'istruzione composta prepara lo SQL reso e apre un cursore `USING`
(`VARCHAR FOR BIT DATA`, `VARCHAR`, `INTEGER`). L'apertura fa l'analisi
semantica, dove il server rifiuta i tipi.

Si misura anche il catalogo su cui si regge il riconoscimento del tipo: il
proprietario del tipo della colonna (`ALL_TAB_COLUMNS.DATA_TYPE_OWNER`,
`SYSCAT.COLUMNS.TYPESCHEMA`). Su Db2, che giudica gli operandi sul catalogo
(`crates/plenora-db-db2/src/operand_catalog.rs`), anche i fatti di `SYSCAT`
su cui il giudizio si regge (`GIUDIZIO_DEL_CATALOGO`): i tipi spatial
distinti su `BLOB`, i tipi dell'utente che il server non crea su una
geometria, la colonna `BLOB` di una vista resa da `ST_CENTROID`, la base di
un alias, gli spazi finali di un identificatore, le tabelle temporanee
dichiarate senza geometrie.

    python scripts/sonde_operandi_server.py --server oracle --riferimenti --docker "wsl docker"
    python scripts/sonde_operandi_server.py --server db2 --container NOME --referto db2.json

`--riferimenti` avvia l'immagine qualificata del prodotto (per digest, dalla
sua fonte: `docker-compose.oracle.yml`, il `FROM` di
`docker/db2-client/Dockerfile`); `--avvia IMMAGINE` un'altra; entrambe usano un
container usa e getta (`docker run --rm`, Db2 `--privileged`) e lo fermano
alla fine. `--container NOME` usa un container gia pronto e non lo tocca.
L'uscita e 0 soltanto se ogni sonda ha l'esito atteso.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "operandi_server_guard.json"
SERVER = ("oracle", "db2")

ORACLE_PASSWORD = "Sonde_Operandi_2026"
DB2_PASSWORD = "sonde_operandi"

AMBIENTE = {
    "oracle": {"ORACLE_PASSWORD": ORACLE_PASSWORD, "APP_USER": "plenora",
               "APP_USER_PASSWORD": ORACLE_PASSWORD},
    "db2": {"LICENSE": "accept", "DB2INSTANCE": "db2inst1", "DB2INST1_PASSWORD": DB2_PASSWORD,
            "DBNAME": "sonde", "BLU": "false", "ENABLE_ORACLE_COMPATIBILITY": "false",
            "UPDATEAVAIL": "NO", "TO_CREATE_SAMPLEDB": "false", "REPODB": "false",
            "PERSISTENT_HOME": "false", "HADR_ENABLED": "false", "IS_OSXFS": "false"},
}

PREPARAZIONE = {
    "oracle": (
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE SONDE_OPERANDI PURGE'; "
        "EXCEPTION WHEN OTHERS THEN IF SQLCODE <> -942 THEN RAISE; END IF; END;",
        "CREATE TABLE SONDE_OPERANDI (ID NUMBER(10), NAME VARCHAR2(20), B RAW(16), "
        "G MDSYS.SDO_GEOMETRY)",
        "INSERT INTO SONDE_OPERANDI VALUES (1, 'a', HEXTORAW('01'), "
        "MDSYS.SDO_GEOMETRY(2001, 4326, MDSYS.SDO_POINT_TYPE(1, 2, NULL), NULL, NULL))",
        "INSERT INTO SONDE_OPERANDI VALUES (2, 'b', HEXTORAW('02'), "
        "MDSYS.SDO_GEOMETRY(2001, 4326, MDSYS.SDO_POINT_TYPE(1, 2, NULL), NULL, NULL))",
        "COMMIT",
    ),
    "db2": (
        "BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '42704' BEGIN END; "
        "EXECUTE IMMEDIATE 'DROP ALIAS PLENORA.SONDE_OPERANDI_A'; END",
        "BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '42704' BEGIN END; "
        "EXECUTE IMMEDIATE 'DROP VIEW PLENORA.SONDE_OPERANDI_V'; END",
        "BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '42704' BEGIN END; "
        "EXECUTE IMMEDIATE 'DROP TABLE PLENORA.SONDE_OPERANDI'; END",
        # Una riga con un tipo spatial non sta in una pagina da 4K (SQL0286N).
        "BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END; "
        "EXECUTE IMMEDIATE 'CREATE BUFFERPOOL SONDE_BP8K SIZE AUTOMATIC PAGESIZE 8K'; END",
        "BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END; "
        "EXECUTE IMMEDIATE 'CREATE LARGE TABLESPACE SONDE_TS8K PAGESIZE 8K "
        "BUFFERPOOL SONDE_BP8K'; END",
        # Le tabelle temporanee di una subquery con una colonna spatial non
        # stanno nello spazio temporaneo da 4K (SQL1585N).
        "BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END; "
        "EXECUTE IMMEDIATE 'CREATE BUFFERPOOL SONDE_BP32K SIZE AUTOMATIC PAGESIZE 32K'; END",
        "BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END; "
        "EXECUTE IMMEDIATE 'CREATE SYSTEM TEMPORARY TABLESPACE SONDE_TMP32K PAGESIZE 32K "
        "BUFFERPOOL SONDE_BP32K'; END",
        "CREATE TABLE PLENORA.SONDE_OPERANDI (ID INTEGER, NAME VARCHAR(20), "
        "B VARCHAR(16) FOR BIT DATA, G ST_GEOMETRY) IN SONDE_TS8K",
        "INSERT INTO PLENORA.SONDE_OPERANDI VALUES "
        "(1, 'a', BX'01', ST_GEOMETRY('POINT (1 2)', 4326)), "
        "(2, 'b', BX'02', ST_GEOMETRY('POINT (1 2)', 4326))",
        # Le relazioni su cui si regge il giudizio del catalogo
        # (`crates/plenora-db-db2/src/operand_catalog.rs`).
        "CREATE VIEW PLENORA.SONDE_OPERANDI_V AS "
        "SELECT ID, ST_CENTROID(G) AS C FROM PLENORA.SONDE_OPERANDI",
        "CREATE ALIAS PLENORA.SONDE_OPERANDI_A FOR PLENORA.SONDE_OPERANDI",
        "COMMIT",
    ),
}

# I fatti del catalogo su cui si regge il giudizio degli operandi di Db2
# (`crates/plenora-db-db2/src/operand_catalog.rs`), che legge SYSCAT nella
# transazione: sonda, istruzione, esito atteso.
GIUDIZIO_DEL_CATALOGO = {
    "db2": (
        ("i tipi spatial di SYSIBM sono distinti su BLOB",
         "SELECT COUNT(*) FROM SYSCAT.DATATYPES WHERE TYPESCHEMA = 'SYSIBM' "
         "AND LEFT(TYPENAME, 3) = 'ST_' AND (METATYPE <> 'T' OR SOURCENAME <> 'BLOB')",
         {"valore": "0"}),
        ("un tipo distinto su ST_GEOMETRY si rifiuta",
         "CREATE DISTINCT TYPE PLENORA.SONDE_DGEO AS ST_GEOMETRY", {"errore": ["SQL20441N"]}),
        ("un tipo strutturato UNDER ST_GEOMETRY si rifiuta",
         "CREATE TYPE PLENORA.SONDE_SGEO UNDER ST_GEOMETRY AS (X INTEGER) MODE DB2SQL",
         {"errore": ["SQL20026N"]}),
        ("una colonna di vista resa da ST_CENTROID e SYSIBM.BLOB",
         "SELECT RTRIM(TYPESCHEMA) || '.' || TYPENAME FROM SYSCAT.COLUMNS "
         "WHERE TABSCHEMA = 'PLENORA' AND TABNAME = 'SONDE_OPERANDI_V' AND COLNAME = 'C'",
         {"valore": "SYSIBM.BLOB"}),
        ("la colonna BLOB della vista si confronta con la geometria senza errore",
         "SELECT COUNT(*) FROM PLENORA.SONDE_OPERANDI_V V, PLENORA.SONDE_OPERANDI T "
         "WHERE V.C = T.G", {"ok": True}),
        ("un alias porta il tipo A e la sua base",
         "SELECT TYPE || ' ' || RTRIM(BASE_TABSCHEMA) || '.' || BASE_TABNAME FROM SYSCAT.TABLES "
         "WHERE TABSCHEMA = 'PLENORA' AND TABNAME = 'SONDE_OPERANDI_A'",
         {"valore": "A PLENORA.SONDE_OPERANDI"}),
        ("il server ignora gli spazi finali di un identificatore",
         "SELECT COUNT(*) FROM PLENORA.\"SONDE_OPERANDI \"", {"valore": "2"}),
        ("una tabella temporanea dichiarata non ha colonne ST_GEOMETRY",
         "DECLARE GLOBAL TEMPORARY TABLE SESSION.SONDE_OPERANDI (G ST_GEOMETRY) "
         "ON COMMIT PRESERVE ROWS NOT LOGGED", {"errore": ["SQL0350N"]}),
    ),
}

# Il catalogo su cui si regge il riconoscimento del tipo geometrico nei
# provider: il proprietario del tipo, non soltanto il nome.
CATALOGO = {
    "oracle": ("SELECT DATA_TYPE_OWNER || '.' || DATA_TYPE FROM ALL_TAB_COLUMNS "
               "WHERE OWNER = 'PLENORA' AND TABLE_NAME = 'SONDE_OPERANDI' AND COLUMN_NAME = 'G'",
               "MDSYS.SDO_GEOMETRY"),
    "db2": ("SELECT RTRIM(TYPESCHEMA) || '.' || TYPENAME FROM SYSCAT.COLUMNS "
            "WHERE TABSCHEMA = 'PLENORA' AND TABNAME = 'SONDE_OPERANDI' AND COLNAME = 'G'",
            "SYSIBM.ST_GEOMETRY"),
}

# Il valore di ogni tipo di bind, nella dichiarazione del blocco.
DICHIARAZIONE = {
    "oracle": {"binary": ("RAW(16)", "HEXTORAW('01')"),
               "string": ("VARCHAR2(20)", "'a'"),
               "integer": ("NUMBER(10)", "1")},
    "db2": {"binary": ("VARCHAR(16) FOR BIT DATA", "BX'01'"),
            "string": ("VARCHAR(20)", "'a'"),
            "integer": ("INTEGER", "1")},
}

CODICE = {
    "oracle": re.compile(r"\b(ORA-\d{5})\b"),
    "db2": re.compile(r"\b(SQL\d{4,5}N)\b"),
}


def carica(percorso: Path = FIXTURE) -> dict:
    return json.loads(percorso.read_text(encoding="utf-8"))


def bind_tipati(piano: object) -> dict[str, str]:
    """Nome e tipo di ogni `typed_parameter` del piano."""
    trovati: dict[str, str] = {}
    pila = [piano]
    while pila:
        nodo = pila.pop()
        if isinstance(nodo, dict):
            if nodo.get("kind") == "typed_parameter":
                trovati[nodo["name"]] = nodo["parameter_type"]
            pila.extend(nodo.values())
        elif isinstance(nodo, list):
            pila.extend(nodo)
    return trovati


def sql_di_controllo(sql: str, colonna: str) -> str:
    """Lo SQL della forma sulla colonna non geometrica: il renderer quota
    ogni identificatore, e `"G"` nomina soltanto la colonna geometrica (e il
    suo alias di uscita). L'equivalenza con la resa del piano di controllo e
    provata offline in `operandi_geometrici.rs`."""
    return sql.replace('"G"', f'"{colonna}"')


def blocco(server: str, sql: str, binds: list[str], tipi: dict[str, str]) -> str:
    """Il blocco che apre un cursore sullo SQL reso con i bind tipati."""
    dichiarazioni = []
    nomi = []
    for indice, nome in enumerate(binds, start=1):
        tipo, valore = DICHIARAZIONE[server][tipi[nome]]
        variabile = f"v{indice}"
        nomi.append(variabile)
        dichiarazioni.append((variabile, tipo, valore))
    testo = sql.replace("'", "''")
    using = f" USING {', '.join(nomi)}" if nomi else ""
    if server == "oracle":
        dichiara = "".join(f"{v} {t} := {x}; " for v, t, x in dichiarazioni)
        return (f"DECLARE c SYS_REFCURSOR; {dichiara}BEGIN OPEN c FOR '{testo}'{using}; "
                "CLOSE c; END;")
    dichiara = "".join(f"DECLARE {v} {t} DEFAULT {x}; " for v, t, x in dichiarazioni)
    return (f"BEGIN {dichiara}DECLARE s STATEMENT; DECLARE c CURSOR FOR s; "
            f"PREPARE s FROM '{testo}'; OPEN c{using}; CLOSE c; END")


def client(server: str, istruzioni: list[str]) -> tuple[list[str], str]:
    """Il client del prodotto dentro il container e il suo ingresso."""
    if server == "oracle":
        corpo = "".join(f"{istruzione}\n/\n" for istruzione in istruzioni)
        ingresso = ("WHENEVER SQLERROR CONTINUE\nSET FEEDBACK OFF\nSET HEADING OFF\n"
                    "SET PAGESIZE 0\nSET SQLBLANKLINES ON\n" + corpo + "EXIT\n")
        return (["sqlplus", "-S", "-L", f"plenora/{ORACLE_PASSWORD}@localhost/FREEPDB1"],
                ingresso)
    corpo = "".join(f"{istruzione}@\n" for istruzione in istruzioni)
    comando = ". ~/sqllib/db2profile && db2 -x -td@ +p connect to sonde@ >/dev/null && db2 -x -td@ +p"
    return (["su", "-", "db2inst1", "-c", comando], corpo)


def esito(server: str, completato: subprocess.CompletedProcess) -> dict:
    """L'esito osservato: il primo codice d'errore del server, o l'ultima riga.

    Un codice nell'uscita e un errore anche con exit 0 (`sqlplus` senza
    `WHENEVER SQLERROR EXIT`); un exit d'errore senza codice e un errore
    senza codice (`db2` rende 4 o 8, 1 e 2 sono «nessuna riga» e «avviso»)."""
    uscita = (completato.stdout or "") + (completato.stderr or "")
    trovato = CODICE[server].search(uscita)
    if trovato:
        return {"errore": trovato.group(1)}
    fallito = completato.returncode >= 4 if server == "db2" else completato.returncode != 0
    if fallito:
        return {"errore": None}
    righe = [riga.strip() for riga in (completato.stdout or "").splitlines() if riga.strip()]
    return {"valore": righe[-1] if righe else None}


def prova(forma: dict, server: str) -> str:
    """La voce della tabella delle guardie che la misura sostiene: `guardia`
    (il server rifiuta), `senza-guardia` (il server esegue un operatore
    scalare sulla geometria), `senza-effetto` (il server esegue perche la
    colonna non e mai letta: nessuna riga dipende dall'operatore), `ammessa`
    (la regola la ammette: `IS NULL`, `COUNT`, `UNION ALL`)."""
    if "errore" in forma["esito"][server]:
        return "guardia"
    if forma.get("ammessa"):
        return "ammessa"
    if forma.get("colonna_mai_letta"):
        return "senza-effetto"
    return "senza-guardia"


def verdetto(atteso: dict, osservato: dict) -> bool:
    if "errore" in atteso:
        return osservato.get("errore") in set(atteso["errore"])
    return "errore" not in osservato


class Docker:
    def __init__(self, prefisso: str, container: str) -> None:
        self.prefisso, self.container = shlex.split(prefisso), container

    def exec(self, argv: list[str], ingresso: str = "", timeout: int = 300
             ) -> subprocess.CompletedProcess:
        return subprocess.run(self.prefisso + ["exec", "-i", self.container] + argv,
                              input=ingresso, capture_output=True, text=True, timeout=timeout)

    def run(self, argv: list[str], timeout: int = 600) -> subprocess.CompletedProcess:
        return subprocess.run(self.prefisso + argv, capture_output=True, text=True,
                              timeout=timeout)


def esegui(docker: Docker, server: str, istruzione: str) -> dict:
    argv, ingresso = client(server, [istruzione])
    return esito(server, docker.exec(argv, ingresso))


def pronto(docker: Docker, server: str) -> bool:
    if server == "oracle":
        argv, ingresso = client(server, ["SELECT 'pronto' FROM DUAL"])
    else:
        argv, ingresso = client(server, ["SELECT 'pronto' FROM SYSIBM.SYSDUMMY1"])
    completato = docker.exec(argv, ingresso, timeout=120)
    return "pronto" in (completato.stdout or "") and not CODICE[server].search(
        (completato.stdout or "") + (completato.stderr or ""))


# Il setup dell'immagine Db2 risponde a una query prima di aver finito:
# riavvia l'istanza dopo la creazione del database, e un'istruzione in quel
# momento esce con 4 senza codice (osservato il 25/10 sulla seconda istruzione
# della preparazione). Il container avviato dallo script si attende fino a
# questa riga del suo log.
SETUP_COMPLETO = {"db2": "Setup has completed"}


def setup_completo(docker: Docker, server: str) -> bool:
    riga = SETUP_COMPLETO.get(server)
    if riga is None:
        return True
    log = docker.run(["logs", docker.container], timeout=120)
    return riga in (log.stdout or "") + (log.stderr or "")


def attendi(docker: Docker, server: str, secondi: int, avviato: bool = False) -> None:
    limite = time.monotonic() + secondi
    while time.monotonic() < limite:
        if (not avviato or setup_completo(docker, server)) and pronto(docker, server):
            return
        time.sleep(5)
    raise SystemExit(f"il server {server} non risponde: il gate non si puo' eseguire")


def prepara(docker: Docker, server: str) -> None:
    for istruzione in PREPARAZIONE[server]:
        osservato = esegui(docker, server, istruzione)
        if "errore" in osservato:
            raise SystemExit(f"preparazione {server} fallita: {osservato['errore']}")


def misura(docker: Docker, server: str, fixture: dict) -> list[dict]:
    esiti = []
    sql_catalogo, atteso_catalogo = CATALOGO[server]
    osservato = esegui(docker, server, sql_catalogo)
    esiti.append({"sonda": "tipo della colonna G nel catalogo", "prova": "catalogo",
                  "atteso": {"valore": atteso_catalogo}, "osservato": osservato,
                  "conforme": osservato == {"valore": atteso_catalogo}})
    for sonda, sql, atteso in GIUDIZIO_DEL_CATALOGO.get(server, ()):
        osservato = esegui(docker, server, sql)
        conforme = (osservato == atteso if "valore" in atteso
                    else verdetto(atteso, osservato))
        esiti.append({"sonda": sonda, "prova": "catalogo", "atteso": atteso,
                      "osservato": osservato, "conforme": conforme})
    for forma in fixture["forme"]:
        resa = forma["sql"][server]
        tipi = bind_tipati(forma["piano"])
        atteso = forma["esito"][server]
        osservato = esegui(docker, server, blocco(server, resa["sql"], resa["binds"], tipi))
        esiti.append({"sonda": forma["forma"], "prova": prova(forma, server), "atteso": atteso,
                      "osservato": osservato, "conforme": verdetto(atteso, osservato)})
        if forma.get("controllo"):
            sql = sql_di_controllo(resa["sql"], forma["controllo"])
            osservato = esegui(docker, server, blocco(server, sql, resa["binds"], tipi))
            esiti.append({"sonda": f"{forma['forma']} su {forma['controllo']}",
                          "prova": "controllo", "atteso": {"ok": True},
                          "osservato": osservato, "conforme": verdetto({"ok": True}, osservato)})
    return esiti


def riferimento(server: str, radice: Path = ROOT) -> str:
    """L'immagine qualificata del prodotto, per digest, dalla sua fonte."""
    if server == "oracle":
        testo = (radice / "docker-compose.oracle.yml").read_text(encoding="utf-8")
        immagini = re.findall(r"^\s+image:\s+(ghcr\.io/gvenzl/oracle-free@sha256:[0-9a-f]{64})\s*$",
                              testo, re.M)
    else:
        testo = (radice / "docker" / "db2-client" / "Dockerfile").read_text(encoding="utf-8")
        immagini = re.findall(r"^FROM\s+(icr\.io/db2_community/db2@sha256:[0-9a-f]{64})\s*$",
                              testo, re.M)
    if len(set(immagini)) != 1:
        raise SystemExit(f"{server}: attesa una sola immagine per digest, trovate {immagini}")
    return immagini[0]


def main() -> int:
    argomenti = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    argomenti.add_argument("--server", choices=SERVER, required=True)
    fonte = argomenti.add_mutually_exclusive_group(required=True)
    fonte.add_argument("--riferimenti", action="store_true",
                       help="avvia l'immagine qualificata del prodotto")
    fonte.add_argument("--avvia", metavar="IMMAGINE", help="avvia un container usa e getta")
    fonte.add_argument("--container", help="un container gia pronto, che lo script non ferma")
    argomenti.add_argument("--docker", default=os.environ.get("PLENORA_DOCKER", "docker"),
                           help="il comando docker (per esempio \"wsl docker\")")
    argomenti.add_argument("--referto", type=Path)
    argomenti.add_argument("--attesa", type=int, default=900, help="secondi per l'avvio")
    argomenti.add_argument("--password", help="la password di plenora o db2inst1 di --container")
    opzioni = argomenti.parse_args()
    if opzioni.password:
        global ORACLE_PASSWORD, DB2_PASSWORD  # noqa: PLW0603 - una sola esecuzione
        ORACLE_PASSWORD = DB2_PASSWORD = opzioni.password

    fixture = carica()
    immagine = None
    if opzioni.container:
        nome = opzioni.container
    else:
        immagine = riferimento(opzioni.server) if opzioni.riferimenti else opzioni.avvia
        nome = f"plenora-sonde-operandi-{opzioni.server}-{os.getpid()}"
    docker = Docker(opzioni.docker, nome)
    if immagine:
        ambiente = [arg for chiave, valore in AMBIENTE[opzioni.server].items()
                    for arg in ("-e", f"{chiave}={valore}")]
        privilegi = ["--privileged"] if opzioni.server == "db2" else []
        avviato = docker.run(["run", "--rm", "-d", *privilegi, "--name", nome, *ambiente,
                              immagine])
        if avviato.returncode != 0:
            print(f"avvio del container fallito: {avviato.stderr.strip()[:300]}",
                  file=sys.stderr)
            return 2
    try:
        attendi(docker, opzioni.server, opzioni.attesa, avviato=immagine is not None)
        prepara(docker, opzioni.server)
        esiti = misura(docker, opzioni.server, fixture)
    finally:
        if immagine:
            docker.run(["stop", nome], timeout=300)
    print(f"{opzioni.server} {immagine or nome}:")
    for riga in esiti:
        print(f"  {'ok ' if riga['conforme'] else 'NO '} [{riga['prova']}] {riga['sonda']}: "
              f"{riga['osservato']}")
    if opzioni.referto:
        opzioni.referto.write_text(json.dumps(
            {"schema_version": 1, "server": opzioni.server, "immagine": immagine,
             "container": None if immagine else nome, "esiti": esiti},
            ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    difformi = [riga for riga in esiti if not riga["conforme"]]
    print(f"sonde operandi {opzioni.server}: {len(esiti)} esiti, {len(difformi)} difformi")
    return 0 if not difformi else 1


if __name__ == "__main__":
    sys.exit(main())
