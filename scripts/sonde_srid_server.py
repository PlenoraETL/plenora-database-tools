#!/usr/bin/env python3
"""Le prove live su cui si regge la regola delle sonde spatial.

`plenora_database_core::spatial_probe::server_guard` dice, per prodotto, dove il
server rifiuta una sonda in un altro sistema di riferimento **prima** che una
riga che dipende dal confronto sia restituita o toccata, e dove no. Il
compilatore portable e il renderer relazionale ammettono una sonda senza
catalogo soltanto dove quella tabella dice «guardia»; altrove la rifiutano.
Una tabella cosi non vale niente se nessuno la misura: questo script la
misura, sul server vero, e rifiuta il gate se un fatto cambia.

Per ciascun prodotto:

- `postgres`: gli operatori di bounding box e le funzioni `ST_*` senza guardia
  non confrontano gli SRID (`&&` con una sonda 3857 rende righe, un
  `ST_Intersects` indicizzato con una sonda lontana zero righe, il cast a
  `geography` fa 4326 di un WKB senza SRID); la guardia sul typmod della
  colonna (`postgres_column_guard`) rifiuta con «mixed SRID» anche sotto
  `NOT`, sulla colonna senza typmod e come chiave di un indice su una tabella
  vuota; la scrittura di un valore in un altro SRID e rifiutata dal typmod;
- `mysql`: 3033 o 3643 in ogni funzione e in `MBRIntersects`, anche
  indicizzati, sotto `NOT`, su una tabella vuota, e in scrittura;
- `mariadb`: nessuna guardia: `ST_Intersects`, `MBRIntersects`, `ST_Within`
  fra una sonda 3857 e una colonna `REF_SYSTEM_ID=4326` rendono righe, e un
  valore 3857 entra nella colonna; `GEOMETRY_COLUMNS.SRID` pubblica 4326, ed e
  da li che la scrittura del provider confronta lo SRID;
- `sqlserver`: nessuna guardia: con SRID diversi `STIntersects` rende `NULL`,
  la riga resta fuori anche sotto `NOT` e con `STEnvelope`, su `geography`
  4326 contro 4269, e un `UPDATE` non tocca niente con successo.

    python scripts/sonde_srid_server.py --server mariadb --riferimenti
    python scripts/sonde_srid_server.py --server sqlserver --avvia IMMAGINE \\
        --docker "wsl docker" --referto sonde-sqlserver.json

`--riferimenti` avvia, una dopo l'altra, le immagini qualificate del prodotto
(per digest, dalla loro fonte: `docker/<prodotto>/references.json`, i compose);
`--avvia IMMAGINE` una sola; entrambe usano container usa e getta (`docker run
--rm`) e li fermano alla fine. `--container NOME` usa un container gia pronto e
non lo tocca. Lo esegue il workflow `spatial-srid-probes`. L'uscita e 0
soltanto se ogni sonda ha l'esito atteso.
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
from dataclasses import dataclass, field
from pathlib import Path

PASSWORD = "Sonde_Srid_2026!"


@dataclass(frozen=True)
class Sonda:
    """Una misura: lo statement, l'esito atteso e che cosa prova.

    `atteso` e `("righe", n)` (il valore dell'ultima riga dell'uscita),
    `("errore", codici)` (uno dei codici del server) o `("ok", None)`.
    `prova` e la voce della tabella delle guardie che la misura sostiene:
    `guardia` (il server rifiuta), `senza-guardia` (il server non confronta),
    `controllo` (lo SRID giusto passa), `catalogo` (cio che il provider
    legge per confrontare da se)."""

    nome: str
    sql: str
    atteso: tuple
    prova: str


@dataclass(frozen=True)
class Server:
    preparazione: tuple[str, ...]
    sonde: tuple[Sonda, ...]
    ambiente: dict = field(default_factory=dict)


def guardia_pg(srid: int, tabella: str, colonna: str, valore: str) -> str:
    """La forma di `postgres_column_guard`, con i nomi come letterali."""
    vuota = "ST_SetSRID('POINT EMPTY'::geometry, {})"
    typmod = (f"COALESCE((SELECT postgis_typmod_srid(a.atttypmod) FROM pg_catalog.pg_attribute a "
              f"WHERE a.attrelid = to_regclass(quote_ident(E'{tabella}')) AND a.attname = E'{colonna}' "
              f"AND a.attnum > 0 AND NOT a.attisdropped), 0)")
    return (f"(CASE WHEN ST_Equals({vuota.format(srid)}, {vuota.format(typmod)}) "
            f"THEN {valore} END)")


def punto_pg(x: float, y: float, srid: int) -> str:
    return f"ST_SetSRID(ST_MakePoint({x}, {y}), {srid})"


MISTO = ("errore", {"XX000"})
SERVER: dict[str, Server] = {
    "postgres": Server(
        preparazione=(
            "CREATE EXTENSION IF NOT EXISTS postgis",
            "DROP TABLE IF EXISTS sonde_t, sonde_e, sonde_n",
            "CREATE TABLE sonde_t (id int, g geometry(Point, 4326), h geography(Point, 4326))",
            "INSERT INTO sonde_t SELECT i, ST_SetSRID(ST_MakePoint(i % 40, i / 40), 4326), "
            "ST_SetSRID(ST_MakePoint(i % 40, i / 40), 4326)::geography FROM generate_series(0, 1999) i",
            "CREATE INDEX ON sonde_t USING gist (g)",
            "ANALYZE sonde_t",
            "CREATE TABLE sonde_e (id int, g geometry(Point, 4326))",
            "CREATE INDEX ON sonde_e USING gist (g)",
            "CREATE TABLE sonde_n (id int, g geometry)",
            "INSERT INTO sonde_n VALUES (1, ST_SetSRID(ST_MakePoint(1, 1), 4326))",
        ),
        sonde=(
            Sonda("&& senza guardia, sonda 3857",
                  f"SELECT count(*) FROM sonde_t WHERE g && {punto_pg(1, 1, 3857)}",
                  ("righe", 1), "senza-guardia"),
            Sonda("ST_Intersects indicizzato senza guardia, sonda 3857 lontana",
                  "SET enable_seqscan = off; SELECT count(*) FROM sonde_t "
                  f"WHERE ST_Intersects(g, {punto_pg(1000, 1000, 3857)})",
                  ("righe", 0), "senza-guardia"),
            Sonda("geography senza guardia, WKB senza SRID",
                  "SELECT count(*) FROM sonde_t WHERE ST_Intersects(h, "
                  "ST_GeomFromWKB(ST_AsBinary(ST_MakePoint(1, 1)))::geography)",
                  ("righe", 1), "senza-guardia"),
            Sonda("&& con la guardia, sonda 3857",
                  "SELECT count(*) FROM sonde_t WHERE g && "
                  + guardia_pg(3857, "sonde_t", "g", punto_pg(1, 1, 3857)),
                  MISTO, "guardia"),
            Sonda("NOT ST_Intersects con la guardia, sonda 3857 lontana",
                  "SET enable_seqscan = off; SELECT count(*) FROM sonde_t WHERE NOT ST_Intersects(g, "
                  + guardia_pg(3857, "sonde_t", "g", punto_pg(1000, 1000, 3857)) + ")",
                  MISTO, "guardia"),
            Sonda("geography con la guardia, sonda 4269",
                  "SELECT count(*) FROM sonde_t WHERE ST_Intersects(h, "
                  + guardia_pg(4269, "sonde_t", "h", punto_pg(1, 1, 4269) + "::geography") + ")",
                  MISTO, "guardia"),
            Sonda("colonna senza typmod con la guardia",
                  "SELECT count(*) FROM sonde_n WHERE g && "
                  + guardia_pg(4326, "sonde_n", "g", punto_pg(1, 1, 4326)),
                  MISTO, "guardia"),
            Sonda("tabella vuota indicizzata con la guardia",
                  "SET enable_seqscan = off; SELECT count(*) FROM sonde_e WHERE ST_Intersects(g, "
                  + guardia_pg(3857, "sonde_e", "g", punto_pg(1, 1, 3857)) + ")",
                  MISTO, "guardia"),
            Sonda("tabella vuota in seqscan con la guardia: nessuna riga, nessuna dipende dalla sonda",
                  "SET enable_indexscan = off; SET enable_bitmapscan = off; SELECT count(*) FROM sonde_e "
                  "WHERE ST_Intersects(g, "
                  + guardia_pg(3857, "sonde_e", "g", punto_pg(1, 1, 3857)) + ")",
                  ("righe", 0), "guardia"),
            Sonda("scrittura di un valore 3857 nella colonna 4326",
                  f"INSERT INTO sonde_t VALUES (9999, {punto_pg(1, 1, 3857)}, NULL)",
                  ("errore", {"22023"}), "guardia"),
            Sonda("controllo: la guardia con lo SRID della colonna",
                  "SELECT count(*) FROM sonde_t WHERE g && "
                  + guardia_pg(4326, "sonde_t", "g", punto_pg(1, 1, 4326)),
                  ("righe", 1), "controllo"),
        ),
    ),
    "mysql": Server(
        preparazione=(
            "DROP TABLE IF EXISTS t, e, u",
            "CREATE TABLE t (id INT, g POINT NOT NULL SRID 4326, SPATIAL INDEX(g))",
            "INSERT INTO t SELECT 1, ST_GeomFromText('POINT(1 1)', 4326, 'axis-order=long-lat')",
            "CREATE TABLE e (id INT, g POINT NOT NULL SRID 4326, SPATIAL INDEX(g))",
            "CREATE TABLE u (id INT, g GEOMETRY NOT NULL)",
            "INSERT INTO u SELECT 1, ST_GeomFromText('POINT(1 1)', 4326, 'axis-order=long-lat')",
        ),
        sonde=tuple(
            Sonda(nome, sql, ("errore", {"3033", "3643"}), "guardia")
            for nome, sql in (
                ("ST_Intersects, sonda 3857",
                 "SELECT count(*) FROM t WHERE ST_Intersects(g, ST_GeomFromText('POINT(1 1)', 3857))"),
                ("ST_Intersects indicizzato, sonda 3857 lontana",
                 "SELECT count(*) FROM t FORCE INDEX (g) "
                 "WHERE ST_Intersects(g, ST_GeomFromText('POINT(5000000 5000000)', 3857))"),
                ("MBRIntersects, sonda 3857",
                 "SELECT count(*) FROM t WHERE MBRIntersects(g, ST_GeomFromText('POINT(1 1)', 3857))"),
                ("MBRIntersects indicizzato, sonda 3857 lontana",
                 "SELECT count(*) FROM t FORCE INDEX (g) "
                 "WHERE MBRIntersects(g, ST_GeomFromText('POINT(5000000 5000000)', 3857))"),
                ("ST_Within, sonda 3857",
                 "SELECT count(*) FROM t WHERE ST_Within(g, ST_Buffer(ST_GeomFromText('POINT(1 1)', 3857), 1))"),
                ("ST_Contains, sonda 3857",
                 "SELECT count(*) FROM t WHERE ST_Contains(ST_Buffer(ST_GeomFromText('POINT(1 1)', 3857), 1), g)"),
                ("NOT ST_Intersects, sonda 3857 lontana",
                 "SELECT count(*) FROM t WHERE NOT ST_Intersects(g, ST_GeomFromText('POINT(50 50)', 3857))"),
                ("ST_Distance, sonda 3857",
                 "SELECT ST_Distance(g, ST_GeomFromText('POINT(1 2)', 3857)) FROM t"),
                ("tabella vuota, sonda 3857",
                 "SELECT count(*) FROM e WHERE ST_Intersects(g, ST_GeomFromText('POINT(1 1)', 3857))"),
                ("colonna senza SRID, sonda 3857",
                 "SELECT count(*) FROM u WHERE ST_Intersects(g, ST_GeomFromText('POINT(1 1)', 3857))"),
                ("scrittura di un valore 3857 nella colonna 4326",
                 "INSERT INTO t VALUES (2, ST_GeomFromText('POINT(1 1)', 3857))"),
            )
        ) + (
            Sonda("controllo: sonda 4326",
                  "SELECT count(*) FROM t WHERE ST_Intersects(g, "
                  "ST_GeomFromText('POINT(1 1)', 4326, 'axis-order=long-lat'))",
                  ("righe", 1), "controllo"),
        ),
    ),
    "mariadb": Server(
        preparazione=(
            "DROP TABLE IF EXISTS t",
            "CREATE TABLE t (id INT, g GEOMETRY REF_SYSTEM_ID=4326 NOT NULL, SPATIAL INDEX(g))",
            "INSERT INTO t VALUES (1, ST_GeomFromText('POINT(1 1)', 4326))",
        ),
        sonde=(
            Sonda("REF_SYSTEM_ID pubblicato dal registro OGC",
                  "SELECT SRID FROM information_schema.GEOMETRY_COLUMNS "
                  "WHERE F_TABLE_SCHEMA = 'sonde' AND F_TABLE_NAME = 't'",
                  ("righe", 4326), "catalogo"),
            # La colonna come la legge `verify_declared_column_srids`: su 11.4
            # `F_GEOMETRY_COLUMN` e vuoto e il nome sta in `G_GEOMETRY_COLUMN`.
            Sonda("il registro OGC nomina la colonna",
                  "SELECT COALESCE(NULLIF(F_GEOMETRY_COLUMN, ''), G_GEOMETRY_COLUMN) "
                  "FROM information_schema.GEOMETRY_COLUMNS "
                  "WHERE F_TABLE_SCHEMA = 'sonde' AND F_TABLE_NAME = 't'",
                  ("righe", "g"), "catalogo"),
            Sonda("ST_Intersects, sonda 3857",
                  "SELECT count(*) FROM t WHERE ST_Intersects(g, ST_GeomFromText('POINT(1 1)', 3857))",
                  ("righe", 1), "senza-guardia"),
            Sonda("MBRIntersects, sonda 3857",
                  "SELECT count(*) FROM t WHERE MBRIntersects(g, ST_GeomFromText('POINT(1 1)', 3857))",
                  ("righe", 1), "senza-guardia"),
            Sonda("ST_Within, sonda 3857",
                  "SELECT count(*) FROM t WHERE ST_Within(g, ST_Buffer(ST_GeomFromText('POINT(1 1)', 3857), 1))",
                  ("righe", 1), "senza-guardia"),
            Sonda("NOT ST_Intersects, sonda 3857 lontana",
                  "SELECT count(*) FROM t WHERE NOT ST_Intersects(g, ST_GeomFromText('POINT(50 50)', 3857))",
                  ("righe", 1), "senza-guardia"),
            Sonda("scrittura di un valore 3857 nella colonna REF_SYSTEM_ID=4326",
                  "INSERT INTO t VALUES (2, ST_GeomFromText('POINT(1 1)', 3857)); "
                  "SELECT count(*) FROM t WHERE id = 2 AND ST_SRID(g) = 3857",
                  ("righe", 1), "senza-guardia"),
        ),
    ),
    "sqlserver": Server(
        preparazione=(
            "IF OBJECT_ID('dbo.t') IS NOT NULL DROP TABLE dbo.t",
            "IF OBJECT_ID('dbo.gg') IS NOT NULL DROP TABLE dbo.gg",
            "CREATE TABLE dbo.t (id INT PRIMARY KEY, g geometry NOT NULL)",
            "INSERT INTO dbo.t VALUES (1, geometry::STGeomFromText('POINT(1 1)', 4326))",
            "CREATE TABLE dbo.gg (id INT PRIMARY KEY, g geography NOT NULL)",
            "INSERT INTO dbo.gg VALUES (1, geography::STGeomFromText('POINT(1 1)', 4326))",
        ),
        sonde=(
            Sonda("STIntersects, sonda 3857",
                  "SELECT count(*) FROM dbo.t "
                  "WHERE g.STIntersects(geometry::STGeomFromText('POINT(1 1)', 3857)) = 1",
                  ("righe", 0), "senza-guardia"),
            Sonda("STIntersects rende NULL con SRID diversi",
                  "SELECT count(*) FROM dbo.t "
                  "WHERE g.STIntersects(geometry::STGeomFromText('POINT(1 1)', 3857)) IS NULL",
                  ("righe", 1), "senza-guardia"),
            Sonda("NOT STIntersects, sonda 3857 lontana",
                  "SELECT count(*) FROM dbo.t "
                  "WHERE NOT (g.STIntersects(geometry::STGeomFromText('POINT(50 50)', 3857)) = 1)",
                  ("righe", 0), "senza-guardia"),
            Sonda("STEnvelope().STIntersects, sonda 3857",
                  "SELECT count(*) FROM dbo.t WHERE g.STEnvelope()"
                  ".STIntersects(geometry::STGeomFromText('POINT(1 1)', 3857).STEnvelope()) = 1",
                  ("righe", 0), "senza-guardia"),
            Sonda("geography 4326 contro 4269",
                  "SELECT count(*) FROM dbo.gg "
                  "WHERE g.STIntersects(geography::STGeomFromText('POINT(1 1)', 4269)) = 1",
                  ("righe", 0), "senza-guardia"),
            Sonda("UPDATE con sonda 3857: nessuna riga, con successo",
                  "UPDATE dbo.t SET id = id "
                  "WHERE g.STIntersects(geometry::STGeomFromText('POINT(1 1)', 3857)) = 1; "
                  "SELECT @@ROWCOUNT",
                  ("righe", 0), "senza-guardia"),
            Sonda("controllo: sonda 4326",
                  "SELECT count(*) FROM dbo.t "
                  "WHERE g.STIntersects(geometry::STGeomFromText('POINT(1 1)', 4326)) = 1",
                  ("righe", 1), "controllo"),
        ),
    ),
}

AMBIENTE = {
    "postgres": {"POSTGRES_PASSWORD": PASSWORD, "PGPASSWORD": PASSWORD},
    "mysql": {"MYSQL_ROOT_PASSWORD": PASSWORD},
    "mariadb": {"MARIADB_ROOT_PASSWORD": PASSWORD},
    "sqlserver": {"ACCEPT_EULA": "Y", "MSSQL_SA_PASSWORD": PASSWORD},
}


def client(server: str, sql: str, database: bool = True) -> list[str]:
    """Il client del prodotto dentro il container, per un solo statement."""
    if server == "postgres":
        # Per TCP: l'immagine avvia un server temporaneo soltanto sul socket
        # per gli script d'inizializzazione, e lo ferma prima di quello vero.
        return ["psql", "-h", "127.0.0.1", "-U", "postgres", "-d", "postgres", "-At",
                "-v", "ON_ERROR_STOP=1",
                "-v", "VERBOSITY=verbose", "-c", sql]
    if server in {"mysql", "mariadb"}:
        programma = "mysql" if server == "mysql" else "mariadb"
        return [programma, "-uroot", f"-p{PASSWORD}", "-N", "-B"] + (
            ["sonde"] if database else []) + ["-e", sql]
    return ["/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost", "-U", "sa", "-P", PASSWORD, "-C",
            "-h", "-1", "-W", "-b"] + (["-d", "sonde"] if database else []) + [
            "-Q", "SET NOCOUNT ON; " + sql]


CODICE = {
    "postgres": re.compile(r"ERROR:\s+([0-9A-Z]{5}):"),
    "mysql": re.compile(r"ERROR (\d+)"),
    "mariadb": re.compile(r"ERROR (\d+)"),
    "sqlserver": re.compile(r"Msg (\d+),"),
}


def esito(server: str, completato: subprocess.CompletedProcess) -> dict:
    """L'esito osservato: il codice d'errore del server, o l'ultima riga."""
    uscita = (completato.stdout or "") + (completato.stderr or "")
    trovato = CODICE[server].search(uscita)
    if completato.returncode != 0 or trovato:
        return {"errore": trovato.group(1) if trovato else None}
    righe = [riga.strip() for riga in (completato.stdout or "").splitlines() if riga.strip()]
    return {"valore": righe[-1] if righe else None}


def verdetto(atteso: tuple, osservato: dict) -> bool:
    tipo, valore = atteso
    if tipo == "errore":
        return osservato.get("errore") in valore
    if tipo == "righe":
        return "errore" not in osservato and osservato.get("valore") == str(valore)
    return "errore" not in osservato


class Docker:
    def __init__(self, prefisso: str, container: str) -> None:
        self.prefisso, self.container = shlex.split(prefisso), container

    def exec(self, argv: list[str], timeout: int = 120) -> subprocess.CompletedProcess:
        return subprocess.run(self.prefisso + ["exec", "-i", self.container] + argv,
                              capture_output=True, text=True, timeout=timeout)

    def run(self, argv: list[str], timeout: int = 600) -> subprocess.CompletedProcess:
        return subprocess.run(self.prefisso + argv, capture_output=True, text=True, timeout=timeout)


def attendi(docker: Docker, server: str, secondi: int) -> None:
    prova = "SELECT 1"
    limite = time.monotonic() + secondi
    while time.monotonic() < limite:
        completato = docker.exec(client(server, prova, database=False))
        if completato.returncode == 0 and "1" in completato.stdout:
            return
        time.sleep(2)
    raise SystemExit(f"il server {server} non risponde: il gate non si puo' eseguire")


def prepara(docker: Docker, server: str) -> None:
    if server in {"mysql", "mariadb", "sqlserver"}:
        crea = ("CREATE DATABASE IF NOT EXISTS sonde" if server != "sqlserver"
                else "IF DB_ID('sonde') IS NULL CREATE DATABASE sonde")
        completato = docker.exec(client(server, crea, database=False))
        if completato.returncode != 0:
            raise SystemExit(f"preparazione {server} fallita: {completato.stderr.strip()[:200]}")
    for sql in SERVER[server].preparazione:
        completato = docker.exec(client(server, sql))
        if completato.returncode != 0:
            raise SystemExit(f"preparazione {server} fallita: {completato.stderr.strip()[:200]}")


ROOT = Path(__file__).resolve().parents[1]


def riferimenti(server: str, radice: Path = ROOT) -> list[str]:
    """Le immagini qualificate del prodotto, per digest, dalla loro fonte:
    `docker/<prodotto>/references.json` per `MySQL` e `MariaDB` (ogni riga),
    il compose del fixture per `PostgreSQL` e SQL Server. Lo script non
    ripete un digest: lo legge dove vive."""
    if server in {"mysql", "mariadb"}:
        documento = json.loads((radice / "docker" / server / "references.json")
                               .read_text(encoding="utf-8"))
        return [f"{server}@{riga['digest']}" for riga in documento["references"]]
    compose = {"postgres": "docker-compose.postgres.yml",
               "sqlserver": "docker-compose.sqlserver.yml"}[server]
    immagini = re.findall(r"^\s+image:\s+(\S+@sha256:[0-9a-f]{64})\s*$",
                          (radice / compose).read_text(encoding="utf-8"), re.M)
    if not immagini or len(set(immagini)) != 1:
        raise SystemExit(f"{compose}: attesa una sola immagine per digest, trovate {immagini}")
    return [immagini[0]]


def misura(docker: Docker, server: str, attesa: int) -> list[dict]:
    attendi(docker, server, attesa)
    prepara(docker, server)
    esiti = []
    for sonda in SERVER[server].sonde:
        osservato = esito(server, docker.exec(client(server, sonda.sql)))
        esiti.append({"sonda": sonda.nome, "prova": sonda.prova,
                      "atteso": [sonda.atteso[0], sorted(sonda.atteso[1])
                                 if isinstance(sonda.atteso[1], set) else sonda.atteso[1]],
                      "osservato": osservato, "conforme": verdetto(sonda.atteso, osservato)})
    return esiti


def main() -> int:
    argomenti = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    argomenti.add_argument("--server", choices=sorted(SERVER), required=True)
    fonte = argomenti.add_mutually_exclusive_group(required=True)
    fonte.add_argument("--riferimenti", action="store_true",
                       help="avvia, una dopo l'altra, ogni immagine qualificata del prodotto")
    fonte.add_argument("--avvia", metavar="IMMAGINE", help="avvia un container usa e getta")
    fonte.add_argument("--container", help="un container gia pronto, che lo script non ferma")
    argomenti.add_argument("--docker", default=os.environ.get("PLENORA_DOCKER", "docker"),
                           help="il comando docker (per esempio \"wsl docker\")")
    argomenti.add_argument("--referto", type=Path)
    argomenti.add_argument("--attesa", type=int, default=240, help="secondi per l'avvio")
    argomenti.add_argument("--password", help="la password di amministrazione di --container")
    opzioni = argomenti.parse_args()
    if opzioni.password:
        global PASSWORD  # noqa: PLW0603 - una sola esecuzione per processo
        PASSWORD = opzioni.password

    if opzioni.container:
        bersagli = [(opzioni.container, None)]
    else:
        immagini = riferimenti(opzioni.server) if opzioni.riferimenti else [opzioni.avvia]
        bersagli = [(f"plenora-sonde-srid-{opzioni.server}-{os.getpid()}-{indice}", immagine)
                    for indice, immagine in enumerate(immagini)]
    referto = []
    for nome, immagine in bersagli:
        docker = Docker(opzioni.docker, nome)
        if immagine:
            ambiente = [arg for chiave, valore in AMBIENTE[opzioni.server].items()
                        for arg in ("-e", f"{chiave}={valore}")]
            avviato = docker.run(["run", "--rm", "-d", "--name", nome, *ambiente, immagine])
            if avviato.returncode != 0:
                print(f"avvio del container fallito: {avviato.stderr.strip()[:300]}",
                      file=sys.stderr)
                return 2
        try:
            esiti = misura(docker, opzioni.server, opzioni.attesa)
        finally:
            if immagine:
                docker.run(["stop", nome], timeout=120)
        print(f"{opzioni.server} {immagine or nome}:")
        for riga in esiti:
            print(f"  {'ok ' if riga['conforme'] else 'NO '} [{riga['prova']}] {riga['sonda']}: "
                  f"{riga['osservato']}")
        referto.append({"immagine": immagine, "container": None if immagine else nome,
                        "esiti": esiti})
    if opzioni.referto:
        opzioni.referto.write_text(json.dumps({"schema_version": 1, "server": opzioni.server,
                                               "misure": referto}, ensure_ascii=False, indent=2)
                                   + "\n", encoding="utf-8")
    difformi = [riga for misura_ in referto for riga in misura_["esiti"] if not riga["conforme"]]
    totale = sum(len(misura_["esiti"]) for misura_ in referto)
    print(f"sonde SRID {opzioni.server}: {len(referto)} server, {totale} esiti, "
          f"{len(difformi)} difformi")
    return 0 if not difformi else 1


if __name__ == "__main__":
    sys.exit(main())
