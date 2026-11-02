#!/usr/bin/env python3
"""Le misure live su cui si regge la regola A delle scritture.

`plenora_database_core::write_admission` ammette una scrittura soltanto per
una coppia fra il tipo Arrow dell'ingresso e il tipo nativo del target (con
tutti i suoi parametri) che il fixture del provider misura sulla major del
server: `tests/fixtures/scrittura_server_<prodotto>.json`. Questo script
produce e verifica quelle misure.

Per ogni immagine qualificata del prodotto lo script avvia un container usa e
getta, lo prepara e lancia la sonda del provider (`live_sonde_scritture_server`
nel crate del provider, `plenora_database_testkit::write_probe`): per ogni
chiave d'ammissione la sonda scrive una batteria fissa di valori limite con il
**percorso di scrittura della libreria** sotto `Strict`, li rilegge con la
lettura della libreria e li confronta valore per valore. Il referto della
sonda e una misura per major.

- senza `--aggiorna`: la sonda stessa confronta la misura con il fixture e
  fallisce se una chiave cambia (l'oracolo di regressione); l'uscita e 0
  soltanto se ogni server misurato coincide;
- con `--aggiorna`: il referto sostituisce, nel fixture, la misura della stessa
  major (con l'immagine misurata), e il fixture si riscrive in forma stabile.

    python scripts/sonde_scritture_server.py --server oracle --riferimenti
    python scripts/sonde_scritture_server.py --server oracle \\
        --container scr8-oracle --porta 15218 --docker "wsl docker" --aggiorna

`--riferimenti` avvia, una dopo l'altra, le immagini qualificate del prodotto
(per digest, dalla loro fonte); `--avvia IMMAGINE` una sola; `--container NOME`
usa un container gia pronto (con `--porta`, la porta pubblicata su
127.0.0.1) e non lo tocca. Lo esegue il workflow `write-admission-probes`.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
TEST = "live_sonde_scritture_server"


@dataclass(frozen=True)
class Prodotto:
    """Come si avvia, si prepara e si misura un prodotto.

    `immagini` legge le immagini qualificate dalla loro fonte; `ambiente` e
    `porta` servono a `docker run`; `pronto` e il comando (dentro il
    container) che risponde 0 quando il server accetta la sonda; `prepara` gli
    eventuali comandi di preparazione (dentro il container); `test_env` le
    variabili della sonda, con `{porta}` sostituita; `nel_container` la
    directory del workspace se la sonda gira dentro il container (Db2);
    `opzioni_run` le opzioni in piu di `docker run` (`--privileged` per Db2)."""

    crate: str
    modulo: str
    immagini: Callable[[], list[str]]
    porta: int
    ambiente: dict = field(default_factory=dict)
    argomenti: tuple[str, ...] = ()
    pronto: tuple[str, ...] = ()
    prepara: tuple[tuple[str, ...], ...] = ()
    test_env: dict = field(default_factory=dict)
    nel_container: str | None = None
    attesa: int = 600
    opzioni_run: tuple[str, ...] = ()
    # Il percorso, nel container, della CA che la sonda deve verificare
    # (`PLENORA_MYSQL_CA`): lo script la copia sull'host prima della sonda.
    ca_nel_container: str | None = None


def riferimento_oracle() -> list[str]:
    documento = json.loads((ROOT / "docker" / "oracle" / "references.json")
                           .read_text(encoding="utf-8"))
    riga = documento["reference"]
    return [f"{riga['image']}@{riga['digest']}"]


DB2_DOCKERFILE = ROOT / "docker" / "db2-client" / "Dockerfile"
DB2_IMMAGINE = "plenora-db2-test:12.1.5"


def riferimento_db2() -> list[str]:
    """L'immagine Db2 si costruisce dal Dockerfile, come in
    `scripts/check_db2_reference.py`: le due basi (la toolchain Rust e Db2
    Community 12.1) sono fissate per digest nel Dockerfile, e il sorgente del
    workspace e copiato in `/workspace`, dove la sonda gira."""
    docker = shlex.split(os.environ.get("PLENORA_DOCKER", "docker"))
    costruito = subprocess.run([*docker, "build", "-f", str(DB2_DOCKERFILE), "-t",
                                DB2_IMMAGINE, str(ROOT)], capture_output=True, text=True,
                               timeout=7200, check=False)
    if costruito.returncode != 0:
        raise SystemExit(f"costruzione dell'immagine Db2 fallita: {costruito.stderr[-300:]}")
    return [DB2_IMMAGINE]


def riferimenti_postgres() -> list[str]:
    """Le major della matrice (`scripts/check_postgres_matrix.py`, per digest)
    e, per ultima, l'immagine di riferimento del fixture Compose
    (`docker-compose.postgres.yml`), che con `--aggiorna` resta quella
    registrata per la sua major: le due immagini della stessa major devono
    dare la stessa misura."""
    matrice = (ROOT / "scripts" / "check_postgres_matrix.py").read_text(encoding="utf-8")
    immagini = [f"{nome.split(':')[0]}@{digest}" for nome, digest in re.findall(
        r'"(postgis/postgis:[0-9]+-[0-9.]+)",\s*"(sha256:[0-9a-f]{64})"', matrice)]
    compose = (ROOT / "docker-compose.postgres.yml").read_text(encoding="utf-8")
    riferimento = re.search(r"image:\s*(postgis/postgis@sha256:[0-9a-f]{64})", compose)
    if len(immagini) != 5 or riferimento is None:
        raise SystemExit("immagini PostgreSQL qualificate non lette: la sonda non si esegue")
    return [*immagini, riferimento.group(1)]


def riferimenti_mysql_mariadb(prodotto: str) -> list[str]:
    """Le immagini qualificate di `MySQL` o `MariaDB`, per digest, da
    `docker/<prodotto>/references.json` (la fonte della matrice)."""
    documento = json.loads((ROOT / "docker" / prodotto / "references.json")
                           .read_text(encoding="utf-8"))
    return [f"docker.io/library/{prodotto}@{riga['digest']}"
            for riga in documento["references"]]


# La suite live di `MySQL`/`MariaDB` verifica sempre il certificato del server
# contro una CA privata (`PLENORA_MYSQL_CA`, nessun `TrustServerCertificate`).
# Il container usa e getta della sonda genera all'avvio una CA e un certificato
# per 127.0.0.1, li passa al server e la CA si copia sull'host
# (`ca_nel_container`): niente volumi e niente segreti preesistenti, anche in
# CI.
def tls_usa_e_getta(server: str) -> tuple[str, ...]:
    return (
        "bash", "-c",
        "set -e; d=/tmp/plenora-tls; mkdir -p $d; cd $d; "
        "openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=plenora-sonda-ca "
        "-keyout ca.key -out ca.pem 2>/dev/null; "
        "openssl req -newkey rsa:2048 -nodes -subj /CN=127.0.0.1 "
        "-keyout server.key -out server.csr 2>/dev/null; "
        "printf 'basicConstraints=CA:FALSE\\nextendedKeyUsage=serverAuth\\n"
        "subjectAltName=IP:127.0.0.1\\n' > ext; "
        "openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial "
        "-days 2 -extfile ext -out server.pem 2>/dev/null; "
        "chown -R mysql:mysql $d; "
        f"exec docker-entrypoint.sh {server} --ssl-ca=$d/ca.pem --ssl-cert=$d/server.pem "
        "--ssl-key=$d/server.key --local-infile=OFF",
    )


MYSQL_AMBIENTE_SONDA = {
    "PLENORA_MYSQL_HOST": "127.0.0.1",
    "PLENORA_MYSQL_PORT": "{porta}",
    "PLENORA_MYSQL_DATABASE": "dataflow_test",
    "PLENORA_MYSQL_USER": "dataflow",
    "PLENORA_MYSQL_PASSWORD": "DataFlow_Test_2026!",
}

PRODOTTI_MYSQL_MARIADB: dict[str, Prodotto] = {
    "mysql": Prodotto(
        crate="plenora-db-mysql",
        modulo="live_tests",
        immagini=lambda: riferimenti_mysql_mariadb("mysql"),
        porta=3306,
        ambiente={
            "MYSQL_RANDOM_ROOT_PASSWORD": "yes",
            "MYSQL_DATABASE": "dataflow_test",
            "MYSQL_USER": "dataflow",
            "MYSQL_PASSWORD": "DataFlow_Test_2026!",
        },
        argomenti=tls_usa_e_getta("mysqld"),
        ca_nel_container="/tmp/plenora-tls/ca.pem",
        # Il server temporaneo dell'inizializzazione non ascolta su TCP: la
        # sonda parte quando risponde quello definitivo.
        pronto=("mysql", "-h127.0.0.1", "--protocol=TCP", "-udataflow",
                "-pDataFlow_Test_2026!", "-e", "SELECT 1", "dataflow_test"),
        test_env={**MYSQL_AMBIENTE_SONDA, "PLENORA_MYSQL_PRODUCT": "mysql"},
    ),
    "mariadb": Prodotto(
        crate="plenora-db-mysql",
        modulo="live_tests",
        immagini=lambda: riferimenti_mysql_mariadb("mariadb"),
        porta=3306,
        ambiente={
            "MARIADB_RANDOM_ROOT_PASSWORD": "yes",
            "MARIADB_DATABASE": "dataflow_test",
            "MARIADB_USER": "dataflow",
            "MARIADB_PASSWORD": "DataFlow_Test_2026!",
        },
        argomenti=tls_usa_e_getta("mariadbd"),
        ca_nel_container="/tmp/plenora-tls/ca.pem",
        pronto=("mariadb", "-h127.0.0.1", "--protocol=TCP", "-udataflow",
                "-pDataFlow_Test_2026!", "-e", "SELECT 1", "dataflow_test"),
        test_env={**MYSQL_AMBIENTE_SONDA, "PLENORA_MYSQL_PRODUCT": "mariadb"},
    ),
}


def riferimenti_sqlserver() -> list[str]:
    """Le major della matrice (`scripts/check_sqlserver_matrix.py`: 2019 e
    2025, per digest) e il riferimento 2022 del fixture Compose
    (`docker-compose.sqlserver.yml`)."""
    matrice = (ROOT / "scripts" / "check_sqlserver_matrix.py").read_text(encoding="utf-8")
    digest = re.findall(r'"([0-9a-f]{64})"', matrice)
    compose = (ROOT / "docker-compose.sqlserver.yml").read_text(encoding="utf-8")
    riferimento = re.search(r"image:\s*(mcr\.microsoft\.com/mssql/server@sha256:[0-9a-f]{64})",
                            compose)
    if len(digest) != 2 or riferimento is None:
        raise SystemExit("immagini SQL Server qualificate non lette: la sonda non si esegue")
    return [f"mcr.microsoft.com/mssql/server@sha256:{valore}" for valore in digest] + [
        riferimento.group(1)]


# `sqlcmd` sta in `/opt/mssql-tools18` dalle immagini recenti, in
# `/opt/mssql-tools` nelle altre.
SQLCMD = ("S=/opt/mssql-tools18/bin/sqlcmd; [ -x \"$S\" ] || S=/opt/mssql-tools/bin/sqlcmd; "
          "\"$S\" -S localhost -U sa -P 'DataFlow_Test_2026!' -C -b ")


PRODOTTI: dict[str, Prodotto] = {
    **PRODOTTI_MYSQL_MARIADB,
    "oracle": Prodotto(
        crate="plenora-db-oracle",
        modulo="write_admission_live_tests",
        immagini=riferimento_oracle,
        porta=1521,
        ambiente={
            "ORACLE_PASSWORD": "Oracle_Test_2026",
            "APP_USER": "plenora",
            "APP_USER_PASSWORD": "Plenora_Test_2026",
        },
        pronto=("healthcheck.sh",),
        test_env={
            "PLENORA_ORACLE_HOST": "127.0.0.1",
            "PLENORA_ORACLE_PORT": "{porta}",
            "PLENORA_ORACLE_SERVICE": "FREEPDB1",
            "PLENORA_ORACLE_USER": "plenora",
            "PLENORA_ORACLE_PASSWORD": "Plenora_Test_2026",
        },
    ),
    # La sonda gira dentro il container (il driver ODBC Db2 non e sull'host):
    # `porta` e la porta del server nel container, non quella pubblicata.
    "db2": Prodotto(
        crate="plenora-db-db2",
        modulo="write_admission_live_tests",
        immagini=riferimento_db2,
        porta=50000,
        ambiente={
            "LICENSE": "accept",
            "DB2INSTANCE": "db2inst1",
            "DB2INST1_PASSWORD": "plenora_test",
            "DBNAME": "plenora",
            "BLU": "false",
            "ENABLE_ORACLE_COMPATIBILITY": "false",
            "UPDATEAVAIL": "NO",
            "TO_CREATE_SAMPLEDB": "false",
            "REPODB": "false",
            "PERSISTENT_HOME": "false",
            "HADR_ENABLED": "false",
            "IS_OSXFS": "false",
        },
        opzioni_run=("--privileged",),
        pronto=("/usr/local/bin/plenora-db2-healthcheck",),
        # Senza un'attivazione esplicita Db2 disattiva il database all'ultima
        # connessione e lo riattiva alla successiva: la sonda apre migliaia di
        # connessioni, e una riattivazione lenta finiva in SQL30081.
        # SQL1490W (gia attivo) esce con 2: un avviso, non un errore.
        prepara=(("su", "-", "db2inst1", "-c",
                  ". ~/sqllib/db2profile && { db2 activate database plenora; "
                  "test $? -le 2; }"),),
        test_env={
            "DB2CODEPAGE": "1208",
            "PLENORA_DB2_HOST": "127.0.0.1",
            "PLENORA_DB2_PORT": "50000",
            "PLENORA_DB2_DATABASE": "plenora",
            "PLENORA_DB2_USER": "db2inst1",
            "PLENORA_DB2_PASSWORD": "plenora_test",
        },
        nel_container="/workspace",
        attesa=1200,
    ),
    # Il database, il login e l'utente della suite live
    # (`docker/sqlserver/init`), con lo schema `dbo` della sonda.
    "sqlserver": Prodotto(
        crate="plenora-db-sqlserver",
        modulo="live_tests::write_semantics::r8",
        immagini=riferimenti_sqlserver,
        porta=1433,
        ambiente={
            "ACCEPT_EULA": "Y",
            "MSSQL_SA_PASSWORD": "DataFlow_Test_2026!",
            "MSSQL_PID": "Developer",
        },
        pronto=("bash", "-c", SQLCMD + "-Q 'SELECT 1'"),
        prepara=(
            ("bash", "-c", SQLCMD + "-Q \"IF DB_ID(N'dataflow_test') IS NULL "
                                     "CREATE DATABASE [dataflow_test];\""),
            ("bash", "-c", SQLCMD + "-Q \"IF SUSER_ID(N'dataflow') IS NULL CREATE LOGIN "
                                     "[dataflow] WITH PASSWORD = N'DataFlow_Test_2026!', "
                                     "CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF;\""),
            ("bash", "-c", SQLCMD + "-d dataflow_test -Q \"IF DATABASE_PRINCIPAL_ID("
                                     "N'dataflow') IS NULL CREATE USER [dataflow] FOR LOGIN "
                                     "[dataflow]; ALTER ROLE [db_owner] ADD MEMBER "
                                     "[dataflow];\""),
        ),
        test_env={
            "PLENORA_SQLSERVER_HOST": "127.0.0.1",
            "PLENORA_SQLSERVER_PORT": "{porta}",
            "PLENORA_SQLSERVER_DATABASE": "dataflow_test",
            "PLENORA_SQLSERVER_USER": "dataflow",
            "PLENORA_SQLSERVER_PASSWORD": "DataFlow_Test_2026!",
        },
    ),
    "postgres": Prodotto(
        crate="plenora-db-postgres",
        modulo="write_admission_live_tests",
        immagini=riferimenti_postgres,
        porta=5432,
        ambiente={
            "POSTGRES_USER": "plenora",
            "POSTGRES_PASSWORD": "plenora_sonda_2026",
            "POSTGRES_DB": "plenora",
        },
        # Per TCP: durante l'inizializzazione il server temporaneo ascolta
        # soltanto sul socket locale.
        pronto=("pg_isready", "-h", "127.0.0.1", "-U", "plenora", "-d", "plenora"),
        test_env={
            "PLENORA_TEST_POSTGRES_DSN": "host=127.0.0.1 port={porta} user=plenora "
                                         "password=plenora_sonda_2026 dbname=plenora "
                                         "sslmode=disable",
            "PLENORA_REQUIRE_LIVE_POSTGRES": "1",
        },
    ),
}


def fixture_path(server: str) -> Path:
    return ROOT / "tests" / "fixtures" / f"scrittura_server_{server}.json"


class Docker:
    def __init__(self, prefisso: str) -> None:
        self.prefisso = shlex.split(prefisso)

    def run(self, argv: list[str], timeout: int = 900) -> subprocess.CompletedProcess:
        return subprocess.run(self.prefisso + argv, capture_output=True, text=True,
                              timeout=timeout, check=False)


def porta_libera() -> int:
    with socket.socket() as sonda:
        sonda.bind(("127.0.0.1", 0))
        return sonda.getsockname()[1]


def attendi(docker: Docker, nome: str, prodotto: Prodotto) -> None:
    if not prodotto.pronto:
        return
    limite = time.monotonic() + prodotto.attesa
    while time.monotonic() < limite:
        if docker.run(["exec", nome, *prodotto.pronto], timeout=120).returncode == 0:
            return
        time.sleep(5)
    raise SystemExit(f"il container {nome} non e pronto: la sonda non si puo eseguire")


def sonda(docker: Docker, nome: str, prodotto: Prodotto, porta: int, immagine: str,
          misura: bool) -> tuple[int, dict | None, str]:
    """Lancia la sonda; restituisce l'uscita, il referto e il log."""
    with tempfile.TemporaryDirectory() as cartella:
        referto = Path(cartella) / "referto.json"
        ambiente = {chiave: valore.replace("{porta}", str(porta))
                    for chiave, valore in prodotto.test_env.items()}
        ambiente["PLENORA_SONDE_SCRITTURE_IMMAGINE"] = immagine
        ambiente["PLENORA_SONDE_SCRITTURE_TRACCIA"] = "1"
        if prodotto.ca_nel_container:
            ca = docker.run(["exec", nome, "cat", prodotto.ca_nel_container])
            if ca.returncode != 0 or "BEGIN CERTIFICATE" not in ca.stdout:
                raise SystemExit(f"CA del container {nome} non letta: la sonda non verifica TLS")
            percorso_ca = Path(cartella) / "ca.pem"
            percorso_ca.write_text(ca.stdout, encoding="utf-8")
            ambiente["PLENORA_MYSQL_CA"] = str(percorso_ca)
        if misura:
            ambiente["PLENORA_SONDE_SCRITTURE_MISURA"] = "1"
        cargo = ["cargo", "test", "--locked", "-p", prodotto.crate, "--lib",
                 f"{prodotto.modulo}::{TEST}", "--", "--ignored", "--exact", "--nocapture"]
        if prodotto.nel_container:
            interno = "/tmp/plenora-referto-scritture.json"
            ambiente["PLENORA_SONDE_SCRITTURE_REFERTO"] = interno
            variabili = [arg for chiave, valore in ambiente.items()
                         for arg in ("-e", f"{chiave}={valore}")]
            completato = docker.run(["exec", *variabili, "-w", prodotto.nel_container, nome,
                                     *cargo], timeout=7200)
            copia = docker.run(["exec", nome, "cat", interno])
            if copia.returncode == 0 and copia.stdout.strip():
                referto.write_text(copia.stdout, encoding="utf-8")
        else:
            ambiente["PLENORA_SONDE_SCRITTURE_REFERTO"] = str(referto)
            completato = subprocess.run(cargo, cwd=ROOT, env={**os.environ, **ambiente},
                                        capture_output=True, text=True, timeout=7200,
                                        check=False)
        log = (completato.stdout or "") + (completato.stderr or "")
        documento = (json.loads(referto.read_text(encoding="utf-8"))
                     if referto.exists() else None)
        return completato.returncode, documento, log


def aggiorna(server: str, misure: list[dict]) -> None:
    percorso = fixture_path(server)
    documento = (json.loads(percorso.read_text(encoding="utf-8")) if percorso.exists()
                 else {"schema_version": 1, "provider": server, "misure": []})
    per_major = {riga["server_major"]: riga for riga in documento["misure"]}
    for misura in misure:
        per_major[misura["server_major"]] = misura
    documento["misure"] = [per_major[major] for major in sorted(per_major)]
    percorso.write_text(json.dumps(documento, ensure_ascii=False, indent=1) + "\n",
                        encoding="utf-8", newline="\n")


def riassunto(misura: dict) -> str:
    esiti: dict[str, int] = {}
    for chiave in misura["chiavi"]:
        esiti[chiave["esito"]] = esiti.get(chiave["esito"], 0) + 1
    return (f"major {misura['server_major']} ({misura['server_version']}): "
            f"{len(misura['chiavi'])} chiavi, "
            + ", ".join(f"{esito} {numero}" for esito, numero in sorted(esiti.items())))


def main() -> int:
    argomenti = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    argomenti.add_argument("--server", choices=sorted(PRODOTTI), required=True)
    fonte = argomenti.add_mutually_exclusive_group(required=True)
    fonte.add_argument("--riferimenti", action="store_true",
                       help="avvia, una dopo l'altra, ogni immagine qualificata del prodotto")
    fonte.add_argument("--avvia", metavar="IMMAGINE", help="avvia un container usa e getta")
    fonte.add_argument("--container", help="un container gia pronto, che lo script non ferma")
    argomenti.add_argument("--porta", type=int, help="la porta pubblicata di --container")
    argomenti.add_argument("--immagine", help="l'immagine di --container, per il referto")
    argomenti.add_argument("--docker", default=os.environ.get("PLENORA_DOCKER", "docker"),
                           help="il comando docker (per esempio \"wsl docker\")")
    argomenti.add_argument("--aggiorna", action="store_true",
                           help="riscrive il fixture con le misure invece di verificarlo")
    argomenti.add_argument("--referto", type=Path)
    opzioni = argomenti.parse_args()
    prodotto = PRODOTTI[opzioni.server]
    docker = Docker(opzioni.docker)

    if opzioni.container:
        if opzioni.porta is None and not prodotto.nel_container:
            raise SystemExit("--container richiede --porta")
        bersagli = [(opzioni.container, None, opzioni.porta or prodotto.porta)]
    else:
        immagini = prodotto.immagini() if opzioni.riferimenti else [opzioni.avvia]
        bersagli = [(f"plenora-sonde-scritture-{opzioni.server}-{os.getpid()}-{indice}",
                     immagine, porta_libera()) for indice, immagine in enumerate(immagini)]
    misure, difformi = [], []
    for nome, immagine, porta in bersagli:
        if immagine:
            ambiente = [arg for chiave, valore in prodotto.ambiente.items()
                        for arg in ("-e", f"{chiave}={valore}")]
            avviato = docker.run(["run", "--rm", "-d", "--name", nome,
                                  "-p", f"127.0.0.1:{porta}:{prodotto.porta}", *ambiente,
                                  *prodotto.opzioni_run, immagine, *prodotto.argomenti])
            if avviato.returncode != 0:
                print(f"avvio del container fallito: {avviato.stderr.strip()[:300]}",
                      file=sys.stderr)
                return 2
        try:
            attendi(docker, nome, prodotto)
            for comando in prodotto.prepara:
                preparato = docker.run(["exec", nome, *comando])
                if preparato.returncode != 0:
                    print(f"preparazione fallita: {preparato.stderr.strip()[:300]}",
                          file=sys.stderr)
                    return 2
            uscita, documento, log = sonda(docker, nome, prodotto, porta,
                                           immagine or opzioni.immagine or "non dichiarata",
                                           opzioni.aggiorna)
        finally:
            if immagine:
                docker.run(["stop", nome], timeout=180)
        if documento is None:
            print(log, file=sys.stderr)
            print(f"{opzioni.server} {immagine or nome}: nessun referto", file=sys.stderr)
            return 2
        print(f"{opzioni.server} {immagine or nome}: {riassunto(documento)}")
        misure.append(documento)
        if uscita != 0:
            difformi.append(immagine or nome)
            print(log, file=sys.stderr)
    if opzioni.referto:
        opzioni.referto.write_text(json.dumps({"schema_version": 1, "server": opzioni.server,
                                               "misure": misure}, ensure_ascii=False,
                                              indent=1) + "\n", encoding="utf-8")
    if opzioni.aggiorna and not difformi:
        aggiorna(opzioni.server, misure)
        print(f"fixture aggiornato: {fixture_path(opzioni.server).relative_to(ROOT)}")
    print(f"sonde scritture {opzioni.server}: {len(misure)} server, {len(difformi)} difformi")
    return 0 if not difformi else 1


if __name__ == "__main__":
    sys.exit(main())
