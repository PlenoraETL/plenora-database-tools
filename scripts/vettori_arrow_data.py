#!/usr/bin/env python3
"""I vettori `arrow-data-v1` di plenora-contracts contro il binario di database-tools.

Arrow Geometry Semantics 1.0 (REJ-004) vuole che un componente che lo dichiara
tratti i vettori invalidi con la categoria che ciascuno porta e accetti quelli
validi, su ogni superficie che riceve Arrow. Ogni vettore diventa un Arrow IPC
file e passa dagli ingressi Arrow della CLI:

- `inspect-dataset`, che legge l'IPC e decodifica ogni geometria senza server:
  gira sempre, anche in CI senza fixture;
- `write` verso PostgreSQL, il percorso pubblico `database.write`, con
  `--postgres-env` che nomina la variabile della DSN di un fixture healthy.
  Ogni vettore scrive una tabella propria in modo `create`, cancellata prima
  e dopo. Un'accettazione si verifica anche nel bersaglio: ogni riga ha lo
  SRID del campo (GEO-008, GEO-009), altrimenti e' `accettazione-difforme`,
  un risultato sbagliato senza errore. Un rifiuto che lascia la tabella ha
  avuto un effetto, qualunque cosa dica la busta.

Un vettore valido rifiutato per un limite del bersaglio e' il verdetto di
**supporto** di GEO-000 (un componente puo' rifiutare un ingresso conforme che
il suo bersaglio non rappresenta): si conta a parte, ed e' ammesso solo per i
limiti che il manifesto dichiara, con la loro categoria. Oggi sono due, della
scrittura PostgreSQL: un CRS che `spatial_ref_sys` del bersaglio non conosce
non si puo' registrare (la colonna lo avrebbe con SRID 0, cioe' senza CRS), e
si rifiuta con `crs` leggendo il catalogo; un ordine degli assi `lat_lon` o
`northing_easting` non si memorizza in una colonna PostGIS, che vuole la
longitudine o l'easting in `x` (GEO-001), e si rifiuta con `unsupported`. Su
un vettore invalido nei valori REJ-002 riporta il limite prima del valore, e
vale lo stesso.

Un'accettazione si verifica anche nelle coordinate: le ordinate rilette con
`ST_AsBinary` devono essere quelle scritte, nello stesso ordine (GEO-001,
GEO-003). Un valore memorizzato con gli assi scambiati ha lo SRID giusto e
nessun errore: senza questo confronto passerebbe per conforme.

Con `--postgres-env` gira anche `dry-run-postgres`, il dry run di
`bulk-write` con la DSN del fixture: deve concordare con la scrittura su ogni
vettore, senza eccezioni. Stesso exit; su un rifiuto stessa categoria, stessa
fase e stesso `retry`, con `remote_effect: none` perche' un dry run non ha
effetti; su un'accettazione la busta deve dire che il catalogo del bersaglio
e' stato letto (`checks.catalog: performed`), altrimenti il dry run avrebbe
taciuto le classi che decide `spatial_ref_sys`. Fino al 24/10 il runner
accettava ogni `dry_run` su cio' che la scrittura rifiutava dopo `validate`, e
13 vettori invalidi nei valori o nel catalogo passavano il dry run.

Con `--postgres-env` girano anche gli scenari del target (`SCENARI`,
`dry-run-postgres-target`): ingressi validi verso bersagli che la scrittura
rifiuta per cio' che chiede al target e non per i valori — privilegi (un ruolo
creato per il gate, con i soli privilegi dello scenario), schema assente,
upsert senza indice unico immediato, mapping `lossy` con una colonna assente,
registrazione dell'id del campo senza LOCK o senza proprieta' — e i controlli
positivi. Il dry run deve concordare con la scrittura come sui vettori. Fino al
25/10 il dry run diceva `dry_run` su ciascuno di questi rifiuti. La quarta
rilettura ha aggiunto row-level security, colonne generate e IDENTITY ALWAYS,
SELECT sulle colonne che un upsert legge, viste non aggiornabili, NOT NULL
senza default assenti dall'ingresso, schema senza USAGE e un `COPY` rifiutato
all'apertura dopo il DELETE di Replace; un rifiuto che cambia le righe del
bersaglio e' `rifiuto-con-effetto`. La regola R ha aggiunto le scritture
distruttive che non vedono tutte le righe (policy RLS anche restrittive,
regole `ON DELETE`, trigger `BEFORE DELETE`, una vista sopra RLS): rifiutate
prima di ogni effetto, nella scrittura e nel dry run. `--solo-scenari` salta i
vettori.

    python scripts/vettori_arrow_data.py --contracts .plenora-contracts \\
        --cli target/debug/plenora-database --referto vettori-arrow-data.json \\
        [--postgres-env PLENORA_TEST_POSTGRES_DSN]

Ogni rifiuto si legge anche con REJ-003: exit 3, `retry.kind: never` e
- per le classi che decide lo schema (supporto compreso) `remote_effect: none`
  e fase `validate` (o `connect`/`probe` per la scrittura, che puo' dover
  leggere il bersaglio; `prepare` e' la deviazione REJ-003 dichiarata nel
  manifesto: database-tools legge il bersaglio in quella fase);
- per le due classi di valore la fase in cui il valore si incontra: `read` per
  `inspect-dataset`, che lo legge dal proprio ingresso, `write` per la
  scrittura; `remote_effect` `none` o `rolled_back` (GEO-018).
La classe la decide l'oracolo dei contratti (`schema_verdict`), non questo
script.

Il referto elenca per vettore e per operazione l'esito osservato e quello
atteso; l'uscita e' 0 solo se ogni vettore ha il verdetto del contratto.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

# pyarrow si importa dove serve, non qui: il self-test delle decisioni
# (`scripts/test_vettori_arrow_data.py`) gira nella spazzata offline, che non
# lo installa.


FASI_DI_SCHEMA = {
    "inspect-dataset": {"validate"},
    # La scrittura puo' dover leggere il bersaglio prima di decidere (REJ-003:
    # `connect` o `probe`); `prepare` e' la deviazione dichiarata, la fase in
    # cui database-tools lo legge.
    "write-postgres": {"validate", "connect", "probe", "prepare"},
}
FASE_DI_VALORE = {"inspect-dataset": "read", "write-postgres": "write"}


def tabella(vettore: dict):
    import pyarrow as pa

    tipi = {"int64": pa.int64(), "binary": pa.binary(), "utf8": pa.utf8()}
    campi, colonne = [], []
    for campo in vettore["fields"]:
        tipo = tipi[campo["type"]]
        md = {k.encode(): v.encode() for k, v in (campo.get("metadata") or {}).items()}
        campi.append(pa.field(campo["name"], tipo, campo.get("nullable", True), metadata=md or None))
        valori = [riga.get(campo["name"]) for riga in vettore["rows"]]
        if tipo == pa.binary():
            valori = [None if v is None else bytes.fromhex(v) for v in valori]
        colonne.append(pa.array(valori, type=tipo))
    md = {k.encode(): v.encode() for k, v in (vettore.get("schema_metadata") or {}).items()}
    return pa.Table.from_arrays(colonne, schema=pa.schema(campi, metadata=md or None))


def busta(argv: list[str]) -> dict:
    esito = subprocess.run(argv, capture_output=True, text=True, timeout=300)
    try:
        documento = json.loads(esito.stdout)
    except json.JSONDecodeError:
        return {"exit": esito.returncode, "category": None, "busta": "illeggibile"}
    errore = documento.get("error") if isinstance(documento, dict) else None
    if not isinstance(errore, dict):
        controlli = documento.get("checks") if isinstance(documento, dict) else None
        controlli = controlli if isinstance(controlli, dict) else {}
        return {"exit": esito.returncode, "category": None, "code": None,
                "catalog": controlli.get("catalog"),
                "row_security": controlli.get("row_security")}
    retry = errore.get("retry")
    return {"exit": esito.returncode, "category": errore.get("category"),
            "phase": errore.get("phase"), "remote_effect": errore.get("remote_effect"),
            "retry": retry.get("kind") if isinstance(retry, dict) else None}


class Postgres:
    """Il bersaglio della scrittura: una tabella per vettore, in `public`."""

    def __init__(self, cli: Path, variabile: str, lavoro: Path) -> None:
        self.cli, self.variabile, self.lavoro = cli, variabile, lavoro

    def tabella(self, nome: str) -> str:
        return "vad_" + hashlib.sha256(nome.encode()).hexdigest()[:16]

    def cancella(self, tabella: str) -> None:
        esito = busta([str(self.cli), "database-execute-ddl", "postgres", self.variabile,
                       f"DROP TABLE IF EXISTS public.{tabella}"])
        if esito["exit"] != 0:
            raise SystemExit("il fixture PostgreSQL non risponde: il gate non si puo' eseguire")

    def scalare(self, sql: str) -> int:
        argv = [str(self.cli), "database-execute-scalar", "postgres", self.variabile, sql]
        esito = subprocess.run(argv, capture_output=True, text=True, timeout=120)
        return json.loads(esito.stdout)["value"]["value"]

    def crs_sconosciuto(self, vettore: dict) -> bool:
        """Il limite dichiarato: un campo geometria il cui CRS, nella forma
        `EPSG:<n>` da cui la scrittura deriva lo SRID, manca da
        `spatial_ref_sys` del bersaglio."""
        for campo in vettore["fields"]:
            md = campo.get("metadata") or {}
            if md.get("ARROW:extension:name") != "geoarrow.wkb":
                continue
            autorita, _, codice = md.get("plenora.geometry.crs_id", "").partition(":")
            if autorita.upper() == "EPSG" and codice.isascii() and codice.isdigit():
                noto = self.scalare("SELECT count(*)::int4 FROM spatial_ref_sys "
                                    f"WHERE auth_name = 'EPSG' AND auth_srid = {int(codice)}")
                if noto == 0:
                    return True
        return False

    def esiste(self, tabella: str) -> bool:
        return self.scalare("SELECT count(*)::int4 FROM pg_tables WHERE schemaname = 'public' "
                            f"AND tablename = '{tabella}'") != 0

    def srid_difformi(self, tabella: str, vettore: dict) -> int:
        """Le righe scritte con uno SRID diverso da quello del campo: il
        dichiarato, o quello di `EPSG:<n>` (GEO-008); un valore senza SRID
        proprio ha quello del campo (GEO-009). Un'accettazione con righe
        difformi e' un risultato sbagliato senza errore."""
        difformi = 0
        for campo in vettore["fields"]:
            md = campo.get("metadata") or {}
            if md.get("ARROW:extension:name") != "geoarrow.wkb":
                continue
            atteso = md.get("plenora.geometry.srid")
            autorita, _, codice = md.get("plenora.geometry.crs_id", "").partition(":")
            if atteso is None and autorita.upper() == "EPSG" and codice.isdigit():
                atteso = codice
            if atteso is None:
                continue
            colonna = '"' + campo["name"].replace('"', '""') + '"'
            difformi += self.scalare(f"SELECT count(*)::int4 FROM public.{tabella} "
                                     f"WHERE ST_SRID({colonna}::geometry) <> {int(atteso)}")
        return difformi

    def coordinate_difformi(self, tabella: str, vettore: dict) -> int:
        """Le righe la cui geometria riletta non ha le ordinate scritte, nello
        stesso ordine. `ST_AsBinary` rende WKB ISO senza SRID: si confrontano
        tipo, dimensioni e coordinate, non i byte, che cambiano con il byte
        order e con la forma del type code."""
        difformi = 0
        righe = sorted(vettore["rows"], key=lambda riga: riga["id"])
        for campo in vettore["fields"]:
            md = campo.get("metadata") or {}
            if md.get("ARROW:extension:name") != "geoarrow.wkb":
                continue
            colonna = '"' + campo["name"].replace('"', '""') + '"'
            testo = self.testo(
                f"SELECT string_agg(coalesce(encode(ST_AsBinary({colonna}::geometry), 'hex'), "
                f"'NULL'), ',' ORDER BY id) FROM public.{tabella}")
            riletti = (testo or "").split(",") if righe else []
            attesi = [riga.get(campo["name"]) for riga in righe]
            if len(riletti) != len(attesi):
                return max(len(attesi), 1)
            for atteso, riletto in zip(attesi, riletti):
                if (atteso is None) != (riletto == "NULL"):
                    difformi += 1
                elif atteso is not None and (coordinate(bytes.fromhex(atteso))
                                             != coordinate(bytes.fromhex(riletto))):
                    difformi += 1
        return difformi

    def testo(self, sql: str) -> str | None:
        argv = [str(self.cli), "database-execute-scalar", "postgres", self.variabile, sql]
        esito = subprocess.run(argv, capture_output=True, text=True, timeout=120)
        return json.loads(esito.stdout)["value"]["value"]

    def dry_run(self, nome: str, ingresso: Path) -> dict:
        """Il dry run di `bulk-write` sullo stesso ingresso, con la stessa
        operazione e la stessa DSN della scrittura: legge il catalogo, non
        scrive."""
        operazione = self.lavoro / f"{nome}-write.json"
        return busta([str(self.cli), "bulk-write", self.variabile, str(operazione),
                      str(ingresso), "--dry-run"])

    def limiti(self, vettore: dict) -> set[str]:
        """Le categorie dei limiti dichiarati che il vettore tocca."""
        limiti = self.limiti_degli_assi(vettore)
        if self.crs_sconosciuto(vettore):
            limiti.add("crs")
        return limiti

    @staticmethod
    def limiti_degli_assi(vettore: dict) -> set[str]:
        limiti = set()
        for campo in vettore["fields"]:
            md = campo.get("metadata") or {}
            if (md.get("ARROW:extension:name") == "geoarrow.wkb"
                    and md.get("plenora.geometry.axis_order") in ASSI_NON_RAPPRESENTABILI):
                limiti.add("unsupported")
        return limiti

    def scrivi(self, nome: str, ingresso: Path, vettore: dict) -> dict:
        tabella = self.tabella(nome)
        self.cancella(tabella)
        operazione = self.lavoro / f"{nome}-write.json"
        operazione.write_text(json.dumps({
            "target": {"schema": "public", "object": tabella},
            "mode": "create", "mapping_policy": "strict",
            "transaction_profile": "single_transaction"}), encoding="utf-8")
        richiesta = self.lavoro / f"{nome}-request.json"
        richiesta.write_text(json.dumps({
            "provider": "postgres", "secret_environment": self.variabile,
            "operation_path": str(operazione)}), encoding="utf-8")
        osservato = busta([str(self.cli), "write", "--input", str(richiesta), "--data", str(ingresso)])
        # Un rifiuto che lascia la tabella ha avuto un effetto, qualunque cosa
        # dica la busta (GEO-018).
        if osservato["exit"] != 0:
            osservato["tabella_rimasta"] = self.esiste(tabella)
        else:
            osservato["srid_difformi"] = self.srid_difformi(tabella, vettore)
            osservato["coordinate_difformi"] = self.coordinate_difformi(tabella, vettore)
            # Un campo `lat_lon` accettato finisce in `x` come longitudine: gli
            # assi sono scambiati anche se le ordinate rilette sono quelle
            # scritte (GEO-001).
            osservato["assi_scambiati"] = int("unsupported" in self.limiti_degli_assi(vettore))
        self.cancella(tabella)
        return osservato


# GEO-001: PostGIS memorizza la longitudine o l'easting in `x`.
ASSI_NON_RAPPRESENTABILI = {"lat_lon", "northing_easting"}


def coordinate(wkb: bytes) -> list:
    """Tipo, dimensioni e ordinate di un WKB o EWKB, in ordine di lettura.

    Il valore e' gia' stato accettato dalla scrittura: qui non si giudica la
    buona forma, si legge soltanto."""
    import struct

    posizione = 0

    def prendi(formato: str, ordine: str) -> tuple:
        nonlocal posizione
        valori = struct.unpack_from(ordine + formato, wkb, posizione)
        posizione += struct.calcsize(ordine + formato)
        return valori

    def geometria() -> list:
        ordine = "<" if wkb[posizione] == 1 else ">"
        prendi("B", ordine)
        (codice,) = prendi("I", ordine)
        z = bool(codice & 0x80000000)
        m = bool(codice & 0x40000000)
        if codice & 0x20000000:
            prendi("I", ordine)
        migliaia, tipo = divmod(codice & 0x0FFFFFFF, 1000)
        if migliaia:
            z, m = {1: (True, False), 2: (False, True), 3: (True, True)}[migliaia]
        larghezza = 2 + z + m
        uscita: list = [tipo, z, m]

        def punti(numero: int) -> None:
            # Un NaN non e uguale a se stesso: le ordinate di `POINT EMPTY`
            # si confrontano come segnaposto, non come numeri.
            uscita.append(tuple("NaN" if valore != valore else valore
                                for valore in prendi("d" * (numero * larghezza), ordine)))

        if tipo == 1:
            punti(1)
        elif tipo in (2, 8):
            punti(prendi("I", ordine)[0])
        elif tipo in (3, 17):
            for _ in range(prendi("I", ordine)[0]):
                punti(prendi("I", ordine)[0])
        else:
            for _ in range(prendi("I", ordine)[0]):
                uscita.append(geometria())
        return uscita

    return geometria()


def rej_003(operazione: str, osservato: dict, di_valore: bool) -> bool:
    """Gli assi del rifiuto che REJ-003 fissa, per la classe del rifiuto."""
    if osservato["exit"] != 3 or osservato.get("retry") != "never":
        return False
    if osservato.get("tabella_rimasta"):
        return False
    if di_valore:
        return (osservato.get("phase") == FASE_DI_VALORE[operazione]
                and osservato.get("remote_effect") in {"none", "rolled_back"})
    return (osservato.get("phase") in FASI_DI_SCHEMA[operazione]
            and osservato.get("remote_effect") == "none")


def verdetto(operazione: str, vettore: dict, osservato: dict, oracolo,
             limite: str | set[str] | None) -> str:
    """`limite` e' la categoria, o l'insieme delle categorie, dei limiti
    dichiarati che il vettore tocca nell'operazione, o `None`."""
    limiti = {limite} if isinstance(limite, str) else set(limite or ())
    accettato = osservato["exit"] == 0
    supporto = osservato["category"] in limiti
    if vettore["expect"] == "valid":
        if accettato:
            difforme = (osservato.get("srid_difformi") or osservato.get("coordinate_difformi")
                        or osservato.get("assi_scambiati"))
            return "accettazione-difforme" if difforme else "conforme"
        if not supporto:
            return "rifiuto-indebito"
        return "rifiuto-di-supporto" if rej_003(operazione, osservato, False) else "assi-del-rifiuto"
    if accettato:
        return "accettazione-indebita"
    if supporto and vettore["category"] == "data_mapping":
        # REJ-002: un limite di supporto si riporta prima di un valore invalido.
        return "rifiuto-di-supporto" if rej_003(operazione, osservato, False) else "assi-del-rifiuto"
    if osservato["category"] != vettore["category"]:
        return "categoria-sbagliata"
    di_valore = oracolo.schema_verdict(vettore) is None
    return "conforme" if rej_003(operazione, osservato, di_valore) else "assi-del-rifiuto"


def accordo_dry_run(dry_run: dict, scrittura: dict, rls_dichiarata: bool = False) -> str:
    """Il dry run concorda con la scrittura su ogni vettore: stesso exit; su
    un rifiuto stessa categoria, stessa fase e stesso `retry`, senza effetti
    (`remote_effect: none`); su un'accettazione con il catalogo del bersaglio
    letto. Un dry run che accetta cio' che la scrittura rifiuta, in qualunque
    fase, e' un'accettazione indebita: chi lo usa per decidere se scrivere
    scriverebbe e fallirebbe, o peggio non saprebbe perche'.

    Su un'accettazione `checks.row_security` dice se una policy RLS decide
    sui valori: `not_performed` solo dove lo scenario lo attende
    (`rls_dichiarata`), `not_applicable` altrove. Tacerlo, o dichiararlo dove
    RLS non si applica, e' un dry run che non dice che cosa ha deciso."""
    if scrittura["exit"] == 0:
        if dry_run["exit"] != 0:
            return "dry-run-rifiuta"
        if dry_run.get("catalog") != "performed":
            return "dry-run-senza-catalogo"
        atteso = "not_performed" if rls_dichiarata else "not_applicable"
        return "conforme" if dry_run.get("row_security") == atteso else "dry-run-rls"
    if dry_run["exit"] == 0:
        return "dry-run-accetta"
    if dry_run["exit"] != scrittura["exit"]:
        return "dry-run-exit"
    if dry_run["category"] != scrittura["category"]:
        return "dry-run-categoria"
    if dry_run.get("phase") != scrittura.get("phase"):
        return "dry-run-fase"
    if dry_run.get("retry") != scrittura.get("retry") or dry_run.get("remote_effect") != "none":
        return "dry-run-assi"
    return "conforme"


def dsn_con_utente(dsn: str, utente: str, password: str) -> str:
    """La stessa DSN con un altro utente: forma URL o `chiave=valore`."""
    import re
    from urllib.parse import quote, urlsplit, urlunsplit

    if "://" in dsn:
        parti = urlsplit(dsn)
        host = parti.netloc.rsplit("@", 1)[-1]
        return urlunsplit(parti._replace(netloc=f"{quote(utente)}:{quote(password)}@{host}"))
    resto = re.sub(r"(^|\s)(user|password)=('(?:[^'\\]|\\.)*'|\S*)", " ", dsn).strip()
    return f"{resto} user={utente} password={password}"


# Gli scenari del target: cio che la scrittura chiede al bersaglio oltre ai
# valori (privilegi, schema, colonne, indice unico dell'upsert). Fino al
# 25/10 il dry run con la DSN diceva `dry_run` su ciascuno dei rifiuti, e il
# runner non li provava. Ogni scenario ha la preparazione del bersaglio, la
# mode, i campi dell'ingresso (`id` int64, `v` utf8), le chiavi, la policy di
# mapping e l'utente: `lettore` e un ruolo creato qui con i soli privilegi
# dello scenario. I controlli positivi tengono il runner onesto: un dry run
# che rifiutasse tutto non concorderebbe.
SCENARI = (
    ("privilegi-append", "CREATE TABLE {t} (id bigint); GRANT SELECT ON {t} TO {r}",
     "append", ("id",), (), "strict", "lettore"),
    ("privilegi-append-concessi", "CREATE TABLE {t} (id bigint); GRANT SELECT, INSERT ON {t} TO {r}",
     "append", ("id",), (), "strict", "lettore"),
    ("privilegi-create", "", "create", ("id",), (), "strict", "lettore"),
    ("schema-assente", "", "create-altrove", ("id",), (), "strict", "proprietario"),
    ("replace-senza-delete", "CREATE TABLE {t} (id bigint); GRANT SELECT, INSERT ON {t} TO {r}",
     "replace", ("id",), (), "strict", "lettore"),
    ("truncate-senza-truncate", "CREATE TABLE {t} (id bigint); GRANT SELECT, INSERT ON {t} TO {r}",
     "truncate_insert", ("id",), (), "strict", "lettore"),
    ("update-senza-update",
     "CREATE TABLE {t} (id bigint PRIMARY KEY, v text); GRANT SELECT, INSERT ON {t} TO {r}",
     "update", ("id", "v"), ("id",), "strict", "lettore"),
    ("delete-senza-delete", "CREATE TABLE {t} (id bigint PRIMARY KEY); GRANT SELECT ON {t} TO {r}",
     "delete_by_keys", ("id",), ("id",), "strict", "lettore"),
    ("upsert-senza-unique", "CREATE TABLE {t} (id bigint, v text)",
     "upsert", ("id", "v"), ("id",), "strict", "proprietario"),
    ("upsert-con-unique", "CREATE TABLE {t} (id bigint UNIQUE, v text)",
     "upsert", ("id", "v"), ("id",), "strict", "proprietario"),
    ("upsert-unique-differibile",
     "CREATE TABLE {t} (id bigint, v text, UNIQUE (id) DEFERRABLE INITIALLY DEFERRED)",
     "upsert", ("id", "v"), ("id",), "strict", "proprietario"),
    ("lossy-colonna-assente", "CREATE TABLE {t} (id bigint)",
     "append", ("id", "v"), (), "lossy", "proprietario"),
    ("lossy-colonne-presenti", "CREATE TABLE {t} (id bigint, v text)",
     "append", ("id", "v"), (), "lossy", "proprietario"),
    # La registrazione dell'id del campo prende un LOCK (UPDATE, DELETE o
    # TRUNCATE) e scrive COMMENT ON COLUMN (proprieta' della tabella).
    ("registrazione-id-senza-lock", "CREATE TABLE {t} (id bigint); GRANT SELECT, INSERT ON {t} TO {r}",
     "append", ("id+field_id",), (), "strict", "lettore"),
    ("registrazione-id-senza-proprieta",
     "CREATE TABLE {t} (id bigint); GRANT SELECT, INSERT, UPDATE ON {t} TO {r}",
     "append", ("id+field_id",), (), "strict", "lettore"),
    # La quarta rilettura (25/10): classi che il dry run accettava e la
    # scrittura rifiutava. Row-level security: senza policy di INSERT per il
    # ruolo ogni riga e rifiutata (INSERT) e `COPY FROM` non e ammesso affatto;
    # con una policy l'esito dipende dai valori, e il dry run lo dichiara
    # (`checks.row_security: not_performed`) invece di dire che e tutto
    # deciso. Il proprietario la salta, se non e forzata.
    ("rls-senza-policy-append",
     "CREATE TABLE {t} (id bigint); ALTER TABLE {t} ENABLE ROW LEVEL SECURITY; "
     "GRANT SELECT, INSERT ON {t} TO {r}",
     "append", ("id",), (), "strict", "lettore"),
    ("rls-senza-policy-truncate",
     "CREATE TABLE {t} (id bigint); ALTER TABLE {t} ENABLE ROW LEVEL SECURITY; "
     "GRANT SELECT, INSERT, TRUNCATE ON {t} TO {r}",
     "truncate_insert", ("id",), (), "strict", "lettore"),
    ("rls-forzata-al-proprietario",
     "CREATE TABLE {t} (id bigint); ALTER TABLE {t} OWNER TO {r}; "
     "ALTER TABLE {t} ENABLE ROW LEVEL SECURITY; ALTER TABLE {t} FORCE ROW LEVEL SECURITY",
     "append", ("id",), (), "strict", "lettore"),
    ("rls-proprietario-non-forzata",
     "CREATE TABLE {t} (id bigint); ALTER TABLE {t} OWNER TO {r}; "
     "ALTER TABLE {t} ENABLE ROW LEVEL SECURITY",
     "append", ("id",), (), "strict", "lettore"),
    ("rls-con-policy-append",
     "CREATE TABLE {t} (id bigint); ALTER TABLE {t} ENABLE ROW LEVEL SECURITY; "
     "GRANT SELECT, INSERT ON {t} TO {r}; "
     "CREATE POLICY vad_inserisce ON {t} FOR INSERT TO {r} WITH CHECK (true)",
     "append", ("id",), (), "strict", "lettore"),
    # Colonne che non ricevono valori: GENERATED ALWAYS ... STORED rifiuta
    # INSERT e COPY; IDENTITY ALWAYS rifiuta INSERT, mentre COPY lo scavalca
    # e scrive il valore dell'ingresso (controllo positivo).
    ("generata-append",
     "CREATE TABLE {t} (id bigint, v text GENERATED ALWAYS AS ('x') STORED)",
     "append", ("id", "v"), (), "strict", "proprietario"),
    ("generata-truncate",
     "CREATE TABLE {t} (id bigint, v text GENERATED ALWAYS AS ('x') STORED)",
     "truncate_insert", ("id", "v"), (), "strict", "proprietario"),
    ("identita-always-append",
     "CREATE TABLE {t} (id bigint GENERATED ALWAYS AS IDENTITY, v text)",
     "append", ("id", "v"), (), "strict", "proprietario"),
    ("identita-always-truncate",
     "CREATE TABLE {t} (id bigint GENERATED ALWAYS AS IDENTITY, v text)",
     "truncate_insert", ("id", "v"), (), "strict", "proprietario"),
    # `ON CONFLICT ... DO UPDATE SET v = EXCLUDED.v` legge le chiavi e le
    # colonne aggiornate: senza SELECT su ciascuna la prima riga e rifiutata.
    ("upsert-senza-select",
     "CREATE TABLE {t} (id bigint PRIMARY KEY, v text); GRANT INSERT, UPDATE ON {t} TO {r}",
     "upsert", ("id", "v"), ("id",), "strict", "lettore"),
    ("upsert-select-solo-chiavi",
     "CREATE TABLE {t} (id bigint PRIMARY KEY, v text); GRANT INSERT, UPDATE ON {t} TO {r}; "
     "GRANT SELECT (id) ON {t} TO {r}",
     "upsert", ("id", "v"), ("id",), "strict", "lettore"),
    ("upsert-privilegi-concessi",
     "CREATE TABLE {t} (id bigint PRIMARY KEY, v text); GRANT SELECT, INSERT, UPDATE ON {t} TO {r}",
     "upsert", ("id", "v"), ("id",), "strict", "lettore"),
    # Una vista che non si aggiorna da sola: INSERT, DELETE e TRUNCATE la
    # rifiutano; una vista aggiornabile accetta il DELETE di Replace ma non
    # `COPY`.
    ("vista-non-aggiornabile-append",
     "CREATE TABLE {b} (id bigint); CREATE VIEW {t} AS SELECT DISTINCT id FROM {b}",
     "append", ("id",), (), "strict", "proprietario"),
    ("vista-non-aggiornabile-truncate",
     "CREATE TABLE {b} (id bigint); CREATE VIEW {t} AS SELECT DISTINCT id FROM {b}",
     "truncate_insert", ("id",), (), "strict", "proprietario"),
    ("vista-non-aggiornabile-delete",
     "CREATE TABLE {b} (id bigint PRIMARY KEY); CREATE VIEW {t} AS SELECT DISTINCT id FROM {b}",
     "delete_by_keys", ("id",), ("id",), "strict", "proprietario"),
    ("vista-aggiornabile-replace",
     "CREATE TABLE {b} (id bigint); CREATE VIEW {t} AS SELECT id FROM {b}",
     "replace", ("id",), (), "strict", "proprietario"),
    ("vista-aggiornabile-append",
     "CREATE TABLE {b} (id bigint); CREATE VIEW {t} AS SELECT id FROM {b}",
     "append", ("id",), (), "strict", "proprietario"),
    # Una colonna NOT NULL senza default che l'ingresso non porta: la prima
    # riga la lascia nulla. Con un default, o nullable, passa.
    ("not-null-assente-append", "CREATE TABLE {t} (id bigint, w text NOT NULL)",
     "append", ("id",), (), "strict", "proprietario"),
    ("not-null-assente-truncate", "CREATE TABLE {t} (id bigint, w text NOT NULL)",
     "truncate_insert", ("id",), (), "strict", "proprietario"),
    ("not-null-con-default", "CREATE TABLE {t} (id bigint, w text NOT NULL DEFAULT 'x')",
     "append", ("id",), (), "strict", "proprietario"),
    # Lo schema senza USAGE: ogni statement sulla tabella e rifiutato.
    ("schema-senza-usage",
     "CREATE SCHEMA {b}; CREATE TABLE {b}.{t} (id bigint); GRANT SELECT, INSERT ON {b}.{t} TO {r}",
     "append-altrove", ("id",), (), "strict", "lettore"),
    # Replace con DELETE e senza INSERT: `COPY` e rifiutato all'apertura,
    # dopo il DELETE e prima di ogni COMMIT. Fino al 25/10 la scrittura usciva
    # in fase `rollback` con effetto ignoto: `tokio-postgres` chiude la
    # connessione e il ROLLBACK non si conferma, ma il server scarta da se'
    # una transazione mai confermata.
    ("replace-senza-insert",
     "CREATE TABLE {t} (id bigint); INSERT INTO {t} VALUES (7), (8), (9); "
     "GRANT SELECT, DELETE ON {t} TO {r}",
     "replace", ("id",), (), "strict", "lettore"),
    # La regola R (25/10): uno statement distruttivo che non vede tutte le
    # righe si rifiuta prima di ogni effetto, nella scrittura e nel dry run.
    # Fino ad allora Replace con una policy `USING (v = 'a')` confermava e
    # lasciava la riga 2 accanto alle nuove; `DO INSTEAD NOTHING` confermava
    # senza sostituire niente; un `BEFORE DELETE` che restituisce NULL salvava
    # la riga; DeleteByKeys e Update pubblicavano `skipped` per la chiave
    # nascosta. Il controllo positivo: un trigger AFTER, che non sopprime
    # righe. (Append su una vista sopra RLS, che il dry run dichiara
    # `not_performed`, passa dal percorso preparato: la CLI scrive con `COPY`,
    # che una vista rifiuta; la prova e nella suite live del provider.)
    ("regola-r-rls-replace",
     "CREATE TABLE {t} (id bigint, v text); INSERT INTO {t} VALUES (1, 'a'), (2, 'b'), (3, 'a'); "
     "ALTER TABLE {t} ENABLE ROW LEVEL SECURITY; GRANT ALL ON {t} TO {r}; "
     "CREATE POLICY vad_vede_a ON {t} USING (v = 'a') WITH CHECK (true)",
     "replace", ("id", "v"), (), "strict", "lettore"),
    ("regola-r-rls-restrittiva-replace",
     "CREATE TABLE {t} (id bigint, v text); INSERT INTO {t} VALUES (1, 'a'), (2, 'b'), (3, 'a'); "
     "ALTER TABLE {t} ENABLE ROW LEVEL SECURITY; GRANT ALL ON {t} TO {r}; "
     "CREATE POLICY vad_tutto ON {t} USING (true) WITH CHECK (true); "
     "CREATE POLICY vad_cancella_a ON {t} AS RESTRICTIVE FOR DELETE USING (v = 'a')",
     "replace", ("id", "v"), (), "strict", "lettore"),
    ("regola-r-rls-delete",
     "CREATE TABLE {t} (id bigint PRIMARY KEY, v text); "
     "INSERT INTO {t} VALUES (1, 'a'), (2, 'b'), (3, 'a'); "
     "ALTER TABLE {t} ENABLE ROW LEVEL SECURITY; GRANT ALL ON {t} TO {r}; "
     "CREATE POLICY vad_vede_a ON {t} USING (v = 'a') WITH CHECK (true)",
     "delete_by_keys", ("id",), ("id",), "strict", "lettore"),
    ("regola-r-rls-update",
     "CREATE TABLE {t} (id bigint PRIMARY KEY, v text); "
     "INSERT INTO {t} VALUES (1, 'a'), (2, 'b'), (3, 'a'); "
     "ALTER TABLE {t} ENABLE ROW LEVEL SECURITY; GRANT ALL ON {t} TO {r}; "
     "CREATE POLICY vad_vede_a ON {t} USING (v = 'a') WITH CHECK (true)",
     "update", ("id", "v"), ("id",), "strict", "lettore"),
    ("regola-r-rule-replace",
     "CREATE TABLE {t} (id bigint); INSERT INTO {t} VALUES (7), (8), (9); "
     "CREATE RULE vad_niente AS ON DELETE TO {t} DO INSTEAD NOTHING",
     "replace", ("id",), (), "strict", "proprietario"),
    ("regola-r-trigger-replace",
     "CREATE TABLE {t} (id bigint); INSERT INTO {t} VALUES (1), (2), (3); "
     "CREATE OR REPLACE FUNCTION {b}_salva() RETURNS trigger LANGUAGE plpgsql AS "
     "$$ BEGIN IF OLD.id = 2 THEN RETURN NULL; END IF; RETURN OLD; END $$; "
     "CREATE TRIGGER vad_salva BEFORE DELETE ON {t} FOR EACH ROW EXECUTE FUNCTION {b}_salva()",
     "replace", ("id",), (), "strict", "proprietario"),
    ("regola-r-trigger-after-replace",
     "CREATE TABLE {t} (id bigint); INSERT INTO {t} VALUES (1), (2), (3); "
     "CREATE OR REPLACE FUNCTION {b}_nota() RETURNS trigger LANGUAGE plpgsql AS "
     "$$ BEGIN RETURN NULL; END $$; "
     "CREATE TRIGGER vad_nota AFTER DELETE ON {t} FOR EACH ROW EXECUTE FUNCTION {b}_nota()",
     "replace", ("id",), (), "strict", "proprietario"),
    ("regola-r-vista-rls-delete",
     "CREATE TABLE {b} (id bigint PRIMARY KEY, v text); "
     "INSERT INTO {b} VALUES (1, 'a'), (2, 'b'); ALTER TABLE {b} ENABLE ROW LEVEL SECURITY; "
     "CREATE POLICY vad_vede_a ON {b} USING (v = 'a') WITH CHECK (true); "
     "CREATE VIEW {t} WITH (security_invoker = true) AS SELECT id, v FROM {b}; "
     "GRANT ALL ON {b} TO {r}; GRANT ALL ON {t} TO {r}",
     "delete_by_keys", ("id",), ("id",), "strict", "lettore"),
)

# Gli scenari in cui RLS si applica con una policy: l'esito dipende dai
# valori, e il dry run deve dirlo invece di tacerlo.
SCENARI_RLS_DICHIARATA = {"rls-con-policy-append"}


SCENARI_DEL_TARGET = "dry-run-postgres-target"


def prepara_vista_rls(sql: str, versione: int) -> str:
    """PG14: vista posseduta dal ruolo soggetto alla policy della base."""
    if versione < 150_000 and "WITH (security_invoker = true)" in sql:
        return (sql.replace("WITH (security_invoker = true)", "")
                + "; GRANT CREATE ON SCHEMA public TO {r};"
                + " ALTER VIEW {t} OWNER TO {r};"
                + " REVOKE CREATE ON SCHEMA public FROM {r}")
    return sql


class Scenari:
    """Gli scenari del target: la scrittura e il dry run, stesso ingresso e
    stessa DSN, devono concordare come sui vettori (`accordo_dry_run`)."""

    RUOLO = "vad_lettore_dry_run"
    SCHEMA_ASSENTE = "vad_schema_assente"
    # L'oggetto ausiliario di uno scenario (`{b}`): la tabella sotto una
    # vista, o lo schema di `append-altrove`.
    AUSILIARIO = "vad_ausiliario"

    def __init__(self, postgres: Postgres) -> None:
        import secrets

        self.postgres = postgres
        self.password = secrets.token_hex(16)
        self.lettore = postgres.variabile + "_VAD_LETTORE"
        os.environ[self.lettore] = dsn_con_utente(
            os.environ[postgres.variabile], self.RUOLO, self.password)

    def ddl(self, sql: str) -> None:
        esito = busta([str(self.postgres.cli), "database-execute-ddl", "postgres",
                       self.postgres.variabile, sql])
        if esito["exit"] != 0:
            raise SystemExit("preparazione di uno scenario del target fallita: il gate non si "
                             "puo' eseguire")

    def ruolo(self, presente: bool) -> None:
        if self.esiste_il_ruolo():
            self.ddl(f"DROP OWNED BY {self.RUOLO}")
            self.ddl(f"DROP ROLE {self.RUOLO}")
        if presente:
            self.ddl(f"CREATE ROLE {self.RUOLO} LOGIN PASSWORD '{self.password}'")

    def esiste_il_ruolo(self) -> bool:
        return self.postgres.scalare("SELECT count(*)::int4 FROM pg_roles "
                                     f"WHERE rolname = '{self.RUOLO}'") != 0

    def ingresso(self, nome: str, campi: tuple[str, ...]) -> Path:
        import pyarrow as pa
        import pyarrow.ipc as ipc

        # `id` non nullable: verso una chiave primaria, un campo nullable e'
        # una perdita di nullability che la policy strict rifiuta prima del
        # target, e lo scenario non proverebbe i privilegi.
        colonne = {"id": (pa.field("id", pa.int64(), nullable=False), pa.array([1, 2], pa.int64())),
                   "v": (pa.field("v", pa.utf8()), pa.array(["a", "b"], pa.utf8()))}
        # `id+field_id`: il campo porta `plenora.field_id`, che la scrittura
        # registra nel commento della colonna.
        con_id = [campo.endswith("+field_id") for campo in campi]
        nomi = [campo.removesuffix("+field_id") for campo in campi]
        campi_arrow = [colonne[nome][0].with_metadata({b"plenora.field_id": b"0"}) if marcato
                       else colonne[nome][0] for nome, marcato in zip(nomi, con_id)]
        t = pa.Table.from_arrays(
            [colonne[nome][1] for nome in nomi],
            schema=pa.schema(campi_arrow,
                             metadata={b"plenora.contract.version": b"1"} if any(con_id) else None))
        percorso = self.postgres.lavoro / f"scenario-{nome}.arrow"
        with pa.OSFile(str(percorso), "wb") as f, ipc.new_file(f, t.schema) as w:
            w.write_table(t)
        return percorso

    def esegui(self) -> list[dict]:
        righe = []
        versione = self.postgres.scalare("SELECT current_setting('server_version_num')::int4")
        self.ruolo(True)
        try:
            for nome, preparazione, mode, campi, chiavi, policy, utente in SCENARI:
                tabella = self.postgres.tabella("scenario-" + nome)
                schema = {"create-altrove": self.SCHEMA_ASSENTE,
                          "append-altrove": self.AUSILIARIO}.get(mode, "public")
                self.pulisci(tabella)
                if preparazione:
                    # `{t}` e' l'oggetto della scrittura; in `append-altrove`
                    # `{b}` e' il suo schema e `{t}` il nome nudo.
                    self.ddl(prepara_vista_rls(preparazione, versione).format(
                        t=tabella if mode == "append-altrove" else f"public.{tabella}",
                        b=self.AUSILIARIO if mode == "append-altrove"
                        else f"public.{self.AUSILIARIO}",
                        r=self.RUOLO))
                operazione = self.postgres.lavoro / f"scenario-{nome}-write.json"
                operazione.write_text(json.dumps({
                    "target": {"schema": schema, "object": tabella},
                    "mode": {"create-altrove": "create",
                             "append-altrove": "append"}.get(mode, mode),
                    "mapping_policy": policy, "keys": list(chiavi),
                    "update_columns": ["v"] if mode == "update" else [],
                    "transaction_profile": "single_transaction"}), encoding="utf-8")
                variabile = self.lettore if utente == "lettore" else self.postgres.variabile
                richiesta = self.postgres.lavoro / f"scenario-{nome}-request.json"
                richiesta.write_text(json.dumps({
                    "provider": "postgres", "secret_environment": variabile,
                    "operation_path": str(operazione)}), encoding="utf-8")
                dati = self.ingresso(nome, campi)
                prima = self.righe(tabella, schema) if preparazione else 0
                # Il dry run prima: la scrittura, se riesce, cambia il target.
                dry_run = busta([str(self.postgres.cli), "bulk-write", variabile,
                                 str(operazione), str(dati), "--dry-run"])
                scrittura = busta([str(self.postgres.cli), "write", "--input", str(richiesta),
                                   "--data", str(dati)])
                righe.append({"scenario": nome, "scrittura": scrittura, "dry_run": dry_run,
                              "verdetto": accordo_dry_run(
                                  dry_run, scrittura, nome in SCENARI_RLS_DICHIARATA)})
                # Un rifiuto non ha effetti: il bersaglio ha le righe di prima.
                if scrittura["exit"] != 0 and preparazione:
                    scrittura["righe"] = [prima, self.righe(tabella, schema)]
                    if prima != scrittura["righe"][1]:
                        righe[-1]["verdetto"] = "rifiuto-con-effetto"
                self.pulisci(tabella)
        finally:
            self.ddl(f"DROP SCHEMA IF EXISTS {self.SCHEMA_ASSENTE} CASCADE")
            self.ddl(f"DROP SCHEMA IF EXISTS {self.AUSILIARIO} CASCADE")
            self.ruolo(False)
        return righe

    def pulisci(self, tabella: str) -> None:
        """Il bersaglio di uno scenario e cio che gli sta intorno: vista,
        tabella sotto la vista, schemi."""
        self.ddl(f"DROP SCHEMA IF EXISTS {self.SCHEMA_ASSENTE} CASCADE")
        self.ddl(f"DROP SCHEMA IF EXISTS {self.AUSILIARIO} CASCADE")
        # `DROP VIEW` su una tabella (o il contrario) e' un errore: prima il
        # tipo dell'oggetto.
        if self.postgres.testo("SELECT coalesce((SELECT relkind::text FROM pg_class "
                               f"WHERE oid = to_regclass('public.{tabella}')), '-')") == "v":
            self.ddl(f"DROP VIEW public.{tabella}")
        self.ddl(f"DROP TABLE IF EXISTS public.{self.AUSILIARIO} CASCADE")
        # Le funzioni dei trigger della regola R.
        for funzione in ("salva", "nota"):
            self.ddl(f"DROP FUNCTION IF EXISTS public.{self.AUSILIARIO}_{funzione}() CASCADE")
        self.postgres.cancella(tabella)

    def righe(self, tabella: str, schema: str) -> int:
        """Le righe del bersaglio, prima della scrittura e dopo un suo
        rifiuto. Uno schema assente non ne ha."""
        if schema == self.SCHEMA_ASSENTE:
            return 0
        return self.postgres.scalare(f"SELECT count(*)::int4 FROM {schema}.{tabella}")


def main() -> int:
    argomenti = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    argomenti.add_argument("--contracts", type=Path, required=True)
    argomenti.add_argument("--cli", type=Path, required=True)
    argomenti.add_argument("--referto", type=Path, required=True)
    argomenti.add_argument("--postgres-env", help="variabile d'ambiente con la DSN del fixture PostgreSQL")
    argomenti.add_argument("--solo-scenari", action="store_true",
                           help="soltanto gli scenari del target (richiede --postgres-env)")
    opzioni = argomenti.parse_args()
    if opzioni.solo_scenari and not opzioni.postgres_env:
        print("--solo-scenari richiede --postgres-env", file=sys.stderr)
        return 2

    sys.path.insert(0, str(opzioni.contracts / "tools"))
    import arrow_data as oracolo
    import pyarrow as pa
    import pyarrow.ipc as ipc

    cartella = opzioni.contracts / "vectors" / "arrow-data-v1"
    vettori = sorted(cartella.glob("*.json"))
    if not vettori:
        print("nessun vettore arrow-data-v1 nel checkout dei contratti", file=sys.stderr)
        return 2
    if opzioni.postgres_env and not os.environ.get(opzioni.postgres_env):
        # Una DSN assente non si legge come un gate passato.
        print("--postgres-env nomina una variabile vuota o assente", file=sys.stderr)
        return 2
    righe, conteggi = [], {}
    with tempfile.TemporaryDirectory(prefix="vettori-arrow-data-") as lavoro:
        lavoro = Path(lavoro)
        postgres = Postgres(opzioni.cli, opzioni.postgres_env, lavoro) if opzioni.postgres_env else None
        operazioni = ["inspect-dataset"] + (["write-postgres", "dry-run-postgres"]
                                            if postgres else [])
        if opzioni.solo_scenari:
            operazioni, vettori = [], []
        for percorso in vettori:
            vettore = json.loads(percorso.read_text(encoding="utf-8"))
            ingresso = lavoro / f"{percorso.stem}.arrow"
            t = tabella(vettore)
            with pa.OSFile(str(ingresso), "wb") as f, ipc.new_file(f, t.schema) as w:
                w.write_table(t)
            scrittura: dict = {}
            for operazione in operazioni:
                if operazione == "inspect-dataset":
                    osservato = busta([str(opzioni.cli), "inspect-dataset", str(ingresso)])
                    v = verdetto(operazione, vettore, osservato, oracolo, None)
                elif operazione == "write-postgres":
                    osservato = scrittura = postgres.scrivi(percorso.stem, ingresso, vettore)
                    v = verdetto(operazione, vettore, osservato, oracolo, postgres.limiti(vettore))
                else:
                    osservato = postgres.dry_run(percorso.stem, ingresso)
                    v = accordo_dry_run(osservato, scrittura)
                conteggi.setdefault(operazione, {})
                conteggi[operazione][v] = conteggi[operazione].get(v, 0) + 1
                righe.append({"vettore": percorso.stem, "operazione": operazione,
                              "atteso": vettore["expect"], "categoria_attesa": vettore.get("category"),
                              "osservato": osservato, "verdetto": v})
        scenari = Scenari(postgres).esegui() if postgres else []
        for riga in scenari:
            conteggi.setdefault(SCENARI_DEL_TARGET, {})
            conteggi[SCENARI_DEL_TARGET][riga["verdetto"]] = (
                conteggi[SCENARI_DEL_TARGET].get(riga["verdetto"], 0) + 1)
    opzioni.referto.write_text(json.dumps({"schema_version": 1, "vettori": len(vettori),
                                           "operazioni": operazioni, "conteggi": conteggi,
                                           "esiti": righe, "scenari_del_target": scenari},
                                          ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for operazione in operazioni:
        print(f"vettori arrow-data-v1, {operazione}: {len(vettori)} esiti: "
              + ", ".join(f"{k} {v}" for k, v in sorted(conteggi[operazione].items())))
    if scenari:
        print(f"{SCENARI_DEL_TARGET}: {len(scenari)} esiti: "
              + ", ".join(f"{k} {v}" for k, v in sorted(conteggi[SCENARI_DEL_TARGET].items())))
    if opzioni.solo_scenari:
        print("vettori arrow-data-v1 non eseguiti: --solo-scenari")
    elif "write-postgres" not in operazioni:
        print("write-postgres non eseguito: serve --postgres-env con un fixture healthy")
    buoni = {"conforme", "rifiuto-di-supporto"}
    return 0 if all(set(c) <= buoni for c in conteggi.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
