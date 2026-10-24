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
`bulk-write`: deve concordare con la scrittura su ogni vettore. Rifiuta, con
la stessa categoria, cio' che la scrittura rifiuta senza contattare il server
(fase `validate`); accetta tutto il resto, perche' cio' che decidono il
catalogo o i valori non e' suo.

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
        return {"exit": esito.returncode, "category": None, "code": None}
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
        """Il dry run di `bulk-write` sullo stesso ingresso e con la stessa
        operazione della scrittura: non contatta il server."""
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


def accordo_dry_run(dry_run: dict, scrittura: dict) -> str:
    """Il dry run concorda con la scrittura: rifiuta con la stessa categoria
    cio' che la scrittura rifiuta senza server (fase `validate`), e accetta
    tutto il resto. Un dry run che accetta cio' che la scrittura rifiuta in
    `validate` e' un'accettazione indebita; uno che rifiuta cio' che la
    scrittura accetta, o rifiuta dopo, un rifiuto indebito."""
    if scrittura["exit"] != 0 and scrittura.get("phase") == "validate":
        if dry_run["exit"] == 0:
            return "dry-run-accetta"
        if dry_run["category"] != scrittura["category"]:
            return "dry-run-categoria"
        return "conforme"
    return "conforme" if dry_run["exit"] == 0 else "dry-run-rifiuta"


def main() -> int:
    argomenti = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    argomenti.add_argument("--contracts", type=Path, required=True)
    argomenti.add_argument("--cli", type=Path, required=True)
    argomenti.add_argument("--referto", type=Path, required=True)
    argomenti.add_argument("--postgres-env", help="variabile d'ambiente con la DSN del fixture PostgreSQL")
    opzioni = argomenti.parse_args()

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
    opzioni.referto.write_text(json.dumps({"schema_version": 1, "vettori": len(vettori),
                                           "operazioni": operazioni, "conteggi": conteggi,
                                           "esiti": righe},
                                          ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for operazione in operazioni:
        print(f"vettori arrow-data-v1, {operazione}: {len(vettori)} esiti: "
              + ", ".join(f"{k} {v}" for k, v in sorted(conteggi[operazione].items())))
    if "write-postgres" not in operazioni:
        print("write-postgres non eseguito: serve --postgres-env con un fixture healthy")
    buoni = {"conforme", "rifiuto-di-supporto"}
    return 0 if all(set(c) <= buoni for c in conteggi.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
