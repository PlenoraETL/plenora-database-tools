# Fase 0 — Contratti e baseline

Stato: **gate offline superato; test live in attesa dei target**  
Sorgente caratterizzata: `C:\Users\Marco\Desktop\plenora\backend`  
Data snapshot iniziale: 2026-07-26

## Obiettivo

La Fase 0 congela ciò che il backend Plenora fa oggi prima di riscriverlo in
Rust. Non assume che ogni comportamento storico sia corretto: lo classifica
come:

- **normativo** — deve essere preservato;
- **da correggere** — il port Rust deve adottare la semantica specificata;
- **legacy compatibile** — preservabile tramite migrazione/alias, non nel nuovo
  protocollo canonico;
- **fuori scope** — non appartiene alla black box database;
- **nuovo** — necessario per database non presenti nel backend.

## Artefatti

| Artefatto | Scopo | Stato |
|---|---|---|
| `backend-inventory.md` | Funzioni e moduli Python da estrarre | iniziale |
| `capability-matrix.md` | Copertura corrente e obiettivo per driver | iniziale |
| `behavioral-compatibility.md` | Regole Python ↔ Rust e golden dataset | iniziale |
| `baseline-plan.md` | Harness e misure prestazionali | iniziale |
| `first-campaign.md` | Risultati della prima campagna eseguibile | completato |
| `pre-database-gate.md` | Checklist offline e regola di ingresso | completato |
| `open-decisions.md` | Decisioni che richiedono target reali | attesa target |
| `../../contracts/v1/` | Contratti JSON e relativi esempi | candidato v1 |
| `../../golden/v1/cases.json` | Catalogo golden macchina | specificato |
| `../adr/0001-black-box-boundary.md` | Confine autonomo della libreria | accettato |
| `../adr/0002-validation-and-remote-preflight.md` | Fasi validate/prepare/execute | accettato |
| `../adr/0003-transaction-outcomes.md` | Commit, parzialità e outcome incerto | accettato |
| `../adr/0004-provider-model-sql-and-arcgis.md` | Provider SQL/REST | accettato |
| `../adr/0005-canonical-types-and-loss-policy.md` | Arrow e perdite esplicite | accettato |
| `../adr/0006-arrow-geoarrow-boundary.md` | Confine dati e geometrie | accettato |
| `../adr/0007-security-errors-and-sql-construction.md` | Segreti/errori/SQL | accettato |
| `../adr/0008-runtime-and-driver-selection-gate.md` | Gate driver/runtime | accettato |

## Definition of done

La Fase 0 è completata quando:

1. ogni funzione database del backend è collegata a un'operazione Rust
   candidata oppure marcata fuori scope;
2. la semantica di read, introspection e di ogni write mode è coperta da
   fixture e oracle differenziale;
3. esiste una matrice server/versione/capability iniziale;
4. tipi, null, decimal, tempo e geometrie hanno dataset golden;
5. errori, redazione e stati transazionali hanno casi fault-injection;
6. l'harness Python produce risultati macchina stabili;
7. le baseline prestazionali iniziali sono archiviate con ambiente completo;
8. gli ADR che bloccano lo scaffold Rust sono accettati;
9. tutti i gap aperti hanno owner logico, priorità e decisione richiesta.

I punti che richiedono un target reale sono intenzionalmente sospesi nel
`pre-database-gate.md`; il gate offline si verifica con:

```powershell
python scripts\check_phase0.py
```

## Sequenza

```text
inventario statico
  → caratterizzazione tramite test Python esistenti
  → fixture golden
  → baseline Python
  → contratti canonici Rust
  → scaffold Fase 1
```

## Confine di scope iniziale

Dentro:

- PostgreSQL/PostGIS;
- MySQL/MariaDB;
- SQL Server geometry/geography;
- Oracle/Oracle Spatial;
- SQLite/SpatiaLite;
- nuovi driver Db2/Db2 Spatial e DuckDB/Spatial;
- ArcGIS Online ed ArcGIS Enterprise Feature Service.

Fuori dalla black box v1:

- persistenza delle configurazioni connessione del backend Plenora;
- router FastAPI, permessi applicativi, Celery e storage degli artefatti;
- conversione finale CSV/Parquet/XLSX, già responsabilità di IO-tools.

ArcGIS vive nello stesso workspace come provider separato: condivide engine,
Arrow, contratti, limiti e outcome, ma non usa il dialect/AST SQL.

## Gap già identificati

- Db2 e DuckDB non sono implementati nel backend corrente.
- SQLite è leggibile/ispezionabile ma non compare nel writer CARICA.
- MariaDB usa in più punti il ramo MySQL, ma la detection non è uniforme.
- Il modello Python riduce molti tipi a `int/real/str/bool/date/geometry`,
  perdendo precisione necessaria al contratto Arrow.
- SQL Server introspeziona `geometry/geography` ma il dettaglio SRID non è
  presente nel modello normalizzato.
- Oracle Spatial è gestito in scrittura, ma l'introspezione corrente non
  normalizza tipo geometrico/SRID come PostGIS.
- `WriteResult` non rappresenta esplicitamente l'esito incerto del commit.
- Il read streaming corrente passa ancora attraverso tuple/DataFrame in alcuni
  percorsi; il confine Rust dovrà essere direttamente `RecordBatch`.
