# plenora-database-tools

Black box Rust per introspezione, lettura e scrittura di dati tabellari e
geospaziali su database relazionali. I provider condividono un contratto solo —
piano, capability, loss report, write outcome — e i dati viaggiano come stream
Arrow/GeoArrow-WKB, non come JSON.

Il dominio sono i database. Non ci sono provider REST, e non c'e un archivio
di documenti che dice come erano le cose prima: la storia sta in Git.

## Dove guardare

| serve | sta in |
| --- | --- |
| cosa il codice dichiara oggi | [`docs/STATO.md`](docs/STATO.md) — generato |
| il contratto | [`contracts/v2/`](contracts/v2/README.md) |
| far girare i fixture | [`docs/operativo.md`](docs/operativo.md) |
| la qualifica MariaDB in corso | [`docs/mariadb/`](docs/mariadb/EVIDENCE.md) |
| perche una decisione e stata presa | `git log` |

`docs/STATO.md` non si modifica a mano: elenca crate, capability per provider,
sub-comandi del CLI e inventario dei test leggendoli dai sorgenti. Se un numero
e sbagliato li, e sbagliato nel codice.

## Cosa fa girare le prove

```powershell
python scripts\phase0_validate.py          # contratti, esempi, golden, domini
python scripts\check_mysql_reference.py    # gate MySQL: statico + live
python scripts\check_postgres_reference.py # gate PostgreSQL
python scripts\check_sqlserver_reference.py
cargo test --workspace -- --skip live_     # unit, senza server
```

I gate live avviano i propri fixture Compose. Ogni provider ha il suo progetto:
i volumi sono prefissati dal progetto e non si toccano fra loro.

## Regole che non cambiano

- una capability resta `false` finche non esiste una prova riproducibile che la
  sostiene: `not_measured` non e un `no`, ed e per questo che non apre niente;
- una modifica incompatibile del contratto richiede una nuova major, non una
  nota di rilascio;
- i documenti non ripetono fatti che vivono nel codice: o li generano da li,
  oppure non li dicono.
