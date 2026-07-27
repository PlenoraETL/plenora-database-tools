# Allineamento ai contratti trasversali — PostgreSQL/PostGIS

Baseline verificata: PostgreSQL 16 / PostGIS 3.4.

Stato: implementazione nel solo `plenora-database-tools`; nessuna dichiarazione
di certificazione avionica.

| Area | Stato | Evidenza |
|---|---|---|
| R2 metadata | implementato | schema versionato, emissione canonica, dual-read legacy e rifiuto delle divergenze |
| R3 geometria | implementato nel core | 16 tipi, XY/XYZ/XYM/XYZM/unknown, encoding e dichiarazione exact/mixed/unresolved |
| R4 CRS | parziale | modello canonico presente; PostgreSQL emette SRID e `declared_unresolved`, ma non inventa authority o axis order |
| R7 risorse | parziale | `ResourceBudget` con lease e aritmetica controllata implementato; propagazione completa nel provider ancora da eseguire |
| R9 errori | implementato | quattro assi; nessuna categoria `OutcomeUnknown`; retry tipizzato |
| R11 cancellazione | implementato | token concreto, child token, deadline dichiarativa, future race-free; nessun polling nel provider |
| R14 write | implementato per il riferimento | rollback dichiarato solo se confermato; commit incerto con recovery obbligatoria |

## Gate eseguito

```powershell
python scripts\check_postgres_hardening.py
```

Risultato del 27 luglio 2026: `passed`.

- container PostgreSQL/PostGIS healthy;
- server mTLS con CA privata healthy;
- rustfmt e Clippy `-D warnings`;
- test core, SQL e provider;
- cancellazione concorrente e server-side;
- schema cache e DDL esterno;
- EWKB, tipi spaziali e quattro dimensionalità;
- commit fault, rollback e recovery.

## Gap successivo

Il prossimo incremento deve propagare una singola istanza di `ResourceBudget`
attraverso `Provider::read`, `query`, `prepare_write` e `write`, collegandola ai
limiti già applicati dal provider. Finché questo non avviene, R7 resta
esplicitamente parziale.
