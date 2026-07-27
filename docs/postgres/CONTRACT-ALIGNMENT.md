# Allineamento ai contratti trasversali — PostgreSQL/PostGIS

Baseline verificata: PostgreSQL 16 / PostGIS 3.4.

Stato: implementazione nel solo `plenora-database-tools`; nessuna dichiarazione
di certificazione avionica.

| Area | Stato | Evidenza |
|---|---|---|
| R2 metadata | implementato | schema versionato, emissione canonica, dual-read legacy e rifiuto delle divergenze |
| R3 geometria | implementato nel core | 16 tipi, XY/XYZ/XYM/XYZM/unknown, encoding e dichiarazione exact/mixed/unresolved |
| R4 CRS | authority ID implementato, axis/definition parziali | `spatial_ref_sys` risolve SRID e authority ID; assenze restano `declared_unresolved`/`missing`; axis order resta `unknown` e nessuna definizione viene inventata |
| R7 risorse | implementato nel profilo PostgreSQL | budget unico read/query/prepare/write; deadline monotona attiva; lease atomiche; reserve/commit per righe, byte e componenti geometriche; scanner EWKB iterativo; rifiuto della sostituzione prepare/execute |
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

## Incremento R7 completato

`ResourceBudget` fa ora parte obbligatoria delle firme `Provider::read`,
`query`, `prepare_write` e `write`. La memoria di costruzione viene prenotata
prima dei builder Arrow; righe, memoria emessa e byte di output sono consumi
cumulativi fail-closed. Le lease di operazione e colonne vengono restituite al
drop dello stream/prepared write. Una write non può cambiare budget dopo il
preflight.

Se un limite si esaurisce dentro una transazione, PostgreSQL esegue un rollback
esplicito: l'errore dichiara `RolledBack` soltanto dopo conferma, altrimenti
`Unknown` e `RequiresRecovery`.

Il contenuto EWKB viene attraversato senza ricorsione e senza allocazioni
proporzionali ai conteggi dichiarati dal payload. Componenti e profondità sono
verificati sia in read sia in write e il consumo geometrico entra nello stesso
budget cumulativo.

`duration_ms` determina una deadline monotona condivisa da tutti i clone del
budget. La scadenza cancella anche il backend PostgreSQL. In write usa rollback
verificato durante le fasi transazionali; durante il commit l'effetto diventa
`Unknown` e richiede recovery.

I limiti spill e decompressione non sono applicabili al data path PostgreSQL
corrente, che non implementa spill né decompressione.
