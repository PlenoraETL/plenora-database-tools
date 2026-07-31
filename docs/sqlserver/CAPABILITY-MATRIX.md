# Matrice capability SQL Server

Legenda:

- **offline**: proprietà verificabile senza server;
- **live-required**: implementazione o dichiarazione subordinata a prova reale;
- **reject**: comportamento escluso dal profilo iniziale.

| Area | Decisione iniziale | Evidenza |
|---|---|---|
| Dialect | T-SQL, baseline SQL Server 2022 / compat 160 | offline |
| Identificatori | quoting `[]`, massimo 128 caratteri per parte | offline |
| Bind | `@pN`, massimo 2.100 parametri | offline |
| Limit | `TOP (n)` senza offset | offline |
| Offset | `ORDER BY … OFFSET n ROWS [FETCH NEXT m ROWS ONLY]` | offline |
| CTE ricorsiva | `WITH`, senza keyword `RECURSIVE` | offline |
| Count | `COUNT_BIG` per il contratto `Count` a 64 bit | offline |
| TLS | cifratura richiesta, verifica certificato default | offline + live-required |
| Trust certificate | solo opt-out esplicito | offline + live-required |
| MARS | disabilitato | offline + live-required |
| Sessione | XACT_ABORT ON, implicit transactions OFF, NOCOUNT ON e opzioni ANSI/ARITHABORT/QUOTED_IDENTIFIER fissate | offline + live-proven |
| Pool | bounded; riuso solo in stato `Ready` | offline + live-required |
| Trait comune `Provider` | connection, capability, inspect, read/query e prepared write | live-proven |
| Cancellazione | quarantena connessione | offline + live-required |
| Commit ambiguo | effetto `Unknown`, recovery obbligatoria | live-proven con taglio TDS fisico |
| Read streaming | Arrow batch con canale TDS a capacità 1 e batch bounded | live-proven |
| Read comune | projection, filter non-spatial bindato, ordering e `TOP` | live-proven |
| Query AST base | singola source, alias, colonne, confronti booleani, `IS NULL`, ordering e limit | live-proven |
| Query AST ricco | CTE, join, aggregate/group/having, window, set operation e offset/fetch; schema output derivato dal server e verificato contro TDS | live-proven |
| Codec scalari read | booleani, interi, float, decimal/money, testo, binari e temporali | live-proven |
| Budget read | righe, colonne, memoria, output, componenti geometriche e deadline | offline + live-proven |
| Prepared write | bind TDS tipizzati: `append`, `truncate_insert`, `update`, `upsert`, `delete_by_keys` | live-proven |
| Create | DDL allow-list da schema Arrow, identificatori quotati, PK opzionale e transazione unica | live-proven |
| Replace | staging nella stessa transazione, schema guard sotto lock e swap con rename atomici | live-proven |
| Visibilità replace | il target originale resta leggibile durante il caricamento; lock esclusivo solo alla pubblicazione | live-proven |
| Dipendenze replace | FK entranti, trigger, permessi, dipendenze/RLS, tracking/CDC/replica, proprietà estese, statistiche/full-text/audit e storage non riproducibile sono rifiutati | offline + live-proven per FK |
| Keyed DML | indice univoco non filtrato obbligatorio; chiavi NULL rifiutate; conteggi insert/update/delete distinti | live-proven |
| Upsert concurrency | `UPDATE` con `UPDLOCK,HOLDLOCK` + `INSERT` condizionale; `MERGE` escluso | live-proven |
| Write transaction | `single_transaction`, lock target e schema guard | live-proven |
| DDL transazionale | create, staging, rename e drop partecipano al rollback completo | live-proven |
| Staged swap | nessuna finestra con target assente; commit incerto vieta retry automatico | live-proven |
| Rollback | vincolo nel batch successivo ripristina truncate e batch precedenti | live-proven |
| Commit incerto | `OutcomeUnknown`, retry automatico vietato | live-proven con risposta TDS fisicamente interrotta |
| Fault pre-commit | rollback verificato, effetto `RolledBack` | live-proven |
| Perdita trasporto in write | nessun falso successo, `Unknown` e recovery | live-proven con taglio socket TDS |
| Blackhole durante read | timeout, effetto `None`, connessione quarantinata | live-proven |
| Risposta rollback persa | `Unknown/RequiresRecovery` anche con rollback server osservato | live-proven con blackhole |
| Bulk write | opt-in; colonne complete/in ordine, scalari verificati, un request bounded per batch | live-proven |
| Bulk differential | prepared/TDS bulk, 100 righe in 4 batch, zero differenze | live-proven |
| Bulk rollback | conflitto nel secondo request ripristina il primo batch | live-proven |
| Bulk tipi verificati | bit, interi, real/float, decimal scala 0–37, nvarchar, varbinary | live-proven |
| Bulk tipi esclusi | temporali, money, XML/UUID, spatial/UDT e varianti non provate | reject; percorso prepared |
| `MERGE` | non usato come default | reject |
| `geometry` | WKB GeoArrow, semantica e dimensioni native preservate | live-proven XY/XYZ/XYM/XYZM |
| `geography` | WKB GeoArrow, semantica e dimensioni native preservate | live-proven XY/XYZ/XYM/XYZM |
| Indici spatial | solo create/replace atomici con primary key clustered; `GEOMETRY_AUTO_GRID` con bounding box derivato dai dati e `GEOGRAPHY_AUTO_GRID` | live-proven per creazione, introspezione e access path forzato |
| Indice `geometry` senza extent | nessun bounding box inventato: errore fail-closed e rollback di tabella/staging | live-proven |
| SRID | sempre esplicito per input spatial | offline + live-required |
| Reprojection | nessuna `ST_Transform` nativa pubblicizzata | reject |
| `FullGlobe` | rifiutato in strict | reject |
| Spatial rich AST scalare | `GeometryType`, `Srid`, `NPoints`, `IsEmpty`, `IsValid`, `IsClosed`, `Intersects`, `Contains`, `Within`, `Disjoint`, `Equals`, `Distance`, `Area`, `Length`, `StartPoint`, `EndPoint`; WKB bindato, preflight semantica/SRID e source fisica singola | offline + live-proven su geometry/geography |
| Predicati spatial in projection | valore `bit` nativo con `NULL` preservato; nei filtri confronto T-SQL `= 1` | offline + live-proven |
| Output query spatial | `StartPoint`/`EndPoint` convertiti con `AsBinaryZM()`; semantica, SRID, Z/M e `FullGlobe` profilati sul risultato selezionato, schema sorgente ricontrollato; ogni altro UDT nudo resta rifiutato | live-proven geometry/geography XYZM |
| Lateral/APPLY e locking | non pubblicizzati finché semantica e schema non sono provati live | reject |
| Introspection estesa | privilegi effettivi, temporal/graph/external e partizioni | live-required |
| Probe riferimento | versione 16.0.4255.1, Developer, compat 160 | live-proven |
| Catalogo riferimento | schemi, oggetti, colonne, vincoli, indici e metadati di tassellazione/bounding box spatial | live-proven |
| Schema token | stabile senza DDL, varia dopo `ALTER TABLE` | live-proven |
| TLS self-signed negativo | policy `Verify` rifiuta la fixture | live-proven |
| TLS self-signed opt-out | connessione solo con eccezione esplicita | live-proven |
| TLS CA privata | catena valida e hostname matching accettati; mismatch rifiutato | live-proven post-RC1 |
| Rotazione certificato | nuovo certificato server, CA stabile e riconnessione verificata | live-proven post-RC1 |
| SRID misti read | rifiuto prima dello streaming | live-proven |
| Z/M/ZM read | WKB ISO lossless con dimensioni uniformi per colonna; profili misti rifiutati | live-proven |
| FullGlobe read | rifiuto prima dello streaming | live-proven post-RC1 |
| Z/M/ZM write | WKB ISO geometry/geography verificato contro il contratto Arrow | live-proven |
| Drop stream parziale | cancellazione e quarantena, nessun riuso | live-proven |
| Differential read/write | zero differenze su scalari, temporali, XML/UUID e spatial | live-proven |
| Schema drift write | rifiuto dopo prepare e prima della prima mutazione | live-proven |
| Schema evolution | opt-in, sole colonne nullable additive; DDL e dati nella stessa transazione | live-proven |
| `time(7)` / `datetime2(7)` a 100 ns | rifiuto se non rappresentabile esattamente in Arrow µs | live-proven |
| `datetimeoffset` | testo RFC3339/ISO 127: offset per riga e 100 ns preservati | live-proven |

## Matrice live prevista

Il riferimento minimo è SQL Server 2022. Prima del freeze del provider:

- SQL Server 2019;
- SQL Server 2022;
- SQL Server 2025;
- Azure SQL Database;
- `geometry` e `geography` con SRID compatibili e incompatibili;
- fault di rete tramite latenza e packet loss su lettura e rollback;
- il taglio socket fisico su scrittura e risposta commit è già live-proven.
