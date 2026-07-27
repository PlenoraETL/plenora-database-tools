# PostgreSQL 16 + PostGIS 3.4

Stato: **driver di riferimento read/write/spatial avanzato operativo**

## Fixture

Il file `docker-compose.postgres.yml` crea:

- container `dataflow-postgres`;
- PostgreSQL 16 e PostGIS 3.4;
- database e utente di test `dataflow`;
- volume `plenora-database-tools_postgres_data`;
- schema `plenora_fixture`;
- 10.000 eventi con decimal, date/time, JSON, binary, `geometry(PointZ,
  4326)` e `geography(Point, 4326)`;
- tabella con identificatori SQL avversari.

Avvio:

```powershell
docker compose -f docker-compose.postgres.yml up -d
```

Stop conservando i dati:

```powershell
docker compose -f docker-compose.postgres.yml stop
```

`down -v` elimina definitivamente il volume e va usato soltanto quando si
vuole rigenerare la fixture.

## Driver Rust

`plenora-db-postgres` implementa:

- connessione redatta e classificazione iniziale SQLSTATE;
- probe PostgreSQL/PostGIS e capability runtime;
- lista cataloghi, schemi e oggetti;
- introspezione strutturale di colonne, enum, domini, collation, relation kind,
  partizioni, viste/materialized view, default, identity, generated,
  constraint, indici/opclass/include/predicati, RLS policy, ACL, owner e
  tablespace;
- projection e identificatori quotati;
- filtri con bind parameter separati;
- ordering e row limit;
- `QueryOperation` eseguibile con CTE anche ricorsive, join e `LATERAL`,
  derived table/subquery, `DISTINCT ON`, set operation, filtri booleani,
  aggregate, group-by, having, window/frame, ordering, offset/limit, locking e
  funzioni PostGIS;
- fast path one-shot anche per `QueryOperation`: i tipi dei bind derivano dai
  valori canonici e lo schema esatto dalla prima riga server; sui risultati
  vuoti viene eseguito soltanto il describe finale, senza rieseguire la SELECT;
- `query_raw` streaming a batch Arrow bounded;
- pool condiviso bounded per credenziale/TLS, timeout di acquisizione e
  timeout di sessione; configurazione nel startup packet sulle connessioni
  nuove e un solo `DISCARD ALL` prima del riuso;
- fast path one-shot tipizzato per letture senza parametri e per filtri
  parametrizzati su tipi built-in, UUID, numeric e geometrie EWKB;
- fallback conservativo al prepare con inferenza server-side per enum, domini,
  composite e ogni combinazione di tipo non riconosciuta; kill switch pubblico
  `with_parameterized_read_fast_path(false)`;
- `PostgresSchemaToken` con OID di database/namespace/relation e fingerprint
  strutturale SHA-256;
- cache schema strict, LRU e bounded (256 oggetti di default): ogni hit viene
  validato sul catalogo; DDL esterno e write Plenora invalidano o ricaricano
  l'oggetto senza usare TTL;
- snapshot di metriche bounded per pool, reset, introspezioni, fast path
  one-shot parametrizzati, fallback prepared, cache schema, invalidazioni,
  cancellazioni, batch/byte/righe e outcome write, senza label o dati sensibili;
- cancellazione server-side delle query e delle scritture in volo; le sessioni
  interrotte o con outcome incerto non rientrano nel pool;
- connect timeout, TCP user timeout e keepalive configurabili;
- bool, interi, float, text, bytea, date, timestamp/timestamptz, JSON/UUID
  testuale e `Decimal128`;
- valori Arrow Date32/Timestamp fuori dal range di `chrono` producono un errore
  `DataMapping` recuperabile e non possono causare panic nel writer;
- `TIME`, `INTERVAL MonthDayNano`, array Arrow nativi di bool, interi, float e
  text, range built-in e composite come `Struct`; enum e domini preservano la
  dichiarazione PostgreSQL;
- `Decimal128` con scala positiva, zero o negativa e parametri decimal/UUID/NULL
  tipizzati;
- geometry/geography come EWKB con metadata GeoArrow-WKB, SRID, dimensioni
  XY/XYZ/XYM/XYZM, tipi curve/surface/collection e semantica spatial;
- validazione fail-closed dell'header EWKB in scrittura rispetto al contratto
  Arrow: byte order, tipo, dimensioni e SRID incompatibili non raggiungono il
  database;
- create, append, replace atomico su staging e truncate-insert;
- update, upsert e delete-by-keys con bind parameter;
- COPY text e COPY binario bounded per
  create/append/replace/truncate-insert;
- COPY binario per array, UUID, interval, range built-in e composite con campi
  scalari supportati;
- evoluzione schema additiva opt-in: soltanto nuove colonne nullable e DDL
  nella stessa transazione del write;
- transazione unica, rollback su errore e outcome esplicito per commit incerto;
- creazione opzionale degli indici GiST spatial;
- Arrow/GeoArrow-WKB verso PostGIS geometry/geography, inclusi Z e SRID;
- catalogo e AST coerenti per 72 funzioni spatial tipizzate, inclusi
  accessor/predicate, processing, metriche, output e clustering;
- operatori spatial indicizzabili bounding-box e KNN (`&&`, `~`, `@`, `<->`,
  `<<->>`) con bind EWKB e verifica del piano GiST;
- filtri `IN`, `BETWEEN`, `LIKE` e predicati spatial EWKB;
- preflight remoto di esistenza, chiavi, tipi, nullability e SRID;
- budget effettivi per batch Arrow e singola cella WKB;
- timeout server-side, application name e TLS Rustls con WebPKI, CA private e
  autenticazione client mTLS;
- fault injection prima e dopo commit;
- confronto differenziale COPY/prepared.
- mutex interni di pool e cache recuperabili dopo poisoning, senza rendere
  inutilizzabile il provider.

## Profili prestazionali

L'API espone due configurazioni misurate, senza cambiare il comportamento
storico di `PostgresProvider::default()`:

```rust
use plenora_db_postgres::{
    PostgresPerformanceProfile, PostgresProvider,
};

let interactive =
    PostgresProvider::for_profile(PostgresPerformanceProfile::LowLatency);
let bulk =
    PostgresProvider::for_profile(PostgresPerformanceProfile::BalancedBulk);
```

`LowLatency` usa batch da 1.024 righe, target soft 1 MiB e COPY text.
`BalancedBulk` usa batch da 8.192 righe, target soft 4 MiB e COPY binary.
`with_performance_profile` applica la stessa scelta a un provider già
configurato, conservando TLS, pool, timeout e limiti.

Le singole opzioni restano sovrascrivibili esplicitamente. Il profilo non
modifica atomicità, semantica spatial o controlli di correttezza.

Il target adattivo può anche essere configurato direttamente:

```rust
let provider = PostgresProvider::new(32_768)
    .with_target_batch_bytes(4 * 1024 * 1024);
```

Il conteggio righe è sempre un tetto. Il target byte chiude il batch dopo la
riga che lo raggiunge e calibra i batch successivi sui byte Arrow reali; una
singola riga o l'arrotondamento dell'allocator possono superare moderatamente
il target soft. `with_byte_limits` continua a imporre il limite hard esatto.
`without_target_batch_bytes` ripristina il solo sizing per righe.

La CLI espone:

```text
postgres-probe <dsn-env>
postgres-describe <dsn-env> <schema> <object>
postgres-read-summary <dsn-env> <schema> <object>
```

Il valore della variabile DSN non viene stampato.

## Test live

Il test live verifica:

- connessione e PostGIS;
- 10.000 righe in 13 batch da 777;
- Decimal128(38,18), timestamp UTC, geometry PointZ e geography;
- EWKB/SRID 4326 e metadata GeoArrow;
- filtro bindato su una colonna non proiettata;
- ordering e limit;
- identificatori con spazi, keyword e quote;
- mancata esposizione della password in un errore di autenticazione;
- tutte le sette modalità write;
- COPY di decimal, JSON, bytea, timestamp, geometry e geography;
- staging replace e indici GiST;
- conteggi committed e stato remoto finale;
- filtri spatial tipizzati;
- rollback iniettato prima del commit;
- outcome incerto iniettato dopo il commit;
- enforcement dei byte budget;
- equivalenza COPY text/COPY binario/prepared su 1.000 righe;
- timeout del pool saturo e riuso sicuro della sessione;
- CTE/join/group-by/having con ordine stabile dei bind;
- CTE ricorsive, derived table, lateral, window, set operation, offset e
  locking con validazione strutturale bounded;
- introspezione avanzata e roundtrip enum/domain/array/TIME;
- introspezione di partizioni, policy RLS, ACL, materialized view e indici
  spatial/espressione/include;
- geometrie XY/XYZ/XYM/XYZM, curve, TIN, collection e geography;
- filtri bounding-box/KNN con prova `EXPLAIN` dell'uso dell'indice GiST;
- roundtrip text/binario di interval, range, composite, UUID e numeric a scala
  negativa;
- cancellazione live di read e write bloccati con verifica del rollback;
- stress concorrente del pool e cancellazione simultanea con recovery;
- schema evolution additiva e typed parameter decimal/UUID/NULL.

Il profilo di sicurezza e resilienza, con garanzie e limiti espliciti, è in
[HARDENING.md](HARDENING.md). Il razionale hazard→invariante→prova e i rischi
residui sono in [SAFETY-CASE.md](SAFETY-CASE.md); è un profilo ingegneristico,
non una certificazione aeronautica.
La politica di versioni verificata è in
[COMPATIBILITY.md](COMPATIBILITY.md).

## Gate di congelamento

```powershell
python scripts\check_postgres_reference.py
```

Il gate aggiuntivo di hardening è:

```powershell
python scripts\check_postgres_hardening.py
```

La matrice delle major PostgreSQL supportate è:

```powershell
python scripts\check_postgres_matrix.py
```

La campagna prestazionale riproducibile è:

```powershell
python scripts\check_postgres_performance.py
```

Il gate micro-prestazionale dedicato agli operatori GiST/KNN è:

```powershell
python scripts\check_postgres_spatial_performance.py
```

Scenari, metriche, baseline e regole anti-regressione sono descritti in
[PERFORMANCE.md](PERFORMANCE.md).

Il data path v3 PostgreSQL/PostGIS è congelato come riferimento. Il gate non
salva il DSN e produce un confronto differenziale dei dati.

## Confine del riferimento completo

Il gate v3 non ha gap aperti nel perimetro del driver general-purpose
PostgreSQL/PostGIS definito dal progetto. Restano volutamente fuori perimetro:

- API amministrative, logical replication e protocollo WAL;
- semantiche specifiche di estensioni arbitrarie o FDW;
- cataloghi completi PostGIS Raster, Topology e SFCGAL;
- un wrapper Rust dedicato per ognuna delle centinaia di funzioni PostGIS;
- migrazioni distruttive o widening implicito dello schema.

Queste aree richiedono capability o moduli separati e non devono essere
pubblicizzate come comportamento implicito del provider general-purpose.
