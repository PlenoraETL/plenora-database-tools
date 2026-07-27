# plenora-database-tools — Vincoli prestazionali e di memoria

Questo documento definisce requisiti misurabili per
`plenora-database-tools`. Ha lo stesso peso di `Architetture.md`: una funzione
corretta ma incapace di mantenere streaming, memoria e round trip entro i
limiti non è completa.

Le prestazioni database dipendono da più livelli:

```text
engine Plenora
  + conversione Arrow
  + driver/client
  + protocollo di rete
  + latenza e banda
  + configurazione server
  + storage, indici, WAL/redo
  + concorrenza e carico esterno
```

I benchmark devono quindi separare il costo controllato dalla libreria dal
costo esterno. Nessun numero singolo è valido per tutti i database.

---

## 1. Scopo

Obiettivi:

- letture e scritture realmente streaming;
- memoria bounded rispetto a fetch, batch, binder e code;
- percorso Arrow minimale;
- round trip ridotti senza alterare la semantica;
- utilizzo dei percorsi bulk solo quando equivalenti al piano richiesto;
- conversioni spatial limitate ai bordi;
- backpressure dal consumer fino al cursor e dall'endpoint fino al producer;
- concorrenza bounded e misurata;
- transazioni e atomicità senza costi nascosti o downgrade silenziosi;
- benchmark riproducibili per ogni driver/capability;
- confronto con backend Plenora Python, percorso Rust grezzo e release
  precedente.

Non sono obiettivi:

- vincere microbenchmark disabilitando validazione o transazioni;
- massimizzare throughput ignorando memoria, WAL o impatto sul server;
- nascondere la latenza con concorrenza illimitata;
- confrontare database diversi come se avessero lo stesso storage;
- rendere il bulk path semanticamente diverso dal prepared path.

---

## 2. Vincoli fondamentali

### V1 — Arrow come rappresentazione unica

Tra engine e chiamante circolano `RecordBatch`, non strutture per-riga
intermedie persistenti.

Lettura:

```text
protocol row/column buffers → Arrow builders → RecordBatch
```

Scrittura:

```text
RecordBatch → column/bind buffers → driver
```

È vietata una pipeline stabile del tipo:

```text
DB row → HashMap<String, Value> → JSON → Arrow
Arrow → JSON → Vec<HashMap<...>> → DB row
```

Adapter per-riga sono ammessi solo quando imposti dal client, devono essere
locali al batch e misurati.

Metriche:

- `arrow_batches`;
- `arrow_bytes`;
- `rows_materialized_as_objects`;
- `bytes_copied`;
- `conversion_allocations`.

### V2 — Hot path minimale

Tutto ciò che può essere risolto in `validate` o `prepare` non viene ripetuto
per riga o batch:

- parsing JSON;
- lookup di operazione;
- risoluzione nomi colonna;
- parsing tipi nativi;
- quoting identificatori;
- rendering SQL;
- scelta placeholder;
- costruzione bind layout;
- risoluzione SRID;
- probe capability;
- scelta bulk/prepared path;
- verifica policy.

Il loop dati usa configurazioni preparate:

```rust
struct PreparedReadColumn {
    source_ordinal: usize,
    output_field: Field,
    decoder: DecoderKind,
}

struct PreparedWriteColumn {
    input_index: usize,
    target_ordinal: usize,
    binder: BinderKind,
}
```

### V3 — Streaming reale in lettura

Per una query di N righe, la memoria deve dipendere da:

```text
fetch corrente
+ batch Arrow in volo
+ prefetch bounded
+ decoder temporanei
+ margine driver
```

e non da N.

Il cursor deve essere server-side o avere comportamento equivalente quando il
driver lo supporta. Un API che internamente carica l'intero result set non può
essere pubblicizzata come streaming.

Il benchmark obbligatorio misura 1M, 10M e 100M righe verificando la pendenza
del picco RSS.

### V4 — Streaming reale in scrittura

Il writer consuma batch progressivamente. Non concatena l'intero input per
costruire un unico statement o buffer bulk.

Memoria:

```text
batch corrente
+ bind/bulk buffer corrente
+ code bounded
+ staging metadata
+ margine driver
```

Il commit globale può mantenere modifiche nel database, ma non autorizza la
libreria a mantenere tutte le righe in RAM.

### V5 — Percorso preparato o bulk

Ordine di preferenza, soggetto a capability e semantica:

1. protocollo bulk binario/columnar;
2. array binding;
3. prepared multi-row statement;
4. prepared statement per-riga.

Il percorso viene scelto in `prepare` in base a:

- server/versione;
- tipi;
- numero colonne e parametri;
- modalità write;
- necessità di `RETURNING`;
- constraint/default/generated columns;
- dimensione stimata;
- atomicità;
- benchmark del driver.

Il fallback è osservabile nelle metriche con una ragione. Non è ammesso
concatenare valori nel SQL per simulare un bulk path.

### V6 — Nessuna copia non richiesta

Devono riusare buffer quando semanticamente possibile:

- projection;
- rename metadata;
- reorder;
- slice di batch;
- pass-through di colonne;
- fan-out interno.

La conversione da buffer del driver ad Arrow può richiedere copia. La libreria
non promette zero-copy universale, ma deve misurare:

- copie obbligatorie;
- copie introdotte dall'engine;
- copie causate dal driver;
- copie spatial.

Una nuova copia su un percorso dichiarato zero-copy blocca il rilascio.

### V7 — Fetch e batch sizing controllati

Tre grandezze distinte:

- `target_rows`;
- `target_batch_bytes`;
- `max_batch_bytes`.

Il target orientativo può adattarsi. Il massimo è duro.

L'adattamento considera:

- larghezza media riga;
- LOB;
- WKB medio/massimo;
- latenza;
- limite bind;
- memoria disponibile;
- throughput osservato;
- capacità del consumer.

Il fetch per sole righe è insufficiente: 10.000 righe con LOB o poligoni
complessi possono essere enormi.

Batch oversized:

- se splittabile prima dell'allocazione finale, viene ridotto;
- altrimenti fallisce con `ResourceLimit`;
- non viene accettato sperando che il consumer lo liberi.

### V8 — Backpressure end-to-end

Lettura:

```text
consumer lento
  → coda piena
  → nessun nuovo batch
  → nessun nuovo fetch
```

Scrittura:

```text
database lento
  → binder occupato
  → coda piena
  → producer sospeso
```

È vietato sostituire la backpressure con:

- code unbounded;
- task per batch illimitati;
- nuove connessioni;
- prefetch driver non controllato;
- accumulo su disco non governato.

### V9 — Pushdown corretto e conveniente

Projection, filter, limit, order e aggregazioni semplici possono essere
eseguiti dal database solo se:

- la semantica del dialect è equivalente;
- null, collation, timezone e cast sono definiti;
- il renderer è capability-gated;
- il piano lo consente;
- il risultato è contrattualmente uguale.

Il pushdown non è sempre più veloce: può impedire riuso, cambiare un indice o
aumentare CPU server. Le decisioni non ovvie devono essere misurate.

Metriche:

- predicate/projection pushed;
- righe lette dal server;
- righe emesse;
- byte di rete evitati;
- tempo server quando disponibile.

### V10 — Parallelismo solo se conveniente

Il parallelismo ha costi:

- connessioni aggiuntive;
- snapshot non identici;
- pressione sul pool;
- contention server;
- perdita dell'ordine;
- memoria e code;
- più WAL/redo;
- lock.

Default conservativo:

- una sessione/cursor per stream;
- pipeline decoder seriale o parallelismo bounded;
- writer singolo per transazione salvo protocollo bulk nativo.

Letture partizionate sono permesse solo con partizioni disgiunte, snapshot
coerente e ordering dichiarato. Scritture parallele solo se la semantica e il
database lo supportano.

### V11 — Connessione non acquisita per batch

La sessione viene acquisita per operazione o segmento coerente, non dentro il
loop batch.

Il pool riduce il costo di handshake, ma non deve:

- nascondere session state sporco;
- trattenere transazioni idle;
- restituire connessioni in stato incerto;
- crescere senza limite.

Metriche separate:

- pool wait;
- connect;
- authenticate;
- probe;
- query prepare;
- first row;
- transfer;
- finalize.

### V12 — Nessuna ottimizzazione a scapito dell'atomicità

Disabilitare transazioni, constraint, WAL o durability non è una
ottimizzazione trasparente.

Ogni benchmark dichiara:

- profilo transazionale;
- autocommit;
- durability server;
- indici/constraint;
- staging;
- commit count;
- isolamento.

Il confronto è valido solo a semantica equivalente.

---

## 3. Vincoli di memoria

### M1 — Budget principale in byte

`max_memory_bytes` governa almeno:

- batch Arrow;
- buffer del decoder;
- builder Arrow;
- bind/array buffer;
- WKB temporaneo;
- code;
- metadata di staging;
- retry buffer esplicitamente autorizzato;
- indici client-side;
- buffer di spill.

Memoria del client/driver nativo non osservabile con precisione viene stimata e
coperta da margine di sicurezza configurabile.

### M2 — Ownership contabile

Ogni batch usa `MemoryLease`. I buffer non-Arrow usano reservation dedicate.

```rust
struct DriverBufferLease {
    reserved_bytes: usize,
    observed_bytes: Option<usize>,
    owner: ExecutionId,
    phase: DatabasePhase,
}
```

Vietato:

- contare lo stesso buffer due volte;
- rilasciare quota quando il driver lo usa ancora;
- assegnare buffer condiviso arbitrariamente a un ramo;
- escludere bind buffer perché “interno al driver”.

### M3 — Governor non per riga

Accounting e metriche non devono aggiungere lock/atomiche per ogni cella.

Target:

- reservation per batch/chunk;
- thread-local counters aggregati;
- sampling per misure costose;
- overhead governor inferiore al 2% sui percorsi ad alto throughput, salvo
  giustificazione.

### M4 — LOB e geometrie grandi

LOB e WKB possono dominare il batch.

Regole:

- limiti per cella;
- streaming LOB se supportato;
- materializzazione esplicita se il tipo Arrow richiede il valore intero;
- nessun `Vec<u8>` duplicato senza motivo;
- geometria validata incrementalmente dove possibile;
- failure prima di allocare dimensioni dichiarate irragionevoli;
- metriche su massimo e distribuzione payload.

### M5 — Reservation anti-deadlock

Un task non attende nuova quota mantenendo indefinitamente memoria revocabile
necessaria al progresso di altri task.

Operatori stimabili acquisiscono reservation sufficiente prima del lavoro.
Operatori adattivi crescono a chunk e, prima di attendere nuova quota:

- pubblicano;
- spillano;
- riducono fetch/batch;
- rilasciano buffer;
- oppure falliscono controllatamente.

### M6 — Spill selettivo

Lo spill può servire per:

- buffer di retry/resume;
- sort/merge client-side esplicitamente previsto;
- staging locale di payload richiesto da un driver;
- recovery.

Non deve compensare un cursor non streaming o una coda illimitata.

Requisiti:

- quota byte e file;
- directory execution-scoped;
- checksum;
- I/O separato dal pool CPU;
- cancellazione;
- cleanup;
- cifratura/policy per dati sensibili;
- metriche lette/scritte.

---

## 4. Vincoli di rete e protocollo

### N1 — Round trip come metrica primaria

Su reti non locali il numero di round trip può dominare. Ogni benchmark
riporta:

- query round trips;
- fetch round trips;
- write round trips;
- commit/rollback round trips;
- metadata/probe round trips.

Il remote preflight combina query quando possibile senza indebolire diagnosi o
privilegi.

### N2 — Time to first batch

La latenza al primo batch è distinta dal throughput steady-state:

```text
pool wait
+ connect/auth
+ probe
+ prepare server
+ execute
+ primo fetch
+ primo decode
```

Va misurata con connessione cold e warm.

### N3 — Banda utile

Metriche:

- wire bytes quando disponibili;
- payload Arrow bytes;
- righe/s;
- MB/s;
- rapporto payload/protocollo;
- compressione;
- CPU per byte.

La compressione è per driver/capability e viene abilitata solo dietro misura su
profili di banda/CPU.

### N4 — Statement e bind limits

Il chunk sizing rispetta:

```text
max_bind_parameters
max_statement_bytes
max_packet_bytes
max_array_bind_rows
max_lob_chunk
```

Il planner calcola il limite prima di costruire statement/buffer.

### N5 — Timeout distinti

Timeout separati:

- pool acquire;
- connect;
- authentication;
- probe;
- statement;
- idle fetch;
- total execution;
- cancel;
- rollback/cleanup.

Un unico timeout globale rende impossibile diagnosticare e dimensionare.

---

## 5. Vincoli di scrittura

### W1 — Batch e commit sono concetti distinti

Un batch Arrow non implica un commit. Il profilo può:

- inviare molti batch in una transazione;
- eseguire array binding per batch;
- fare commit per chunk solo se richiesto.

Metriche separano `batches`, `driver_chunks` e `commits`.

### W2 — Bulk path semanticamente equivalente

Prepared e bulk path devono produrre gli stessi:

- valori;
- null;
- cast;
- default;
- errori di constraint;
- SRID;
- conteggi;
- stato transazionale.

Se il bulk loader salta trigger, constraint o generated semantics, è una
capability distinta e richiede opt-in.

### W3 — Indici e constraint

Per `create/replace`, creare indici prima o dopo il load ha impatto enorme. La
strategia è fisica, capability-gated e misurata.

Non si disabilitano constraint su una tabella esistente come ottimizzazione
implicita.

Metriche:

- load time;
- index build time;
- constraint validation time;
- lock wait;
- finalization time.

### W4 — Staging e swap

Il benchmark di `replace` misura separatamente:

- create staging;
- load;
- validate;
- index;
- swap/rename;
- cleanup;
- lock duration;
- spazio server temporaneo.

“Replace atomico” è dichiarato solo se il database garantisce il publish
richiesto.

### W5 — Upsert/update

Scenari:

- 100% insert;
- 100% update;
- mix 50/50;
- chiavi ordinate/casuali;
- righe strette/larghe;
- conflitto basso/alto;
- indice presente/assente.

Il benchmark riporta rows affected distinguendo insert/update quando il driver
lo consente.

### W6 — Idempotenza e recovery

L'overhead di idempotency key, recovery record e verifica outcome viene
misurato. Non può essere rimosso dai benchmark “ottimizzati” se fa parte del
profilo di produzione.

### W7 — Impatto server

Quando disponibile si raccolgono:

- WAL/redo/log bytes;
- temp bytes;
- CPU server;
- buffer/cache reads;
- lock wait;
- rows scanned;
- index pages;
- replication lag.

Queste metriche non sono confrontabili direttamente tra vendor, ma rilevano
regressioni nello stesso ambiente.

---

## 6. Vincoli spaziali

### G1 — WKB come confine, non come passaggio ripetuto

Per una cella:

```text
native DB spatial → WKB canonico → Arrow
Arrow WKB → native DB spatial
```

È vietata una catena non necessaria:

```text
native → WKT → String → parser → WKB → parser → native
```

WKT è fallback capability-gated e osservabile.

### G2 — SRID separato dal payload

WKB OGC può non contenere SRID; EWKB può contenerlo. Il driver mantiene:

- payload;
- SRID;
- CRS;
- geometry/geography;
- dimensioni.

Il batch non esegue query per-riga per recuperare SRID. Metadata e SQL vengono
preparati per ottenere tutto nel minor numero di round trip.

### G3 — Geometrie reali

Benchmark obbligatori:

- Point;
- LineString da 2, 50 e 10.000 coordinate;
- Polygon semplice e con molti anelli;
- MultiPolygon;
- GeometryCollection;
- geometrie miste;
- null e empty;
- payload Z/M;
- SRID omogeneo e mismatch;
- geometrie invalide secondo policy;
- celle vicine a `max_wkb_cell_bytes`.

Metriche:

- geometrie/s;
- coordinate/s;
- WKB MB/s;
- decode/encode count;
- payload medio/p95/max;
- allocazioni;
- perdita Z/M;
- righe rifiutate.

### G4 — Pushdown spatial

Predicate/bounding box spatial possono ridurre drasticamente rete, ma la
semantica varia.

Il benchmark confronta:

- nessun pushdown;
- bbox/index filter;
- predicato esatto server-side;
- combinazione bbox + exact;
- indice spatial presente/assente.

Il piano riporta candidate rows, matched rows e query plan quando disponibile.

### G5 — Nessuna riproiezione nei benchmark di I/O

La riproiezione non è una funzione implicita di database-tools. Se un piano
nativo esplicito usa funzioni server-side, il benchmark la etichetta
separatamente e non la confonde con il costo di lettura/scrittura.

### G6 — ArcGIS REST bounded

Per ArcGIS Online/Enterprise la risorsa critica è la richiesta HTTP:

- `maxRecordCount` e capability di pagination vengono rilevati;
- pagine, feature e payload byte hanno limiti distinti;
- un consumer lento arresta nuove query di pagina;
- edit batch usa un massimo in feature e byte;
- nessuna task per pagina/edit viene creata senza limite;
- rate limit rispetta `Retry-After` con jitter e budget totale;
- refresh token è single-flight;
- geometrie Esri JSON vengono convertite una volta al confine WKB;
- gli outcome sono misurati per feature, batch e layer.

Metriche aggiuntive:

```text
HTTP requests
HTTP payload bytes
pages
features/page
edits/request
rate-limit responses
retry-after time
token refresh count/time
ArcGIS error-code distribution
partial feature failures
```

---

## 7. Execution plan e osservabilità

### E1 — Configurazioni preparate

Nel percorso dati non devono comparire:

- `serde_json::Value`;
- lookup catalogo;
- parsing SQL;
- lookup colonna per nome;
- probe server;
- branching vendor generico.

### E2 — Modalità fisiche esplicite

Esempi:

- `CursorStreaming`;
- `PagedStreaming`;
- `PreparedBatchWrite`;
- `ArrayBindWrite`;
- `NativeBulkWrite`;
- `StagedReplace`;
- `KeyedUpdate`;
- `NativeUpsert`;
- `MergeFallback`;
- `ClientSideFallback`.

La modalità scelta appare nelle metriche e nel dry-run redatto.

### E3 — Nessuna perdita di osservabilità

Ottimizzare/fondere fasi non elimina metriche logiche:

- validate;
- pool/connect;
- probe;
- prepare;
- execute server;
- fetch/write;
- conversion;
- staging;
- finalize;
- commit/rollback;
- cleanup.

### E4 — Statistiche non cambiano la semantica

Row count, width e selectivity `Estimated` possono cambiare fetch, bulk path e
parallelismo. Non possono:

- cambiare righe risultanti;
- scegliere un mapping lossy;
- degradare atomicità;
- omettere un controllo;
- abilitare retry non sicuro.

---

## 8. Invarianti prestazionali

1. La memoria di un read streaming non cresce linearmente con le righe totali.
2. La memoria di un write streaming non cresce linearmente con le righe totali.
3. Nessun parsing JSON avviene nel loop dati.
4. Nessun lookup colonna per nome avviene quando è disponibile un indice.
5. Nessuna coda, prefetch o pool è illimitato.
6. Nessuna connessione viene acquisita per batch.
7. Nessun valore SQL viene interpolato per ottenere throughput.
8. Projection/reorder/pass-through non copiano buffer senza necessità.
9. Il governor non opera per riga.
10. Il consumer lento propaga backpressure fino al cursor.
11. Il database lento propaga backpressure fino al producer.
12. Il bulk path è semanticamente equivalente al path di riferimento.
13. Fetch e bind rispettano limiti in byte oltre che in righe.
14. Le geometrie non attraversano conversioni WKT non dichiarate.
15. Nessuna query per-riga recupera metadata/SRID.
16. Il parallelismo viene usato solo con beneficio misurato.
17. Le metriche distinguono costo client, rete e server.
18. I benchmark confrontano profili transazionali equivalenti.
19. Il picco RSS viene riportato per ogni benchmark principale.
20. Una regressione oltre budget blocca il rilascio.

---

## 9. Metriche obbligatorie

### 9.1 Generali

```text
rows/s
Arrow MB/s
wire MB/s
wall time
CPU time client
peak RSS client
bytes allocated
allocation count
bytes copied
peak governed memory
queue high-water mark
average/max batch rows
average/max batch bytes
cancel latency
cleanup latency
```

### 9.2 Connessione e rete

```text
pool acquire time
connect/auth time
probe round trips/time
prepare round trips/time
execute round trips/time
fetch/write round trips
time to first row
time to first batch
idle network time
reconnect count
```

### 9.3 Lettura

```text
rows fetched
rows emitted
server fetches
fetch rows/bytes
decode time
Arrow build time
predicate/projection pushdown
LOB bytes
schema validation time
```

### 9.4 Scrittura

```text
rows received
rows bound
rows committed
rows rolled back
rows with unknown outcome
driver chunks
bind/encode time
server write time
commit count/time
rollback time
staging/index/finalization time
rejected rows
WAL/redo/temp bytes when available
```

### 9.5 Spatial

```text
geometries/s
coordinates/s
WKB MB/s
average/p95/max WKB bytes
WKB encode/decode count
WKT fallback count
SRID queries
candidate/matched rows
spatial server time
```

Metric labels non contengono endpoint completi, SQL, nomi sensibili o valori.

---

## 10. Benchmark gate

### 10.1 Livelli di confronto

Ogni driver ha tre baseline:

1. `driver-raw`: uso minimale del client Rust senza engine;
2. `plenora-current`: engine corrente;
3. `plenora-previous`: ultima release.

Durante la migrazione si aggiunge:

4. backend Plenora Python con semantica equivalente.

Il costo dell'engine è:

```text
plenora-current - driver-raw
```

misurato su throughput, latenza, memoria e round trip.

### 10.2 Ambienti

Profili obbligatori:

- `local`: client e server sulla stessa macchina/rete Docker;
- `lan`: latenza bassa controllata;
- `wan-sim`: latenza e banda controllate;
- `server-constrained`: CPU/I/O server limitati;
- `client-constrained`: memoria/CPU client limitate.

Ogni risultato registra:

- CPU/RAM/storage;
- OS/kernel;
- Rust e dipendenze;
- database/versione/edition;
- estensioni spatial;
- configurazione rilevante;
- client library;
- container/image digest;
- latenza/banda;
- schema, indici e statistiche;
- warm/cold cache;
- profilo transazionale.

### 10.3 Dataset tabellari

Scale:

- 0 righe;
- 1 riga;
- 1k;
- 100k;
- 1M;
- 10M;
- 100M dove infrastruttura e licenza lo permettono.

Forme:

- narrow numerico;
- wide, 100+ colonne;
- stringhe corte/lunghe e Unicode;
- null 0%, 5%, 50%, 100%;
- decimal boundary;
- date/time/timezone;
- UUID/JSON;
- binary/LOB;
- valori altamente comprimibili e incomprimibili.

### 10.4 Letture

Benchmark:

- connection cold/warm;
- introspezione;
- full scan;
- projection;
- predicate selettività 0%, 1%, 50%, 100%;
- limit/preview;
- ordered read;
- aggregate/scalar;
- LOB;
- result set vuoto;
- cancellazione prima e dopo il primo batch;
- consumer lento;
- schema drift;
- resume quando supportato.

### 10.5 Scritture

Per `create`, `append`, `replace`, `truncate_insert`, `update`, `upsert`:

- tabella vuota e popolata;
- indici assenti/presenti;
- constraint/trigger;
- prepared vs array bind vs bulk;
- batch da 1, 100, 1k, 10k righe e target byte;
- singola transazione;
- staging/swap;
- commit per chunk esplicito;
- rollback a 1%, 50%, 99%;
- disconnessione prima/durante/dopo commit;
- conflict rate upsert;
- producer lento;
- server lento;
- quota memoria stretta.

### 10.6 Spatial

Per ogni driver spatial:

- introspezione colonna/SRID;
- read/write Point, LineString, Polygon, Multi*, collection;
- null/empty;
- payload semplici e complessi;
- Z/M;
- SRID match/mismatch;
- geometry/geography;
- bbox/exact pushdown;
- indice spatial;
- bulk spatial se disponibile;
- round-trip semantico e `LossReport`.

### 10.7 ArcGIS

- connection/auth cold e token warm;
- service/folder/item discovery;
- layer introspection;
- read offset pagination;
- read ObjectID-window pagination;
- Point/Polyline/Polygon e geometrie grandi;
- add/update/delete/applyEdits;
- upsert con lookup chiave;
- multi-layer;
- replace service/layer;
- `rollbackOnFailure` supportato/non supportato;
- risposta parziale per-feature;
- timeout prima e dopo l'invio edit;
- HTTP 429/`Retry-After`;
- token expiry durante read/write;
- server con `maxRecordCount` basso;
- consumer/producer lento.

### 10.8 Memoria e concorrenza

- read crescente con consumer veloce/lento;
- write crescente con server veloce/lento;
- pool saturo;
- acquire timeout;
- due/quattro/otto esecuzioni concorrenti;
- batch molto larghi;
- LOB/WKB al limite;
- cancellazione con code piene;
- buffer driver trattenuti;
- spill;
- cleanup dopo panic/crash simulato;
- connessione incerta esclusa dal pool.

### 10.9 Fault injection

Fault point deterministici:

- DNS/connect;
- auth;
- capability probe;
- prepare statement;
- primo fetch;
- fetch intermedio;
- bind;
- chunk write;
- create staging;
- finalize;
- commit request;
- commit response;
- rollback;
- cleanup.

Ogni test verifica stato, outcome, righe, oggetti staging e assenza di segreti.

---

## 11. Budget di regressione

Le soglie definitive vengono congelate dopo la baseline per driver. Valori
iniziali:

- throughput read/write: nessuna regressione >5% sui percorsi principali;
- operazioni complesse/spatial: >10% richiede motivazione e approvazione;
- time to first batch warm: nessuna regressione >10%;
- picco RSS streaming: nessun aumento >5%;
- round trip: nessun aumento non giustificato;
- allocazioni/copie: nessun aumento sui percorsi dichiarati zero-copy;
- WKT fallback: deve restare zero dove esiste WKB;
- connessioni: nessuna connessione aggiuntiva per batch/partizione non
  dichiarata;
- memoria: pendenza rispetto alle righe totali compatibile con O(1) per
  streaming;
- outcome/atomicità: nessuna ottimizzazione può modificare la classificazione.

Rumore:

- warmup;
- almeno 5 ripetizioni per benchmark breve;
- mediana e p95;
- intervallo/confidenza o dispersione;
- soglia applicata solo se superiore al rumore misurato;
- runner dedicati per gate principali.

Una regressione può essere accettata solo con:

- causa;
- beneficio;
- database/versioni interessati;
- confronto completo;
- decisione registrata;
- nuova baseline approvata.

---

## 12. Matrice driver

Il gate non richiede che ogni driver abbia ogni capability. Richiede che ciò
che dichiara sia misurato.

| Area | PostgreSQL | MySQL | SQL Server | Oracle | Db2 | SQLite | DuckDB | ArcGIS |
|---|---|---|---|---|---|---|---|---|
| read streaming | obbligatorio | obbligatorio | obbligatorio | obbligatorio | obbligatorio | obbligatorio | obbligatorio | paginato |
| prepared write | obbligatorio | obbligatorio | obbligatorio | obbligatorio | obbligatorio | obbligatorio | obbligatorio | edit batch |
| bulk path | COPY | capability | bulk/TVP | array/direct path | capability | transaction batches | appender | applyEdits/append |
| spatial WKB | PostGIS | spatial | geometry/geography | SDO | extender | SpatiaLite | spatial | Esri JSON ↔ WKB |
| staging replace | obbligatorio | obbligatorio | obbligatorio | obbligatorio | obbligatorio | file/transaction profile | transaction profile | service/layer strategy |
| outcome fault test | obbligatorio | obbligatorio | obbligatorio | obbligatorio | obbligatorio | embedded crash test | embedded crash test | per feature/batch/layer |

I nomi nella riga bulk sono direzioni di indagine, non capability promesse:
solo implementazione e testkit possono promuoverle a supportate.

---

## 13. Roadmap prestazionale

### Fase 0 — Baseline backend

- misurare funzioni Python correnti;
- catturare dataset e semantica;
- misurare round trip e picco RSS;
- definire ambienti riproducibili.

### Fase 1 — Harness e driver raw

- harness comune;
- dataset deterministici;
- metriche client/server;
- baseline raw per ogni client Rust;
- fault proxy e network shaping.

### Fase 2 — PostgreSQL/PostGIS

- read/write streaming;
- COPY text/COPY binario/prepared comparison;
- spatial WKB;
- memory slope;
- staging/upsert;
- gate iniziale.

Baseline v3 corrente su 1.000 righe della fixture completa:

| percorso | tempo |
|---|---:|
| COPY text | 37.454 µs |
| COPY binario | 39.742 µs |
| prepared | 315.877 µs |

Le tre tabelle risultanti sono differenzialmente equivalenti. Questa misura
non dimostra che COPY binario sia sempre più veloce: a 1.000 righe il costo di
setup e codifica domina. Le campagne successive devono misurare almeno
1.000/100.000/1.000.000 righe, throughput, RSS, WAL e varianza su più run.

Il gate v3 aggiunge anche prove funzionali, separate dal microbenchmark, per
COPY binario di array/UUID/interval/range/composite, cancellazione server-side
di read e write e schema evolution additiva. Queste prove misurano correttezza
e boundedness, non vengono sommate ai tempi COPY della tabella precedente.

La campagna v1 è ora implementata da
`scripts/check_postgres_performance.py`. Usa manifest versionati, separa
lettura e scrittura da batch materializzati, raccoglie mediana/p95, time to
first batch, byte Arrow, picco RSS, WAL e metriche del pool. I profili
`narrow`, `wide` e `spatial` verificano dopo ogni strategia di scrittura
l'equivalenza bidirezionale con `EXCEPT ALL`.

Lo smoke da tre campioni serve a validare l'harness. Una baseline congelabile
richiede almeno cinque campioni nello stesso ambiente ed è confrontata con i
budget di `benchmarks/baseline/postgres-performance-budget.json`. Ambienti con
major PostgreSQL, linea PostGIS, piattaforma o numero di CPU differenti sono
marcati `not_comparable`.

I risultati hanno congelato due profili pubblici:

- `PostgresPerformanceProfile::LowLatency`: 1.024 righe/batch e COPY text;
- `PostgresPerformanceProfile::BalancedBulk`: 8.192 righe/batch e COPY binary.

Il default resta low-latency per compatibilità. La selezione bulk è esplicita
e non modifica profilo transazionale, correttezza, limiti in byte o semantica
spatial.

Entrambi i profili applicano anche un target soft adattivo: 1 MiB per
low-latency e 4 MiB per bulk. Il reader stima i byte durante la decodifica,
autocalibra la stima sui byte Arrow effettivi e prealloca i builder in base al
target. Il limite `max_batch_bytes` rimane hard e viene verificato sul batch
finito.

Il percorso cold/warm PostgreSQL è stato inoltre misurato e ottimizzato:

- configurazione di timeout e `application_name` nel startup packet;
- zero query di reset/configurazione sulla connessione nuova;
- un solo `DISCARD ALL` a ogni riuso, senza ridurre l'isolamento;
- protocollo one-shot tipizzato per le letture senza bind;
- metriche distinte per reset, introspezioni e fast path.

Su sei scenari read-only a 100.000 righe e 20 campioni, la mediana di
acquisizione/preparazione è diminuita dall'1% al 14,85%; i tempi totali restano
entro il budget di regressione. La cache dei metadati non viene introdotta
come TTL cieca.

Il driver PostgreSQL usa ora un `PostgresSchemaToken` validato sul server e una
cache LRU bounded. OID e firma strutturale rilevano DDL esterno; commit e
outcome incerto delle write invalidano esplicitamente il target. Nel confronto
A/B a 100.000 righe la cache riduce l'acquire del 34–44%, mantenendo il tempo
totale fra +0,22% e -1,27%.

Anche i filtri parametrizzati built-in e spatial usano ora il protocollo
one-shot con tipi espliciti. Il planner è conservativo: enum, domini, composite
e combinazioni ambigue mantengono il prepare server-side. Su 20 campioni per
profilo l'acquire migliora del 4,29–8,59%; il totale su 100.000 righe resta fra
-0,44% e +1,36%, quindi neutro rispetto al costo dominante di materializzazione.
Fast path e fallback hanno contatori separati e un kill switch pubblico.

Il medesimo protocollo è applicato al `QueryOperation` completo senza
ricostruire localmente i tipi risultanti di CTE, join e aggregate: lo schema
arriva dalla prima riga PostgreSQL, mentre un result set vuoto richiede solo il
describe. Su query parametrizzate narrow da 1.000 righe il totale mediano
migliora del 6,26% e il p95 del 16,69%.

Gli operatori PostGIS bounding-box e KNN hanno inoltre un gate specifico che
non misura soltanto la latenza: `EXPLAIN` deve provare l'uso dell'indice GiST.
Sul riferimento PostgreSQL 16/PostGIS 3.4, 50 campioni su 100 righe hanno
registrato 189 µs di mediana e 263 µs di p95. Il gate fallisce oltre 50 ms di
mediana, 100 ms di p95 o in assenza dell'indice atteso.

### Fase 3 — Driver principali

- MySQL/MariaDB;
- SQL Server;
- SQLite/SpatiaLite;
- confronto cross-dialect senza confronti vendor fuorvianti;
- ottimizzazioni per capability.

### Fase 4 — Oracle e Db2

- runner dedicati/licenziati;
- array binding/LOB;
- spatial;
- recovery;
- baseline stabile.

### Fase 5 — ArcGIS

- baseline REST raw;
- read paginato;
- edit batch e applyEdits;
- conversione Esri JSON/WKB;
- rate limit e token refresh;
- lifecycle e multi-layer;
- fault injection per outcome parziale.

### Fase 6 — Ottimizzazioni avanzate

- batch sizing adattivo;
- letture partizionate con snapshot provato;
- compressione protocollo;
- decoder columnar/zero-copy quando possibile;
- ulteriori bulk path;
- prepared statement cache bounded;
- auto-tuning guidato da metriche, mai semantico.

---

## 14. Criterio di successo

La libreria soddisfa gli obiettivi solo se:

- legge e scrive dataset maggiori della RAM con memoria bounded;
- mantiene backpressure reale;
- aggiunge overhead contenuto rispetto al client Rust grezzo;
- riduce drasticamente il costo rispetto al backend Python dove oggi esiste
  conversione per-riga;
- usa round trip e connessioni in modo controllato;
- preserva tipi, CRS, transazioni e outcome;
- mantiene prepared e bulk path semanticamente equivalenti;
- rende misurabile ogni fallback;
- supera fault injection e benchmark gate per ogni capability pubblicizzata;
- produce risultati riproducibili.

Principio finale:

> `plenora-database-tools` non è veloce perché invia più lavoro possibile al
> database: è veloce quando muove il minimo indispensabile di dati, memoria e
> round trip mantenendo intatte le garanzie dichiarate.
