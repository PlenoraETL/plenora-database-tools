# Campagna prestazionale PostgreSQL/PostGIS

La campagna prestazionale è separata dai gate di correttezza. Il suo scopo è
misurare il data path reale della libreria, rendere visibili i colli di
bottiglia e impedire regressioni dopo il congelamento di una baseline.

## Cosa misura

Il runner usa tre dataset deterministici:

- `narrow`: bigint, double, boolean e testo nullable;
- `wide`: testo largo, Decimal128, timestamp UTC, JSONB e bytea;
- `spatial`: geometry PointZ, geography Point, SRID 4326 e JSONB.

Per ogni scenario registra:

- tempo di acquisizione/preparazione della lettura;
- time to first batch;
- tempo totale, numero di batch, byte Arrow materializzati e righe/secondo;
- preparazione e scrittura isolate per COPY text, COPY binary e prepared;
- byte WAL prodotti;
- picco RSS del processo Rust;
- metriche bounded del provider.

La scrittura riceve batch Arrow già materializzati. Il suo tempo non include
quindi la lettura lazy della sorgente. Dopo ogni write, `EXCEPT ALL` in
entrambe le direzioni deve restituire zero differenze.

## Profili di esecuzione

Lo smoke rapido usa 1.000 righe, un warm-up e tre campioni:

```powershell
python scripts\check_postgres_performance.py `
  --manifest benchmarks\manifests\postgres-performance-smoke.json `
  --output benchmarks\results\postgres16-postgis34-performance-smoke.json
```

La baseline statistica usa 1.000 e 100.000 righe, un warm-up e cinque
campioni:

```powershell
python scripts\check_postgres_performance.py `
  --manifest benchmarks\manifests\postgres-performance-reference.json `
  --freeze benchmarks\baseline\postgres16-postgis34-performance-reference.json
```

Il gradino da 1.000.000 di righe e il confronto fra batch da
1.024/8.192/32.768 righe sono separati perché non appartengono al gate rapido:

```powershell
python scripts\check_postgres_performance.py `
  --manifest benchmarks\manifests\postgres-performance-scale.json

python scripts\check_postgres_performance.py `
  --manifest benchmarks\manifests\postgres-performance-batch-tuning.json
```

Il DSN può essere passato in `PLENORA_TEST_POSTGRES_DSN`. Non viene scritto nei
risultati. In assenza della variabile, il runner usa il container locale
`dataflow-postgres`.

## Confronto anti-regressione

```powershell
python scripts\check_postgres_performance.py `
  --manifest benchmarks\manifests\postgres-performance-reference.json `
  --baseline benchmarks\baseline\postgres16-postgis34-performance-reference.json
```

Le soglie sono in
`benchmarks/baseline/postgres-performance-budget.json`. Un confronto è valido
solo se coincidono major PostgreSQL, major/minor PostGIS, piattaforma e numero
di CPU. Un ambiente diverso produce `not_comparable`, non un falso fallimento.

La baseline può essere congelata soltanto se il manifest fornisce almeno il
numero minimo di campioni richiesto dal budget. I risultati smoke non sono
baseline numeriche.

## Prima baseline congelata

La baseline locale PostgreSQL 16/PostGIS 3.4 del 27 luglio 2026 ha prodotto
queste mediane a 100.000 righe:

| profilo | read | COPY text | COPY binary | prepared | picco RSS |
|---|---:|---:|---:|---:|---:|
| narrow | 20,1 ms | 61,7 ms | 44,1 ms | 7.974 ms | 10,3 MB |
| wide | 84,7 ms | 222,8 ms | 168,0 ms | 8.632 ms | 33,9 MB |
| spatial | 69,7 ms | 129,0 ms | 119,0 ms | 8.263 ms | 21,4 MB |

COPY binary è il percorso più rapido sui tre profili a questa scala. Prepared
è circa 51–181 volte più lento e produce più WAL; resta un fallback
funzionale, non il percorso bulk. A 1.000 righe COPY text e binary sono invece
molto vicini perché domina il costo di setup.

Questi dati hanno indicato l'ordine dell'analisi successiva:

1. misurare il batch tuning e il gradino da un milione di righe;
2. valutare COPY binary come default bulk senza ridurre la copertura tipi;
3. profilare allocazioni/copie dei codec wide e spatial;
4. misurare separatamente reset sessione, preparazione e cold/warm pool.

I numeri completi, inclusi p95, WAL e campioni raw, sono in
`benchmarks/baseline/postgres16-postgis34-performance-reference.json`.

La campagna da un milione di righe conferma che il percorso resta scalabile:

| profilo | read righe/s | COPY text righe/s | COPY binary righe/s | Arrow materializzato | RSS |
|---|---:|---:|---:|---:|---:|
| narrow | 5,06 M | 1,92 M | 2,88 M | 34,9 MB | 41,4 MB |
| wide | 1,16 M | 543 k | 816 k | 256,5 MB | 241,5 MB |
| spatial | 2,95 M | 886 k | 1,14 M | 145,9 MB | 145,8 MB |

Il report congelato è
`benchmarks/baseline/postgres16-postgis34-performance-scale.json`.

Il batch tuning mostra un compromesso netto:

- 1.024 minimizza il time to first batch, ma penalizza molto le scritture;
- 32.768 massimizza spesso COPY binary, ma porta il primo batch wide a circa
  26 ms e aumenta il picco RSS wide/spatial di circa il 45–52%;
- 8.192 è il punto di equilibrio locale per bulk e streaming general-purpose.

Il risultato suggerisce di non sostituire globalmente il default interattivo
con 32.768. La prossima ottimizzazione dovrebbe distinguere un profilo
low-latency da uno bulk, oppure adottare un sizing adattivo in byte. I dati
completi sono in
`benchmarks/baseline/postgres16-postgis34-performance-batch-tuning.json`.

I due costruttori pubblici hanno un manifest live mirato:

```powershell
python scripts\check_postgres_performance.py `
  --manifest benchmarks\manifests\postgres-performance-profiles.json `
  --baseline benchmarks\baseline\postgres16-postgis34-performance-profiles.json
```

Il gate anti-regressione del solo reader usa 20 campioni e due warm-up, senza
generare WAL:

```powershell
python scripts\check_postgres_performance.py `
  --manifest benchmarks\manifests\postgres-performance-adaptive-read-regression.json `
  --baseline benchmarks\baseline\postgres16-postgis34-performance-profiles.json
```

Con nearest-rank, il p95 non coincide così con un singolo massimo su appena
cinque misure e il risultato non viene perturbato da checkpoint delle
scritture.

Il gate post-implementazione è passato su tutti i sei scenari. La variazione
della mediana rispetto alla baseline pre-adattività è compresa fra `-1,86%` e
`+0,61%`; non risultano regressioni. La nuova baseline read-only a 20 campioni
è in
`benchmarks/baseline/postgres16-postgis34-adaptive-read-regression.json`.

Questa distinzione è implementata nell'API con
`PostgresPerformanceProfile::LowLatency` e
`PostgresPerformanceProfile::BalancedBulk`. Il default resta equivalente a
`LowLatency`; il profilo bulk deve essere richiesto esplicitamente e combina
batch 8.192 con COPY binary.

## Sizing adattivo in byte

Il reader conta il costo Arrow mentre decodifica la riga, senza una seconda
conversione. Quando la stima raggiunge il target chiude il batch e calibra il
rapporto stima/byte reali per il batch successivo. Anche la capacità iniziale
dei builder deriva dal target, evitando di preallocare il tetto massimo di
righe.

Il target è soft; `max_batch_bytes` resta il limite hard verificato sul
`RecordBatch` finito. La metrica `read_target_limited_batches` rende
osservabile quante chiusure sono dovute ai byte anziché al numero di righe.

La campagna dedicata è:

```powershell
python scripts\check_postgres_performance.py `
  --manifest benchmarks\manifests\postgres-performance-adaptive-bytes.json
```

Risultati mediani a 100.000 righe e tetto 32.768:

| profilo | target | TTFR | max batch | COPY binary | RSS |
|---|---:|---:|---:|---:|---:|
| wide | 1 MiB | 2,86 ms | 1,21 MiB | 230,9 ms | 68,6 MB |
| wide | 4 MiB | 12,04 ms | 4,38 MiB | 131,5 ms | 27,9 MB |
| spatial | 1 MiB | 4,33 ms | 0,96 MiB | 121,6 ms | 19,2 MB |
| spatial | 4 MiB | 17,36 ms | 3,75 MiB | 89,3 ms | 17,0 MB |

Con batch 32.768, 1 MiB frammenta troppo il flusso wide. Il profilo
`LowLatency` evita questo caso perché mantiene anche il tetto a 1.024 righe.
Per `BalancedBulk`, 4 MiB riduce sensibilmente TTFR e RSS rispetto al solo
tetto da 32.768, con una penalità di throughput contenuta.

La baseline completa è in
`benchmarks/baseline/postgres16-postgis34-performance-adaptive-bytes.json`.

## Reset sessione e fast path one-shot

L'analisi cold/warm ha eliminato due round-trip non necessari:

- timeout e `application_name` sono parametri di startup della connessione;
- il primo checkout non esegue più `DISCARD ALL` su una sessione appena creata;
- un checkout riusato esegue un solo `DISCARD ALL`, che ripristina i default di
  startup e conserva l'isolamento completo;
- le letture senza bind usano `query_typed_raw`, inviando
  parse/describe/bind/execute in un'unica sequenza.

La prova live contamina deliberatamente GUC, tabella temporanea e prepared
statement. Al checkout seguente tutto deve essere rimosso e i contatori devono
registrare esattamente un reset per ogni riuso.

Su 20 campioni read-only a 100.000 righe, rispetto alla baseline precedente:

| profilo | batch | acquire prima | acquire dopo | variazione |
|---|---:|---:|---:|---:|
| narrow | 1.024 | 1.421 µs | 1.210 µs | -14,85% |
| narrow | 8.192 | 1.341 µs | 1.272 µs | -5,18% |
| wide | 1.024 | 1.405 µs | 1.298 µs | -7,58% |
| wide | 8.192 | 1.403 µs | 1.389 µs | -1,00% |
| spatial | 1.024 | 1.427 µs | 1.293 µs | -9,39% |
| spatial | 8.192 | 1.446 µs | 1.294 µs | -10,51% |

Le mediane totali variano fra -5,67% e +3,45%, entro il budget; non aumenta il
numero di connessioni. In ogni scenario: 22 checkout, una connessione nuova,
21 riusi, 21 reset, 22 introspezioni e 22 fast path.

La baseline post-ottimizzazione è
`benchmarks/baseline/postgres16-postgis34-session-fast-path.json`.

## SchemaToken e cache metadati strict

La cache non usa TTL. La chiave contiene fingerprint della connessione, schema
e oggetto. Ogni entry conserva:

- OID di database, namespace e relation;
- firma canonica dello stato strutturale in `pg_catalog`;
- fingerprint SHA-256 pubblico;
- colonne già convertite nel modello PostgreSQL/Arrow.

Il primo accesso esegue una introspezione completa. Gli accessi successivi
confrontano una firma leggera; se OID o struttura cambiano, l'entry viene
invalidata e ricaricata. La firma copre nomi e ordine delle colonne, OID e
typmod dei tipi, nullability, default, identity/generated e struttura dei tipi
composite. Le write della libreria invalidano inoltre il target dopo commit o
outcome incerto.

La capacità predefinita è 256 oggetti ed è configurabile con
`with_schema_cache_capacity`; zero disabilita la cache. LRU, hit, miss,
controlli token, eviction e invalidazioni sono metriche bounded.

Il test A/B usa lo stesso binario, 20 campioni, due warm-up, 100.000 righe e
batch da 8.192:

| profilo | acquire cache off | acquire cache on | variazione | tempo totale |
|---|---:|---:|---:|---:|
| narrow | 1.786 µs | 1.178 µs | -34,05% | +0,22% |
| wide | 2.074 µs | 1.157 µs | -44,24% | -0,29% |
| spatial | 2.159 µs | 1.202 µs | -44,33% | -1,27% |

Ogni scenario cached registra un solo miss, 21 hit validati e una sola
introspezione completa. La baseline è
`benchmarks/baseline/postgres16-postgis34-schema-cache.json`; il manifest è
`benchmarks/manifests/postgres-performance-schema-cache.json`.

## Fast path one-shot parametrizzato

Per i filtri `ReadOperation`, il driver ricostruisce l'ordine esatto dei bind
prodotto dal renderer e associa ogni valore al tipo PostgreSQL del campo.
Quando l'associazione è certa usa `query_typed_raw`; per enum, domini,
composite, valori incompatibili o bind non ricostruibili conserva il percorso
`prepare` con inferenza server-side. Il comportamento può essere disabilitato
con `with_parameterized_read_fast_path(false)`.

Sono coperti bool, interi, float, testo, bytea, date e timestamp, JSON/JSONB,
numeric, UUID, NULL tipizzati e parametri spatial EWKB/distanza. I contatori
`read_parameterized_typed_fast_paths` e `read_prepared_fallbacks` rendono
osservabile la decisione senza label dinamiche.

La campagna A/B usa 20 campioni, due warm-up, 100.000 righe e batch da 8.192:

| profilo | acquire prepared | acquire one-shot | variazione | tempo totale |
|---|---:|---:|---:|---:|
| narrow | 1.199,5 µs | 1.121,0 µs | -6,54% | +1,28% |
| wide | 1.201,5 µs | 1.150,0 µs | -4,29% | +1,36% |
| spatial | 1.304,5 µs | 1.192,5 µs | -8,59% | -0,44% |

Ogni scenario esegue 22 letture: con il fast path attivo registra 22 one-shot
parametrizzati e zero fallback; disattivandolo registra 22 fallback prepared.
Il costo end-to-end su 100.000 righe resta sostanzialmente neutro perché è
dominato dalla materializzazione Arrow. Il vantaggio riguarda il round-trip
di avvio ed è quindi più rilevante per query piccole e frequenti.

Baseline e manifest:
`benchmarks/baseline/postgres16-postgis34-parameterized-fast-path.json` e
`benchmarks/manifests/postgres-performance-parameterized-fast-path.json`.

## QueryOperation one-shot

`tokio-postgres` non espone le colonne conservate internamente nel
`RowStream` one-shot. Il driver evita sia una cache schema non strict sia
un'inferenza locale incompleta dell'AST:

- invia direttamente la query con i tipi canonici dei parametri;
- usa le colonne della prima `Row` per costruire lo schema Arrow esatto;
- rimette la prima riga davanti allo stream, senza perderla o duplicarla;
- se il risultato è vuoto esegue il solo describe, senza ripetere la SELECT;
- se la tipizzazione è rifiutata, usa il prepare server-side.

Questo vale senza percorsi speciali per CTE, join, aggregate, having e
funzioni PostGIS. I parametri geometrici diretti nell'AST PostgreSQL vengono
resi come `ST_GeomFromEWKB($n)`.

Su 50 campioni parametrizzati narrow da 1.000 righe:

| modalità | acquire mediano | totale mediano | totale p95 |
|---|---:|---:|---:|
| prepared | 289,0 µs | 463,5 µs | 605,0 µs |
| one-shot | 262,0 µs | 434,5 µs | 504,0 µs |
| variazione | -9,34% | -6,26% | -16,69% |

La baseline è
`benchmarks/baseline/postgres16-postgis34-query-fast-path.json`; il manifest è
`benchmarks/manifests/postgres-performance-query-fast-path.json`.

## Operatori spatial indicizzati

Un gate separato esegue una query tipizzata bounding-box più KNN su 100 righe,
con cinque warm-up e 50 campioni. Prima delle misure usa `EXPLAIN` e richiede
esplicitamente il nome dell'indice GiST nel piano: un risultato veloce ottenuto
con scan accidentale non è considerato prova sufficiente.

Sul riferimento PostgreSQL 16/PostGIS 3.4 del 27 luglio 2026:

| campioni | righe | mediana | p95 | indice |
|---:|---:|---:|---:|---|
| 50 | 100 | 189 µs | 263 µs | `events_geom_gix` |

Il gate è fail-closed: fallisce se il piano non usa l'indice, se la mediana
supera 50 ms o se il p95 supera 100 ms. Si esegue con:

```powershell
python scripts\check_postgres_spatial_performance.py
```

## Interpretazione

Si ottimizza un solo collo di bottiglia per volta. Ogni modifica deve:

1. conservare zero differenze semantiche;
2. superare i gate PostgreSQL di correttezza e hardening;
3. migliorare mediane e p95 nello stesso ambiente;
4. non spostare il costo oltre i budget RSS o WAL.

I tempi assoluti di macchine diverse non vanno confrontati. Per analizzare
latenza WAN, TLS/mTLS, concorrenza o server gestiti si usano manifest e target
separati, senza sostituire la baseline locale.
