# Benchmark Fase 0

## Indice

Questa cartella contiene tre famiglie di misure, con regole diverse.

1. **Campagne su provider reali** (il resto di questo documento): richiedono
   PostgreSQL, MySQL o SQL Server, sono guidate da manifest in `manifests/`,
   producono report in `results/` e hanno budget in `baseline/`. PostgreSQL e
   SQL Server hanno anche baseline congelate; MySQL confronta una baseline
   soltanto quando viene fornita e l'ambiente coincide. Queste sono gate: se
   superano il budget applicabile, falliscono.
2. **Microbenchmark Rust offline**: girano senza database, misurano le
   superfici CPU-bound del workspace (rendering SQL, compilazione dei read
   plan, ispezione EWKB, contratto Arrow, pipeline dei piani e primitive
   applicative Core v3). Documento e
   numeri misurati in [`offline-rust-microbench.md`](offline-rust-microbench.md),
   raw in `raw/offline-rust-microbench.jsonl`. **Non sono un gate**: i budget
   prestazionali di quelle superfici non sono stati fissati e il workflow
   `.github/workflows/rust-microbench.yml` si limita a misurare e pubblicare.
3. **Python SDK vs subprocess CLI**: parity bench live sul driver Postgres,
   in `crates/plenora-database-py/python/tests/bench_*.py`. Misura latenza
   per-chiamata dal Python (in-process PyO3 vs subprocess CLI). Procedura e
   criteri di confronto in
   [`crates/plenora-database-py/README.md`](../crates/plenora-database-py/README.md#performance)
   I numeri appartengono al report della singola corsa: non vengono copiati in
   un documento destinato a durare. Opt-in, non gate.

## Harness

PostgreSQL/PostGIS:

```powershell
$env:PLENORA_PHASE0_PG_DSN = "<dsn fixture>"
python scripts\phase0_harness.py postgres `
  --warmup 2 `
  --repeat 10 `
  --output benchmarks\raw\phase0-postgres-smoke.jsonl
```

Le variabili d'ambiente non vengono copiate nei risultati. Gli errori
registrati contengono solo categoria e messaggio generico.

## Formato

Ogni file è JSONL:

1. envelope ambiente;
2. un record per caso;
3. digest dei risultati normalizzati;
4. tempi monotonic in nanosecondi;
5. RSS osservato prima/dopo.

`--repeat` e `--warmup` si applicano a tutti i casi.

## Aggregazione

```powershell
python scripts\phase0_report.py `
  benchmarks\raw\phase0-postgres-smoke.jsonl `
  --json benchmarks\baseline\phase0-smoke-report.json `
  --markdown benchmarks\baseline\phase0-smoke-report.md
```

Il report calcola mediana, p95 nearest-rank, min/max, delta RSS e stabilità del
digest semantico. I raw iniziali hanno un solo campione e restano smoke di
convalida; non vanno presentati come baseline statistica. La baseline
definitiva richiede piu campioni di questi.

## Raw iniziali

- `raw/phase0-postgres-smoke.jsonl`.

Nessun comando viene eseguito automaticamente contro un target. Endpoint e
segreti sono richiesti solo quando viene esplicitamente aperto il gate
database.

## Campagna PostgreSQL/PostGIS

Il benchmark del driver Rust di riferimento è separato dall'harness Fase 0:

```powershell
python scripts\check_postgres_performance.py
```

Lo smoke usa
`manifests/postgres-performance-smoke.json`; la campagna da congelare usa
`manifests/postgres-performance-reference.json`. I report locali sono scritti
in `results/`, mentre una baseline numerica richiede `--freeze` e almeno cinque
campioni. Le soglie anti-regressione sono in
`baseline/postgres-performance-budget.json`.

I manifest `postgres-performance-scale.json` e
`postgres-performance-batch-tuning.json` isolano rispettivamente il gradino da
un milione di righe e il confronto 1.024/8.192/32.768 righe per batch.
`postgres-performance-adaptive-bytes.json` confronta target da 1 MiB e 4 MiB
su dati wide e spatial.

La baseline
`baseline/postgres16-postgis34-session-fast-path.json` congela 20 campioni
read-only dopo l'ottimizzazione startup/reset e del protocollo one-shot. I raw
includono i contatori `session_resets`, `catalog_introspections` e
`read_typed_fast_paths`.

Il manifest `postgres-performance-schema-cache.json` confronta nello stesso
run capacità zero e capacità 256. La baseline
`baseline/postgres16-postgis34-schema-cache.json` congela il costo di
validazione strict e i contatori hit/miss/token/eviction.

Il manifest `postgres-performance-parameterized-fast-path.json` confronta
prepare e one-shot tipizzato sugli stessi filtri parametrizzati narrow, wide e
spatial. La baseline
`baseline/postgres16-postgis34-parameterized-fast-path.json` contiene 20
campioni per variante e i contatori
`read_parameterized_typed_fast_paths`/`read_prepared_fallbacks`.

Il manifest `postgres-performance-query-fast-path.json` confronta
`QueryOperation` prepared e one-shot su 50 campioni da 1.000 righe. La
baseline `baseline/postgres16-postgis34-query-fast-path.json` congela acquire,
totale, p95 e i contatori `query_typed_fast_paths`/
`query_prepared_fallbacks`.

La soglia e la forma del confronto vivono in `scripts/check_postgres_performance.py`,
che e anche cio che le applica.

## SQL Server

La campagna SQL Server usa il manifest
`manifests/sqlserver-performance-reference.json`, il budget assoluto
`baseline/sqlserver-performance-budget.json` e la baseline congelata
`baseline/sqlserver2022-performance-reference.json`.

```powershell
python scripts\check_sqlserver_performance.py `
  --baseline benchmarks/baseline/sqlserver2022-performance-reference.json `
  --output assurance-results/sqlserver-performance.json
```

Read, prepared, TDS bulk, create e replace vengono misurati sul provider reale;
ogni scrittura deve confermare tutte le righe e il differenziale SQL deve
restare a zero. La soglia e in `scripts/check_sqlserver_performance.py`.

## MySQL

La campagna MySQL copre le superfici live gia qualificate: lettura Arrow e
scrittura `Append` in `SingleTransaction`. Manifest e budget sono
rispettivamente `manifests/mysql-performance-reference.json` e
`baseline/mysql-performance-budget.json`.

```powershell
python scripts\check_mysql_performance.py
```

Senza `--baseline` il gate applica i limiti assoluti e dichiara il confronto
storico `not_requested`; non inventa una baseline da una singola corsa.
