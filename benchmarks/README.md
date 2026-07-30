# Benchmark Fase 0

## Harness

```powershell
python scripts\phase0_harness.py inventory `
  --output benchmarks\raw\phase0-inventory.jsonl
```

PostgreSQL/PostGIS:

```powershell
$env:PLENORA_PHASE0_PG_DSN = "<dsn fixture>"
python scripts\phase0_harness.py postgres `
  --warmup 2 `
  --repeat 10 `
  --output benchmarks\raw\phase0-postgres-smoke.jsonl
```

ArcGIS:

```powershell
$env:PLENORA_PHASE0_ARCGIS_URL = "http://127.0.0.1:58080"
$env:PLENORA_PHASE0_ARCGIS_TOKEN = "<token fixture>"
python scripts\phase0_harness.py arcgis `
  --warmup 2 `
  --repeat 10 `
  --output benchmarks\raw\phase0-arcgis-smoke.jsonl
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

L’inventario statico viene sempre misurato una sola volta; `--repeat` e
`--warmup` si applicano ai casi di provider.

## Aggregazione

```powershell
python scripts\phase0_report.py `
  benchmarks\raw\phase0-inventory.jsonl `
  benchmarks\raw\phase0-postgres-smoke.jsonl `
  benchmarks\raw\phase0-arcgis-smoke.jsonl `
  --json benchmarks\baseline\phase0-smoke-report.json `
  --markdown benchmarks\baseline\phase0-smoke-report.md
```

Il report calcola mediana, p95 nearest-rank, min/max, delta RSS e stabilità del
digest semantico. I raw iniziali hanno un solo campione e restano smoke di
convalida; non vanno presentati come baseline statistica. La baseline
definitiva segue `docs/phase-0/baseline-plan.md`.

## Raw iniziali

- `raw/phase0-inventory.jsonl`;
- `raw/phase0-postgres-smoke.jsonl`;
- `raw/phase0-arcgis-smoke.jsonl`.

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

La specifica completa è in `docs/postgres/PERFORMANCE.md`.

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
restare a zero. La specifica è in `docs/sqlserver/PERFORMANCE.md`.
