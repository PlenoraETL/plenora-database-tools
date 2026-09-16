# Benchmark

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
   applicative del Core). Documento e
   numeri misurati in [`offline-rust-microbench.md`](offline-rust-microbench.md),
   raw in `raw/offline-rust-microbench.jsonl`. **Non sono un gate**: i budget
   prestazionali di quelle superfici non sono stati fissati e il workflow
   `.github/workflows/rust-microbench.yml` si limita a misurare e pubblicare.
3. **Python SDK vs subprocess CLI**: parity bench live sul driver Postgres,
   in [`test_benchmark_parity.py`](../crates/plenora-database-py/python/tests/test_benchmark_parity.py). Misura latenza
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
  --output benchmarks\results\phase0-postgres-smoke.jsonl
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
  benchmarks\results\phase0-postgres-smoke.jsonl `
  --json benchmarks\results\phase0-smoke-report.json `
  --markdown benchmarks\results\phase0-smoke-report.md
```

Il report calcola mediana, p95 nearest-rank, min/max, delta RSS e stabilità del
digest semantico. Una misura con un solo campione serve come smoke di
convalida, non come baseline statistica. Raw e report della singola corsa
restano in `results/`, esclusa dal versionamento; `baseline/` contiene i
budget e i riferimenti numerici mantenuti per i confronti prestazionali.

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

I manifest `postgres-performance-schema-cache.json`,
`postgres-performance-parameterized-fast-path.json` e
`postgres-performance-query-fast-path.json` confrontano cache, preparazione
e percorsi tipizzati. Le baseline JSON in `baseline/` contengono ambiente,
campioni e contatori; sono riferimenti per il confronto, non misure delle
prestazioni di ogni build corrente.

La soglia e la forma del confronto vivono in `scripts/check_postgres_performance.py`,
che e anche cio che le applica.

## Benchmark CLI su PostgreSQL

La suite [`cli_benchmarks.rs`](../crates/plenora-database-cli/tests/cli_benchmarks.rs)
esegue OLTP, lettura, scrittura e spatial. Il gate PostgreSQL la esegue in CI
per verificare conteggi, formato e ordinamento dei percentili. Il confronto
dei tempi con una baseline e separato e si abilita indicando esplicitamente
il file da usare in `PLENORA_CLI_BENCHMARK_BASELINE`.

Con una fixture PostgreSQL/PostGIS raggiungibile, dalla radice del repository:

```powershell
$env:PLENORA_TEST_POSTGRES_DSN = "<dsn fixture>"
# Solo per una fixture locale plaintext; per TLS configurare la CA.
$env:PLENORA_TLS_INSECURE_LOCAL = "1"
cargo test -p plenora-database-cli --test cli_benchmarks -- --ignored --test-threads=1 --nocapture
```

Per confrontare anche il p95, impostare prima della stessa invocazione:

```powershell
$env:PLENORA_CLI_BENCHMARK_BASELINE = "<percorso assoluto alla baseline compatibile>"
```

La forma del file e in
[`benchmark_baseline.json`](../crates/plenora-database-cli/tests/fixtures/benchmark_baseline.json).
I suoi tempi dipendono da macchina, database e profilo di compilazione:
non costituiscono soglie portabili per i runner CI. Senza la variabile,
la suite dichiara `baseline_comparison=not_requested`.

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
