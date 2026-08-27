# Microbenchmark Rust offline

**Documento generato** da `raw/offline-rust-microbench.jsonl`; non va
modificato a mano. Si aggiorna con:

```powershell
python scripts\render_offline_microbench.py
```

## Scopo

Le misure coprono parsing e validazione dei piani, rendering SQL,
ispezione EWKB, contratto Arrow e compilazione dei read plan. Non aprono
connessioni e non misurano decodifica righe, batching o bulk copy.

Non esiste un budget per queste superfici. Il workflow
`.github/workflows/rust-microbench.yml` misura e pubblica, ma non blocca
la PR. Il JSONL versionato non registra hardware, sistema operativo o
commit: questi numeri sono uno snapshot, non una baseline confrontabile.

## Esecuzione

```bash
cargo build --release --locked --examples \
  --package plenora-database-core \
  --package plenora-database-sql \
  --package plenora-database-engine \
  --package plenora-db-mysql \
  --package plenora-db-sqlserver
```

Ogni esempio riceve iterazioni e ripetizioni e scrive JSONL su stdout.
Per una futura baseline autorevole il verdetto dovra includere almeno
commit, toolchain, profilo, sistema operativo e identita della macchina.

## Misure versionate

### `plan_pipeline`

| scenario | iterazioni | ripetizioni | ns/op | op/s | RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| `parse_and_validate_contract_read` | 2000 | 9 | 2.381,0 | 419.994,4 | 2752 |
| `parse_only_contract_read` | 2000 | 9 | 1.476,6 | 677.224,1 | 2816 |
| `parse_and_validate_wide_16c_8f` | 2000 | 9 | 4.448,4 | 224.798,9 | 2816 |
| `parse_and_validate_wide_256c_64f` | 2000 | 9 | 33.361,8 | 29.974,4 | 2816 |

### `sql_render`

| scenario | iterazioni | ripetizioni | ns/op | op/s | RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| `render_select_narrow_postgres` | 5000 | 9 | 677,3 | 1.476.529,1 | 2276 |
| `render_select_wide_postgres` | 5000 | 9 | 5.425,3 | 184.321,9 | 2404 |
| `render_filter_tree_depth8_postgres` | 5000 | 9 | 64.350,0 | 15.540,0 | 2568 |
| `render_query_join_group_postgres` | 5000 | 9 | 2.673,4 | 374.060,6 | 2568 |
| `render_select_narrow_mysql` | 5000 | 9 | 613,0 | 1.631.197,4 | 2568 |
| `render_select_wide_mysql` | 5000 | 9 | 5.040,4 | 198.396,3 | 2568 |
| `render_filter_tree_depth8_mysql` | 5000 | 9 | 53.667,7 | 18.633,2 | 2580 |
| `render_query_join_group_mysql` | 5000 | 9 | 2.649,1 | 377.480,8 | 2580 |
| `render_select_narrow_sqlserver` | 5000 | 9 | 730,5 | 1.368.980,5 | 2580 |
| `render_select_wide_sqlserver` | 5000 | 9 | 5.522,6 | 181.073,7 | 2580 |
| `render_filter_tree_depth8_sqlserver` | 5000 | 9 | 65.754,1 | 15.208,2 | 2588 |
| `render_query_join_group_sqlserver` | 5000 | 9 | 2.697,8 | 370.677,2 | 2588 |
| `render_select_spatial_postgres` | 5000 | 9 | 582,9 | 1.715.530,7 | 2588 |
| `render_select_spatial_mysql` | 5000 | 9 | 580,9 | 1.721.326,2 | 2588 |

### `ewkb_inspect`

| scenario | iterazioni | ripetizioni | ns/op | op/s | RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| `point_srid` | 2000 | 9 | 12,6 | 79.488.096,7 | 2468 |
| `linestring_64` | 2000 | 9 | 12,8 | 78.128.051,9 | 2532 |
| `linestring_4096` | 2000 | 9 | 12,5 | 79.722.565,5 | 2532 |
| `polygon_4rings_256` | 2000 | 9 | 18,3 | 54.704.595,2 | 2532 |
| `multipolygon_64x2x128` | 2000 | 9 | 671,3 | 1.489.685,8 | 2532 |
| `collection_depth32` | 2000 | 9 | 265,7 | 3.764.217,0 | 2532 |

### `arrow_contract`

| scenario | iterazioni | ripetizioni | ns/op | op/s | RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| `validate_schema_narrow_9f` | 2000 | 9 | 2.833,4 | 352.927,2 | 2256 |
| `validate_schema_wide_128f` | 2000 | 9 | 41.783,7 | 23.932,8 | 2368 |
| `validate_schema_spatial_64f` | 2000 | 9 | 18.481,8 | 54.107,3 | 2368 |
| `parse_field_contract_wide_128f` | 2000 | 9 | 41.184,5 | 24.281,0 | 2368 |

### `mysql_read_plan`

| scenario | iterazioni | ripetizioni | ns/op | op/s | RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| `compile_narrow_8c_2f` | 2000 | 9 | 3.283,3 | 304.575,0 | 2192 |
| `compile_wide_128c_2f` | 2000 | 9 | 70.028,9 | 14.279,8 | 2536 |
| `compile_wide_128c_32f` | 2000 | 9 | 76.020,8 | 13.154,3 | 2576 |
| `compile_spatial_16c_2f` | 2000 | 9 | 16.033,7 | 62.368,5 | 2576 |

### `sqlserver_read_plan`

| scenario | iterazioni | ripetizioni | ns/op | op/s | RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| `compile_narrow_8c` | 2000 | 9 | 2.480,0 | 403.226,4 | 2136 |
| `compile_wide_128c` | 2000 | 9 | 59.897,9 | 16.695,1 | 2572 |
| `compile_spatial_16c` | 2000 | 9 | 10.805,8 | 92.542,8 | 2572 |

## Decisioni ancora aperte

- quali scenari meritano un budget;
- quale margine usare sul runner che eseguira il gate;
- se una regressione debba bloccare o soltanto produrre un avviso.

Finche queste decisioni non sono esplicite, il workflow resta una misura
e non viene presentato come gate.
