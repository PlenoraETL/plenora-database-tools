# Microbenchmark Rust offline

## Cosa sono e cosa non sono

Questi benchmark misurano le superfici che il workspace attraversa **prima**
di parlare con un database: parsing e validazione dei piani, rendering SQL,
compilazione dei read plan, ispezione EWKB, validazione del contratto Arrow.
Sono complementari alle campagne su provider reali
(`scripts/check_postgres_performance.py`,
`scripts/check_sqlserver_performance.py`,
`scripts/check_mysql_performance.py`), che misurano il data path e hanno le
loro baseline in `baseline/`.

**Nessun budget prestazionale e' stato fissato per queste superfici.** I
numeri qui sotto sono misure, non soglie: descrivono lo stato del ramo alla
data indicata e servono come punto di partenza per una decisione che spetta al
proprietario del repository. Il workflow CI
(`.github/workflows/rust-microbench.yml`) esegue e pubblica, e basta: non
confronta, non blocca, non ha `--fail-under`. Un gate con una soglia inventata
sarebbe peggio di nessun gate, perche' passerebbe sempre dando l'illusione di
proteggere qualcosa.

Quando i budget verranno decisi, il posto giusto per congelarli e'
`baseline/`, come per le altre campagne, e il confronto va aggiunto al
workflow come passo esplicito.

## Esecuzione

I benchmark sono esempi autonomi, come in `plenora-data-tools`: nessuna
dipendenza da un framework, output JSONL su stdout, un record per scenario.

```bash
cargo build --release --locked --examples \
  --package plenora-database-core \
  --package plenora-database-sql \
  --package plenora-database-engine \
  --package plenora-db-mysql \
  --package plenora-db-sqlserver

for binary in bench_plan_pipeline bench_sql_render bench_ewkb \
              bench_arrow_contract bench_mysql_read_plan \
              bench_sqlserver_read_plan
do
  "target/release/examples/$binary" 2000 9
done
```

I due argomenti sono iterazioni per scenario e ripetizioni; il record riporta
la **mediana** delle ripetizioni. Il profilo `release` e' l'unico
rappresentativo: in `debug` il costo e' dominato dai controlli del
compilatore.

Il raw prodotto e' in `raw/offline-rust-microbench.jsonl`.

## Superfici misurate

| binario | crate | superficie |
| --- | --- | --- |
| `bench_plan_pipeline` | `plenora-database-engine` | `parse_and_validate`: deserializzazione, invarianti, fingerprint SHA-256 |
| `bench_sql_render` | `plenora-database-sql` | `Renderer::render_select`, `render_filter`, `render_query` sui tre dialect |
| `bench_ewkb` | `plenora-database-core` | `inspect_ewkb_detailed` su point, linestring, polygon, multipolygon, collection annidata |
| `bench_arrow_contract` | `plenora-database-core` | `validate_schema_contract` e `FieldContract::parse` sul confine Arrow |
| `bench_mysql_read_plan` | `plenora-db-mysql` | `MysqlReadPlan::compile` da descrizione di catalogo |
| `bench_sqlserver_read_plan` | `plenora-db-sqlserver` | `SqlServerReadPlan::compile` da descrizione di catalogo |

Il data path (decodifica righe, batching Arrow, bulk copy, TDS) non e' qui:
richiede un server e resta nelle campagne Python.

## Misure di riferimento

Ambiente: WSL2 Ubuntu su Intel Core i9-13900K (32 thread logici), rustc 1.92.0
(toolchain pinnata del repository), profilo `release`, 2.000 iterazioni per
scenario, mediana su 9 ripetizioni. Misure prese il 2026-08-06 su
`feat/sanitizer-and-rust-benchmarks`.

Una macchina desktop dedicata e' piu' veloce e molto meno rumorosa di un
runner GitHub condiviso: i numeri di CI saranno diversi e non vanno confrontati
direttamente con questi.

### `plan_pipeline`

| scenario | ns/op | op/s |
| --- | ---: | ---: |
| `parse_and_validate_contract_read` | 2.381 | 419.994 |
| `parse_only_contract_read` | 1.477 | 677.224 |
| `parse_and_validate_wide_16c_8f` | 4.448 | 224.799 |
| `parse_and_validate_wide_256c_64f` | 33.362 | 29.974 |

Il piano di contratto costa ~2,4 us end-to-end. Il solo `serde` ne prende
1,5 us: validazione, riserializzazione canonica e SHA-256 valgono il restante
38%. Su un piano con 256 proiezioni e 64 termini di filtro si arriva a 33 us,
sempre irrilevante rispetto a una singola round-trip di rete.

### `sql_render`

| scenario | ns/op | op/s |
| --- | ---: | ---: |
| `render_select_narrow_postgres` | 677 | 1.476.529 |
| `render_select_wide_postgres` | 5.425 | 184.322 |
| `render_filter_tree_depth8_postgres` | 64.350 | 15.540 |
| `render_query_join_group_postgres` | 2.673 | 374.061 |
| `render_select_narrow_mysql` | 613 | 1.631.197 |
| `render_select_wide_mysql` | 5.040 | 198.396 |
| `render_filter_tree_depth8_mysql` | 53.668 | 18.633 |
| `render_query_join_group_mysql` | 2.649 | 377.481 |
| `render_select_narrow_sqlserver` | 730 | 1.368.980 |
| `render_select_wide_sqlserver` | 5.523 | 181.074 |
| `render_filter_tree_depth8_sqlserver` | 65.754 | 15.208 |
| `render_query_join_group_sqlserver` | 2.698 | 370.677 |
| `render_select_spatial_postgres` | 583 | 1.715.531 |
| `render_select_spatial_mysql` | 581 | 1.721.326 |

I tre dialect stanno entro il 20% l'uno dall'altro su ogni forma: il costo e'
nella struttura dell'AST, non nelle differenze di quoting. `render_filter` su
un albero booleano bilanciato di profondita' 8 (256 confronti, 511 nodi) costa
~64 us, cioe' ~125 ns per nodo. `ST_Intersects` non e' rappresentabile in
SQL Server senza tipo e SRID risolti, quindi lo scenario spatial esiste solo
per PostgreSQL e MySQL.

### `ewkb_inspect`

| scenario | payload (B) | componenti | profondita' | ns/op | op/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| `point_srid` | 25 | 2 | 1 | 12,6 | 79.488.097 |
| `linestring_64` | 1.037 | 65 | 1 | 12,8 | 78.128.052 |
| `linestring_4096` | 65.549 | 4.097 | 1 | 12,5 | 79.722.565 |
| `polygon_4rings_256` | 16.413 | 1.029 | 1 | 18,3 | 54.704.595 |
| `multipolygon_64x2x128` | 263.245 | 16.577 | 2 | 671,3 | 1.489.686 |
| `collection_depth32` | 553 | 49 | 33 | 265,7 | 3.764.217 |

Il risultato piu' informativo: una linestring da 4.096 punti (64 kB) costa
quanto un point da 25 byte. Lo scanner non legge le coordinate, verifica che
il blocco dichiarato stia nel buffer e lo salta; il costo e' proporzionale al
numero di **header** attraversati, non ai byte. Un multipolygon con 64 parti
costa 671 ns perche' ha 64 header figli, e una collection annidata a
profondita' 33 ne costa 266 per lo stesso motivo. Per questo il record non
riporta un throughput in byte: sarebbe un numero grande e privo di senso.

### `arrow_contract`

| scenario | campi | ns/op | ns/campo | op/s |
| --- | ---: | ---: | ---: | ---: |
| `validate_schema_narrow_9f` | 9 | 2.833 | 315 | 352.927 |
| `validate_schema_wide_128f` | 128 | 41.784 | 326 | 23.933 |
| `validate_schema_spatial_64f` | 64 | 18.482 | 289 | 54.107 |
| `parse_field_contract_wide_128f` | 128 | 41.185 | 322 | 24.281 |

Il costo e' lineare nei campi e stabile a ~300 ns per campo, indipendente dal
fatto che il campo sia geometrico: i lookup di metadati dominano su tutto il
resto. Praticamente tutto il costo di `validate_schema_contract` e' in
`FieldContract::parse`, che da sola vale 41 us sui 42 dello schema a 128
campi.

### `mysql_read_plan`

| scenario | colonne | ns/op | ns/colonna | op/s |
| --- | ---: | ---: | ---: | ---: |
| `compile_narrow_8c_2f` | 8 | 3.283 | 410 | 304.575 |
| `compile_wide_128c_2f` | 128 | 70.029 | 547 | 14.280 |
| `compile_wide_128c_32f` | 128 | 76.021 | 594 | 13.154 |
| `compile_spatial_16c_2f` | 16 | 16.034 | 1.002 | 62.369 |

### `sqlserver_read_plan`

| scenario | colonne | ns/op | ns/colonna | op/s |
| --- | ---: | ---: | ---: | ---: |
| `compile_narrow_8c` | 8 | 2.480 | 310 | 403.226 |
| `compile_wide_128c` | 128 | 59.898 | 468 | 16.695 |
| `compile_spatial_16c` | 16 | 10.806 | 675 | 92.543 |

Le due compilazioni sono nello stesso ordine di grandezza, con SQL Server
leggermente piu' economico a parita' di colonne nonostante le conversioni
esplicite in proiezione. Le colonne spatial costano circa il doppio delle
scalari: i metadati geometrici da allegare al campo Arrow sono nove chiavi
contro due. Su 128 colonne si resta sotto i 76 us, quindi la compilazione del
piano non e' un candidato a ottimizzazione finche' la cache di schema
continua a evitarla nella maggior parte delle preparazioni.

## Costo di `overflow-checks` in release

Prima misura usata per una decisione invece che per un archivio.

Il default di Rust disattiva i controlli di overflow in release: un'aritmetica
oltre il tipo avvolge in silenzio. Per un componente a semantica fail-closed
e' il verso sbagliato, ma il costo andava misurato e non stimato. Stesso
hardware, stessa procedura, unica variabile `overflow-checks` nel profilo.

| scenario | prima (ns/op) | dopo (ns/op) | delta |
|---|---:|---:|---:|
| `render_select_narrow_mysql` | 627,0 | 656,5 | +4,7% |
| `render_query_join_group_postgres` | 2.625,7 | 2.708,3 | +3,1% |
| `render_select_wide_postgres` | 5.346,5 | 5.484,7 | +2,6% |
| `render_filter_tree_depth8_postgres` | 62.809,9 | 63.430,4 | +1,0% |
| `render_select_wide_mysql` | 4.953,2 | 4.998,0 | +0,9% |
| `validate_schema_spatial_64f` | 18.779,9 | 18.888,3 | +0,6% |
| `validate_schema_narrow_9f` | 2.836,9 | 2.849,3 | +0,4% |
| `validate_schema_wide_128f` | 41.673,3 | 41.840,0 | +0,4% |
| `parse_field_contract_wide_128f` | 41.225,3 | 41.255,4 | +0,1% |
| `multipolygon_64x2x128` | 676,2 | 667,2 | -1,3% |
| `linestring_4096` | 12,9 | 12,7 | -1,6% |
| `polygon_4rings_256` | 18,4 | 18,1 | -1,9% |
| `point_srid` | 13,0 | 12,6 | -2,9% |
| `linestring_64` | 13,2 | 12,8 | -3,4% |
| `collection_depth32` | 278,0 | 266,2 | -4,2% |

**Mediana +0,4%**, peggiore +4,7%, migliore -4,2%. Sei scenari su quindici
risultano piu' veloci: l'effetto reale e' dentro il rumore di misura per la
maggior parte del carico, e il caso peggiore vale 29 nanosecondi su
un'operazione di rendering SQL.

Il gradiente ha comunque una logica leggibile: il costo si concentra sul
rendering SQL, che fa aritmetica su indici e posizioni di placeholder, mentre
la scansione EWKB non ne risente — coerente con il fatto che quello scanner e'
O(header) e salta le coordinate invece di leggerle.

Verifica di accompagnamento: `cargo test --workspace --release --locked` con i
controlli attivi passa 375 test su 375. Nessun overflow latente nel percorso
esercitato dalla suite.

## Cosa resta aperto

1. **I budget.** Nessuna soglia e' stata fissata. Servono almeno: quale
   scenario merita un gate, con quale margine sopra la misura di CI, e se il
   gate deve bloccare la PR o solo segnalare.
2. **La baseline di CI.** Le misure qui sono su hardware desktop. Prima di
   qualunque soglia serve una serie di run del workflow su runner GitHub, per
   conoscere la dispersione reale dell'ambiente in cui il gate girerebbe.
3. **Il data path.** Restano fuori perche' richiedono un database; il
   confronto MySQL e' ancora `not_requested` per assenza di baseline misurata,
   e questi microbenchmark non lo colmano.
