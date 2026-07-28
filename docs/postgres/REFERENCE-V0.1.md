# PostgreSQL/PostGIS reference v0.1

Stato: **riferimento implementativo congelato**.

Questa baseline chiude il provider general-purpose PostgreSQL/PostGIS prima
dell'avvio di SQL Server. Non dichiara certificazione aeronautica né copertura
di funzioni amministrative, replica logica o di ogni estensione PostGIS.

## Identità del riferimento

| Elemento | Baseline |
|---|---|
| crate | `plenora-db-postgres` 0.1.x |
| server di riferimento | PostgreSQL 16 |
| estensione di riferimento | PostGIS 3.4 |
| formato tabellare pubblico | Arrow `RecordBatch` |
| formato spaziale pubblico | GeoArrow-WKB / EWKB validato |
| contratto capability | schema version 1, rilevato a runtime |
| data path congelato | `postgres-postgis-data-path-v3` |
| profilo avanzato | `postgres-postgis-advanced-profile-v1` |

La matrice PostgreSQL 14–18 in [COMPATIBILITY.md](COMPATIBILITY.md) resta una
prova aggiuntiva. PostgreSQL 16/PostGIS 3.4 rimane il riferimento usato per
fault injection, performance, copertura e confronto dei codec.

## Freeze verificabile

Il freeze non dipende soltanto dalla documentazione:

| Confine | Evidenza automatica |
|---|---|
| API Rust pubblica v0.1 | `crates/plenora-db-postgres/tests/public_api_v0_1.rs` compila esclusivamente contro export pubblici |
| contratto comune `Provider` | `plenora-database-testkit::verify_provider_contract` |
| connessione e identità | provider/versione non vuoti e provider coerente |
| capability | schema v1, coerenza spaziale/transazionale, limiti espliciti maggiori di zero e assenza duplicati |
| introspezione | ID operazione canonico e documento JSON object |
| cancellazione preventiva | `Cancelled/Connect/None/Never` senza apertura di una nuova operazione |
| operazione fuori perimetro | `Unsupported/Probe/None/Never` |
| write preflight | boundary `preflight.rs`, prima di ogni mutazione |
| correttezza live | `check_postgres_reference.py` e `check_postgres_hardening.py` |
| prestazioni | campagna riproducibile e gate GiST/KNN |
| copertura | CI `cargo-llvm-cov` con soglie production-focused |

Una modifica incompatibile dell'API pubblica, un documento capability
contraddittorio o un envelope di errore differente rompe quindi un test o un
gate. Una modifica intenzionalmente breaking richiede una nuova baseline,
non l'attenuazione del test v0.1.

## Matrice capability congelata

Le capability esatte restano runtime-dependent: versione server, PostGIS,
privilegi ed estensioni vengono osservati e non dedotti dal nome del provider.
Sul riferimento PostgreSQL 16/PostGIS 3.4 il profilo atteso è:

| Area | Stato v0.1 | Note |
|---|---|---|
| test connessione, identità e versione | supportato | segreti redatti |
| TLS | supportato | WebPKI, CA privata e mTLS |
| cataloghi, schemi, oggetti e describe | supportato | token strutturale e cache strict |
| read Arrow streaming | supportato | cursor, backpressure, budget e cancellazione server-side |
| projection/filter/order/pagination | supportato | identificatori quotati e valori bindati |
| AST relazionale | supportato | visita bounded, CTE/join/window/set/lateral/locking |
| create/append/update/upsert/replace/delete | supportato | transazione e recovery esplicite |
| COPY text, COPY binary, prepared | supportato | differenziale dati obbligatorio |
| schema evolution | opt-in | soltanto colonne nullable additive |
| geometry e geography | supportato | XY/XYZ/XYM/XYZM, SRID e CRS authority |
| indice spaziale | supportato | GiST e operatori bbox/KNN verificati con `EXPLAIN` |
| catalogo spatial tipizzato | supportato | 72 funzioni nel catalogo Rust |
| Raster/Topology/SFCGAL | fuori perimetro | richiedono moduli e capability separati |
| WAL/replica/amministrazione cluster | fuori perimetro | non appartengono al data path |

`probe_capabilities` è l'autorità per una connessione concreta. Questa tabella
definisce il profilo del riferimento, non autorizza fallback se il probe
osserva una capacità assente.

## Superficie API v0.1

Sono congelati come API pubblica del provider:

- `PostgresProvider` e tutti i builder di configurazione;
- `PostgresPerformanceProfile`, `PostgresInsertMode` e
  `PostgresSchemaEvolution`;
- `PostgresTlsMode`, `PostgresTlsConfig` e `PostgresNetworkOptions`;
- `PostgresFaultPoint`;
- `PostgresMetricsSnapshot`;
- `PostgresSchemaToken`;
- l'implementazione del trait comune `Provider`.

I moduli interni (`catalog`, `preflight`, `query_plan`, `query_execution`,
`read_stream`, `write/*`, pool e cache) non sono API pubblica. Possono essere
rifattorizzati se le prove pubbliche, differenziali, live e prestazionali
restano verdi.

## Evidenza riproducibile

```powershell
python scripts\check_pre_database.py
python scripts\check_postgres_reference.py
python scripts\check_postgres_hardening.py
python scripts\check_postgres_spatial_performance.py
python scripts\check_postgres_performance.py
python scripts\check_postgres_matrix.py
```

La CI conserva per commit identità di toolchain/container/server, log e report
JSON. Le evidenze sono riproducibili e revisionabili, ma non sostituiscono
indipendenza della verifica, qualification dei tool o processo di
certificazione.

## Condizione di passaggio a SQL Server

SQL Server deve riusare senza fork:

- trait `Provider`, error envelope, cancellation token e resource budget;
- AST validato e renderer di dialetto;
- Arrow/GeoArrow-WKB e loss policy;
- suite `verify_provider_contract`;
- forma dei report CI e della matrice di evidenza.

Restano vendor-specific e devono vivere nel nuovo driver:

- protocollo e pool/session reset;
- catalogo `sys.*`;
- mapping `datetime2`, `datetimeoffset`, `uniqueidentifier`, `decimal`,
  `geometry` e `geography`;
- bulk copy, transazioni, staging/swap e recovery;
- costruttori e predicati spatial SQL Server.

Il driver SQL Server non deve copiare internamente il comportamento
PostgreSQL: deve dimostrare lo stesso contratto pubblico con implementazione e
capability proprie.
