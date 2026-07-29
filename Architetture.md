# plenora-database-tools — Architettura

Black box Rust autoconsistente per leggere, ispezionare e scrivere database
relazionali e spaziali attraverso un protocollo uniforme Arrow-in / Arrow-out.

Il progetto porta in Rust le funzioni database oggi presenti nel backend
Plenora e le organizza secondo lo stesso modello di `plenora-data-tools`:
piano dichiarativo, validazione fail-closed, preparazione fisica per singola
esecuzione, streaming a batch, limiti di risorsa e metriche verificabili.

Database inizialmente inclusi nel disegno:

- PostgreSQL e PostGIS;
- MySQL e MariaDB;
- Microsoft SQL Server, inclusi `geometry` e `geography`;
- Oracle Database e Oracle Spatial;
- IBM Db2 e Db2 Spatial Extender;
- SQLite e SpatiaLite;
- DuckDB e relativa estensione Spatial;
- ArcGIS Online ed ArcGIS Enterprise Feature Service.

ArcGIS è incluso come provider geospaziale REST, non come dialect SQL. Usa lo
stesso confine Arrow/GeoArrow-WKB e lo stesso engine di contratti, limiti,
capability e outcome, ma mantiene sessione, paginazione e scrittura distinte
dai database relazionali.

Il supporto effettivo di una funzione non è dedotto dal nome del database:
dipende da server, versione, driver compilato, estensioni installate, privilegi
e capability rilevate a runtime.

Principi non negoziabili, trattati come criteri di accettazione:

- `#![forbid(unsafe_code)]` in tutti i crate Plenora;
- black box autonoma: nessuna dipendenza da `plenora-data-tools`,
  `plenora-IO-tools` o dal backend Python;
- Arrow come confine dati pubblico; geometrie in GeoArrow-WKB;
- validazione locale completa prima di aprire una connessione;
- verifica remota completa prima della prima mutazione;
- SQL generato da AST, identificatori quotati dal dialect e valori sempre
  bindati quando il protocollo del database lo consente;
- nessuna credenziale nel piano, nelle metriche o negli errori;
- nessuna riproiezione, coercizione o perdita di precisione implicita;
- streaming e backpressure reali sia in lettura sia in scrittura;
- transazioni modellate esplicitamente, senza promettere atomicità che il
  database o l'operazione non possono garantire;
- una perdita di connessione durante il commit può produrre
  `OutcomeUnknown`: non viene trasformata falsamente in successo o rollback;
- limiti applicati prima delle allocazioni, dei fetch e delle espansioni;
- prestazioni e memoria sono criteri di accettazione; la specifica compagna è
  `Prestazioni.md`.

---

## 1. Modello mentale

La libreria è un appliance software incorporabile:

```text
                    ┌──────────────────────────────────┐
Plan JSON ─────────▶│ validate                         │
                    │   piano + contratti + policy     │
                    └──────────────┬───────────────────┘
                                   │ ValidatedDatabasePlan
Runtime secrets ───┐               ▼
Arrow input ───────┼──────▶ prepare + execute
Cancellation ─────┘               │
                                  ├──▶ stream RecordBatch
                                  ├──▶ catalogo/schema
                                  ├──▶ scalar/diagnostica
                                  └──▶ WriteOutcome
```

Il piano descrive **cosa** fare. Non contiene:

- password, token, wallet, certificati o stringhe di connessione complete;
- numero di thread;
- dimensione concreta dei fetch;
- scelta del protocollo bulk;
- nomi delle tabelle di staging;
- hint fisici non portabili;
- decisioni di retry non semanticamente sicure.

Questi elementi appartengono al contesto runtime o al piano fisico.

L'API pubblica segue due fasi:

```rust
validate(
    plan_json: &str,
    contracts: &DatabaseContracts,
) -> Result<ValidatedDatabasePlan>

execute(
    validated: &ValidatedDatabasePlan,
    runtime: DatabaseRuntime,
) -> Result<DatabaseOutput>
```

Internamente `execute` è suddiviso in:

```text
prepare locale
  → acquire connection
  → probe capability e privilegi
  → remote preflight
  → costruzione PhysicalDatabasePlan
  → execute
  → finalize / commit / rollback / recovery report
```

`execute` accetta solo il prodotto di `validate`: un piano non validato non
può raggiungere driver o rete.

### 1.1 Un'operazione, un provider target

La v1 esegue un piano contro una sola sorgente o destinazione provider. Un
piano può ricevere uno stream Arrow esterno oppure produrne uno, ma non
coordina transazioni distribuite tra database o servizi diversi.

Le copie database-to-database vengono orchestrate dal chiamante:

```text
database-tools/read(source) → Arrow stream → database-tools/write(target)
```

Questo mantiene esplicita l'impossibilità generale di un commit atomico tra
due sistemi eterogenei. Un coordinatore distribuito potrà essere aggiunto in
un progetto separato, senza alterare la semantica di questa libreria.

---

## 2. Confine dati e contratti

### 2.1 Arrow come rappresentazione unica

Il confine pubblico è uno stream di `arrow_array::RecordBatch`. Tutti i crate
del workspace dipendono dalla versione Arrow fissata nel workspace tramite il
re-export di `plenora-database-core`.

Un aggiornamento della versione Arrow è potenzialmente breaking per gli
utilizzatori Rust e viene trattato come tale.

Non esiste un modello tabellare Plenora alternativo ad Arrow. I buffer propri
dei driver vengono convertiti nel minor numero possibile di passaggi:

```text
protocollo DB → decoder driver → builder Arrow → RecordBatch
RecordBatch → binder/bulk encoder driver → protocollo DB
```

### 2.2 Geometrie

Il formato geometrico canonico al confine è:

- colonna Arrow `Binary` o `LargeBinary`;
- `ARROW:extension:name = geoarrow.wkb`;
- metadato `geo` con CRS, SRID e semantica geometry/geography quando nota;
- una cella WKB per riga.

Il modello dei contratti è estensibile a più colonne geometriche e alle
dimensioni `XY`, `XYZ`, `XYM`, `XYZM`. Ogni driver dichiara esattamente ciò che
preserva. Un profilo iniziale può limitarsi a `XY`, ma deve rifiutare o
registrare esplicitamente la perdita di Z/M.

Le rappresentazioni native vengono convertite ai bordi:

| Database | Lettura canonica indicativa | Scrittura canonica indicativa |
|---|---|---|
| PostgreSQL/PostGIS | `ST_AsEWKB`/protocollo binario | `ST_GeomFromEWKB` o binder nativo |
| MySQL/MariaDB | `ST_AsWKB` + `ST_SRID` | `ST_GeomFromWKB` |
| SQL Server | `STAsBinary()` + `STSrid` | `geometry/geography::STGeomFromWKB` |
| Oracle Spatial | `SDO_UTIL.TO_WKBGEOMETRY` o adapter oggetto | `SDO_UTIL.FROM_WKBGEOMETRY`/adapter |
| Db2 Spatial | `ST_AsBinary` + SRID | costruttore `ST_GeomFromWKB` equivalente |
| SpatiaLite | `AsBinary` + SRID | `GeomFromWKB` |
| DuckDB Spatial | `ST_AsWKB` | `ST_GeomFromWKB` |

I nomi concreti e la disponibilità sono capability del dialect/versione, non
stringhe hard-coded nell'engine.

Regole:

- nessuna riproiezione implicita;
- nessun cambio geometry ↔ geography implicito;
- SRID sconosciuto resta sconosciuto, non diventa `0` o `4326`;
- CRS incompatibile con la destinazione è errore prima della scrittura;
- payload, profondità, componenti e coordinate sono limitati;
- endianess WKB non ha significato semantico;
- uguaglianza geometrica nei test non coincide con uguaglianza dei byte WKB.

### 2.3 `DatabaseDataContract`

Lo schema Arrow da solo non descrive abbastanza il bordo database:

```rust
pub struct DatabaseDataContract {
    pub schema: SchemaRef,
    pub fields: Vec<DatabaseFieldContract>,
    pub geometries: Vec<GeometryColumnContract>,
    pub active_geometry: Option<FieldId>,
    pub keys: Vec<KeyContract>,
    pub properties: ContractProperties,
}
```

`DatabaseFieldContract` conserva almeno:

- `FieldId` stabile;
- nome logico;
- tipo Arrow;
- nullability;
- tipo nativo dichiarato, se proveniente da introspezione;
- precisione, scala, lunghezza e charset quando applicabili;
- default/generazione/identity come metadata, non come testo SQL eseguibile;
- stato `readable`, `writable`, `generated`;
- livello di confidenza e scope della proprietà.

Le proprietà sono `Declared`, `Proven`, `Estimated` o `Unknown`. Solo
`Proven` può soddisfare una precondizione semantica. Le stime possono guidare
fetch size, strategia bulk e memoria, mai cambiare il risultato.

### 2.4 Mapping tipi e perdita informativa

Il mapping è una funzione versionata e bidirezionale:

```text
NativeType + DriverCapabilities + MappingPolicy
    ↔ Arrow DataType + FieldMetadata + LossReport
```

Politiche:

- `Strict`: ogni perdita o ambiguità è errore;
- `Compatible`: coercizioni documentate e reversibili quando possibile;
- `Lossy`: consentita solo se dichiarata nel piano e riportata;
- `Native`: preserva il tipo tramite metadata/estensione quando Arrow non ha
  un equivalente standard.

Devono essere trattati esplicitamente almeno:

- interi signed/unsigned;
- decimal precision/scale, incluso overflow oltre Decimal128;
- float, `NaN`, infinito e signed zero;
- char/varchar/nchar/nvarchar, charset e collation;
- binary/varbinary/blob/raw;
- date, time, timestamp con e senza timezone;
- intervalli;
- boolean e rappresentazioni emulative;
- UUID/GUID;
- JSON/XML;
- enum/domain/user-defined types;
- array e tipi composti;
- LOB in-line o streaming;
- rowid/identity/generated columns;
- geometrie e CRS.

`LossReport` fa parte dell'output di introspezione e scrittura. Non è solo un
log.

---

## 3. Struttura del workspace

```text
plenora-database-tools/
├── Cargo.toml
├── Architetture.md
├── Prestazioni.md
├── crates/
│   ├── plenora-database-core/
│   ├── plenora-database-sql/
│   ├── plenora-database-engine/
│   ├── plenora-db-postgres/
│   ├── plenora-db-mysql/
│   ├── plenora-db-sqlserver/
│   ├── plenora-db-oracle/
│   ├── plenora-db-db2/
│   ├── plenora-db-sqlite/
│   ├── plenora-db-duckdb/
│   ├── plenora-provider-arcgis/
│   ├── plenora-database-testkit/
│   └── plenora-database-cli/
├── tests/
├── fuzz/
├── benchmarks/
└── docs/adr/
```

### 3.1 `plenora-database-core`

Contiene esclusivamente fondamenta portabili:

- re-export Arrow;
- errori sanitizzati;
- contratti e `FieldId`;
- CRS e metadata GeoArrow-WKB;
- giudice unico del contratto canonico Arrow, condiviso da tutti i provider;
- limiti;
- capability model;
- piano pubblico e output comuni;
- trait dei driver;
- metriche e stati transazionali;
- mapping policy e `LossReport`.

Non contiene client concreti, runtime async, pool o dialect SQL.

I crate provider possono aggiungere adattatori per il proprio sotto-namespace
(`plenora.postgres.*`, `plenora.sqlserver.*`), ma non possono duplicare il
giudizio sulle chiavi canoniche. PostgreSQL conserva nel proprio adattatore
solo tipo/dichiarazione/type-kind nativi; PostgreSQL e SQL Server chiamano
entrambi `plenora_database_core::field_contract::FieldContract`.

### 3.2 `plenora-database-sql`

Contiene:

- AST SQL tipizzato;
- identifier model separato dai valori;
- quoting per dialect;
- placeholder e binding;
- DDL portabile;
- espressioni di projection/filter/order/limit;
- generazione di insert/update/upsert/merge;
- frammenti spatial capability-gated;
- rendering deterministico e redatto per diagnostica.

L'AST non pretende che ogni database implementi lo stesso sottoinsieme. Il
renderer richiede una `DialectCapabilities` e fallisce se il nodo non è
rappresentabile correttamente.

Il testo SQL arbitrario è un'operazione distinta, disabilitabile tramite
policy. Non passa dal renderer portabile e deve dichiarare:

- se è read-only o mutante;
- schema di output atteso;
- parametri tipizzati;
- timeout;
- policy multi-statement;
- livello di fiducia.

### 3.3 `plenora-database-engine`

Implementa:

- parsing limitato del piano;
- catalogo operazioni;
- `validate`;
- fingerprint;
- `prepare`;
- lifecycle di sessione;
- remote preflight;
- executor streaming;
- resource governor;
- cancellazione;
- retry sicuro;
- protocollo transazionale;
- metriche;
- cleanup e recovery report.

L'engine conosce solo i trait dei provider. Non contiene `match` sui nomi
PostgreSQL, Oracle, Db2 o ArcGIS.

### 3.4 Driver SQL

Ogni driver implementa:

- parsing delle sole opzioni non segrete;
- apertura e verifica sessione;
- identificazione server/versione;
- capability probe;
- introspezione;
- mapping tipi;
- compilazione SQL dal dialect;
- cursor/fetch streaming;
- binder e percorso bulk;
- transazioni e savepoint;
- staging e swap supportati;
- conversioni spatial;
- classificazione errori;
- cancellazione;
- cleanup specifico.

Una funzione non supportata deve restituire `UnsupportedCapability`, mai
simulare un comportamento diverso.

### 3.5 Provider ArcGIS

`plenora-provider-arcgis` implementa ArcGIS Online ed Enterprise:

- autenticazione token, OAuth/client credentials e refresh;
- discovery di portale, folder, item, service e layer;
- introspezione campi, ObjectID, GlobalID, domini, subtype e spatial reference;
- query paginata con offset oppure finestre di ObjectID;
- conversione Esri JSON ↔ GeoArrow-WKB;
- create service/layer;
- append, update e delete tramite API feature;
- upsert con chiave esplicita;
- replace con strategia service/layer capability-gated;
- `applyEdits`, `rollbackOnFailure`, `useGlobalIds` e job asincroni quando
  pubblicizzati dal servizio;
- più layer con outcome separato per layer;
- rate limit, `Retry-After` e backpressure HTTP.

Non usa `plenora-database-sql`. La semantica dei filtri REST viene compilata da
un renderer dedicato, mantenendo identificatori e valori separati.

ArcGIS non promette una transazione equivalente a un database SQL. L'atomicità
dipende dalle capability del servizio e gli outcome parziali fanno parte del
contratto pubblico.

### 3.6 `plenora-database-testkit`

Fornisce test di conformità riutilizzabili da ogni driver:

- naming e quoting avversari;
- mapping round-trip;
- null, boundary numerici e Unicode;
- timestamp/timezone;
- geometrie e SRID;
- transazioni e rollback;
- perdita di connessione;
- retry;
- backpressure;
- introspezione;
- cleanup dello staging;
- redazione dei segreti.

Un driver non è dichiarato supportato finché non supera la matrice obbligatoria
per le capability che pubblicizza.

### 3.7 CLI

Il CLI è un wrapper sottile. Legge piano e riferimenti runtime, invoca la
libreria e serializza output/metriche. Non contiene logica SQL o di mapping.

---

## 4. Trait e capability dei provider

### 4.1 Trait logico

Il contratto concettuale è:

```rust
pub trait DataProviderDriver: Send + Sync {
    fn id(&self) -> DriverId;
    fn kind(&self) -> ProviderKind;
    fn static_capabilities(&self) -> StaticCapabilities;

    async fn connect(
        &self,
        endpoint: &Endpoint,
        credentials: &dyn SecretProvider,
        context: &ExecutionContext,
    ) -> Result<Box<dyn ProviderSession>>;
}

pub trait ProviderSession {
    async fn probe(&mut self) -> Result<RuntimeCapabilities>;
    async fn inspect(&mut self, request: InspectRequest)
        -> Result<DatabaseMetadata>;
    async fn open_reader(&mut self, plan: PreparedRead)
        -> Result<Box<dyn BatchStream>>;
    async fn open_writer(&mut self, plan: PreparedWrite)
        -> Result<Box<dyn BatchWriter>>;
    async fn begin(&mut self, options: TransactionOptions)
        -> Result<TransactionHandle>;
    async fn cancel(&mut self) -> Result<CancelOutcome>;
}
```

La firma Rust definitiva potrà usare GAT, associated types o boxing. La
semantica non cambia.

`ProviderKind` distingue almeno `SqlDatabase`, `EmbeddedDatabase` e
`ArcGisFeatureService`. Le operazioni comuni usano capability di
inspect/read/write; transazioni SQL e lifecycle ArcGIS sono estensioni
specifiche, non metodi finti obbligatori per tutti.

Driver sincroni o FFI non bloccano il pool CPU: vengono adattati a worker I/O
dedicati e bounded. Un adapter può contenere unsafe code di terze parti, ma
nessun crate Plenora introduce proprio unsafe.

### 4.2 Capability

Le capability sono dati versionati, non booleani sparsi:

```rust
pub struct RuntimeCapabilities {
    pub server: ServerIdentity,
    pub transactions: TransactionCapabilities,
    pub reads: ReadCapabilities,
    pub writes: WriteCapabilities,
    pub ddl: DdlCapabilities,
    pub types: TypeCapabilities,
    pub spatial: SpatialCapabilities,
    pub limits: ServerLimits,
}
```

Esempi:

- cursor server-side;
- array binding;
- binary copy/bulk loader;
- `RETURNING`;
- transactional DDL;
- savepoint;
- atomic rename/swap;
- temporary table semantics;
- native upsert o merge;
- maximum bind parameters;
- maximum identifier length;
- geometry/geography, SRID, Z/M;
- funzioni WKB e indici spaziali;
- ArcGIS max record count, pagination e supported query formats;
- ArcGIS add/update/delete/applyEdits, rollback-on-failure e GlobalID;
- ArcGIS service/layer lifecycle e job asincroni;
- cancel out-of-band;
- isolation levels.

La capability effettiva è l'intersezione:

```text
compilata nel driver
∩ supportata dal server/versione
∩ disponibile nelle estensioni
∩ permessa dai privilegi
∩ consentita dalla policy runtime
```

### 4.3 Errori

Categorie pubbliche:

- `Plan`;
- `Contract`;
- `Authentication`;
- `Authorization`;
- `Connectivity`;
- `Timeout`;
- `Cancelled`;
- `UnsupportedCapability`;
- `SchemaChanged`;
- `ConstraintViolation`;
- `SerializationFailure`;
- `Deadlock`;
- `DataMapping`;
- `Spatial`;
- `ResourceLimit`;
- `Transaction`;
- `OutcomeUnknown`;
- `DriverInternal`.

Ogni errore dichiara quando possibile:

- fase;
- nodo/operazione;
- driver e classe server;
- retryability;
- stato transazionale noto;
- SQLSTATE/vendor code sanitizzato;
- source chain interna.

Non contiene SQL con valori interpolati, DSN, password, token, wallet path,
payload, valori di colonne o WKB.

---

## 5. Piano dichiarativo

### 5.1 Forma

Esempio di lettura:

```json
{
  "schema_version": 1,
  "connection": "source_main",
  "operation": {
    "id": "database.read",
    "source": {
      "catalog": "app",
      "schema": "public",
      "object": "roads"
    },
    "projection": ["id", "name", "geometry"],
    "filter": {
      "op": "and",
      "args": [
        {"op": "gte", "field": "updated_at", "param": "from_time"},
        {"op": "is_not_null", "field": "geometry"}
      ]
    },
    "order_by": [{"field": "id", "direction": "asc"}]
  },
  "output": {"kind": "arrow_stream"}
}
```

Esempio di scrittura:

```json
{
  "schema_version": 1,
  "connection": "target_main",
  "operation": {
    "id": "database.write",
    "target": {"schema": "public", "object": "roads"},
    "mode": "upsert",
    "keys": ["id"],
    "schema_policy": "strict",
    "transaction_profile": "single_transaction",
    "spatial": {"srid_policy": "require_match"}
  },
  "input": {"kind": "arrow_stream"}
}
```

`connection` è un riferimento opaco risolto dal runtime. Il piano non contiene
il segreto.

### 5.2 Operazioni v1

Catalogo minimo:

- `database.test_connection`;
- `database.list_catalogs`;
- `database.list_schemas`;
- `database.list_objects`;
- `database.describe_object`;
- `database.read`;
- `database.scalar`;
- `database.preview`;
- `database.write`;
- `database.create`;
- `database.append`;
- `database.replace`;
- `database.truncate_insert`;
- `database.update`;
- `database.upsert`;
- `database.delete_by_keys`;
- `database.create_index`;
- `database.drop_index`;
- `database.native_query`.

Catalogo ArcGIS:

- `arcgis.test_connection`;
- `arcgis.list_folders`;
- `arcgis.list_items`;
- `arcgis.list_services`;
- `arcgis.list_layers`;
- `arcgis.describe_layer`;
- `arcgis.read`;
- `arcgis.count`;
- `arcgis.create_service`;
- `arcgis.create_layer`;
- `arcgis.append`;
- `arcgis.update`;
- `arcgis.upsert`;
- `arcgis.delete_by_keys`;
- `arcgis.replace`;
- `arcgis.apply_edits`;
- `arcgis.publish_layers`.

Le operazioni comuni potranno avere alias `provider.*`, ma gli ID ArcGIS
restano espliciti quando la semantica non ha un equivalente SQL.

Gli alias del backend Python sono migrati verso ID canonici versionati.

### 5.3 Limiti del piano

`PlanLimits` viene applicato durante il parsing, prima della deserializzazione
completa:

- byte JSON;
- profondità;
- numero di espressioni;
- fan-out delle espressioni;
- byte per identificatore;
- numero di colonne;
- parametri;
- chiavi;
- clausole `IN`;
- byte di SQL nativo;
- numero di statement;
- byte di configurazione driver.

Campi sconosciuti sono rifiutati. Default e alias vengono canonicalizzati
prima del `plan_hash`.

### 5.4 Identità del piano validato

`ValidatedDatabasePlan` include:

- hash canonico del piano;
- fingerprint dei contratti;
- fingerprint del catalogo operazioni;
- versioni di mapping tipi e renderer SQL;
- versione engine;
- driver richiesto;
- capability minime;
- policy hash;
- contratti attesi di input/output.

Non include capability osservate su una connessione specifica: appartengono al
`PhysicalDatabasePlan`.

---

## 6. `validate`, `prepare`, `execute`

### 6.1 `validate`: puro e senza rete

`validate`:

1. applica `PlanLimits`;
2. deserializza con `deny_unknown_fields`;
3. migra schema e alias;
4. risolve l'operazione nel catalogo;
5. valida identificatori e parametri;
6. valida contratti Arrow e geometrici;
7. valida mapping richiesti;
8. calcola capability minime;
9. costruisce SQL AST, non testo SQL definitivo;
10. calcola fingerprint;
11. restituisce un oggetto immutabile.

Non apre connessioni, non risolve DNS, non legge segreti e non tocca dati.

### 6.2 `prepare`: verifica remota senza mutazioni

`prepare`:

1. risolve endpoint e segreti tramite handle runtime;
2. acquisisce una sessione con timeout;
3. verifica identità e versione server;
4. rileva estensioni e capability;
5. verifica privilegi necessari, senza modificare lo schema;
6. introspeziona sorgente/destinazione;
7. confronta schema remoto e fingerprint attesi;
8. risolve indici di colonna e mapping;
9. seleziona cursor, fetch size, binder/bulk path;
10. renderizza SQL e bind layout;
11. sceglie profilo transazionale e strategia staging;
12. stima memoria, round trip e spazio temporaneo;
13. produce `PhysicalDatabasePlan`.

Se una verifica richiede necessariamente una mutazione, appartiene a
`execute` e deve essere reversibile o registrata nello stato transazionale.

Il piano fisico non è serializzato come contratto pubblico: è valido per una
specifica sessione, versione, schema e istante.

### 6.3 `execute`: read path

Il percorso di lettura:

```text
open cursor
  → fetch bounded
  → decode colonne
  → validate batch dinamico
  → attach contract/sequence
  → yield GovernedBatch
  → consumer backpressure
  → close cursor/session
```

Proprietà:

- il primo batch non viene pubblicato prima che schema e mapping siano validi;
- memoria non proporzionale al totale delle righe;
- il consumer lento limita i fetch;
- `max_input_rows`, byte, batch e payload sono verificati incrementalmente;
- l'ordine è quello dichiarato dal piano; senza `order_by` non viene promesso;
- una lettura interrotta dopo output parziale restituisce errore terminale: il
  chiamante sa quanti batch ha ricevuto, ma non può considerarli snapshot
  completo;
- retry automatico dopo aver emesso righe è vietato, salvo protocollo
  resumable esplicito e semanticamente provato.

### 6.4 `execute`: write path

Il percorso generale:

```text
preflight completato
  → begin transaction/staging
  → consume GovernedBatch
  → validate + bind/encode
  → write bounded chunk
  → verify counts/checks
  → finalize indexes/constraints
  → commit or swap
  → classify outcome
  → cleanup
```

Il writer non riceve un `Vec<RecordBatch>` ma uno stream con backpressure.
Ogni batch viene contabilizzato fino al completamento del protocollo driver.

Modalità:

- `create`: crea un nuovo oggetto e fallisce se esiste;
- `append`: inserisce nello schema esistente;
- `replace`: costruisce staging e sostituisce secondo capability;
- `truncate_insert`: truncate esplicito seguito da caricamento;
- `update`: aggiorna tramite chiavi;
- `upsert`: insert/update deterministico tramite chiavi;
- `delete_by_keys`: elimina solo le chiavi ricevute.

Nel provider PostgreSQL il percorso è suddiviso in unità con autorità limitata:

| Unità | Responsabilità | Non può decidere |
|---|---|---|
| `catalog.rs` | conversione fail-closed delle righe di catalogo e metadati avanzati di relazioni, vincoli, indici, policy e privilegi | dichiarare valida una cache, scegliere una connessione o modificare oggetti |
| `catalog/schema.rs` | lettura colonne e token strutturali esatti, inclusi tipi avanzati e CRS PostGIS | riusare un token senza confronto esatto o aggiornare la cache |
| `catalog/capabilities.rs` | rilevazione versione PostgreSQL/PostGIS e documento capability | dichiarare capability non osservate dal server |
| `catalog/listing.rs` | enumerazione deterministica di cataloghi, schemi e oggetti | descrivere strutture, aggirare lo schema richiesto o alterare il catalogo |
| `connection.rs` | validazione DSN/rete/timeout, trust TLS/mTLS, fingerprint e apertura della connessione | gestire il pool, eseguire query o esporre materiale crittografico |
| `parameter_codec.rs` | inferenza conservativa dei tipi bind e conversione `ParameterValue` → `ToSql` | renderizzare SQL, scegliere fast path/fallback o aprire sessioni |
| `pool.rs` | semaphore bounded, checkout, riuso, invalidazione e restituzione RAII delle sessioni | costruire TLS/DSN, eseguire query o rendere riutilizzabile una sessione incerta |
| `query_plan.rs` | piano immutabile PostgreSQL per projection, filtri, SQL di dialetto e nomi dei bind | aprire connessioni, leggere valori dei parametri o eseguire fallback |
| `query_execution.rs` | bind PostgreSQL, fast path tipizzato, fallback prepared, cancellazione e consegna allo stream | modificare AST/SQL, interpolare valori o dichiarare riutilizzabile una sessione incerta |
| `preflight.rs` | esistenza e modo del target, compatibilità schema, chiavi, CRS authority e `LossReport` prima della mutazione | eseguire write, mutare schema/cache o attenuare una perdita non ammessa |
| `read_stream.rs` | backpressure Arrow, lease di risorse, limiti geometrici, deadline e cancellazione server-side | costruire SQL, scegliere parametri o riutilizzare sessioni incerte |
| `schema_cache.rs` | token strutturali, LRU bounded, invalidazione e recovery da poisoning | decidere quando interrogare il catalogo o dichiarare valido uno schema remoto |
| `write.rs` | orchestrazione, confini di transazione e ordine delle fasi | formato dei tipi o politica di recovery in autonomia |
| `write/plan.rs` | compilazione immutabile del contratto colonna | eseguire SQL o consumare batch |
| `write/recovery.rs` | cancellazione backend, rollback e classificazione dell'esito incerto | avviare o confermare una scrittura |
| `write/resources.rs` | prenotazione e commit dei lease di righe, byte, memoria e componenti geometrici | oltrepassare il budget o cambiare la transazione |
| `write/sql.rs` | DML, quoting, placeholder e dichiarazioni PostgreSQL | leggere valori Arrow |
| `write/value_codec.rs` | COPY text e rappresentazioni condivise di array, range e composite | pubblicare, fare commit o scegliere la modalità di scrittura |
| `write/binary_codec.rs` | COPY binary, tipi wire PostgreSQL ed encoding numerico deterministico | eseguire SQL, scegliere il piano o dichiarare l'esito |
| `write/prepared_codec.rs` | binding tipizzato Arrow → parametri prepared | eseguire SQL, fare fallback o mutare il contratto colonna |

Questi confini sono safety boundaries: l'autorità di dichiarare `Committed` resta
nell'orchestratore, mentre recovery e risorse possono soltanto restringere
l'esito o fallire. Gli encoder text, binary e prepared restano distinti perché
costituiscono tre implementazioni differenziali dello stesso risultato.

L'AST relazionale, la validazione strutturale e il renderer multi-dialetto
restano nei crate condivisi. Ogni driver mantiene invece piani ed esecuzione
specifici.

Il crate `plenora-db-sqlserver` usa direttamente Tiberius/TDS e, nelle fasi
offline iniziali, separa:

| Unità | Responsabilità |
|---|---|
| `config.rs` | configurazione strutturata, redazione credenziali, TLS required e opt-out esplicito |
| `connection.rs` | apertura TCP/TDS, bootstrap drenato, timeout e quarantena |
| `session.rs` | invarianti di sessione e criterio di riuso |
| `pool.rs` | capacità bounded, checkout cancellabile e rientro solo `Ready` |
| `recovery.rs` | macchina a stati pura per transazione ed esito ambiguo |
| `error.rs` | classificazione nativa redatta e `Unknown` durante commit incerto |
| `catalog/probe.rs` | versione, edition, compatibility level, tipi spatial e listing visibile |
| `catalog/schema.rs` | colonne, identity/computed, vincoli, indici e token strutturale |
| `types.rs` | piano immutabile SQL Server→Arrow e proiezioni T-SQL exact/fail-closed |
| `arrow.rs` | decoder checked scalari, decimal e temporali senza panic |
| `read.rs` | stream TDS/Arrow bounded, budget, preflight spatial e schema guard |
| `write/plan.rs` | mapping Arrow→target, SQL prepared e capability fail-closed |
| `write/codec.rs` | bind TDS checked, temporali, Decimal128 e WKB spatial |
| `write/resources.rs` | lease batch per righe, memoria, output e geometrie |
| `write/mod.rs` | lock target, schema guard, transazione, rollback e outcome |

Il provider SQL Server riusa i contratti condivisi ma non dipende da
`tokio-postgres`, dai tipi wire PostgreSQL o dalle funzioni PostGIS. Probe,
catalogo, codec Arrow e read streaming XY per `geometry`/`geography` sono
verificati sul riferimento SQL Server 2022. Anche prepared write
`append`/`truncate_insert`, rollback e schema drift guard sono provati live.
I confini pre-commit, perdita trasporto e conferma commit persa sono coperti da
fault deterministici interni ai test: nessun hook entra nell'API pubblica.
Un proxy TCP di test prova inoltre il taglio fisico del socket TDS durante
write e dopo il commit server ma prima della conferma client, oltre al blackhole
durante read e dopo rollback server. Bulk, le altre modalità di write,
latenza/packet loss su read/rollback e i profili spatial avanzati restano unità
distinte e diventeranno capability solo dopo la relativa prova live.

`replace` non significa automaticamente atomic rename. L'output dichiara la
garanzia realmente ottenuta.

### 6.5 Stato transazionale

La macchina a stati minima:

```text
NotStarted
  → SessionReady
  → TransactionBegun
  → StagingPrepared
  → Writing
  → Finalizing
  → CommitRequested
      ├── Committed
      ├── RolledBack
      └── OutcomeUnknown
```

`OutcomeUnknown` è obbligatorio quando, dopo `CommitRequested`, il client perde
la connessione senza una prova affidabile dell'esito.

```rust
pub enum WriteOutcome {
    Committed(CommitReport),
    RolledBack(RollbackReport),
    OutcomeUnknown(RecoveryReport),
    PartiallyCommitted(PartialCommitReport),
}
```

`PartiallyCommitted` è possibile solo per un profilo esplicitamente
non-atomico, come commit per chunk. Non è un successo pieno.

`RecoveryReport` contiene esclusivamente identificatori non segreti:

- execution/idempotency key;
- database object;
- staging object, se presente;
- ultima fase certa;
- conteggi confermati;
- istruzioni macchina per verifica/cleanup;
- causa sanitizzata.

### 6.6 Profili di atomicità

- `ReadOnly`: nessuna mutazione;
- `SingleTransaction`: tutte le modifiche in una transazione;
- `StagedSwap`: caricamento staging e publish finale secondo capability;
- `ChunkCommitted`: commit per chunk, esplicitamente parziale;
- `BestEffortDdl`: solo se richiesto per database con DDL non transazionale.

Il default per una scrittura è il profilo più forte compatibile con
l'operazione, senza degradazione silenziosa. Se il profilo richiesto non è
supportato, il piano fallisce prima della mutazione.

### 6.7 Retry e idempotenza

Il retry è una decisione semantica, non una reazione generica agli errori.

Retry consentito:

- connessione prima di aprire una transazione;
- read prima di emettere il primo batch;
- errori di serializzazione/deadlock quando l'intera transazione è
  ripetibile e nessun esito è incerto;
- chunk idempotente con protocollo provato.

Retry vietato:

- dopo `CommitRequested` con esito sconosciuto;
- dopo output di lettura senza resume token;
- su SQL nativo mutante non dichiarato idempotente;
- quando cambierebbe l'ordine o duplicherebbe righe;
- quando staging o identity non sono recuperabili.

Ogni write può ricevere una `IdempotencyKey`; il supporto concreto è capability
del driver/piano e non una promessa universale.

### 6.8 Schema drift

Tra `prepare` ed esecuzione lo schema può cambiare. La libreria:

- confronta identity/version token quando disponibile;
- valida ogni batch decodificato;
- classifica mismatch come `SchemaChanged`;
- non ricompila silenziosamente una scrittura già iniziata;
- può rifare `prepare` solo prima della prima mutazione.

---

## 7. Protocollo di esecuzione e risorse

### 7.1 Contesto unico

```rust
pub struct ExecutionContext {
    pub execution_id: ExecutionId,
    pub cancellation: CancellationToken,
    pub resources: Arc<ResourceGovernor>,
    pub temp_store: Arc<TempStore>,
    pub metrics: Arc<dyn MetricsSink>,
    pub secrets: Arc<dyn SecretProvider>,
}
```

Tutti i componenti ricevono handle allo stesso contesto.

### 7.2 Batch governati

```rust
pub struct GovernedBatch {
    pub batch: RecordBatch,
    pub lease: Arc<MemoryLease>,
    pub sequence: BatchSequence,
}
```

La quota segue il batch fino all'ultimo consumer. I buffer condivisi sono
contati una volta; buffer driver, builder Arrow, bind buffer e conversioni WKB
sono inclusi o stimati con margine dichiarato.

### 7.3 Backpressure

Code sempre bounded. Il ciclo non deve consentire:

- cursor che prefetcha senza limite;
- producer che accumula batch mentre il writer è lento;
- binder che conserva tutti i batch fino al commit;
- pool di connessioni usato come coda illimitata;
- LOB caricati integralmente senza limite esplicito.

Un task non attende memoria mantenendo risorse revocabili che impediscono il
progresso globale. Worker CPU, I/O di rete e I/O di spill sono separati.

### 7.4 Connessioni

Una sessione ha ownership chiara e non viene usata concorrentemente se il
driver non lo garantisce. Il pool:

- ha limite globale e per endpoint;
- applica timeout di acquire;
- valida le connessioni restituite;
- scarta sessioni in stato transazionale incerto;
- azzera stato di sessione controllabile;
- non espone credenziali nelle chiavi o metriche;
- non apre una connessione per batch.

### 7.5 Cancellazione

Il catalogo dichiara:

- `Cooperative`;
- `BoundaryOnly`;
- `DriverCancel`;
- `NonInterruptible`.

La cancellazione:

1. impedisce nuovi fetch/chunk;
2. invoca cancel del driver se sicuro;
3. drena/chiude le code;
4. tenta rollback quando l'esito è noto;
5. classifica l'esito senza inventarlo;
6. pulisce staging e temporanei recuperabili;
7. rende osservabile la latenza del driver non interrompibile.

### 7.6 Panic e crash

I panic intercettabili al confine sono convertiti in errore interno e avviano
cancellazione/rollback. `catch_unwind` non copre abort, OOM, kill esterni o
crash nativi.

Le scritture complesse usano:

- nomi staging derivati da `execution_id`;
- tabella/record di recovery opzionale;
- heartbeat diagnostico;
- cleanup idempotente;
- scavenging conservativo che verifica prima lo stato remoto.

Lo scavenger non elimina un oggetto staging solo perché vecchio: deve provare
che nessuna esecuzione attiva lo possiede.

---

## 8. Sicurezza

### 8.1 Segreti

I segreti entrano solo tramite `SecretProvider` runtime. Sono tipi redacted,
non serializzabili e con `Debug` oscurato.

Vietato:

- credenziali nel JSON;
- DSN completo negli errori;
- SQL con parametri interpolati nei log;
- dump di batch;
- WKB/WKT nei messaggi;
- token in metric labels;
- password in environment snapshot diagnostici.

### 8.2 SQL injection

Identificatori e valori sono tipi distinti. Un identificatore non è mai un bind
parameter: viene validato come componente e quotato dal dialect. I valori non
sono concatenati.

Liste, bulk values, limiti e funzioni spatial rispettano il massimo numero di
bind del server. Nessun fallback a interpolazione.

### 8.3 Privilegi minimi

Il remote preflight calcola i privilegi richiesti dall'operazione. La libreria
non richiede privilegi amministrativi generici se bastano `SELECT`, `INSERT`,
`UPDATE`, `CREATE` su uno schema specifico.

### 8.4 SQL nativo

`database.native_query` è esplicitamente meno portabile. Deve essere
policy-gated e separare:

- query singola read-only;
- statement mutante;
- script multi-statement.

Il parser locale non può provare la semantica di tutti i dialect. Quando non
può provare che una query è read-only, la tratta come mutante.

---

## 9. Introspezione

Il modello normalizzato include:

- server/catalog/schema;
- tabelle, viste, materialized view, synonym dove applicabile;
- colonne e tipi nativi;
- primary/unique/foreign key;
- check constraint;
- indici, inclusi spaziali;
- identity/sequence/generated columns;
- default;
- statistiche disponibili con confidenza;
- geometrie: tipo, SRID, dimensioni, geometry/geography;
- capability di lettura/scrittura per oggetto.

L'introspezione conserva anche un blocco vendor-specific tipizzato/versionato,
senza obbligare il modello comune a fingere equivalenze inesistenti.

Risultati catalogo grandi sono streamabili o paginati e soggetti a limiti.

---

## 10. Compatibilità e versionamento

Sono versionati separatamente:

- schema del piano;
- catalogo operazioni;
- config schema di ogni operazione;
- mapping tipi per driver;
- contract analysis;
- renderer SQL;
- protocollo recovery;
- formato metriche;
- API Rust del workspace.

Il fingerprint deriva da versioni esplicite, non dall'hash del binario.

Alias e migrazioni:

- immutabili per `schema_version`;
- deterministici;
- idempotenti;
- coperti da golden test;
- mai dipendenti dal server remoto.

La compatibilità server è una matrice pubblica per driver/versione/capability,
non un generico “supporta Oracle” o “supporta MySQL”.

---

## 11. Fasi di lavoro

### Fase 0 — Contratti e baseline

- estrarre inventario delle funzioni dal backend Plenora;
- congelare semantica osservata e test differenziali;
- definire piano, contratti, errori, limiti e capability;
- misurare baseline Python e round trip minimi dei driver;
- scrivere ADR fondamentali.

### Fase 1 — Core, SQL e testkit

- workspace e lint;
- Arrow/GeoArrow-WKB;
- mapping policy e `LossReport`;
- AST SQL;
- trait driver;
- testkit di conformità;
- parser/validator del piano.

### Fase 2 — PostgreSQL/PostGIS end-to-end

- introspezione;
- read streaming;
- append/create;
- replace staging;
- update/upsert;
- spatial;
- transazioni, cancellazione e recovery;
- benchmark e confronto con backend.

Stato del riferimento PostgreSQL/PostGIS avanzato:

- `QueryOperation` copre CTE ricorsive, derived table, join/lateral,
  `DISTINCT ON`, subquery, aggregate, group-by/having, window, set operation,
  offset/limit e locking;
- pool condiviso bounded con reset sessione e timeout di acquisizione;
- COPY text e binario, più percorso prepared;
- catalogo strutturale per relation, partizioni, viste/materialized view,
  enum/domain/collation, constraint, indici/opclass/include/predicati, RLS,
  ACL, owner e tablespace;
- `PostgresSchemaToken` con OID e fingerprint strutturale, cache LRU bounded e
  validazione catalogo su ogni hit;
- protocollo one-shot tipizzato per read senza bind e filtri parametrizzati
  built-in/spatial, con fallback prepared per tipi custom o ambigui;
- `QueryOperation` one-shot indipendente dalla forma dell'AST: schema dalla
  prima riga, describe senza riesecuzione per risultati vuoti e fallback
  prepared se PostgreSQL rifiuta i tipi canonici;
- validazione iterativa e bounded dell'AST relazionale prima di qualunque
  rendering ricorsivo;
- 72 funzioni PostGIS tipizzate e operatori bounding-box/KNN indicizzabili,
  senza frammenti SQL utente;
- EWKB fail-closed e metadata GeoArrow per XY/XYZ/XYM/XYZM, curve, surface,
  collection, TIN e geography;
- Arrow nativo per array scalari, `TIME`, `INTERVAL`, range e composite;
- cancellazione server-side read/write, keepalive e sessioni incerte escluse
  dal pool;
- evoluzione schema opt-in limitata a nuove colonne nullable transazionali.
- safety case hazard→invariante→prova e gate dedicato al piano GiST/KNN;
  il profilo non equivale a una certificazione aeronautica.

PostgreSQL è il driver pilota, non il luogo in cui inserire assunzioni comuni.

### Fase 3 — MySQL, SQL Server e SQLite

- port dei dialect;
- mapping e bulk path;
- geometry/geography;
- matrice differenziale;
- fault injection.

Stato corrente SQL Server:

- `SqlServerProvider` implementa il trait comune senza conservare secret nei
  piani e riusa un pool bounded partizionato dal fingerprint del secret;
- il testkit comune verifica connection, capability e introspezione sul
  riferimento SQL Server 2022;
- `ReadOperation` supporta projection, filtri non-spatial con bind TDS,
  ordering e `TOP`, mantenendo schema token e preflight spatial;
- `QueryOperation` usa la validazione iterativa comune e supporta CTE, join,
  aggregate/group/having, window, set operation e offset/fetch;
- lo schema Arrow dell'output è derivato una volta tramite
  `sys.dm_exec_describe_first_result_set` e confrontato fail-closed con il
  primo `COLMETADATA` TDS, anche quando il risultato contiene zero righe;
- output spatial UDT nudo, lateral/APPLY, locking e projection calcolate senza
  alias deterministico restano chiusi finché non esiste una prova dedicata;
- `append` e `truncate_insert` attraversano il trait comune senza duplicare i
  resource lease tra prepare ed execute;
- la suite live seriale copre 23 test, inclusi fault TDS fisici, contratto
  comune, rich query e read/query/write attraverso l'API provider.

### Fase 4 — Oracle e Db2

- client/runtime supportati;
- mapping LOB/decimal/time;
- Oracle Spatial e Db2 Spatial;
- merge/upsert e staging;
- test su infrastruttura licenziata;
- capability downgrade espliciti.

### Fase 5 — ArcGIS Online/Enterprise

- autenticazione e discovery;
- introspezione service/layer;
- read paginato Arrow;
- write feature e multi-layer;
- conversione Esri JSON/WKB;
- replace/lifecycle;
- rate limit, retry e outcome parziali;
- benchmark HTTP e fault injection.

### Fase 6 — DuckDB e maturità operativa

- driver embedded;
- spatial extension;
- hardening pool/governor;
- benchmark cross-driver;
- recovery/scavenging;
- API stabilization.

---

## 12. ADR richiesti

1. Confine Arrow e compatibilità delle versioni.
2. Contratto geometrico, CRS, Z/M e geometry/geography.
3. Mapping tipi e `LossReport`.
4. AST SQL, quoting e policy SQL nativo.
5. Capability model e probing remoto.
6. Separazione `ValidatedDatabasePlan` / `PhysicalDatabasePlan`.
7. Stato transazionale, `OutcomeUnknown` e recovery.
8. Profili di atomicità e staging/swap per driver.
9. Retry, idempotenza e letture resumable.
10. Resource accounting, backpressure e connessioni.
11. Cancellazione, panic, crash e cleanup.
12. Determinismo, ordering e conteggi.
13. Fingerprint, versionamento e migrazione.
14. Sicurezza dei segreti e redazione.
15. Semantica dei limiti.
16. Benchmark e budget di regressione.

---

## 13. Invarianti

Criteri di accettazione verificabili:

1. Nessun piano non validato raggiunge un driver.
2. `validate` non apre rete, non legge segreti e non produce side effect.
3. Nessuna mutazione avviene prima del remote preflight.
4. Nessuna credenziale o valore dati appare in errore, log o metrica.
5. Identificatori e valori SQL non condividono lo stesso tipo/percorso.
6. Nessuna coercizione, riproiezione o perdita è implicita.
7. Una capability non provata non può essere usata.
8. Un read streaming non materializza l'intero risultato.
9. Un write streaming non materializza l'intero input.
10. Ogni batch e buffer driver ha ownership contabile.
11. Nessuna coda, prefetch o pool è illimitato.
12. Nessun retry può duplicare effetti o righe.
13. Una disconnessione dopo richiesta di commit non viene classificata come
    rollback senza prova.
14. Un profilo atomico non degrada silenziosamente a commit parziale.
15. Nessun errore o panic intercettabile viene riportato come `Committed`.
16. Una sessione con stato incerto non torna nel pool.
17. Nessun driver/provider pubblicizza capability non coperte dal testkit.
18. Il risultato semantico non dipende da fetch size o parallelismo.
19. L'ordine è garantito solo se dichiarato e realizzato dal piano.
20. Le regressioni oltre il budget di `Prestazioni.md` bloccano il rilascio.

---

## 14. Rischi e mitigazioni

| Rischio | Mitigazione |
|---|---|
| API dei driver Rust molto diverse | trait semantico + adapter sync/async + testkit |
| Assunzioni PostgreSQL nel core | engine solo su capability; review cross-driver |
| SQL injection tramite identificatori | AST, tipo `Identifier`, quoting del dialect |
| Segreti nei log | tipi redacted e test automatici su error chain |
| Mapping lossy invisibile | policy esplicita + `LossReport` |
| Decimal/timestamp corrotti | boundary test e round-trip per driver |
| CRS/SRID interpretato male | metadata espliciti, nessuna default inference |
| DDL non transazionale | capability + profilo atomicità dichiarato |
| Commit riuscito ma risposta persa | `OutcomeUnknown` + recovery protocol |
| Retry che duplica righe | retry gate semantico e idempotency key |
| Schema cambia dopo prepare | schema token + validazione incrementale |
| Cursor/prefetch usa memoria illimitata | fetch bounded e backpressure |
| Pool esaurito/starvation | limiti, timeout, ownership sessione |
| Bulk path cambia semantica | test differenziale bulk vs prepared path |
| Staging orfano dopo crash | naming execution-scoped + recovery/scavenger prudente |
| Privilegi insufficienti a metà write | preflight e capability probe prima della mutazione |
| Oracle/Db2 richiedono librerie native | feature isolate, runner dedicati, capability build-time |
| ArcGIS ha atomicità diversa per server/layer | capability `applyEdits` + outcome per batch/layer |
| Rate limit e token ArcGIS | retry bounded con `Retry-After`, refresh single-flight, redazione |
| Funzioni spatial divergenti | WKB canonico + matrice geometrica differenziale |
| SQL nativo elude garanzie | operazione separata e policy-gated |
| Benchmark solo localhost | profili latenza/banda e dataset remoti controllati |
| “Supporto database” troppo generico | matrice pubblica server/versione/capability |

---

## 15. Decisioni registrate

I numeri non vengono riassegnati.

- **D0 — Black box autoconsistente.** Nessuna dipendenza dagli altri progetti
  Plenora; interoperabilità attraverso Arrow/GeoArrow-WKB.
- **D1 — Arrow-in / Arrow-out.** Unica rappresentazione tabellare pubblica.
- **D2 — GeoArrow-WKB al confine.** Formati nativi solo dentro i driver.
- **D3 — API type-state `validate → execute`.** `prepare` è interno e
  per-esecuzione.
- **D4 — `validate` puro.** Nessuna rete o segreto.
- **D5 — Remote preflight prima delle mutazioni.**
- **D6 — Workspace multi-crate con driver isolati.**
- **D7 — Engine capability-driven.** Nessun branching sul vendor nel core.
- **D8 — SQL AST e bind obbligatori.** SQL nativo come escape hatch separato.
- **D9 — Mapping versionato con policy e `LossReport`.**
- **D10 — Nessuna riproiezione/coercizione implicita.**
- **D11 — Una destinazione transazionale per piano v1.**
- **D12 — Streaming bounded in entrambe le direzioni.**
- **D13 — `ValidatedDatabasePlan` semantico e
  `PhysicalDatabasePlan` per sessione.**
- **D14 — Stato transazionale esplicito con `OutcomeUnknown`.**
- **D15 — Atomicità per profili, senza downgrade silenzioso.**
- **D16 — Retry solo quando semanticamente sicuro.**
- **D17 — Resource governor e lease seguono batch e buffer driver.**
- **D18 — Pool, rete, CPU, spill e cancellazione sono un unico protocollo.**
- **D19 — Capability effettiva come intersezione build/server/privilegi/policy.**
- **D20 — Segreti solo tramite provider runtime e sempre redatti.**
- **D21 — Catalogo e mapping con versioni esplicite e fingerprint.**
- **D22 — Contratti estensibili a più geometrie e XY/XYZ/XYM/XYZM; supporto
  concreto fail-closed.**
- **D23 — SQL senza `ORDER BY` non promette ordine.**
- **D24 — Prestazioni e memoria sono criteri di accettazione.**
- **D25 — PostgreSQL/PostGIS è driver pilota, non modello implicito del core.**
- **D26 — ArcGIS è un provider Feature Service nativo.** Condivide engine e
  contratti ma non viene modellato come dialect SQL né come transazione SQL.

Principio finale:

> `plenora-database-tools` deve rendere uniformi le garanzie, non fingere che
> database diversi abbiano la stessa semantica.
