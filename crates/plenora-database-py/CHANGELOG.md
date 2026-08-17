# Changelog — plenora-database Python SDK

Tutte le release seguono [semver](https://semver.org). Fino alla v1.0.0
l'API è considerata "prevista stabile ma non frozen" — breaking change
sono possibili in una minor (`0.x → 0.x+1`) e sempre documentati qui.

I wheel di ogni release sono allegati come asset alla release GitHub
corrispondente. Il tag ha il prefisso `py-` (es. `py-v0.1.3`) per non
confondersi con il ciclo di release del Rust workspace (che usa tag
`vX.Y.Z` senza prefisso).

---

## [0.10.0] — 2026-08-17

Contratto `Replace` su **entrambi** i provider, TLS fail-closed nei comandi
legacy del CLI, e piani che vengono rifiutati invece che accettati e ignorati.
La minor non è cosmetica: `Replace` fa una cosa diversa da prima su
PostgreSQL e su MySQL, e tre superfici che accettavano piani senza onorarli
ora li respingono. Le note di migrazione sono in fondo, divise per superficie.

Include anche `NativeQueryPolicy` MySQL e il `SessionContext` MySQL
funzionante — commit successivi al tag `py-v0.9.2`, quindi non presenti negli
artifact 0.9.2.

### 🚨 BREAKING — PostgreSQL: `Replace` scrive nel target esistente

Prima `Replace` costruiva una tabella di staging con lo schema del piano, ci
scriveva dentro, faceva `DROP TABLE` dell'originale e `ALTER TABLE ... RENAME`.
Il risultato aveva le righe giuste e nient'altro: object identity nuova, e
indici, foreign key, trigger, check, default, grant, sequence e opzioni della
tabella spariti insieme all'originale. Il preflight lo sapeva e si toglieva di
mezzo — `Replace` saltava sia il controllo di esistenza sia il confronto
colonna per colonna — quindi un target inesistente non era un errore ma un
invito a fabbricarne uno con lo schema parziale del piano.

Ora `Replace` è ciò che il contratto dice: **target già esistente,
`DELETE FROM`, bulk insert, tutto nella stessa transazione**.

- il target **deve esistere**: un target assente è `PlenoraNotFoundError`.
  Solo `create` può trovarlo assente;
- il target **non viene ricreato**: identity, indici, unique, foreign key,
  trigger, check, default, grant, sequence e `reloptions` sono quelli di
  prima della scrittura, e i trigger vedono le righe nuove;
- il target passa dal confronto colonna per colonna, come `append`;
- un errore o una cancellazione **dopo il `DELETE`** ripristinano le righe
  precedenti: è tutto nella stessa transazione, e su PostgreSQL non c'è
  nessun commit implicito che sopravviva al rollback;
- `truncate_insert` resta su `TRUNCATE`, che su PostgreSQL è transazionale:
  la differenza fra le due modalità non è più la sicurezza, è il costo.

I test live di `crates/plenora-db-postgres/tests/golden_write_replace.rs`
fissano il contratto su una fixture con identity PK, default, CHECK, unique
index, foreign key, trigger e grant, confrontando l'impronta di `pg_class.oid`,
`pg_constraint`, `pg_indexes`, `pg_trigger`, `relacl`, `reloptions` e
`last_value` della sequence prima e dopo.

### Contratto Replace/TruncateInsert — MySQL passa a 6 modalità su 7

`write_mode="replace"` torna disponibile su MySQL, ma con una semantica
diversa da quella che aveva prima di essere rimossa: **`DELETE FROM` +
INSERT bulk nella stessa transazione InnoDB**, non più staging persistente
+ `RENAME TABLE` + tabella di backup.

La differenza è osservabile:

- il target **deve già esistere** — un target assente è `PlenoraNotFoundError`,
  non un invito a crearlo. Chi vuole creare la tabella usa `"create"`;
- il target **non viene ricreato**: object identity, indici, unique, foreign
  key, trigger, check, default, grant e `AUTO_INCREMENT` sono quelli di prima
  della scrittura. Il vecchio pattern li perdeva tutti, perché la tabella
  pubblicata era una tabella nuova costruita dallo schema Arrow;
- un errore, una cancellazione o un mapping failure **dopo il DELETE**
  annullano tutto e riportano le righe precedenti. Prima il `RENAME` faceva
  commit implicito, quindi lo swap sopravviveva al rollback.

`write_mode="truncate_insert"` **resta fail-closed** su MySQL con
`PlenoraUnsupportedError`. `TRUNCATE TABLE` è DDL con commit implicito: le
righe sparirebbero prima dell'INSERT e nessun rollback le riporterebbe
indietro. Non viene emulata con `DELETE` perché sarebbe un'altra cosa con lo
stesso nome — `AUTO_INCREMENT` non azzerato, trigger e log riga per riga
attivi. Il messaggio di rifiuto rinvia esplicitamente a `"replace"`, e il
rifiuto arriva in `prepare_write`, prima del checkout dal pool e quindi
prima di qualunque effetto remoto.

Su PostgreSQL `"truncate_insert"` resta disponibile e usa un vero
`TRUNCATE`: lì è transazionale, quindi rollback-safe.

Le capability pubblicate da `capabilities()` riflettono le modalità
effettivamente disponibili: su MySQL `create`, `append`, `update`, `upsert`,
`replace`, `delete_by_keys` e `bulk` passano a `true`; `staged_swap` resta
`false`, perché nessuna modalità pubblica più una tabella al posto di
un'altra.

### Fix P1 — Upsert MySQL fail-closed sugli unique index

`INSERT ... ON DUPLICATE KEY UPDATE` scatta su **qualsiasi** PRIMARY
KEY / UNIQUE index in conflitto, non solo sulle `keys` dichiarate. Con
un secondo unique index una riga in ingresso poteva collidere
sull'indice sbagliato e aggiornare **silenziosamente la riga
sbagliata**.

Il preflight Upsert ora verifica (fail-closed, prima di aprire la
transazione, sia in `prepare` sia in `write`):
1. esiste un PK/UNIQUE index le cui colonne coincidono (come insieme)
   con le `keys` — l'ancora del match;
2. **nessun altro** PK/UNIQUE index con colonne diverse;
3. nessun unique index funzionale/espressione (non confrontabile).

Fuori da questi vincoli l'Upsert ritorna `PlenoraUnsupportedError`.

Novità di supporto:
- `MysqlObjectDescription` ora espone `indexes: [MysqlIndex]`
  (name/unique/column_backed/columns), letti da
  `information_schema.statistics`.
- Lo `schema_token` include gli indici: una modifica agli indici fra
  `prepare` ed esecuzione cambia il token e non passa inosservata.

### Fix — Upsert keys-only idempotente

Un Upsert con schema di sole colonne key non degrada più a un INSERT
nudo (che erra sul primo duplicate key): rende
`ON DUPLICATE KEY UPDATE k=k` (no-op) per semantica insert-or-ignore.

### 🚨 BREAKING — CLI: TLS MySQL fail-closed nei comandi legacy `mysql-*`

I comandi `mysql-*` degradavano silenziosamente a `TrustServerCertificate`
quando non veniva passata una CA: TLS attivo, **nessuna verifica** del
certificato del server. Il path moderno `database-probe mysql` era già
fail-closed; ora lo è anche il parser legacy.

Senza flag il certificato è verificato contro le CA di sistema. I due flag
sono order-independent e mutuamente esclusivi:

- `--tls-ca-path-env <name>` — CA privata, letta dal file indicato dalla
  variabile;
- `--tls-insecure-skip-verify` — opt-in esplicito, nessuna verifica, per
  dev/test.

### TLS — nessuna variabile del CLI viene più letta a metà

`postgres_provider_for_pfm` usciva presto due volte: sull'interruttore
insicuro prima di leggere qualsiasi cosa, e sull'assenza della CA prima di
leggere certificato e chiave. Chi impostava l'identità client dimenticando la
CA otteneva un provider **senza mTLS e senza avviso**: credeva di avere un
canale autenticato e non l'aveva.

Ora `PLENORA_PG_CA_PATH`, `PLENORA_PG_CLIENT_CERT_PATH`,
`PLENORA_PG_CLIENT_KEY_PATH` e `PLENORA_TLS_INSECURE_LOCAL` vengono lette
tutte prima di scegliere, e le combinazioni incoerenti sono errori:

- identità client senza CA — il certificato resterebbe inutilizzato;
- certificato senza chiave, o viceversa;
- interruttore insicuro insieme a materiale TLS — le due richieste si
  contraddicono, e indovinare quale vince sarebbe peggio che fermarsi.

Nello stesso passaggio è caduta una regressione introdotta durante il ciclo:
le variabili venivano lette con l'helper di `database-probe`, che tratta
"nome indicato ma variabile assente" come errore del chiamante, quindi **ogni
comando senza CA privata falliva** in `validate` — cioè proprio la
configurazione di produzione, quella che verifica contro i root pubblici. Non
la copriva nessun test, perché tutti impostavano o l'interruttore insicuro o
la CA privata.

### Piani rifiutati invece che accettati e ignorati

Tre superfici accettavano un piano e non lo onoravano. Un piano ignorato è
peggio di un piano rifiutato: il chiamante resta a credere che sia stato
applicato.

- **`keys` / `update_columns` su `Replace`** (PostgreSQL e MySQL):
  `PlenoraInvalidPlanError` in preflight, prima di qualunque contatto con il
  server. `Replace` non ha semantica di chiave; chi vuole una PRIMARY KEY usa
  `create`. Su MySQL erano già rifiutate, ma come `Unsupported` ("il provider
  non lo fa") invece di `InvalidPlan` ("la mode non significa questo, su
  nessun provider"), e il messaggio nominava `Append` anche quando la mode era
  un'altra.
- **`create_spatial_index` fuori da `Create`** (PostgreSQL):
  `PlenoraInvalidPlanError` per ogni mode diversa da `create`. Il flag emette
  `CREATE INDEX` senza `IF NOT EXISTS`: su un target che l'indice ce l'ha già
  — cioè qualunque target che non sia stato appena creato — funzionerebbe
  solo la prima volta.
- **PRIMARY KEY di `Create` con colonne nullable** (PostgreSQL e MySQL):
  `PlenoraInvalidPlanError` in preflight su entrambi. PostgreSQL coercizzava
  in silenzio — la tabella creata usciva con `NOT NULL` che lo schema Arrow
  non dichiarava — e MySQL rifiutava tardi, con l'errore server 1171 durante
  la scrittura, dentro il percorso che poi deve dichiarare cosa è rimasto sul
  server.

### SDK — Arrow: la normalizzazione scende nei tipi annidati

La conversione dei tipi a offset largo si fermava al primo livello:
`large_list<large_string>` diventava `list<large_string>`, che il writer
rifiuta esattamente come prima — la conversione sembrava fatta e non lo era.
Ora la ricorsione attraversa `list` **e** `struct`, e ogni campo figlio
conserva nome, nullability e metadata.

I metadata non sono un dettaglio: per i `composite` PostgreSQL **sono** il
contratto della colonna, e per i `range` distinguono un estremo aperto da un
estremo nullo. Ricostruire il campo con i default trasformerebbe una colonna
tipizzata in testo anonimo. È il caso che ha fatto fallire `copy_from` con
`pandas` 3 e `pyarrow` 25, che producono `large_string` dove le versioni
precedenti producevano `string`.

`acopy_from` non duplica più l'implementazione sincrona: due copie della
stessa cosa divergono alla prima modifica di una sola.

### SDK — MySQL: `SessionContext`, `NativeQueryPolicy`, indici

- **`SessionContext` ora funziona.** Il core impone chiavi `namespace.name`,
  quindi con un punto; MySQL scriveva `SET @plenora_ctx_{name}` e rifiutava
  ogni nome non alfanumerico. Le due validazioni erano mutuamente esclusive:
  una capability pubblicata che nessuna chiave valida poteva esercitare. La
  validazione delega ora al core, e un test live lega l'assert ai valori — la
  riga esiste solo se entrambe le variabili sono arrivate.
- **`NativeQueryPolicy` su MySQL**, parity con PostgreSQL: `begin` di
  `MysqlSession` e `AsyncMysqlSession` accetta `native_query_policy` e
  `context`, e `TransactionOptions::pfm_defaults()` è finalmente fail-closed
  su entrambi i provider.
- **`MysqlObjectDescription.indexes`** espone name/unique/column_backed/
  columns da `information_schema.statistics`, e lo `schema_token` li include:
  una modifica agli indici fra `prepare` ed esecuzione cambia il token e non
  passa inosservata.

### SDK — diagnostica: cosa è rimasto sul server

- un errore **dopo** un COMMIT riuscito non dichiara più che non è successo
  niente: il residuo passa da un punto unico, caso ambiguo incluso;
- un `Create` fallito dichiara la tabella rimasta invece di `RolledBack`;
- `execute_ddl` MySQL usa il text protocol e dichiara l'incertezza in volo;
- MySQL read batching: una riga che non entra nel batch apre il successivo
  invece di essere persa nel carry-over.

### Migrazione da 0.9.x

**PostgreSQL — `Replace`.** Se lo usavi per *creare* il target, passa a
`mode="create"`: ora un target assente è `PlenoraNotFoundError`. Se lo usavi
per *sostituire* il contenuto, non devi fare niente e ci guadagni: indici,
vincoli, trigger, grant e identity sopravvivono alla scrittura, e un errore a
metà stream riporta le righe di prima. Se passavi `keys`, `update_columns` o
`create_spatial_index` insieme a `Replace`, quei campi erano ignorati e ora
sono `PlenoraInvalidPlanError`: toglili, oppure sposta la creazione su
`create`. Se creavi tabelle con una PRIMARY KEY su colonne nullable nello
schema Arrow, dichiarale `not null`: prima Postgres correggeva in silenzio.

**MySQL — `Replace` e `TruncateInsert`.** `mode="replace"` torna disponibile
dopo essere stato rimosso in 0.9.1, ma **non** è il vecchio pattern staging +
`RENAME`: è `DELETE FROM` + insert nella stessa transazione, il target deve
esistere e non viene ricreato. Chi aveva adottato il workaround `begin()` +
`DELETE FROM` + `copy_from(mode="append")` può tornare a `mode="replace"`,
che fa la stessa cosa in una transazione sola. `mode="truncate_insert"` resta
`PlenoraUnsupportedError` su MySQL — `TRUNCATE` è DDL con commit implicito,
quindi non è rollback-safe — e il messaggio rinvia a `"replace"`. Su
PostgreSQL `truncate_insert` continua a funzionare. Se usavi `upsert` con più
di un unique index sul target, ora è fail-closed: dichiara `keys` che
coincidano con un solo indice, o passa da `update`.

**TLS.** Nessun default del SDK cambia in questa release: `connect()` verifica
il certificato del server dalla 0.9.0 (ADR-011) e `connect_mysql()` dalla
0.9.1, e `tls_mode="insecure_trust_server"` resta l'unico opt-out esplicito
sul lato MySQL. Cambia il
CLI, e cambia in modo fail-closed: i comandi `mysql-*` senza CA contro un
server con certificato non verificabile dalle CA di sistema ora **falliscono**
invece di fidarsi. Usa `--tls-ca-path-env <name>` con la CA privata, o
`--tls-insecure-skip-verify` solo in dev. Sul lato PostgreSQL del CLI, se
impostavi `PLENORA_PG_CLIENT_CERT_PATH` / `PLENORA_PG_CLIENT_KEY_PATH` senza
`PLENORA_PG_CA_PATH` non avevi mTLS pur credendo di averlo: ora è un errore
esplicito, e vanno impostate tutte e tre.

**CLI.** Oltre al TLS: i sottocomandi legacy accettano i flag in qualunque
ordine, e le combinazioni contraddittorie (CA privata insieme
all'interruttore insicuro) sono rifiutate invece di essere risolte a
indovinare. Se una pipeline si appoggiava al comportamento "senza CA mi fido
comunque", va aggiornata prima dell'upgrade.

**SDK.** Nessuna firma Python cambia in modo incompatibile. `copy_from` e
`acopy_from` accettano ora tabelle Arrow che prima venivano rifiutate —
`large_string`, `large_binary`, `large_list` e `struct` annidati, cioè ciò che
`pandas` 3 con `pyarrow` 25 produce di default — quindi un workaround di
`cast()` a monte diventa superfluo ma resta innocuo. `MysqlSession.begin`
accetta due parametri nuovi (`context`, `native_query_policy`), entrambi
opzionali. `MysqlObjectDescription` porta un campo nuovo, `indexes`: chi
serializza quella struttura con uno schema chiuso deve prevederlo.

---

## [0.9.2] — 2026-08-15

Fix pre-PyPI review. 0.9.1 aveva chiuso i 3 P0 MySQL ma il default
`copy_from` restava incompatibile col validator MySQL, gli stub
top-level MySQL erano incompleti, la doc citava "7 WriteMode" e il
probe MySQL non fail-closed su MariaDB.

### Fix P0 — `copy_from` MySQL default `mapping_policy="strict"`

Prima: wrapper Python passava `mapping_policy="compatible"` come
default. Il provider MySQL rifiuta con `PlenoraUnsupportedError`
(`"richiede MappingPolicy::Strict finche il loss preflight non e
qualificato"`). Ogni chiamata `s.copy_from(...)` senza override
esplicito falliva.

Ora default `"strict"` in `copy_from` sync + `acopy_from` async.
Il pyo3 signature default nel binding nativo è allineato.

### Fix P1 — Stub top-level MySQL

`plenora_database/__init__.pyi` ora esporta:
- `connect_mysql(host, database, user, password, port, tls_ca_pem, tls_mode) -> MysqlSession`
- `aconnect_mysql(...) -> AsyncMysqlSession`
- `MysqlSession`, `AsyncMysqlSession`, `SessionContext`,
  `PlenoraCommitOutcomeUnknownError` (mancanti pre-0.9.2).

Type checker (mypy, pyright, pylance) ora riconoscono correttamente
il MySQL SDK dall'import top-level.

### Fix P1 — Documentazione WriteMode MySQL

Docstring `copy_from` / `acopy_from` / `MysqlSession.copy_from`
(binding nativo) aggiornati:
- **5 modalità disponibili**: `append`, `create`, `upsert`, `update`,
  `delete_by_keys`.
- **2 fail-closed** (post 0.9.1): `replace`, `truncate_insert` con
  motivazione + workaround.

Prima dicevano "tutti 7 WriteMode" — legacy da 0.8.x.

### Fix P1 — Probe MySQL fail-closed su MariaDB

`probe_server` (`catalog.rs`) ora rileva MariaDB da `VERSION()` o
`@@version_comment` e ritorna `PlenoraUnsupportedError`. MariaDB
non è testato/qualificato: sequenze, ON DUPLICATE KEY, spatial
GEOMETRYCOLLECTION, prepared statement cache, isolation semantics —
tutti punti di divergenza silenziosa.

Il provider dedicato MariaDB resta in roadmap.

## [0.9.1] — 2026-08-15

Hardening MySQL. Chiude 3 findings **P0** identificati dalla review
MySQL post-0.9.0. Nessun cambio alle API Postgres.

### 🚨 BREAKING — MySQL write modes `Replace` e `TruncateInsert` rimossi

Entrambi erano **unsafe** su MySQL e producevano risultati diversi
dal contratto Plenora:

- **`Replace`** su MySQL usava pattern staging + `RENAME TABLE`
  atomico. Il problema: `build_create_table_sql` MySQL non ricrea
  indici secondari, foreign key, trigger, check constraint,
  tablespace, partizioni, permessi ACL. Il target dopo Replace
  perdeva metadati non riproducibili — silenzio strutturale.

- **`TruncateInsert`** usava `TRUNCATE TABLE` prima del bulk INSERT.
  `TRUNCATE` è DDL su MySQL/InnoDB e fa **commit implicito**: il
  rollback della transazione non ripristina i dati eliminati. Se
  il bulk INSERT successivo fallisce, il target resta vuoto.

Entrambi ora fail-closed `PlenoraUnsupportedError` con messaggio
esplicativo + workaround suggerito (`Create`+`Append`, o `Update`
con `DELETE FROM`).

**Workaround per Replace**:
```python
with s.begin() as tx:
    tx.execute("DELETE FROM t")             # rollback-safe (DML)
    s.copy_from("public", "t", data,
                mode="append")               # bulk
```

**Impatto**: consumer che si appoggia su questi due modi vede
`PlenoraUnsupportedError` all'invocazione. Nessun rollback runtime
richiesto — il codice fallisce prima di toccare il DB.

### Fix P0 — MysqlTransaction su deadlock

Prima del fix: se MySQL rollback la transazione lato server per
deadlock (errcode 1213) o timeout ambiguo, `MysqlTransaction`
manteneva `open = true`. Le scritture successive andavano in
**autocommit** — silent write fuori dalla tx supposta.

Ora `execute()` chiude la tx (`open = false`) se l'errore ha
`RemoteEffect::RolledBack` o `Unknown`. Le successive `execute` /
`commit` ricevono `PlenoraInvalidPlanError` "transazione già
chiusa".

### Fix P1 — MySQL SDK TLS secure-by-default (parity Postgres 0.9.0)

Prima: `connect_mysql(dsn, tls_ca_pem=None)` settava
`MysqlCertificatePolicy::TrustServerCertificate` come fallback —
TLS attivo ma **senza verifica del certificato server**
(vulnerabile a MITM).

Ora:
- Default `tls_mode="require"` = `MysqlCertificatePolicy::Verify`
  (WebPKI trust store pubblico o CA privata se `tls_ca_pem` passata).
- Opt-in esplicito `tls_mode="insecure_trust_server"` per test/dev
  locali (nome esplicito).

```python
# Prima (0.9.0)
s = p.connect_mysql("host", "db", "u", "p")                # ← TrustServerCertificate silente

# Dopo (0.9.1) — produzione
s = p.connect_mysql("host", "db", "u", "p")                # ← Verify WebPKI

# Dopo (0.9.1) — dev locale
s = p.connect_mysql("host", "db", "u", "p",
                    tls_mode="insecure_trust_server")
```

Parametro esteso anche a `aconnect_mysql`.

### Documentazione

- Docstring `connect_mysql` aggiornato con WriteMode residui +
  parametri TLS espliciti.
- Stub `.pyi` allineato.

## [0.9.0] — 2026-08-15

Security-hardening + PFM-ITS-DB-001 (CHG-001..004) + chiusura
audit review 2026-08-15 (12/15 findings originali + 7 findings
duplicazioni + 8 findings post-refactor + 6 findings post-PFM + 4
findings pre-release).

### 🚨 BREAKING — TLS secure-by-default (ADR-011)

`plenora_database.connect(dsn)` e `aconnect(dsn)` ora usano
**TLS `require`** (WebPKI trust store pubblico) per default. Prima
usavano TLS disabilitato.

**Impatto**: connessioni verso Postgres senza TLS (es. Docker
plaintext dev/staging) falliscono con `PlenoraIoError` al probe.

**Migrazione**:

```python
# Prima (0.8.x)
s = plenora_database.connect("host=localhost user=... dbname=...")

# Dopo (0.9.0) — produzione (default sicuro)
s = plenora_database.connect("host=...prod... user=... dbname=...")

# Dopo (0.9.0) — dev/test locale contro Docker senza TLS
s = plenora_database.connect(
    "host=localhost user=... dbname=...",
    tls_mode="insecure_local",
)
```

Valori supportati per `tls_mode`:
- `"require"` (default): TLS + WebPKI trust store pubblico.
- `"insecure_local"`: TLS disabilitato. **Solo per test/dev.**

Per **CA privata / mTLS**: costruire il provider Rust in-process (via
Rust binding low-level); `connect(dsn, tls_mode=...)` copre solo i
due preset più comuni.

Motivazione ADR-011: prima del fix, `probe_capabilities` (usato dal
setup) applicava TLS Require, mentre i comandi operativi PFM
applicavano Disabled — un endpoint che passava il probe poteva poi
essere connesso plaintext. Ora coerente.

### New — SessionContext transaction-local (CHG-002)

Nuova pyclass `plenora_database.SessionContext` per propagare al
database contesto della richiesta (tenant, actor, correlation_id,
ecc.) via `SET LOCAL` — transaction-local, no leak fra riusi della
connessione dal pool.

```python
import plenora_database as p

ctx = p.SessionContext()
ctx.insert_public("app.tenant_id", "42")
ctx.insert_internal("app.correlation_id", "req-abc123")
ctx.insert_sensitive("app.actor_email", "alice@example.com")

with p.connect(dsn) as s, s.begin(context=ctx) as tx:
    rows = tx.execute_returning_rows(
        "SELECT current_setting('app.tenant_id', true)"
    )
```

- `insert_public/internal/sensitive(name, value)` — value = str|int|bool
- `get(name)` / `classification(name)` / `keys()` / `__len__`
- Sensitive values → `[REDACTED]` in `Debug`/`repr`

### New — NativeQueryPolicy in begin (CHG-003)

`Session.begin()` e `AsyncSession.begin()` accettano
`native_query_policy="allow"|"deny"`.

Il modo `"deny"` restringe agli statement CRUD OLTP
(`SELECT`/`WITH`/`INSERT`/`UPDATE`/`DELETE`/`VALUES`/`TABLE`/`MERGE`)
+ rifiuta DDL, session commands, multi-statement.

```python
with p.connect(dsn) as s, s.begin(native_query_policy="deny") as tx:
    tx.execute("SELECT id FROM users")     # ok
    tx.execute("DROP TABLE users")         # PlenoraInvalidPlanError
```

**Nota**: `deny` è un classifier lessicale sul primo keyword. Non è
un parser SQL completo — funzioni amministrative come
`SELECT set_config(...)` passano. Va inteso come protezione da
errori accidentali, non come sandbox anti-adversarial.

### New — PlenoraCommitOutcomeUnknownError (CHG-004)

Nuova classe di eccezione dedicata per commit con esito ignoto
(disconnessione fra `COMMIT` e ACK, timeout, cancel mid-commit).

Estende `PlenoraInternalError` — consumer che filtrano su
`PlenoraError` o `PlenoraInternalError` continuano a intercettarla,
ma chi vuole gestire quarantine/recovery separatamente può filtrare
direttamente su `PlenoraCommitOutcomeUnknownError`.

Attributi aggiuntivi sull'istanza:
- `automatic_retry_allowed: bool` (sempre `False`)
- `recovery_action: str` (istruzione human-readable per verifica
  out-of-band)

`ErrorPhase` è ora sempre `Commit` (prima era `Write` in alcuni
path — bug: la fase incerta è il COMMIT non il DML).

### Hardening — Spatial policy + EWKB validation

- **`SpatialPredicate::Contains` / `Within` + `Geography`** → ora
  fail-closed `PlenoraUnsupportedError`. PostGIS espone quelle
  funzioni solo per `geometry`; prima producevano SQL invalido a
  runtime.
- **`DWithin { distance_meters }` + `Geometry` + SRID geografico
  (4326, 4269, 4267, 4258, 4283)** → fail-closed `PlenoraInvalidPlanError`.
  Su quei SRID PostGIS misura in gradi (silent wrong result vs nome
  del campo). Consumer deve usare `SpatialSemantics::Geography`.
- **EWKB validation obbligatoria** al costruttore
  `SpatialReference.validated()` E nel compiler portable. Prima era
  possibile dichiarare `srid: 3857` con EWKB WGS84 e ottenere SQL
  che sovrascriveva il SRID silenziosamente.
- **`spatial.geometry()` / `spatial.geography()`** ora usano
  `SpatialReference.validated`.

### Hardening — Cancellation client-side in-flight

Tutti i metodi `Transaction` / `AsyncTransaction` che aprono
operazioni su Postgres wrappano ora il client await in
`tokio::select` col cancellation token: `execute`, `query`,
`savepoint`, `rollback_to_savepoint`, `release_savepoint`,
`query_stream`, `execute_conditional_update`, `commit`, e
`RowStream::next_batch`.

Su cancel:
- Il pool client viene invalidato (`open = false`).
- Chiamate successive sulla stessa tx ricevono `PlenoraInvalidPlanError`.
- `RemoteEffect::Unknown` per Write/Commit (query potenzialmente
  applicata server-side prima del taglio del canale).

**Limite noto**: la cancellazione è **client-side in-flight**. Il
SDK non invia `CancelRequest` (protocollo Postgres) al server —
richiederebbe una nuova connessione TCP e non è thread-safe rispetto
al client in uso. Il comando server-side può continuare fino al
`statement_timeout` di sessione (default 30s). Consumer che vuole
cancel server-side deve settare `statement_timeout_ms` esplicito
nelle `TransactionOptions` (o via SQL `SET LOCAL statement_timeout`).

### Hardening — Errori + robustezza

- **SQLSTATE preservato** nei path write Postgres (era collassato a
  `Protocol`). Ora `PlenoraConflictError` (23xxx), `PlenoraNotFoundError`
  (42P01/42703), `PlenoraInvalidPlanError` (42601), ecc.
- **`Insert.rows()` / `Upsert.rows()`** fail-closed se le chiavi non
  combaciano con la prima riga (prima le chiavi extra venivano
  silently ignorate).
- **JSON float non-finito** (`NaN`, `Infinity`) → `PlenoraError`
  invece di silent coercion a `null`.
- **CLI parser typed params** non strippa più quote asimmetriche/
  interne. Solo coppie matched vengono rimosse.
- **Hash nome indice spaziale** ora FNV-1a (stabile cross-version
  Rust) — era `DefaultHasher` che può cambiare a upgrade toolchain.
- **SRID > `i32::MAX`** → `PlenoraInvalidPlanError` (prima saturava
  a `i32::MAX`).

### Deduplicazioni

- SRID geografici e quoting identificatori consolidati in
  `plenora-database-core` (single-source-of-truth).
- `default_budget()` × 5 → `budget::session_budget` /
  `write_bulk_budget`.
- Commit-unknown constructor × 7 → `errors_commit::commit_outcome_unknown`
  (con `ErrorPhase::Commit` + provider derivato dinamicamente da
  `TransactionScope::provider_kind()`).
- `Renderer::quote` propaga `Result` invece di fallback silente
  (identificatori invalidi ora fail-closed).

### Fix — Import Python top-level

`import plenora_database` risolve ora correttamente
`PlenoraCommitOutcomeUnknownError`. Fix retroattivo se stavi
usando 0.9.0-dev / snapshot pre-release.

### Novità versioning

Workspace Rust bump `1.1.0 → 1.2.0` per allineamento con SDK Python
`0.9.0`. Gli ADR di supporto sono in `docs/adr/`:
- `0011-tls-secure-by-default.md`
- `0012-portable-spatial-distance-units.md` (target futuro)
- `0013-resource-budget-semantics.md` (target futuro)

Review dettagliata in `docs/reviews/2026-08-15-postgres-cli-sdk-review.md`.

---

## [0.8.1] — 2026-08-14

Verify typed params helpers su MySQL + doc onesto sui gap residui.

### Verified

- **Typed params helpers** (`p.uuid`, `p.decimal`, `p.date`,
  `p.timestamp`, `p.timestamptz`, `p.null`) funzionano identici su
  MySQL — nessun codice nuovo necessario. Il decorator `TypedValue`
  è provider-agnostic (`_plenora_typed_kind` + `_plenora_typed_value`
  attribute); `parameter.rs` MySQL mappa `ParameterValue::Uuid` /
  `Decimal` / `Date` / `Timestamp` / `TimestampTz` come text strings
  native (MySQL accetta tutti come `Value::Bytes(str.as_bytes())`).

- 2 nuovi test in `test_mysql_session.py`:
  - `test_typed_params_uuid_and_decimal_roundtrip`: INSERT con
    `p.uuid()`, `p.decimal("1234.56")`, `p.date("2026-08-14")` +
    verify roundtrip
  - `test_null_typed_param`: `p.null("text")` come parametro

### Gap residuo dichiarato (roadmap definitiva)

Il SDK MySQL v0.8.x ha **parity sostanziale** con Postgres su:

- ✅ Session sync + async (`connect_mysql` + `aconnect_mysql`)
- ✅ execute / execute_scalar / execute_returning_rows / execute_ddl
  (sync + async)
- ✅ begin + Transaction / AsyncTransaction (savepoints,
  conditional_update)
- ✅ read + aread (streaming Arrow IPC)
- ✅ copy_from + acopy_from (7 WriteMode bulk write)
- ✅ Typed params (uuid/date/timestamp/timestamptz/decimal/null)
- ✅ Context manager sync + async

**Restano fuori** (richiedono cross-crate refactor in
`plenora-database-core::portable::compiler`):

- ❌ Portable AST builders (`select/insert/update/delete/upsert`).
  Il compiler oggi hardcoda `compile_postgres`; aggiungere
  `compile_mysql` richiede ~200-300 righe di rendering nuovo con
  regole MySQL (placeholder `?`, backtick quoting, `ON DUPLICATE KEY
  UPDATE`, no `RETURNING` universale). Nuova sessione dedicata.

- ❌ Spatial predicates pythonic (`where_spatial(...)`, dipende dai
  portable AST builders sopra). Le 26 funzioni ST_* sono già dichiarate
  verified nel provider Rust MySQL v1.2 — manca solo il wrapper Python
  che genera portable AST spatial.

### Compatibilità

- 100% backward-compat con v0.8.0.
- Nessun cambio API — solo test + doc.

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.8.1>

---

## [0.8.0] — 2026-08-14

**AsyncMysqlSession** — variante asyncio del SDK MySQL. Parity con
`AsyncSession` Postgres per la superficie non-portable-AST.

### Added

- **`aconnect_mysql(host, database, user, password, port=None, tls_ca_pem=None)`**
  → awaitable → `AsyncMysqlSession`. Factory async.

- **`AsyncMysqlSession`** con API async completa:

  ```python
  async with await p.aconnect_mysql("localhost", "db", "u", "p") as s:
      n = await s.execute("INSERT INTO t VALUES (?, ?)", [1, "x"])
      v = await s.execute_scalar("SELECT COUNT(*) FROM t")
      rows = await s.execute_returning_rows("SELECT id FROM t WHERE amount>?", [10])
      await s.execute_ddl("CREATE INDEX idx_t ON t(id)")

      # Transaction
      tx = await s.begin(isolation="serializable")
      await tx.execute("...")
      await tx.commit()  # or await tx.rollback()

      # Streaming read
      reader = await s.aread("db", "large_table", limit=10000)
      async for chunk in reader:
          batch = ipc.open_stream(io.BytesIO(chunk)).read_all()

      # Bulk write
      outcome = await s.acopy_from("db", "events", ipc_bytes, mode="upsert", keys=["id"])
  ```

- Metodi:
  - `execute` / `execute_scalar` / `execute_returning_rows` /
    `execute_ddl` — coroutines
  - `begin(isolation, read_only, statement_timeout_ms)` →
    `AsyncTransaction` (provider-agnostic, ereditato dal path Postgres)
  - `aread(schema, object, projection, order_by, limit)` →
    `AsyncBatchReader` (streaming Arrow IPC async)
  - `acopy_from(schema, table, ipc_bytes, mode, ...)` — bulk write
    async (7 WriteMode, come `copy_from` sync)
  - `__aenter__/__aexit__/close/is_closed/server_version/__repr__`

### Design

Nuovo modulo `async_mysql_session.rs` (~380 righe) con pattern
`future_into_py` per convertire Rust future in Python awaitable.
Riusa gli helper generici di `write.rs` (parse_mode/profile/
mapping_policy, decode_ipc_stream, make_operation, VecBatchStream)
e `arrow_reader.rs` (make_read_operation, default_budget,
AsyncBatchReader). Zero duplicazione del pyclass `AsyncTransaction`
(già provider-agnostic).

### Compatibilità

- 100% backward-compat con v0.7.0.
- Additiva: `aconnect_mysql` + `AsyncMysqlSession` sono nuove.

### Roadmap SDK MySQL post-0.8

- **Portable AST builders** (`select/insert/update/delete/upsert`) —
  richiede `compile_portable_for_provider(Mysql)` nel core facade
  (oggi Postgres-only). Cross-crate refactor.
- **Spatial predicates** + `SpatialReference` — 26 funzioni ST_* già
  verified nel provider Rust MySQL v1.2; SDK builder pythonic da
  aggiungere (dipende da portable AST).
- **Typed params helper** (`uuid`/`date`/`decimal`/`null`) — probabilmente
  già funziona (decorator string-based provider-agnostic); serve verify
  + doc.

### Wheel

- `plenora_database-0.8.0-cp310-abi3-manylinux_2_34_x86_64.whl`
- `plenora_database-0.8.0-cp310-abi3-macosx_11_0_arm64.whl`
- `plenora_database-0.8.0-cp310-abi3-win_amd64.whl`

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.8.0>

---

## [0.7.0] — 2026-08-14

Streaming Arrow read MySQL — `MysqlSession.read()`.

### Added

- **`MysqlSession.read(schema, object, projection=None, order_by=None, limit=None)`**
  → ritorna `BatchReader` (pyclass provider-agnostic ereditato dal path
  Postgres). Il consumer itera bytes Arrow IPC stream chunk-by-chunk:

  ```python
  import io, pyarrow.ipc as ipc

  with p.connect_mysql("localhost", "db", "u", "p") as s:
      for chunk in s.read("mydb", "large_events", limit=100_000, order_by=[("id", "asc")]):
          batch = ipc.open_stream(io.BytesIO(chunk)).read_all()
          process(batch)
  ```

- **Refactor `arrow_reader.rs`**: helper generici ora `pub(crate)`
  (`make_read_operation`, `default_budget`, `BatchReader` pyclass) —
  riusati dal nuovo `mysql_arrow_reader.rs`.

- **Nuovo modulo `mysql_arrow_reader.rs`** (~70 righe): `open_mysql_reader`
  che chiama `MysqlProvider::read()` e ritorna un `BatchReader`
  identico al path Postgres. Zero duplication del pyclass o della
  logica IPC streaming.

### Design

Streaming server-side via `mysql_async` cursor: bounded, no
materializzazione dell'intero result set. Batch size decisa dal
provider (non configurabile dal SDK).

### Compatibilità

- 100% backward-compat con v0.6.0.
- Aggiunta additiva su `MysqlSession`.

### Roadmap SDK MySQL post-0.7

- `AsyncMysqlSession` con `aread()` e `acopy_from()` async
- Portable AST builders (`select/insert/update/delete/upsert`) —
  richiede `compile_portable_for_provider(Mysql)` nel core facade
- Spatial predicates + `SpatialReference` — 26 funzioni ST_* già
  verified nel provider Rust MySQL v1.2
- Typed params helper (uuid/date/decimal) — probabilmente funziona
  già, serve solo verify + doc

### Wheel

- `plenora_database-0.7.0-cp310-abi3-manylinux_2_34_x86_64.whl`
- `plenora_database-0.7.0-cp310-abi3-macosx_11_0_arm64.whl`
- `plenora_database-0.7.0-cp310-abi3-win_amd64.whl`

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.7.0>

---

## [0.6.0] — 2026-08-14

Completa il pattern bulk-write MySQL: `MysqlSession.copy_from` con tutti
7 WriteMode + accettazione flessibile dell'input (pyarrow / pandas /
list-of-dict / bytes IPC).

### Added

- **`MysqlSession.copy_from(schema, table, source, mode='append',
  transaction_profile='single_transaction', mapping_policy='compatible',
  keys=None, update_columns=None) → dict`** — bulk write MySQL:

  ```python
  import pyarrow as pa
  with p.connect_mysql("localhost", "db", "u", "p") as s:
      # Append (default)
      tbl = pa.table({"id": [1, 2, 3], "label": ["a", "b", "c"]})
      outcome = s.copy_from("mydb", "events", tbl)

      # Create (CREATE TABLE dallo schema Arrow + INSERT bulk)
      outcome = s.copy_from("mydb", "events_new", tbl, mode="create")

      # Upsert (INSERT ... ON DUPLICATE KEY UPDATE)
      outcome = s.copy_from("mydb", "events", tbl, mode="upsert", keys=["id"])

      # Update (staging TEMPORARY table + UPDATE JOIN)
      outcome = s.copy_from("mydb", "events", tbl, mode="update", keys=["id"])

      # Replace (staging persistent + RENAME atomic swap)
      outcome = s.copy_from("mydb", "events", tbl, mode="replace")

      # DeleteByKeys (DELETE ... WHERE (keys) IN (...))
      outcome = s.copy_from("mydb", "events", keys_tbl, mode="delete_by_keys", keys=["id"])
  ```

- **`source` accetta**:
  - `pyarrow.Table` / `pyarrow.RecordBatch` / `list[pyarrow.RecordBatch]`
  - `list[dict]` (convertito via `pa.Table.from_pylist`)
  - `pandas.DataFrame` (convertito via `pa.Table.from_pandas`)
  - `bytes` (Arrow IPC stream self-contained per zero-copy)

- **Mode / profile / policy**: stessi valori di `Session.copy_from`
  Postgres. Il provider MySQL supporta tutti 7 WriteMode (v1.2 core
  Rust); ora esposti dal SDK Python.

- **Wrapper Python `_MysqlSessionWrapper`** in `__init__.py`: aggiunge
  ergonomia `copy_from` con auto-conversion `source → ipc_bytes` via
  helper `_to_ipc_bytes` (riusato dal path Postgres). L'API sottostante
  `MysqlSession._native.copy_from(schema, table, ipc_bytes, ...)`
  rimane accessibile per il consumer che preferisce bytes precompilati.

### Design

Zero duplication su Postgres:
- Helper generici in `write.rs` (parse_mode/profile/mapping_policy,
  decode_ipc_stream, make_operation, default_budget, VecBatchStream,
  outcome_into_py, wrap_outcome) sono ora `pub(crate)` e usati sia
  dal path Postgres sia MySQL.
- Nuovo modulo `mysql_write.rs` (~100 righe) contiene solo la
  differenza: `Arc<MysqlProvider>` invece di `Arc<PostgresProvider>`
  nella chiamata `prepare_write` + `write`.

### Compatibilità

- 100% backward-compat con v0.5.0 (nessun cambio API esistente).
- `MysqlSession` API additiva: `copy_from` nuovo, tutto il resto
  invariato.

### Wheel

- `plenora_database-0.6.0-cp310-abi3-manylinux_2_34_x86_64.whl`
- `plenora_database-0.6.0-cp310-abi3-macosx_11_0_arm64.whl`
- `plenora_database-0.6.0-cp310-abi3-win_amd64.whl`

**Release**: <https://github.com/PlenoraETL/generic-database-tools/releases/tag/py-v0.6.0>

---

## [0.5.0] — 2026-08-14

Completa il pattern OLTP MySQL: `MysqlSession.begin()` + savepoints
via il `Transaction` provider-agnostic esistente.

### Added

- **`MysqlSession.begin(isolation=None, read_only=None, statement_timeout_ms=None)`**
  → ritorna la classe `Transaction` (provider-agnostic, ereditata dal
  path Postgres). Sblocca tutti i pattern OLTP dal Python MySQL:

  ```python
  with s.begin(isolation="serializable") as tx:
      tx.execute("INSERT INTO t VALUES (?, ?)", [1, "x"])
      tx.savepoint("sp1")
      tx.execute("...")
      tx.rollback_to_savepoint("sp1")
      tx.release_savepoint("sp1")
      # commit auto su __exit__; rollback su eccezione
  ```

- Metodi ereditati da `Transaction` disponibili anche per MySQL:
  - `execute` / `execute_scalar` / `execute_returning_rows`
  - `savepoint` / `rollback_to_savepoint` / `release_savepoint`
  - `commit` / `rollback` / `conditional_update`
  - `is_active`, `__enter__/__exit__`, `__repr__`

- MySQL non ha `deferrable` (parametro Postgres-only) — non esposto.

### Design

Zero duplication: `Transaction` è già un wrapper sopra
`Box<dyn TransactionScope>` — non provider-specific. La modifica è
piccola (~40 righe) nel `mysql_session.rs`: parsing opzioni + call a
`provider.begin_transaction` + `Transaction::new(scope)`.

### Compatibilità

- 100% backward-compat con v0.4.0: nessun cambio all'API esistente.
- Aggiunta additiva su `MysqlSession`.

### Wheel

- `plenora_database-0.5.0-cp310-abi3-manylinux_2_34_x86_64.whl`
- `plenora_database-0.5.0-cp310-abi3-macosx_11_0_arm64.whl`
- `plenora_database-0.5.0-cp310-abi3-win_amd64.whl`

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.5.0>

---

## [0.4.0] — 2026-08-14

Prima esposizione MySQL nel SDK Python (scaffold, non feature parity
con Postgres).

### Added

- **`connect_mysql(host, database, user, password, port=None, tls_ca_pem=None)`** —
  factory per aprire una sessione MySQL. Non usa DSN libpq (che è
  Postgres-specifico); accetta componenti separati.
- **`MysqlSession`** (nuova classe Python + nativa Rust) con subset:
  - `execute(sql, params) → int` (affected_rows, DML in tx dedicata)
  - `execute_scalar(sql, params) → Any` (SELECT 1 riga × 1 colonna)
  - `execute_returning_rows(sql, params) → list[dict]` (SELECT con rows)
  - `execute_ddl(sql) → None` (DDL raw, autocommit MySQL)
  - `close()`, `__enter__/__exit__`, `is_closed`, `server_version`, `__repr__`
- Placeholder syntax: `?` (convenzione MySQL, non `$1` come Postgres).
- Type stubs `.pyi` (`_native.pyi` + `MysqlSession` export in `__init__.pyi`
  se presente).
- Test live `test_mysql_session.py` (6 test): connect + server_version,
  execute/scalar/rows roundtrip, NULL handling, context manager, DDL
  autocommit visibility.

### Not Included (roadmap SDK MySQL post-0.4)

- `begin()` + `Transaction` context manager (savepoints, conditional_update)
- `copy_from` bulk write (7 WriteMode via Arrow IPC)
- `read()` streaming Arrow
- Portable AST builders (`select/insert/update/delete/upsert`)
- Spatial predicates + `SpatialReference`
- Typed params (uuid/decimal/date/etc. — MySQL binding usa string
  passthrough per ora)
- `AsyncMysqlSession` async variant
- Metrics + inspect namespace (analogo a Session Postgres)

### Motivo scaffold

Il gap Consumer Surface era enorme: prima MySQL era raggiungibile solo
via API Rust diretta o CLI generic `database-probe`. Questa release
sblocca il pattern OLTP base (probe + execute + query + DDL) dal Python
consumer, che è sufficiente per validare il driver end-to-end e per
casi d'uso semplici (script batch, migrazione dati, integration tests).
Il resto delle capability arriverà quando serve un consumer PFM
concreto — evita over-engineering di feature non richieste.

### Compatibilità

100% backward-compatible con v0.3.0: nessun cambio all'API Postgres
(`Session`, `AsyncSession`, `Transaction`, `copy_from`, `read`, portable
builders, ecc.). L'aggiunta è additiva (`connect_mysql` + `MysqlSession`
sono nuove).

### Wheel

- `plenora_database-0.4.0-cp310-abi3-manylinux_2_34_x86_64.whl`
- `plenora_database-0.4.0-cp310-abi3-macosx_11_0_arm64.whl`
- `plenora_database-0.4.0-cp310-abi3-win_amd64.whl`

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.4.0>

---

## [0.3.0] — 2026-08-14

Completa la superficie Postgres del SDK. Tre gap P1 chiusi:
1. bulk write UPSERT/UPDATE/DELETE
2. read() con projection/order_by/limit
3. `copy_from` accetta pandas.DataFrame + list[dict]

### Added

- **`copy_from(mode="upsert", keys=[...])`** — INSERT ... ON CONFLICT
  DO UPDATE dallo schema Arrow, con conflict target dato dai `keys`.
  Sblocca ETL idempotenti (import periodici con chiave primaria).
- **`copy_from(mode="update", keys=[...], update_columns=[...])`** —
  UPDATE ... FROM dallo staging (implementato dal provider Rust).
- **`copy_from(mode="delete_by_keys", keys=[...])`** — DELETE ... USING
  dallo staging (basta la colonna key nel dataset).
- **`read(projection=[...], order_by=[...], limit=N)`** — SELECT
  projection + ORDER BY + LIMIT sul cursore server-side. Prima
  scaricava tutta la tabella. `order_by` è list di
  `("column", "asc"|"desc")`.
- **`copy_from(source=list[dict])`** — convertito via
  `pyarrow.Table.from_pylist`. Ergonomia per script Python senza
  pyarrow object model esposto.
- **`copy_from(source=pandas.DataFrame)`** — convertito via
  `pyarrow.Table.from_pandas`. Zero-boilerplate per data scientist
  che partono da pandas.
- Validation early: `keys` è obbligatorio per upsert/update/delete_by_keys;
  errore con messaggio chiaro se assente. Rifiutato per gli altri mode
  (Append/Create/etc.) per prevenire mismatch.
- 12 nuovi test in `test_v030_p1.py` (upsert happy path + error paths,
  read projection/limit/order_by, list[dict] + pandas + edge cases).

### Compatibilità

- **Backward-compat** con v0.2.0 per il pattern `copy_from(mode="append")`
  senza `keys` (funziona identico).
- Chi passava `keys=[...]` per mode diverso da upsert/update/delete_by_keys
  in v0.2.0 (impossibile — parametro non esisteva) ora otterrebbe errore
  early: nessun consumer impattato.
- API sync + async stubs `.pyi` aggiornati.

### Stato Postgres SDK

Dopo v0.3.0 la copertura Postgres è **complete** rispetto ai gap
P1 identificati per il primo consumer target (PFM). Restano gap P2
(async cancellation graceful, pool config esposto, altre 67 funzioni
PostGIS come builder) rimandati a rispettivi minor bump quando
prioritari.

Da v0.3.0 il SDK è pronto come **base pattern** per esporre MySQL e
SQL Server (che il core Rust supporta già). Il pattern binding
(session/tx/copy_from/read) è stabilizzato — le duplicazioni cross-provider
si applicano con la stessa struttura.

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.3.0>

---

## [0.2.0] — 2026-08-14

Nuove capability bulk-write. Prima minor bump del SDK.

### Added

- **`copy_from(mode="create")`** ora funziona end-to-end: crea la
  tabella target dallo schema Arrow prima del COPY. Sblocca il pattern
  ETL scratch (`load parquet/csv into new_table`) senza DDL preventivo
  dal consumer.
- **`copy_from(mode="replace")`** e **`mode="truncate_insert"`** sono
  parimenti operativi (già supportati dal provider Postgres, ora
  raggiungibili dal SDK).
- Test `test_copy_from_mode_create_builds_table_from_arrow_schema`:
  verifica DDL applicato + righe landed dopo `copy_from(mode="create")`.
- Test `test_copy_from_mode_create_conflicts_if_target_exists`:
  verifica che `mode="create"` su target esistente restituisca
  `PlenoraConflictError` (Conflict del preflight).

### Fixed (core Rust)

- `plenora_db_postgres::write::execute` chiamava `row_diagnostics::validate_input`
  per **tutti** i mode, ma la funzione rifiutava esplicitamente ogni mode
  diverso da `Append + SingleTransaction` con messaggio
  `"la diagnostica PostgreSQL supporta solo Append con SingleTransaction"`.
  Risultato: `Create`, `Replace`, `TruncateInsert` (e altri) erano
  raggiungibili solo escludendo row diagnostics via
  `declared_input_rows() = None` sullo stream, cosa che il SDK non fa.
- Fix: `validate_input` viene ora invocato solo per `Append + SingleTransaction`
  (che è l'unico scenario dove ha senso — quarantina di righe individuali
  su target esistente). Gli altri mode saltano il gate diagnostico e
  vanno al path normale.

### Compatibilità

- **Backward-compat** con v0.1.3 al 100% per il pattern `mode="append"`
  (default).
- Nessun cambio API o firma. È un'aggiunta di capability precedentemente
  bloccata da un check troppo restrittivo.
- Il workspace Rust (`plenora-db-postgres`) tocca la funzione
  `execute`; unit test invariati (103/103 pass).

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.2.0>

---

## [0.1.3] — 2026-08-14

Bugfix di `copy_from` introdotto in v0.1.2.

### Fixed

- `copy_from` / `acopy_from` hardcodavano `MappingPolicy::Strict`, che
  boccia il pattern comune "Arrow nullable → PG NOT NULL" (severity
  `DataLoss`). Riprodotto live: 4/8 test bulk write fallivano su una
  tabella con vincoli `NOT NULL`.

### Changed

- **Default cambiato** (silent): `copy_from(mapping_policy=)` ora è
  `"compatible"` invece di `"strict"` hardcoded. È la scelta ragionevole
  per input pyarrow tipici (tutti i campi nullable per default). Chi
  vuole il vecchio comportamento passa esplicitamente `mapping_policy="strict"`.

### Added

- Parametro `mapping_policy: str` esposto a `Session.copy_from` /
  `AsyncSession.acopy_from` (default `"compatible"`).
- 2 nuovi test: `test_copy_from_strict_policy_rejects_nullable_to_not_null`
  e `test_copy_from_invalid_mapping_policy_raises_invalid_plan`.
- Suite live totale: **156 test** (154 in v0.1.2 → 156 in v0.1.3).

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.1.3>

---

## [0.1.2] — 2026-08-14

Nuova feature (`copy_from`) + fix API contract + documentazione observability.

### Added

- **`Session.copy_from(schema, table, source, ...)`** e
  **`AsyncSession.acopy_from(...)`**: bulk write via `prepare_write` +
  `write` del provider Postgres (COPY internamente per mode `append`).
  Accetta `pyarrow.Table` / `RecordBatch` / iterable / bytes IPC.
  Ritorna dict con struttura `WriteOutcome` del core Rust.
- 8 nuovi test in `test_copy_from.py`: happy path (sync/async) + error
  paths (mode/profile invalidi, tipo source non supportato, iterable
  vuoto).
- README: sezione "Bulk write (COPY)" con esempi sync + async.
- README: sezione "Observability" con esempio structured logging
  OpenTelemetry-compatibile a partire da `Session.metrics()` e dai
  campi diagnostici delle `PlenoraError`.

### Removed (BREAKING)

- Parametro `batch_rows: int | None = None` rimosso da `Session.read()`
  e `AsyncSession.aread()`. In v0.1.0 e v0.1.1 era **accettato ma
  silenziosamente ignorato** (il core `Provider::read()` non espone una
  batch size esplicita). Rimuoverlo è più onesto che mantenere un
  contract violato.
- **Migration**: chi passava `batch_rows=N` esplicitamente riceverà
  `TypeError`; rimuovere il kwarg dalla chiamata. La size dei batch
  emessi dallo stream è decisa dal provider (Postgres: bounded dal
  buffer del cursore server-side).

**Note**: v0.1.2 aveva un bug in `copy_from` (default `mapping_policy`
troppo rigido). Fixato in v0.1.3.

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.1.2>

---

## [0.1.1] — 2026-08-14

Bugfix del version mismatch scoperto in v0.1.0.

### Fixed

- `p.version()` restituiva `"1.1.0"` (versione del Rust workspace,
  ereditata via `version.workspace = true`) invece di `"0.1.0"`
  (versione dichiarata in `pyproject.toml` e usata nel filename del
  wheel). Confondeva `pip check` e semver gate consumer-side.

### Changed

- `Cargo.toml` del crate `plenora-database-py` ora dichiara la sua
  version esplicitamente (`version = "0.1.1"`), separata dal Rust
  workspace. Coincide con `pyproject.toml`.

**Compatibilità**: 100% backward-compatible con v0.1.0 a livello API.

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.1.1>

---

## [0.1.0] — 2026-08-14

Prima release stabile del binding Python. PyO3 abi3-py310 sopra al core
Rust di `plenora-database-tools`.

### Added

- **Sync + Async parity**: `Session` + `AsyncSession` con context
  manager, `Transaction` + `AsyncTransaction` con savepoint e
  `conditional_update` (optimistic-lock).
- **Portable AST builder Pythonic**: `Select` / `Insert` / `Update` /
  `Delete` / `Upsert` provider-agnostic.
- **PostGIS end-to-end**: `where_spatial` + `SpatialReference` per
  predicati geometrici cross-SRID (5 predicati: intersects, contains,
  within, bounding_box, d_within).
- **Streaming Arrow**: `read()` / `aread()` restituiscono
  `BatchReader` che emette Arrow IPC stream chunk-per-chunk.
- **Error hierarchy**: 19 classi tipizzate sotto `PlenoraError` mappate
  su `ErrorCategory` del core Rust.
- **Type stubs** (`*.pyi` + `py.typed`): PEP 561 completo.
- **Wheel multi-platform**: Linux (manylinux_2_34), macOS aarch64,
  Windows x86_64. Un unico wheel `abi3-py310` per piattaforma
  (compatibile Python 3.10 / 3.11 / 3.12 / 3.13).
- **Performance**: ~13× più veloce del subprocess CLI su happy path
  scalar (0.62 ms/call vs 8.40 ms/call, misurato in
  `test_benchmark_parity.py`).

### Scope

- **Coperto**: Postgres 15+ / PostGIS 3.x — OLTP, streaming read, spatial, tx.
- **Non coperto** (roadmap successiva): MySQL, SQL Server (driver Rust
  presenti nel workspace ma non ancora esposti al SDK).
- **DDL plane**: fuori scope v0.1.

### Driver Rust — fix inclusi (pre-Fase-3)

- **P0.7**: Decimal + UUID binding nel path OLTP (dual-representation
  text/binary).
- **P0.8**: NUMERIC decoder wire format nel path OLTP.
- **H7.1**: filtri catalog contro system schemas.
- **H7.2**: cancel-aware `BatchStream::next_batch(&CancellationToken)`.
- Portable spatial: `::geography` cast semanticamente corretto per
  `PortableStatement`.

**Note**: v0.1.0 aveva un bug in `p.version()` (mismatch con
`pyproject.toml`). Fixato in v0.1.1.

**Release**: <https://github.com/PlenoraETL/plenora-database-tools/releases/tag/py-v0.1.0>
