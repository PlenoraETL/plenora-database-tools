# Hardening PostgreSQL/PostGIS

Stato: **profilo hardening v1 implementato sul riferimento PostgreSQL 16 /
PostGIS 3.4**.

Questo profilo non aggiunge nuove promesse SQL: rende verificabili lifecycle,
sicurezza e comportamento sotto errore del data path v3.

## Garanzie aggiunte

- Il pool è bounded e partizionato da fingerprint di credenziale, modalità TLS,
  opzioni di rete e timeout di sessione.
- Una connessione nuova riceve timeout e `application_name` nel pacchetto di
  startup, senza una query di configurazione. Ogni connessione riusata esegue
  un solo `DISCARD ALL`, che ripristina quegli stessi default ed elimina anche
  prepared statement e oggetti temporanei.
- Un errore di reset rende la sessione non riutilizzabile.
- La cache schema è LRU e bounded. Non considera mai una TTL come prova di
  validità: confronta OID e firma strutturale del catalogo prima di ogni hit.
- Il token include stato di colonne, tipi, typmod, nullability, default,
  identity/generated e campi composite. Un DDL esterno forza il refresh.
- Una write Plenora invalida il target dopo commit e anche quando l'outcome è
  incerto.
- Il fast path parametrizzato viene usato soltanto quando bind, colonna e tipo
  PostgreSQL built-in sono determinabili senza ambiguità. Enum, domini,
  composite e mismatch ricadono sul prepare con inferenza server-side.
- `QueryOperation` usa il one-shot con tipi canonici. Se PostgreSQL rifiuta la
  tipizzazione, il driver torna al prepare; la SELECT non viene eseguita due
  volte. Per un risultato vuoto il describe ricava lo schema senza una seconda
  esecuzione.
- Una sessione con errore di protocollo/stream, write fallita, commit incerto,
  cancellazione o stream abbandonato non rientra nel pool.
- Le cancellazioni raggiungono il backend PostgreSQL con timeout bounded.
- Il confine usa un `CancellationToken` concreto con wake race-free; il data
  path non esegue più polling temporizzato per osservare la cancellazione.
- Dopo cancellazioni concorrenti il pool deve ristabilire una connessione sana.
- `PostgresTlsMode::Disabled` forza `sslmode=disable`;
  `PostgresTlsMode::Require` forza `sslmode=require` e verifica hostname e
  catena tramite Rustls/WebPKI. Il DSN non può indebolire la modalità scelta
  dall'API.
- `PostgresTlsConfig` accetta `WebPKI`, CA private PEM, catene client e chiavi
  PEM PKCS#1/PKCS#8/SEC1; il pool separa anche configurazioni TLS differenti.
- Il materiale mTLS viene compilato in un connector Rustls condiviso, non ha
  getter o serializzazione e appare sempre redatto in `Debug`.
- Il parser numeric interno rifiuta segni multipli, whitespace, esponenti e
  forme ambigue non prodotte dal mapping supportato.
- L'encoding binary numeric è verificato deterministicamente su zero, segno,
  scale positive/negative e limiti `i128`.
- Escaping di range e composite con quote, backslash, newline e Unicode ha un
  unico encoder condiviso.
- Date32 e Timestamp Arrow fuori dal range del mapping temporale falliscono
  come `DataMapping`; COPY e prepared non contengono conversioni temporali che
  possano andare in panic.
- Gli errori attraversano il confine con categoria, fase, effetto remoto e
  disposizione di retry indipendenti. `OutcomeUnknown` non è più una causa.
- Una cancellazione di write tenta un rollback esplicito: dichiara
  `RolledBack` soltanto se PostgreSQL lo conferma, altrimenti `Unknown` con
  `RequiresRecovery`.
- Gli schemi Arrow emessi dichiarano `plenora.contract.version=1`; i campi
  PostGIS e PostgreSQL usano i namespace canonici `plenora.geometry.*` e
  `plenora.postgres.*`. Le chiavi legacy sono accettate in lettura, ma una
  divergenza viene rifiutata.
- I mutex di pool e cache recuperano esplicitamente lo stato da
  `PoisonError`; le strutture protette sono container semplici e bounded.
- `QueryOperation` viene validato con una visita iterativa prima del renderer:
  massimo 64 livelli e 4.096 nodi strutturali/espressioni.
- Il validatore verifica anche arità e contesto booleano, sorgenti
  table/derived mutuamente esclusive, regole `DISTINCT ON`, window frame,
  set-operation, offset deterministico e lock applicabili.
- Gli input geometry/geography vengono rifiutati prima della transazione se
  l'header EWKB non concorda con tipo, dimensioni o SRID del contratto Arrow.
- Catalogo spatial e renderer sono verificati in lockstep; ogni variante
  tipizzata deve avere nome SQL, arità e classificazione espliciti.

## Metriche bounded

`PostgresProvider::metrics_snapshot()` restituisce soli contatori `u64`:

| Area | Contatori |
|---|---|
| pool | checkout, riusi, nuove connessioni, timeout |
| lifecycle | reset delle sessioni riusate, sessioni invalidate, cancellazioni |
| schema | controlli token, hit, miss, eviction e invalidazioni cache |
| read | introspezioni catalogo complete, fast path tipizzati `ReadOperation`/`QueryOperation`, fallback prepared, batch, righe, byte Arrow, batch chiusi dal target adattivo |
| write | commit, righe confermate, outcome unknown |

Non esistono label dinamiche. Snapshot e metriche non contengono DSN, SQL,
hostname, database, utente, nomi di oggetti o valori. I contatori usano atomiche
relaxed: servono per osservabilità operativa, non per sincronizzare il data
path.

## Budget di risorse end-to-end

Le operazioni read, query e write richiedono un `ResourceBudget` condiviso.
Il provider:

- prenota atomicamente operazioni concorrenti e colonne, restituendole al drop;
- prenota righe, memoria e output prima di costruire o inviare un batch;
- converte la quota effettivamente usata in consumo cumulativo con
  `ResourceLease::commit`, restituendo solo la parte inutilizzata;
- applica `cell_bytes` alle celle PostGIS oltre ai limiti locali del provider;
- attraversa EWKB in modo iterativo, limitando componenti e profondità senza
  fidarsi dei conteggi incorporati nel payload;
- impedisce di sostituire il budget tra `prepare_write` e `write`;
- applica `duration_ms` come deadline monotona, cancellando il backend senza
  polling;
- effettua rollback esplicito se il budget si esaurisce dentro una transazione,
  senza dichiarare il rollback quando PostgreSQL non lo conferma.
- applica la stessa regola a errori del producer, codec, SQL, trigger, DDL,
  pubblicazione staged e fault injection prima del commit.

Questa contabilità è deliberatamente conservativa: un batch consegnato al
chiamante resta contabilizzato fino al termine del budget, perché il contratto
Arrow restituisce un `RecordBatch` che può sopravvivere allo stream.

## Risoluzione CRS PostGIS

L'introspezione collega il typmod PostGIS a `spatial_ref_sys`. Quando la riga
contiene authority e codice, Arrow emette `crs_resolution=resolved`,
`crs_id=<authority>:<code>` e lo SRID osservato. In assenza di authority il CRS
resta esplicitamente `declared_unresolved`; SRID 0 diventa `missing`.

L'axis order resta `unknown` e la definizione WKT non viene emessa: il provider
non deduce semantica degli assi da stringhe non analizzate. Il token della cache
schema include authority, codice e versione MVCC della riga SRS. In write, un
CRS dichiarato resolved viene confrontato con `spatial_ref_sys` prima di
qualsiasi DDL o transazione.

## Prove automatiche

Il gate esegue:

1. rustfmt e Clippy con warning negati;
2. test deterministici dei codec senza database;
3. suite live read/write/PostGIS;
4. 120 letture concorrenti su pool massimo 4, con conteggi esatti;
5. quattro query lente cancellate simultaneamente, assenza di backend rimasti
   attivi e recovery del pool;
6. rollback prima del commit e `OutcomeUnknown` dopo commit;
7. reset di una sessione contaminata: GUC, tabella temporanea e prepared
   statement devono sparire conservando i default di startup;
8. cache schema: miss, hit validato, modifica DDL esterna, nuovo fingerprint,
   refresh e LRU eviction;
9. metriche di pool, reset, schema, introspezione, fast path, streaming, scrittura,
   cancellazione e invalidazione;
10. server con CA privata e `clientcert=verify-full`, rifiuto senza identità,
   cancellazione server-side mTLS e recovery.
11. pianificazione tipizzata dei bind built-in e spatial, con fallback
    deterministico per i tipi custom.
12. schema `QueryOperation` ricavato dalla prima riga e describe sicuro per
    result set vuoti.
13. estremi Date32/Timestamp, poisoning intenzionale e AST oltre i budget,
    tutti convertiti in esiti controllati.
14. query avanzate CTE/derived/lateral/window/set/locking e operatori GiST/KNN
    senza escape hatch SQL.
15. header EWKB incoerenti e cataloghi spatial divergenti rifiutati
    deterministicamente.
16. piano `EXPLAIN` con indice GiST e gate separato su mediana/p95.
17. budget righe esaurito live senza emettere righe eccedenti, rilascio delle
    lease strutturali e rifiuto della sostituzione del budget di write.
18. geometry bomb EWKB troncate, profonde o oltre il budget componenti
    rifiutate; test live senza emissione del batch eccedente.
19. risoluzione live `EPSG:4326`, inclusione SRS nel token strutturale e rifiuto
    preflight di un authority ID incoerente con lo SRID.
20. deadline read con cancellazione server-side e nessun backend residuo;
    deadline write con rollback confermato e zero righe pubblicate.
21. errore trigger dopo l'avvio della write e fault pre-commit: rollback
    confermato, execution ID presente e nessuna riga/DDL pubblicati.

Esecuzione:

```powershell
python scripts\check_postgres_hardening.py
```

Il safety case del profilo è in [SAFETY-CASE.md](SAFETY-CASE.md). Le sue claim
sono accettabili soltanto insieme alle prove automatiche elencate; il documento
non dichiara conformità a DO-178C, ED-12C o ad altri standard aeronautici.

## Limiti TLS dichiarati

CA privata e mTLS sono ora coperti live. Restano fuori dal profilo corrente
CRL/OCSP configurabili, chiavi cifrate con password, integrazione diretta con
HSM/PKCS#11 e rotazione live senza ricreare il provider/pool.

La fixture dedicata si avvia con:

```powershell
docker compose -f docker-compose.postgres-tls.yml up -d
```

Certificati e chiavi di test sono generati nel volume Docker
`plenora-postgres-tls_postgres_tls_certs`, mai nel repository.

## Matrice versioni ancora necessaria

Il riferimento PostgreSQL 16/PostGIS 3.4 è affiancato dal gate completo sulle
major PostgreSQL 14–18, PostGIS 3.5–3.6 e dal server mTLS con CA privata.
Risultati e policy sono in [COMPATIBILITY.md](COMPATIBILITY.md).

Restano campagne esterne per TLS con CA pubblica reale, Linux arm64 e servizi
PostgreSQL gestiti con i rispettivi vincoli di privilegi ed estensioni.
