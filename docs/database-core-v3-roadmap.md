# Database Core v3: piano di evoluzione

Questo documento descrive una direzione, non lo stato corrente della libreria.
Lo stato e le capability restano generati in [`STATO.md`](STATO.md); il
contratto pubblicato resta in [`contracts/v2/`](../contracts/v2/README.md).
Il nome «Core v3» identifica il programma di lavoro: una nuova major nasce
soltanto quando una modifica incompatibile del contratto la rende necessaria.

## Obiettivo

Portare la libreria da insieme coerente di driver, piani e binding a piattaforma
applicativa per database concorrenti e multiutente, utilizzabile al posto di
SQLAlchemy nei casi coperti e qualificati.

Non serve clonare tutta SQLAlchemy. Il vantaggio competitivo deve stare dove la
base attuale e gia orientata: contratti fail-closed, streaming Arrow, spatial,
esiti di commit espliciti, cancellazione, budget di risorse e comportamento
misurato per provider.

Il prodotto finale deve offrire tre livelli separabili:

1. **Core relazionale**: engine, connessioni, transazioni, metadata, expression
   language e risultati;
2. **unit of work**: mapping oggetti, identity map, flush e concorrenza
   ottimistica;
3. **schema lifecycle**: confronto metadata e piani di migrazione espliciti.

Un utente deve poter adottare soltanto il primo livello senza pagare il costo o
la complessita degli altri due.

## Vincoli di progetto

- Le API esistenti restano operative durante la migrazione. Le nuove superfici
  entrano prima come aggiunte; una rimozione aspetta una major.
- Una feature comune viene esposta solo quando ogni provider che la dichiara ha
  una prova live riproducibile.
- Nessun retry automatico puo nascondere un commit dall'esito ignoto.
- Engine e factory possono essere condivisi fra task e thread; sessione,
  transazione e unit of work appartengono a una sola unita concorrente.
- Sync e async condividono modello, validazione e compilatore. Cambia soltanto
  il bordo di esecuzione.
- Il percorso Arrow bulk resta distinto dal percorso OLTP a oggetti: unirli
  produrrebbe un'astrazione piu debole per entrambi.
- Reflection e migrazioni non possono trasformare un fatto non misurato in una
  capability implicita.
- Gli errori pubblici continuano a non includere SQL bindato, DSN o valori.

## Diagnosi architetturale

La diagnosi iniziale che collocava mapper, identity map e flush planner fra le
assenze non descrive piu il punto di partenza. Quelle fondamenta, insieme al
lifecycle di sessione, al mapping dichiarativo e alle migrazioni a DAG, sono
ora parte dell'API applicativa. Lo stato preciso resta quello generato in
[`STATO.md`](STATO.md); questa roadmap ordina il lavoro residuo.

Le aree da consolidare sono:

- tre rappresentazioni parzialmente sovrapposte delle query fra
  `core::portable`, `core::query` e `database-sql`;
- builder Python mutabili che costruiscono dizionari JSON e sono gia legati a
  una sessione;
- reflection resa come dizionari, quindi senza identita e tipi stabili;
- operazioni specializzate (Arrow, DDL e inspect) ancora eseguite dal trait
  provider, sebbene il loro lifecycle e la cancellazione siano governati
  dall'`Engine` pubblico comune;
- risultati distinti per scalar, righe e Arrow, senza un unico protocollo di
  consumo;
- implementazioni sync e async esposte in superfici parallele;
- qualifica ORM provider per provider, con Geometry chiusa sulle righe sostenute
  dai gate live e ancora negata fuori dalla matrice misurata;
- inheritance single-table/joined-table e pianificazione di migrazioni da diff
  di metadata; il runner esplicito a DAG e gia disponibile.

La prima riduzione di complessita non arriva comprimendo funzioni, ma eliminando
queste rappresentazioni e orchestrazioni duplicate.

## Profondita 1: fondazione Core

### 1.1 Un solo IR relazionale

Definire un intermediate representation canonico, immutabile e indipendente dal
provider. Deve coprire selezione, join, subquery, CTE, aggregati, finestre, set
operation, lock e DML.

La destinazione non e negoziabile:

| superficie attuale | destinazione |
| --- | --- |
| `core::portable` | adapter temporaneo verso `core::relational`, poi rimosso alla major |
| `core::query` | assorbito nel modello canonico `core::relational` |
| tipi AST di `database-sql` | eliminati; il crate riceve IR validato e produce statement abbassati |

I tre nomi di stadio ammessi descrivono trasformazioni dello stesso modello,
non tre alberi concorrenti:

```text
Relational IR -> Validated IR -> Lowered statement
```

`Validated IR` puo essere un newtype che prova l'avvenuta validazione e
`Lowered statement` contiene SQL e layout dei bind. Nessuno dei due reintroduce
un secondo linguaggio di espressioni.

Il flusso previsto e:

```text
API Rust/Python -> IR canonico -> validazione capability -> lowering dialetto
                -> statement + bind tipizzati -> provider
```

`portable`, `query` e `database-sql` non devono restare tre modelli pubblici che
si traducono a vicenda. Durante la transizione si introducono adapter interni;
alla major successiva resta pubblico un solo modello. Le differenze dei
provider vivono in profili di capability e funzioni di lowering, non in nodi
duplicati o varianti del tipo `PostgresExpression`/`MysqlExpression`.

Gate d'uscita:

- golden test identico per tutti i dialetti;
- nessun valore incorporato nel SQL renderizzato;
- fingerprint stabile dello statement e del layout dei bind;
- fuzzing di costruzione, lowering e serializzazione;
- inventario automatico dei nodi ancora duplicati uguale a zero.

### 1.2 Engine pubblico

Introdurre `Engine` come proprietario di configurazione, provider, pool,
capability osservate, cache di compilazione e metriche. `Engine` crea sessioni;
non rappresenta una transazione e non conserva stato applicativo per utente.

Responsabilita minime:

- checkout con timeout distinto dal connect timeout;
- reset o quarantena della connessione al rientro nel pool;
- rotazione dei secret senza inserirli nell'identita della cache;
- dispose e health check espliciti;
- limiti di pool, backpressure e metriche uniformi;
- factory sync e async sopra lo stesso stato condiviso.

Gate d'uscita:

- suite concorrente comune ai provider qualificati;
- nessun riuso di session context fra utenti;
- connessione interrotta o transazione abbandonata mai restituita sana al pool;
- saturazione del pool classificata separatamente da timeout di rete e query.

### 1.3 Result e Row uniformi

Una chiamata deve restituire un `Result` con protocollo comune:

- `all`, `first`, `one`, `one_or_none`, `scalar` e `scalar_one`;
- iterazione streaming a memoria limitata;
- accesso per posizione, nome e descrittore di colonna;
- schema e metadata disponibili senza consumare tutte le righe;
- conversione esplicita verso Arrow, dizionari e mapper.

Il risultato non trattiene una connessione oltre il proprio lifecycle e una
chiusura anticipata deve applicare la stessa quarantena gia richiesta agli
stream provider.

### 1.4 Metadata tipizzati

Introdurre oggetti immutabili `MetaData`, `Table`, `Column`, `Index`,
`ForeignKey` e `Constraint`. La reflection popola questi oggetti attraverso un
catalogo comune e conserva separatamente gli attributi nativi del provider.

La cache deve avere token di schema, TTL configurabile e invalidazione
esplicita. Nessun processo deve assumere che centinaia di tabelle siano
immutabili per tutta la sua vita.

Gate d'uscita:

- round trip reflection su fixture comuni;
- preservazione di chiavi, nullability, default, identity e tipi spatial
  soltanto dove osservabili;
- modifica DDL rilevata dal token prima di eseguire un piano preparato stale;
- nessun dizionario non tipizzato nella nuova API pubblica.

## Profondita 2: expression language e sessioni

### 2.1 Expression language Python

Costruire oggetti immutabili e componibili, scollegati dalla sessione:

```python
# Bozza illustrativa, non contratto pubblico.
users = table("users", column("id"), column("tenant_id"), column("name"))
statement = select(users.c.id, users.c.name).where(users.c.tenant_id == bind("tenant"))

with engine.session() as session:
    rows = session.execute(statement, {"tenant": 42}).all()
```

La stessa espressione deve essere riusabile, compilabile offline e condivisibile
fra chiamanti. I valori restano separati dall'albero e dai log.

Funzioni richieste prima di dichiarare il Core completo:

- alias, join e correlazione;
- funzioni scalar, aggregati e finestre;
- CTE ricorsive e set operation;
- `RETURNING` solo sui provider qualificati;
- operatori spatial con semantica geometry/geography esplicita;
- escape hatch SQL nativo governato dalla policy esistente.

### 2.2 Session lifecycle

La sessione diventa il confine per una sequenza di lavoro, non un alias della
connessione. Deve supportare:

- autobegin documentato oppure begin esplicito selezionabile;
- context manager con rollback certo in caso di eccezione;
- transazioni annidate implementate tramite savepoint solo dove qualificate;
- contesto tenant/audit applicato a ogni checkout e ripulito al rilascio;
- divieto di uso concorrente della stessa sessione;
- scadenza, cancellazione e stato di commit osservabili.

La modalita consigliata per server Python e un `Engine` globale e una sessione
per request/task. Una sessione globale condivisa non deve essere resa possibile
per accidente.

### 2.3 Parita sync/async

I wrapper non devono duplicare regole di validazione e costruzione AST. Una
specifica di superficie, verificata dai test, genera o alimenta gli stub sync e
async; i terminali differiscono soltanto per `await`.

Gate d'uscita:

- stessa matrice di casi e stessi errori strutturati;
- firma corrispondente negli stub;
- zero implementazioni duplicate di regole query fra sync e async;
- wheel ABI3 verificata sulle versioni Python supportate.

## Profondita 3: unit of work

Questo livello va iniziato soltanto dopo la stabilizzazione del Core.

Componenti:

- registry fra classi Python e tabelle tipizzate;
- identity map per chiave primaria e tenant;
- stati `transient`, `pending`, `persistent`, `dirty`, `deleted`, `detached`;
- snapshot degli attributi e dirty tracking;
- flush planner ordinato per foreign key;
- insert/update/delete raggruppati senza trasformare l'OLTP in bulk Arrow;
- colonne versione per concorrenza ottimistica;
- relazioni con caricamento esplicito e strategie batch.

Scelta deliberata: niente lazy loading implicito come default. L'I/O nascosto
e particolarmente rischioso in async, rende imprevedibili le query e complica
la diagnosi N+1. Il caricamento deve essere dichiarato nella query o richiesto
esplicitamente.

Gate d'uscita:

- nessuna doppia istanza della stessa identita nella stessa unit of work;
- flush atomico e ordinamento deterministico;
- conflitto di versione distinto da riga assente;
- rollback ripristina lo stato locale o lo invalida esplicitamente;
- test N+1 e conteggio statement;
- nessun retry automatico di un flush con outcome unknown.

## Profondita 4: schema lifecycle e migrazioni

La prima versione deve produrre un piano, non eseguire modifiche implicite.

Pipeline:

```text
metadata desiderati + reflection osservata
    -> diff tipizzato
    -> classificazione safe / lossy / unsupported / requires-lock
    -> piano ordinato e fingerprintato
    -> approvazione
    -> esecuzione con journal
```

Requisiti:

- rename solo con indicazione esplicita, mai inferito dalla somiglianza;
- operazioni non transazionali isolate dal gruppo transazionale;
- lock e durata stimabile resi visibili prima dell'esecuzione;
- resume basato su journal e fingerprint;
- SQL di migrazione separato dall'escape hatch applicativo;
- tipi e indici spatial preservati soltanto con evidence del provider.

Non e un obiettivo iniziale importare automaticamente revisioni Alembic. Un
adapter potra arrivare dopo che il modello di migrazione nativo e stabile.

## Profondita 5: compatibilita SQLAlchemy

La compatibilita va misurata per casi d'uso, non dichiarata in blocco.

Livelli proposti:

1. **Core ergonomics**: engine, session, statement, result e reflection;
2. **declarative mapping**: modelli, relazioni e unit of work;
3. **migration workflow**: diff, revisioni e applicazione;
4. **adapter ecosystem**: integrazioni con framework e tool esterni.

Ogni livello ha una suite differenziale su programmi equivalenti. Non e
necessario accettare oggetti SQLAlchemy o replicarne ogni comportamento
storico: l'obiettivo e rendere migrabili le applicazioni, non ereditare tutte
le ambiguita dell'API di riferimento.

## Sequenza di implementazione

Le fasi A, B e il nucleo della fase D hanno gia prodotto le fondamenta oggi
esposte. Non sono piu la sequenza operativa corrente; restano qui come forma
architetturale del programma. Le capability effettive non si deducono da
questa sezione, ma dal documento generato e dai gate nominati dal codice.

### Fase A: consolidamento senza rotture

1. misurare duplicazioni fra i tre AST e fissare la baseline;
2. scegliere l'IR canonico con un ADR breve;
3. introdurre adapter interni dalle API correnti;
4. spostare lowering, bind e fingerprint sul nuovo IR;
5. mantenere verdi i contratti v2 e tutti i provider.

### Fase B: Core pubblico additivo

1. `Engine` e lifecycle sessione;
2. metadata tipizzati e reflection;
3. statement immutabili e bind separati;
4. `Result` uniforme;
5. API e stub sync/async;
6. guida di migrazione dalla `Session` attuale.

### Fase C: qualifica applicativa

1. fixture con centinaia di tabelle e metadata concorrenti;
2. carico multiutente con pool saturato, cancellazioni e fault injection;
3. benchmark compilazione, cache, checkout e streaming;
4. applicazione Python campione con repository e transazioni per request;
5. supporto dichiarato provider per provider.

### Fase D: unit of work

1. mapper e identity map senza relazioni;
2. flush insert/update/delete;
3. versioning ottimistico;
4. relazioni e loader espliciti;
5. adapter per framework web.

### Fase E: migrazioni

1. modello diff e classificazione rischio;
2. rendering provider-specifico;
3. journal, resume e dry-run;
4. campagne live distruttive su fixture isolate;
5. eventuale adapter Alembic.

## Misure di successo

Il programma non si valuta soltanto contando feature o righe.

- Riduzione di almeno il 20% del codice duplicato nelle superfici query,
  binding e sync/async, misurata su una baseline versionata. Il totale del
  repository non e una metrica utile mentre si aggiunge un unit of work.
- Una sola definizione semantica per ogni nodo relazionale.
- Parita sync/async verificata automaticamente.
- Nessun `match` sul provider nel livello applicativo comune.
- Tutte le capability nuove inizialmente negate e aperte da gate live.
- Nessun payload nei messaggi di errore pubblici.
- Budget e cancellazione attraversano ogni nuova API.
- Overhead del nuovo strato misurato separatamente dal tempo del database.
- Workload multiutente senza perdita di session context, starvation o riuso di
  connessioni compromesse.

## Macro-blocco Geometry ORM multi-provider chiuso

La matrice qualificata copre PostgreSQL/PostGIS, MySQL, MariaDB, SQL Server e
Db2 senza dedurre supporto per somiglianza fra dialetti. I gate live coprono
Point, LineString, Polygon, `NULL`, SRID, EWKB invalido, dimensionalita
qualificate, insert, update, query, round trip e predicati spatial. I verdetti
VM e Db2 sono registrati in [`sdk/VM-EVIDENCE.md`](sdk/VM-EVIDENCE.md).

Niente lazy loading implicito resta una scelta intenzionale. `joinedload` sulle
collezioni, relationship composite, mixin astratti, ereditarieta concrete e
migrazioni esplicite a DAG sono ora implementati; single-table/joined-table
inheritance e la generazione di revisioni da diff restano fuori dalla
superficie pubblicata.

## Prossimo macro-blocco eseguibile

Il passo successivo e stabilizzare il delta per una release minor dell'SDK:

1. integrare la branch tramite review e CI completa;
2. eseguire sweep offline, supply-chain gate e fuzz lungo sul commit candidato;
3. costruire e verificare la matrice wheel sulle piattaforme dichiarate;
4. pubblicare una release candidate e promuoverla solo con tutti i gate
   eseguiti e verdi.

Il fuzz lungo e la matrice wheel finale non sono ancora stati eseguiti su
questo delta e non vanno conteggiati come passati.
