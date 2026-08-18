# ADR 0014 - MariaDB: evidenza prima del provider

## Stato

Accettata (2026-08-17), **decisa (2026-08-18)**: il metodo di questa ADR ha
prodotto la sua evidenza, e la scelta architetturale che rimandava e ora presa
— si veda "Decisione architetturale" in fondo. Apre il ciclo MariaDB che ADR
0012 aveva rimandato
("MariaDB non viene dedotta come compatibile e richiedera una campagna
separata"). Questa ADR non decide se MariaDB avra un provider dedicato o sara
qualificata sotto quello MySQL: decide **come** si arrivera a quella scelta.

## Contesto

Oggi MariaDB e nel repository in tre forme, e nessuna e un supporto:

* `ProviderKind::Mariadb` esiste nel core, il CLI lo accetta come nome di
  provider, e compare in `contracts/v1/*.schema.json` e in
  `golden/v1/cases.json` — cioe nei contratti, dove un valore enumerato non
  implica un'implementazione;
* nessun crate lo implementa;
* il provider `mysql` fa **fail-close alla probe**: riconosce MariaDB da
  `product_version` / `version_comment` e ritorna `Unsupported` prima di
  qualunque scrittura, nominando le divergenze note — sequenze,
  `INSERT ... ON DUPLICATE KEY`, spatial `GEOMETRYCOLLECTION`, cache dei
  prepared statement, semantica di isolamento.

La domanda aperta e quale delle due strade prendere:

1. **provider dedicato** `plenora-db-mariadb`, sul modello di
   `plenora-db-mysql`;
2. **qualificazione** di MariaDB come riga della matrice del provider MySQL,
   con rimozione del fail-close dove l'evidenza la sostiene.

Sono lavori di taglia diversa, e la differenza fra i due dipende da quanto
MariaDB diverge davvero — non da quanto si presume che diverga. Le cinque
divergenze citate nel messaggio di fail-close sono state scritte da una
review, non misurate: alcune potrebbero non riguardare le superfici che il
provider usa, altre potrebbero essere piu profonde di come suonano.

## Decisione

**L'evidenza viene prima della scelta**, con lo stesso metodo che ADR 0010
fissa per SQL Server: capability atomiche dopo evidenza riproducibile.

1. Il ciclo apre con una **fixture di evidenza**, non con un riferimento
   qualificato: `docker-compose.mariadb.yml`, con le versioni fissate per
   digest immutabile in `docker/mariadb/references.json` e la stessa fixture
   TLS a CA privata del riferimento MySQL. I ruoli dichiarati sono `evidence`
   e `compatibility`, mai `baseline`: `baseline` la farebbe leggere come
   piattaforma supportata, che e l'equivoco che il fail-close esiste per
   impedire.
2. Il **fail-close resta**. Non viene rimosso, allentato o reso opzionale
   finche una capability non ha una prova che la sostiene. Un provider che
   accetta e poi diverge in silenzio e peggio di uno che rifiuta.
3. Le fixture devono poter **stare su tutte insieme** — progetti Compose
   distinti, container e porte distinti — perche l'evidenza e un confronto:
   fra le due versioni di MariaDB, e fra MariaDB e MySQL. Una descrizione di
   una sola non dice se una divergenza dipende dal fork o dalla versione.
4. Il generatore TLS e **condiviso**, parametrizzato per nome host. Due copie
   della stessa fixture divergono alla prima correzione applicata a una sola.
5. Quando l'evidenza sara raccolta, la scelta fra provider dedicato e
   qualificazione sara registrata in una ADR successiva, con i risultati
   allegati. Questa ADR non la anticipa.

## Riferimenti

| ruolo | versione | digest |
|---|---|---|
| `evidence` | MariaDB 12.3.2 | `sha256:759869cb6f003234a95c6384cdee245b4bce7de26913fe607a8110362c0c007d` |
| `compatibility` | MariaDB 11.8.8 LTS | `sha256:d9f7eb2637296652f24b484afd5d246f759f49f5babcadc6a9e344c9acb75fbf` |

**Correzione (2026-08-17).** La prima stesura di questa ADR dichiarava 11.8.8
"l'ultima LTS". Non lo e: il tag `lts` di Docker Hub risolve 12.3.2, cioe lo
stesso digest della riga `evidence`. La riga principale del ciclo e ora
12.3.2, e 11.8.8 resta come `compatibility`.

Resta perche l'evidenza gia raccolta su una versione non si butta quando ne
esce un'altra — anzi, per un fork il confronto fra due versioni e esso stesso
evidenza: dice se una divergenza dal comportamento MySQL appartiene a MariaDB
o a una sua release. Se il ciclo mostrera che servono altre righe, si
aggiungono al documento, che e gia la sola fonte letta da compose e script.

## Conseguenze

* Il repository guadagna due fixture MariaDB avviabili e fissate, e un
  documento che dice cosa sono: evidenza, non supporto.
* Nessuna promessa cambia. `docs/PROVIDER-MATURITY-MATRIX.md`,
  `docs/mysql/README.md` e il messaggio di fail-close continuano a dire che
  MariaDB non e qualificata, e restano veri.
* Il costo dell'evidenza e reale: ogni divergenza va osservata su una fixture
  viva e registrata. E il prezzo per non scoprire in produzione che il fork
  differiva su una superficie che nessuno aveva provato.

## Decisione architetturale (2026-08-18)

L'evidenza e raccolta — `docs/mariadb/EVIDENCE.md`, due tranche, 38 sonde su
tre server fissati per digest — e risponde alla domanda che questa ADR aveva
lasciato aperta. La scelta e **un crate solo**.

### Cosa dice la misura

Le cinque divergenze che il messaggio di fail-close dichiarava erano tre false
e una rovesciata. Cio che divide davvero i due motori non e il driver:
protocollo, TLS, tipi wire (dodici colonne su quattordici identiche), valori
decodificati (stesso digest sulle quattordici), macchina a stati della
sessione — cancellazione in volo, quarantena, riuso — sono gli stessi. Le due
versioni di MariaDB non divergono fra loro su nessuna sonda.

Cio che diverge sta in quattro punti, e sono decisioni di **prodotto** dentro
codice che per il resto e comune. Un crate separato duplicherebbe tutto il
resto per isolarle.

### La forma

* **Un crate**, `plenora-db-mysql`, con una implementazione interna condivisa.
* **Due provider pubblici distinti**, `MysqlProvider` e `MariadbProvider`.
* **Un profilo esplicito per prodotto**, interno, che possiede cio che
  diverge.
* **Nessuna selezione automatica.** `MysqlProvider` continua a rifiutare
  MariaDB alla probe, e `MariadbProvider` rifiuta MySQL con la stessa
  simmetria. Un provider che si adatta al server che trova sceglie per il
  consumer, e lo fa nel punto in cui il consumer non sta guardando: chi
  dichiara `mysql` e finisce su MariaDB ha un problema di configurazione, non
  una comodita da assecondare.
* **CLI e SDK avranno superfici esplicite** — `mariadb` fra i provider di
  `database-probe`, `connect_mariadb` nel SDK — non un flag su quelle
  esistenti.

### Cosa possiede il profilo

Ogni riga e una divergenza misurata, non una previsione:

| il profilo possiede | perche |
|---|---|
| `ProviderKind` | i due provider dichiarano identita diverse nei verdetti e negli errori |
| riconoscimento e versioni ammesse | oggi la detection e un `contains("mariadb")` su due stringhe; diventa il modo in cui ogni profilo riconosce **il proprio** motore e rifiuta l'altro |
| SQL del timeout e conversione dell'unita | `MAX_EXECUTION_TIME` (ms) non esiste su MariaDB, che usa `max_statement_time` (s): errore 1193 misurato |
| query di catalogo e indici funzionali | `information_schema.statistics.EXPRESSION` non esiste su MariaDB: errore 1054 misurato, e blocca ogni lettura che passa dal catalogo |
| normalizzazione dei metadata wire | dalla stessa DDL `document JSON` escono `native_type=json` e `native_type=text`: divergenza misurata nello schema Arrow pubblicato |
| strategia SRID e capability spatial | l'attributo `SRID` di colonna e `information_schema.columns.SRS_ID` non esistono su MariaDB |

Sul metadata wire il profilo deve anche **decidere cosa `MYSQL_NATIVE_TYPE`
annoti** — il tipo del wire o quello della DDL — perche oggi il contratto non
lo dice e le due letture portano a due normalizzazioni diverse. La scelta va
registrata dove il campo e definito, non dedotta dal comportamento.

### Cosa resta fail-closed

MariaDB **non e qualificata** da questa decisione: la decisione riguarda dove
vivra il codice, non cosa e stato dimostrato. Restano fail-closed, e ci
restano finche non c'e una misura che le sostenga:

* **spatial su MariaDB** — la decodifica di una geometry attraverso il
  provider non e mai stata osservata: su MySQL la regola sull'SRID dichiarato
  la rifiuta, e su MariaDB il catalogo non risponde;
* **commit ambiguo** — `not_measured` su tutti e tre i server, perche
  osservarlo richiede fault injection deterministica sul `COMMIT`;
* **lettura via catalogo** — `not_measured` su MariaDB, dipendente
  dall'errore 1054.

Nessuna delle tre blocca la scelta del crate condiviso, e nessuna delle tre
puo essere pubblicata come capability prima di essere misurata.

### Conseguenza sul bypass

Il bypass di solo test resta cio che e — `#[cfg(test)]`, scoped, senza
superficie pubblica — finche `MariadbProvider` non esiste. Quando esistera,
sara quel provider a raggiungere le stesse superfici alla luce del sole, e il
bypass avra esaurito il suo compito.
