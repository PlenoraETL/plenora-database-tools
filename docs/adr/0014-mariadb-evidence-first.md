# ADR 0014 - MariaDB: evidenza prima del provider

## Stato

Accettata (2026-08-17). Apre il ciclo MariaDB che ADR 0012 aveva rimandato
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
   qualificato: `docker-compose.mariadb.yml`, MariaDB 11.8.8 LTS fissata per
   digest immutabile in `docker/mariadb/references.json`, con la stessa
   fixture TLS a CA privata del riferimento MySQL. Il ruolo dichiarato e
   `evidence`: chiamarlo `baseline` la farebbe leggere come piattaforma
   supportata, che e l'equivoco che il fail-close esiste per impedire.
2. Il **fail-close resta**. Non viene rimosso, allentato o reso opzionale
   finche una capability non ha una prova che la sostiene. Un provider che
   accetta e poi diverge in silenzio e peggio di uno che rifiuta.
3. La fixture MariaDB e quella MySQL devono poter **stare su insieme** —
   progetti Compose distinti, porte distinte — perche l'evidenza e un
   confronto fra le due, non una descrizione di una sola.
4. Il generatore TLS e **condiviso**, parametrizzato per nome host. Due copie
   della stessa fixture divergono alla prima correzione applicata a una sola.
5. Quando l'evidenza sara raccolta, la scelta fra provider dedicato e
   qualificazione sara registrata in una ADR successiva, con i risultati
   allegati. Questa ADR non la anticipa.

## Riferimento

MariaDB 11.8 LTS, versione esatta 11.8.8, digest
`sha256:d9f7eb2637296652f24b484afd5d246f759f49f5babcadc6a9e344c9acb75fbf`.

E l'ultima LTS, coerente con la scelta fatta per MySQL, dove la baseline e la
piu recente qualificata e le LTS precedenti sono righe di compatibilita. Se
il ciclo mostrera che la versione conta — e per un fork e plausibile — le
righe si aggiungono al documento, che e gia la sola fonte letta da compose e
script.

## Conseguenze

* Il repository guadagna una fixture MariaDB avviabile e fissata, e un
  documento che dice cosa e: evidenza, non supporto.
* Nessuna promessa cambia. `docs/PROVIDER-MATURITY-MATRIX.md`,
  `docs/mysql/README.md` e il messaggio di fail-close continuano a dire che
  MariaDB non e qualificata, e restano veri.
* Il costo dell'evidenza e reale: ogni divergenza va osservata su una fixture
  viva e registrata. E il prezzo per non scoprire in produzione che il fork
  differiva su una superficie che nessuno aveva provato.
