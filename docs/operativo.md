# Operativo

Cio che i file Compose non dicono da soli. Tutto il resto — porte, volumi,
container, immagini — si legge dai `docker-compose.*.yml`, che sono la fonte:
questo documento non li ricopia.

## Progetti Compose

Ogni provider ha il proprio progetto, dichiarato dalla riga `name:` del suo
Compose. Compose usa il nome del progetto per isolare container, reti e volumi;
provider diversi non si toccano. `--remove-orphans` riguarda i servizi del
progetto corrente che non sono piu dichiarati nel file Compose: non serve nei
comandi ordinari di queste fixture.

## Fixture Oracle

Oracle viene qualificato soltanto su Linux AMD64. Il Compose fissa immagine e
piattaforma; il gate confronta quei valori con `docker/oracle/references.json`,
attende l'healthcheck e interroga la versione del server prima del verdetto.
Il riferimento usa la variante `full`: la campagna deve provare anche Oracle
Spatial e rifiuta un'immagine standard priva di quella componente.
Il data path funzionale usa il listener TCP locale. La stessa fixture configura
anche un listener TCPS con certificato firmato da una CA privata effimera: il
gate prova sia la connessione con quella CA sia il rifiuto dello stesso server
senza trust. Questa e una prova di autenticazione del server; non dichiara una
mutua autenticazione client, che il fixture non configura.

```bash
docker compose -f docker-compose.oracle.yml up -d --wait
python scripts/check_oracle_reference.py
docker compose -f docker-compose.oracle.yml down --volumes
```

Le credenziali non sono duplicate nel runner: vengono lette dal container
avviato. `down --volumes` elimina i dati del solo progetto Oracle ed e usato
dal workflow alla fine della campagna.

## Conflitti fra progetti locali

I `container_name` espliciti sono unici sull'host, anche fra progetti Compose
distinti. Se `up` segnala un nome occupato, identificare prima il progetto
proprietario del container con `docker inspect` e la label
`com.docker.compose.project`. Fermare o rimuovere soltanto la fixture non piu
necessaria, usando il suo Compose e il suo nome di progetto.

I volumi sono separati per progetto. Un conflitto di nome del container non
richiede la cancellazione dei volumi; `down --volumes` si usa soltanto quando
si vuole eliminare anche i dati della fixture selezionata.

## Reset del fixture Db2

Il gate Db2 tratta `PLENORA_TEST` come schema di fixture sacrificabile: a ogni
inizializzazione elimina soltanto gli oggetti che il fixture stesso conosce e
ricrea lo schema. L'healthcheck attende il completamento del reset corrente e
verifica anche l'inventario nel catalogo. Se trova uno stato diverso da quello
atteso fallisce, invece di allargare la cancellazione. Il volume dell'istanza
resta persistente tra i riavvii; `down --volumes` e riservato alla chiusura
esplicita della campagna.

L'immagine Community non assume la presenza di `SYSTOOLSPACE`: il reset usa DDL
esplicito e verificato. Questo rende il secondo avvio una parte della prova di
idempotenza, non una condizione lasciata alla storia del volume locale.

## Db2 con la semantica VARCHAR2

La regola S (`plenora_database_core::store_rewrite`) legge `varchar2_compat`
da `SYSIBMADM.DBCFG` nella transazione di ogni scrittura Db2: con `ON` il testo
vuoto si salva come `NULL` e si rifiuta. Il parametro si fissa alla creazione
del database, e il fixture del gate non ne crea uno con la semantica attiva.
La prova `write_rewrite_ora_tests::db2_ora_s_testo_vuoto_con_varchar2_compat_non_diventa_null`
non e quindi nel gate (il suo nome non contiene il prefisso dei test live) e si
esegue a mano, nel container del fixture:

```sh
su - db2inst1 -c "db2set DB2_COMPATIBILITY_VECTOR=ORA && db2stop force && db2start \
  && db2 create database PLNORA && db2set DB2_COMPATIBILITY_VECTOR= \
  && db2stop force && db2start"
PLENORA_DB2_ORA_DATABASE=plnora cargo test --locked -p plenora-db-db2 --lib \
  db2_ora_s -- --ignored
```

Il vettore si toglie subito dopo la creazione: `varchar2_compat` resta `ON` nel
nuovo database e `OFF` in `plenora`.

## Le misure della regola A

Una scrittura si ammette soltanto per una coppia fra il tipo Arrow
dell'ingresso e il tipo nativo del target che il fixture del provider misura
sulla major del server (`plenora_database_core::write_admission`, la regola A):
`tests/fixtures/scrittura_server_<prodotto>.json`. Una coppia o una major non
misurata si rifiuta con ogni politica, come non provata. Le misure si producono
e si verificano con la sonda, che scrive con il percorso della libreria una
batteria fissa di valori limite per ogni chiave e li rilegge con la sua lettura:

```bash
python scripts/sonde_scritture_server.py --server oracle --riferimenti            # verifica
python scripts/sonde_scritture_server.py --server oracle --riferimenti --aggiorna # riscrive il fixture
```

Senza `--aggiorna` la sonda confronta ogni misura con il fixture e fallisce se
una chiave cambia: e il gate del workflow `write-admission-probes`. Con
`--aggiorna` il referto sostituisce, nel fixture, la misura della stessa major;
la differenza si rilegge in PR come ogni altra prova. `--container NOME
--porta P` usa un container gia pronto. Per la diagnosi, con la sola misura,
`PLENORA_SONDE_SCRITTURE_FILTRO` restringe le chiavi,
`PLENORA_SONDE_SCRITTURE_TRACCIA=1` stampa gli errori (senza dati) e
`PLENORA_SONDE_SCRITTURE_CONSERVA=1` lascia la tabella `PLN_SONDA_W`.

Rientrare in un limite della regola A (una coppia rifiutata perche non
misurata, una major nuova, una lunghezza fuori da quelle misurate) e sempre lo
stesso passo: aggiungere la chiave all'elenco chiuso della sonda del provider
(`live_sonde_scritture_server`), misurarla con `--aggiorna` e far rileggere il
fixture. I limiti dichiarati della regola stanno nel modulo
`plenora_database_core::write_admission`. Per Oracle, il registro dei limiti
include [zero negativo e collation delle chiavi](../crates/plenora-db-oracle/src/write_admission.rs):
la dichiarazione di una perdita non sostituisce la qualifica del bind.
Il confronto delle chiavi con catalogo incompleto si rifiuta anche in
[MySQL/MariaDB](../crates/plenora-db-mysql/src/write.rs) e
[SQL Server](../crates/plenora-db-sqlserver/src/write/keys.rs): nessuna
collation mancante viene sostituita dal default dello staging.
Il rifiuto in MySQL/MariaDB vale anche per le colonne testuali non chiave
dello staging Update: metadati assenti, vuoti o non rappresentabili non
provano la semantica del target. Il controllo e le regressioni sono in
`build_temp_staging_sql` e `write_tests.rs` dello stesso crate.
