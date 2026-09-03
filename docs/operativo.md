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
Il listener della fixture e plaintext per scelta esplicita di test: non prova
TCPS e non autorizza ad aprire quella capability.

```bash
docker compose -f docker-compose.oracle.yml up -d --wait
python scripts/check_oracle_reference.py
docker compose -f docker-compose.oracle.yml down --volumes
```

Le credenziali non sono duplicate nel runner: vengono lette dal container
avviato. `down --volumes` elimina i dati del solo progetto Oracle ed e usato
dal workflow alla fine della campagna.

## Migrazione (una tantum)

**Migrazione (una tantum).** A collidere sono soltanto i **container**: i
`container_name` sono fissi, quindi quelli del vecchio progetto `database-tools`
occupano i nomi che i nuovi progetti vogliono usare. I volumi no — sono
prefissati dal progetto, quindi `database-tools_mysql_data` e
`plenora-mysql_mysql_data` convivono senza conflitto.

Rimuovere i soli container, prima del primo `up`:

```bash
docker rm -f dataflow-mariadb \
             dataflow-mariadb-certgen \
             dataflow-mariadb-11 \
             dataflow-mariadb-11-certgen \
             dataflow-mariadb-10 \
             dataflow-mariadb-10-certgen \
             dataflow-mysql \
             dataflow-mysql-certgen \
             dataflow-postgres \
             dataflow-postgres-tls \
             dataflow-postgres-tls-certgen \
             plenora-age \
             dataflow-sqlserver \
             dataflow-sqlserver-certgen \
             dataflow-sqlserver-init \
             plenora-oracle
```

**Non** cancellare i volumi del vecchio progetto: non e necessario e non e
reversibile. Restano orfani e inerti; chi vuole recuperare lo spazio puo
elencarli con `docker volume ls | grep database-tools` e rimuoverli quando ha
verificato che i nuovi riferimenti funzionano — ma e una decisione sua, non un
passo della migrazione.

I nuovi progetti ripartono da volumi vuoti e le fixture nascono dagli script in
`docker/*/init`, che il primo avvio riesegue.

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
