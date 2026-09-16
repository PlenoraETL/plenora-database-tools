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
