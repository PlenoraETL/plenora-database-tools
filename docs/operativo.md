# Operativo

Cio che i file Compose non dicono da soli. Tutto il resto — porte, volumi,
container, immagini — si legge dai `docker-compose.*.yml`, che sono la fonte:
questo documento non li ricopia.

## Progetti Compose

Ogni provider ha il proprio progetto, dichiarato dalla riga `name:` del suo
Compose. I volumi sono prefissati dal progetto, quindi provider diversi non si
toccano. **Non** va passato `--remove-orphans`: con progetti distinti che
condividono un demone, quell'opzione rimuove i container degli altri.

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
             dataflow-mysql \
             dataflow-mysql-certgen \
             dataflow-postgres \
             dataflow-postgres-tls \
             dataflow-postgres-tls-certgen \
             dataflow-sqlserver \
             dataflow-sqlserver-certgen \
             dataflow-sqlserver-init
```

**Non** cancellare i volumi del vecchio progetto: non e necessario e non e
reversibile. Restano orfani e inerti; chi vuole recuperare lo spazio puo
elencarli con `docker volume ls | grep database-tools` e rimuoverli quando ha
verificato che i nuovi riferimenti funzionano — ma e una decisione sua, non un
passo della migrazione.

I nuovi progetti ripartono da volumi vuoti e le fixture nascono dagli script in
`docker/*/init`, che il primo avvio riesegue.
