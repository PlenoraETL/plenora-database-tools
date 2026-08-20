# Matrice della semantica di sessione

Misurata attraverso il driver e il percorso reale del pool, non con il client. Generata da `scripts/check_session_matrix.py`; non modificare a mano.

Misurata da un albero pulito: il runner lo pretende prima di avviare Docker, e verifica che HEAD non si muova durante la corsa. Il commit e nel verdetto JSON.

```sql
SET SESSION autocommit = 1, time_zone = '+00:00', sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'
```

## Riferimenti

| chiave | riferimento | versione | digest osservato |
| --- | --- | --- | --- |
| `mysql` | MySQL 9.7 | 9.7.2 | `sha256:257388edf9c84dbc` |
| `mariadb-12` | MariaDB 12.3 | 12.3.2-MariaDB-ubu2404 | `sha256:759869cb6f003234` |
| `mariadb-11` | MariaDB 11.8 LTS | 11.8.8-MariaDB-ubu2404 | `sha256:d9f7eb2637296652` |

**13 sonde: 13 coincidono, 0 divergono.**

## Sonde

| sonda | superficie | `mysql` | `mariadb-12` | `mariadb-11` | esito |
| --- | --- | --- | --- | --- | --- |
| `bootstrap.statement` | bootstrap | accepted `73f668200524de79` | accepted `73f668200524de79` | accepted `73f668200524de79` | same |
| `bootstrap.pool` | bootstrap | accepted `73f668200524de79` | accepted `73f668200524de79` | accepted `73f668200524de79` | same |
| `bootstrap.after_return` | bootstrap | accepted `0a9a31068d3ce11a` | accepted `0a9a31068d3ce11a` | accepted `0a9a31068d3ce11a` | same |
| `transaction.isolation.read_uncommitted` | isolation | accepted `0a467d059c78cfb1` | accepted `0a467d059c78cfb1` | accepted `0a467d059c78cfb1` | same |
| `transaction.isolation.read_committed` | isolation | accepted `79c0b603bfcd8584` | accepted `79c0b603bfcd8584` | accepted `79c0b603bfcd8584` | same |
| `transaction.isolation.repeatable_read` | isolation | accepted `2153781dec3713d9` | accepted `2153781dec3713d9` | accepted `2153781dec3713d9` | same |
| `transaction.isolation.serializable` | isolation | accepted `7dc421f3d9f24ef4` | accepted `7dc421f3d9f24ef4` | accepted `7dc421f3d9f24ef4` | same |
| `transaction.access_mode.absent` | access_mode | accepted `4a611582f6d6970c` | accepted `4a611582f6d6970c` | accepted `4a611582f6d6970c` | same |
| `transaction.access_mode.read_only` | access_mode | accepted `d4dbc729c663576b` | accepted `d4dbc729c663576b` | accepted `d4dbc729c663576b` | same |
| `transaction.access_mode.read_write` | access_mode | accepted `4a611582f6d6970c` | accepted `4a611582f6d6970c` | accepted `4a611582f6d6970c` | same |
| `transaction.context` | context | accepted `822b33ad87c148a0` | accepted `822b33ad87c148a0` | accepted `822b33ad87c148a0` | same |
| `transaction.commit` | durability | accepted `ece7554d89658f41` | accepted `ece7554d89658f41` | accepted `ece7554d89658f41` | same |
| `transaction.rollback` | durability | accepted `68e245e69e9803da` | accepted `68e245e69e9803da` | accepted `68e245e69e9803da` | same |

## Dettagli

### `bootstrap.statement`

il server applica l'esatto SESSION_BOOTSTRAP_SQL del pool

* **mysql** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
* **mariadb-12** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
* **mariadb-11** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION

### `bootstrap.pool`

il pool consegna sessioni gia bootstrappate

* **mysql** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
* **mariadb-12** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
* **mariadb-11** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION

### `bootstrap.after_return`

una sessione consegnata dopo il rientro di una sporca e bootstrappata

* **mysql** — accepted: connessione riusata=false autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
* **mariadb-12** — accepted: connessione riusata=false autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
* **mariadb-11** — accepted: connessione riusata=false autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION

### `transaction.isolation.read_uncommitted`

la sessione dichiara il livello richiesto

* **mysql** — accepted: READ-UNCOMMITTED
* **mariadb-12** — accepted: READ-UNCOMMITTED
* **mariadb-11** — accepted: READ-UNCOMMITTED

### `transaction.isolation.read_committed`

la sessione dichiara il livello richiesto

* **mysql** — accepted: READ-COMMITTED
* **mariadb-12** — accepted: READ-COMMITTED
* **mariadb-11** — accepted: READ-COMMITTED

### `transaction.isolation.repeatable_read`

la sessione dichiara il livello richiesto

* **mysql** — accepted: REPEATABLE-READ
* **mariadb-12** — accepted: REPEATABLE-READ
* **mariadb-11** — accepted: REPEATABLE-READ

### `transaction.isolation.serializable`

la sessione dichiara il livello richiesto

* **mysql** — accepted: SERIALIZABLE
* **mariadb-12** — accepted: SERIALIZABLE
* **mariadb-11** — accepted: SERIALIZABLE

### `transaction.access_mode.absent`

una scrittura dentro la transazione e ammessa o rifiutata secondo la modalita

* **mysql** — accepted: scrittura ammessa
* **mariadb-12** — accepted: scrittura ammessa
* **mariadb-11** — accepted: scrittura ammessa

### `transaction.access_mode.read_only`

una scrittura dentro la transazione e ammessa o rifiutata secondo la modalita

* **mysql** — accepted: rifiuto codice=1792 categoria=Execution effetto=None retry=Never
* **mariadb-12** — accepted: rifiuto codice=1792 categoria=Execution effetto=None retry=Never
* **mariadb-11** — accepted: rifiuto codice=1792 categoria=Execution effetto=None retry=Never

### `transaction.access_mode.read_write`

una scrittura dentro la transazione e ammessa o rifiutata secondo la modalita

* **mysql** — accepted: scrittura ammessa
* **mariadb-12** — accepted: scrittura ammessa
* **mariadb-11** — accepted: scrittura ammessa

### `transaction.context`

il session context e leggibile dalla variabile utente dopo START TRANSACTION

* **mysql** — accepted: acme
* **mariadb-12** — accepted: acme
* **mariadb-11** — accepted: acme

### `transaction.commit`

l'esito dichiarato corrisponde a cio che resta sul server

* **mysql** — accepted: righe=1
* **mariadb-12** — accepted: righe=1
* **mariadb-11** — accepted: righe=1

### `transaction.rollback`

l'esito dichiarato corrisponde a cio che resta sul server

* **mysql** — accepted: righe=0
* **mariadb-12** — accepted: righe=0
* **mariadb-11** — accepted: righe=0

