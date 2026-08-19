# Matrice della semantica di sessione

Misurata attraverso il driver e il percorso reale del pool, non con il client. Generata da `scripts/check_session_matrix.py`; non modificare a mano.

Misurata su `b80a8e2fcfbd287c5fe4bcf7cf5cca00e8206c95`, albero con modifiche non committate. Il documento entra nel commit successivo.

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
| `bootstrap.statement` | bootstrap | accepted `162e93483e01318b` | accepted `162e93483e01318b` | accepted `162e93483e01318b` | same |
| `bootstrap.pool` | bootstrap | accepted `162e93483e01318b` | accepted `162e93483e01318b` | accepted `162e93483e01318b` | same |
| `bootstrap.pool_reuse` | bootstrap | accepted `162e93483e01318b` | accepted `162e93483e01318b` | accepted `162e93483e01318b` | same |
| `transaction.isolation.read_uncommitted` | isolation | accepted `e7dfc6b352c5f8fa` | accepted `e7dfc6b352c5f8fa` | accepted `e7dfc6b352c5f8fa` | same |
| `transaction.isolation.read_committed` | isolation | accepted `a9f9113ad03776ec` | accepted `a9f9113ad03776ec` | accepted `a9f9113ad03776ec` | same |
| `transaction.isolation.repeatable_read` | isolation | accepted `bf58a565209a97c9` | accepted `bf58a565209a97c9` | accepted `bf58a565209a97c9` | same |
| `transaction.isolation.serializable` | isolation | accepted `116f091085c9f96c` | accepted `116f091085c9f96c` | accepted `116f091085c9f96c` | same |
| `transaction.access_mode.absent` | access_mode | accepted `4a611582f6d6970c` | accepted `4a611582f6d6970c` | accepted `4a611582f6d6970c` | same |
| `transaction.access_mode.read_only` | access_mode | accepted `6828dec434c3d756` | accepted `6828dec434c3d756` | accepted `6828dec434c3d756` | same |
| `transaction.access_mode.read_write` | access_mode | accepted `4a611582f6d6970c` | accepted `4a611582f6d6970c` | accepted `4a611582f6d6970c` | same |
| `transaction.context` | context | accepted `359bdbe6b003659b` | accepted `359bdbe6b003659b` | accepted `359bdbe6b003659b` | same |
| `transaction.commit` | durability | accepted `8a44dbd7dab540a9` | accepted `8a44dbd7dab540a9` | accepted `8a44dbd7dab540a9` | same |
| `transaction.rollback` | durability | accepted `5b72f84ade787da2` | accepted `5b72f84ade787da2` | accepted `5b72f84ade787da2` | same |

## Dettagli

### `bootstrap.statement`

il server accetta l'esatto SESSION_BOOTSTRAP_SQL del pool

* **mysql** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION isolation=REPEATABLE-READ
* **mariadb-12** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION isolation=REPEATABLE-READ
* **mariadb-11** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION isolation=REPEATABLE-READ

### `bootstrap.pool`

il pool consegna sessioni gia bootstrappate

* **mysql** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION isolation=REPEATABLE-READ
* **mariadb-12** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION isolation=REPEATABLE-READ
* **mariadb-11** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION isolation=REPEATABLE-READ

### `bootstrap.pool_reuse`

il pool consegna sessioni gia bootstrappate

* **mysql** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION isolation=REPEATABLE-READ
* **mariadb-12** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION isolation=REPEATABLE-READ
* **mariadb-11** — accepted: autocommit=1 time_zone=+00:00 sql_mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION isolation=REPEATABLE-READ

### `transaction.isolation.read_uncommitted`

il livello richiesto e quello che la sessione dichiara

* **mysql** — accepted: String("READ-UNCOMMITTED") | I64(0) | I64(1)
* **mariadb-12** — accepted: String("READ-UNCOMMITTED") | I64(0) | I64(1)
* **mariadb-11** — accepted: String("READ-UNCOMMITTED") | I64(0) | I64(1)

### `transaction.isolation.read_committed`

il livello richiesto e quello che la sessione dichiara

* **mysql** — accepted: String("READ-COMMITTED") | I64(0) | I64(1)
* **mariadb-12** — accepted: String("READ-COMMITTED") | I64(0) | I64(1)
* **mariadb-11** — accepted: String("READ-COMMITTED") | I64(0) | I64(1)

### `transaction.isolation.repeatable_read`

il livello richiesto e quello che la sessione dichiara

* **mysql** — accepted: String("REPEATABLE-READ") | I64(0) | I64(1)
* **mariadb-12** — accepted: String("REPEATABLE-READ") | I64(0) | I64(1)
* **mariadb-11** — accepted: String("REPEATABLE-READ") | I64(0) | I64(1)

### `transaction.isolation.serializable`

il livello richiesto e quello che la sessione dichiara

* **mysql** — accepted: String("SERIALIZABLE") | I64(0) | I64(1)
* **mariadb-12** — accepted: String("SERIALIZABLE") | I64(0) | I64(1)
* **mariadb-11** — accepted: String("SERIALIZABLE") | I64(0) | I64(1)

### `transaction.access_mode.absent`

una scrittura dentro la transazione e ammessa o rifiutata secondo la modalita

* **mysql** — accepted: scrittura ammessa
* **mariadb-12** — accepted: scrittura ammessa
* **mariadb-11** — accepted: scrittura ammessa

### `transaction.access_mode.read_only`

una scrittura dentro la transazione e ammessa o rifiutata secondo la modalita

* **mysql** — accepted: scrittura rifiutata: Execution
* **mariadb-12** — accepted: scrittura rifiutata: Execution
* **mariadb-11** — accepted: scrittura rifiutata: Execution

### `transaction.access_mode.read_write`

una scrittura dentro la transazione e ammessa o rifiutata secondo la modalita

* **mysql** — accepted: scrittura ammessa
* **mariadb-12** — accepted: scrittura ammessa
* **mariadb-11** — accepted: scrittura ammessa

### `transaction.context`

il session context e leggibile dalla variabile utente dopo START TRANSACTION

* **mysql** — accepted: String("acme")
* **mariadb-12** — accepted: String("acme")
* **mariadb-11** — accepted: String("acme")

### `transaction.commit`

l'esito dichiarato corrisponde a cio che resta sul server

* **mysql** — accepted: righe=1 attese=1 coerente=true
* **mariadb-12** — accepted: righe=1 attese=1 coerente=true
* **mariadb-11** — accepted: righe=1 attese=1 coerente=true

### `transaction.rollback`

l'esito dichiarato corrisponde a cio che resta sul server

* **mysql** — accepted: righe=0 attese=0 coerente=true
* **mariadb-12** — accepted: righe=0 attese=0 coerente=true
* **mariadb-11** — accepted: righe=0 attese=0 coerente=true

