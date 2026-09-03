# Stato del codice

**Documento generato.** Non va modificato a mano: ogni riga qui sotto
e letta dai sorgenti da `scripts/render_state.py`, e una guardia
verifica che rigenerarlo non produca differenze. Se un numero e
sbagliato, e sbagliato nel codice — oppure il documento e vecchio, e
si aggiorna cosi:

```powershell
python scripts\render_state.py
```

## Crate

| crate | versione |
| --- | --- |
| `plenora-database-cli` | 3.0.0 |
| `plenora-database-core` | 3.0.0 |
| `plenora-database-engine` | 3.0.0 |
| `plenora-database-py` | 3.0.0 |
| `plenora-database-sql` | 3.0.0 |
| `plenora-database-testkit` | 3.0.0 |
| `plenora-db-db2` | 3.0.0 |
| `plenora-db-mysql` | 3.0.0 |
| `plenora-db-postgres` | 3.0.0 |
| `plenora-db-sqlserver` | 3.0.0 |

## Contratto attivo

La major attiva e `contracts/v2/`, e contiene:

- `age-admin-capabilities.schema.json`
- `age-capabilities.schema.json`
- `capabilities.schema.json`
- `common.schema.json`
- `golden-manifest.schema.json`
- `loss-report.schema.json`
- `plan.schema.json`
- `read-checkpoint.schema.json`
- `write-outcome.schema.json`

E l'unica nel worktree: le major precedenti stanno in Git, e nessun
file qui dentro le referenzia. Il gate offline fallisce se una di esse
torna nell'albero di lavoro, o se un riferimento la nomina.

## Capability dichiarate

Cio che ciascuna dichiarazione di capability contiene, letto da dove
e scritta. Un valore che non e un letterale — `spatial` su PostgreSQL
dipende dalla presenza di PostGIS — resta l'espressione sorgente:
risolverla qui sarebbe un'affermazione che il codice non fa.

### `reads`

| reads | PostgreSQL | MySQL | MariaDB | SQL Server | IBM Db2 LUW |
| --- | --- | --- | --- | --- | --- |
| `streaming` | `true` | `true` | `true` | `true` | `true` |
| `server_cursor` | `false` | `false` | `false` | `false` | `false` |
| `pagination` | `true` | `true` | `true` | `true` | `true` |
| `projection` | `true` | `true` | `true` | `true` | `true` |
| `filter` | `true` | `true` | `true` | `true` | `true` |
| `ordering` | `true` | `true` | `true` | `true` | `true` |
| `resumable` | `true` | `true` | `true` | `true` | `true` |

### `writes`

| writes | PostgreSQL | MySQL | MariaDB | SQL Server | IBM Db2 LUW |
| --- | --- | --- | --- | --- | --- |
| `create` | `true` | `true` | `true` | `true` | `true` |
| `append` | `true` | `true` | `true` | `true` | `true` |
| `truncate_insert` | `true` | `false` | `false` | `true` | `false` |
| `update` | `true` | `true` | `true` | `true` | `true` |
| `upsert` | `true` | `true` | `true` | `true` | `true` |
| `replace` | `true` | `true` | `true` | `true` | `true` |
| `delete_by_keys` | `true` | `true` | `true` | `true` | `true` |
| `bulk` | `true` | `true` | `true` | `true` | `true` |
| `array_binding` | `false` | `false` | `false` | `false` | `true` |
| `returning` | `false` | `false` | `false` | `false` | `false` |
| `rollback_on_failure` | `true` | `true` | `true` | `true` | `true` |

### `transactions`

| transactions | PostgreSQL | MySQL | MariaDB | SQL Server | IBM Db2 LUW |
| --- | --- | --- | --- | --- | --- |
| `single_transaction` | `true` | `true` | `true` | `true` | `true` |
| `savepoints` | `true` | `true` | `true` | `true` | `true` |
| `transactional_ddl` | `true` | `false` | `false` | `true` | `true` |
| `staged_swap` | `false` | `false` | `false` | `true` | `false` |
| `scope` | `TransactionScope::Transaction` | `TransactionScope::Transaction` | `TransactionScope::Transaction` | `TransactionScope::Transaction` | `TransactionScope::Transaction` |

## Sub-comandi del CLI

Dal catalogo che il binario espone. La feature e quella che li
compila: un comando la cui feature non e stata compilata esiste nel
progetto ma non in quel binario, e il CLI lo dice invece di stampare
l'aiuto.

| comando | feature |
| --- | --- |
| `benchmark-oltp` | `postgres` |
| `benchmark-read` | `postgres` |
| `benchmark-spatial` | `postgres` |
| `benchmark-write` | `postgres` |
| `bulk-write` | `postgres` |
| `conditional-update` | `postgres` |
| `database-describe` | `sempre` |
| `database-execute-ddl` | `sempre` |
| `database-execute-scalar` | `sempre` |
| `database-execute-sql` | `sempre` |
| `database-inspect-catalogs` | `sempre` |
| `database-inspect-objects` | `sempre` |
| `database-inspect-schemas` | `sempre` |
| `database-portable-execute` | `sempre` |
| `database-probe` | `sempre` |
| `database-query-summary` | `sempre` |
| `database-read-ipc` | `sempre` |
| `database-read-summary` | `sempre` |
| `database-write-ipc` | `sempre` |
| `diagnose` | `postgres` |
| `doctor` | `postgres` |
| `execute-ddl` | `postgres` |
| `execute-scalar` | `postgres` |
| `execute-sql` | `postgres` |
| `explain` | `postgres` |
| `inspect-catalogs` | `postgres` |
| `inspect-database` | `postgres` |
| `inspect-dataset` | `sempre` |
| `inspect-objects` | `postgres` |
| `inspect-schemas` | `postgres` |
| `inspect-tables` | `postgres` |
| `mysql-conditional-update` | `mysql` |
| `mysql-describe` | `mysql` |
| `mysql-execute-ddl` | `mysql` |
| `mysql-execute-scalar` | `mysql` |
| `mysql-execute-sql` | `mysql` |
| `mysql-inspect-schemas` | `mysql` |
| `mysql-inspect-tables` | `mysql` |
| `mysql-probe` | `mysql` |
| `mysql-transaction-test` | `mysql` |
| `pool-status` | `postgres` |
| `portable-compile` | `sempre` |
| `portable-execute` | `postgres` |
| `postgres-describe` | `postgres` |
| `postgres-probe` | `postgres` |
| `postgres-query` | `postgres` |
| `postgres-read-ipc` | `postgres` |
| `postgres-read-summary` | `postgres` |
| `postgres-write-ipc` | `postgres` |
| `profile-check` | `postgres` |
| `profile-list` | `postgres` |
| `session-context-test` | `postgres` |
| `test-cancellation` | `postgres` |
| `test-concurrency` | `postgres` |
| `test-spatial` | `postgres` |
| `test-streaming` | `postgres` |
| `transaction-test` | `postgres` |
| `validate-plan` | `sempre` |

## Inventario dei test dei provider

Conteggio delle funzioni Rust annotate come test nei crate provider.
MySQL e MariaDB condividono un crate e restano quindi una riga sola.

| provider | test Rust |
| --- | --- |
| PostgreSQL | 302 |
| MySQL + MariaDB | 299 |
| SQL Server | 151 |
| IBM Db2 LUW | 61 |

### Famiglie del gate MySQL

Le tre famiglie che il gate MySQL distingue, contate sulla sorgente.

| famiglia | test |
| --- | --- |
| `live_default` | 39 |
| `live_reference` | 34 |
| `unit` | 224 |
