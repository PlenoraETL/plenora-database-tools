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
| `plenora-database-cli` | 1.2.0 |
| `plenora-database-core` | 1.2.0 |
| `plenora-database-engine` | 1.2.0 |
| `plenora-database-py` | 0.10.0 |
| `plenora-database-sql` | 1.2.0 |
| `plenora-database-testkit` | 1.2.0 |
| `plenora-db-mysql` | 1.2.0 |
| `plenora-db-postgres` | 1.2.0 |
| `plenora-db-sqlserver` | 1.2.0 |

## Contratto attivo

La major attiva e `contracts/v2/`, e contiene:

- `capabilities.schema.json`
- `common.schema.json`
- `golden-manifest.schema.json`
- `loss-report.schema.json`
- `plan.schema.json`
- `write-outcome.schema.json`

Le major precedenti restano leggibili ma sono ritirate: nessuno le
referenzia, e il gate offline fallisce se la major attiva torna a
farlo.

## Capability pubblicate

Cio che ciascun provider dichiara, letto dalla sua dichiarazione. Un
valore che non e un letterale — `spatial` su PostgreSQL dipende dalla
presenza di PostGIS — resta l'espressione sorgente: risolverla qui
sarebbe un'affermazione che il codice non fa.

### `reads`

| reads | PostgreSQL | MySQL | MariaDB | SQL Server |
| --- | --- | --- | --- | --- |
| `streaming` | `true` | `true` | `true` | `true` |
| `server_cursor` | `false` | `false` | `false` | `false` |
| `pagination` | `true` | `false` | `false` | `true` |
| `projection` | `true` | `true` | `true` | `true` |
| `filter` | `true` | `true` | `true` | `true` |
| `ordering` | `true` | `true` | `true` | `true` |
| `resumable` | `false` | `false` | `false` | `false` |

### `writes`

| writes | PostgreSQL | MySQL | MariaDB | SQL Server |
| --- | --- | --- | --- | --- |
| `create` | `true` | `true` | `false` | `true` |
| `append` | `true` | `true` | `true` | `true` |
| `truncate_insert` | `true` | `false` | `false` | `true` |
| `update` | `true` | `true` | `false` | `true` |
| `upsert` | `true` | `true` | `false` | `true` |
| `replace` | `true` | `true` | `false` | `true` |
| `delete_by_keys` | `true` | `true` | `false` | `true` |
| `bulk` | `true` | `true` | `false` | `true` |
| `array_binding` | `false` | `false` | `false` | `false` |
| `returning` | `false` | `false` | `false` | `false` |
| `rollback_on_failure` | `true` | `true` | `false` | `true` |

## Sub-comandi del CLI

- `benchmark-oltp`
- `benchmark-read`
- `benchmark-spatial`
- `benchmark-write`
- `bulk-write`
- `conditional-update`
- `database-probe`
- `db2`
- `diagnose`
- `doctor`
- `duckdb`
- `execute-ddl`
- `execute-scalar`
- `execute-sql`
- `explain`
- `inspect-catalogs`
- `inspect-database`
- `inspect-dataset`
- `inspect-objects`
- `inspect-schemas`
- `inspect-tables`
- `mariadb`
- `mysql`
- `mysql-conditional-update`
- `mysql-describe`
- `mysql-execute-ddl`
- `mysql-execute-scalar`
- `mysql-execute-sql`
- `mysql-inspect-schemas`
- `mysql-inspect-tables`
- `mysql-probe`
- `mysql-transaction-test`
- `oracle`
- `pool-status`
- `portable-compile`
- `portable-execute`
- `postgres`
- `postgres-describe`
- `postgres-probe`
- `postgres-query`
- `postgres-read-ipc`
- `postgres-read-summary`
- `postgres-write-ipc`
- `profile-check`
- `profile-list`
- `session-context-test`
- `sqlite`
- `sqlserver`
- `test-cancellation`
- `test-concurrency`
- `test-spatial`
- `test-streaming`
- `transaction-test`
- `validate-plan`

## Inventario dei test MySQL

Le tre famiglie che il gate MySQL distingue, contate sulla sorgente.

| famiglia | test |
| --- | --- |
| `live_default` | 37 |
| `live_reference` | 25 |
| `unit` | 189 |
