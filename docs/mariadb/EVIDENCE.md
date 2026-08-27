# Evidenza MariaDB

**Documento generato.** Elenca i riferimenti e le sonde leggendo i
cataloghi eseguibili. Si aggiorna con:

```powershell
python scripts\render_mariadb_evidence.py
```

Il codice pubblica `MariadbProvider` come prodotto distinto dentro il
crate condiviso con MySQL. La selezione resta esplicita: un provider
MySQL rifiuta MariaDB e quello MariaDB rifiuta MySQL.

Questo inventario **non equivale a un gate live passato**. Gli esiti, il
commit e l'identita delle immagini appartengono al verdetto JSON della
singola corsa. Se il gate non e stato eseguito, non e passato.

## Come riprodurre

```powershell
docker compose -f docker-compose.mysql.yml up -d --wait
docker compose -f docker-compose.mariadb.yml up -d --wait
python scripts/check_mariadb_divergence.py
python scripts/check_mariadb_driver.py
python scripts/check_session_campaign.py
```

`check_mariadb_divergence.py` misura SQL e cataloghi direttamente;
`check_mariadb_driver.py` attraversa driver e provider; la campagna di
sessione rigenera `SESSION-MATRIX.md`. Le famiglie `raw` e `provider`
restano distinte perche la prima misura il protocollo e la seconda il
percorso realmente pubblicato.

## Riferimenti

| ruolo | riferimento | versione | digest |
| --- | --- | --- | --- |
| `evidence` | MariaDB 12.3 | 12.3.2 | `sha256:759869cb6f003234a95c6384cdee245b4bce7de26913fe607a8110362c0c007d` |
| `compatibility` | MariaDB 11.8 LTS | 11.8.8 | `sha256:d9f7eb2637296652f24b484afd5d246f759f49f5babcadc6a9e344c9acb75fbf` |
| `compatibility` | MariaDB 10.11 LTS | 10.11.19 | `sha256:ce66c7be32a03aabe7241d0a10993a2db827ef652a35d25727d92a832ac8ef73` |

Versione, digest, container e porta hanno una sola fonte:
`docker/mariadb/references.json`.

## Sonde SQL e catalogo

| superficie | sonda | domanda |
| --- | --- | --- |
| probe | `probe.version` | cosa risponde VERSION() |
| probe | `probe.version_comment` | cosa risponde @@version_comment |
| probe | `probe.lower_case_table_names` | come tratta il case dei nomi |
| probe | `probe.sql_mode` | quale sql_mode dichiara |
| probe | `probe.transaction_isolation` | quale isolamento dichiara, con il nome che il provider usa |
| sessione | `session.max_execution_time` | accetta il timeout di statement che il provider imposta |
| sessione | `session.isolation_serializable` | accetta SERIALIZABLE e lo rilegge uguale |
| sessione | `session.context_variable` | regge la chiave puntata del SessionContext |
| catalogo | `catalog.statistics_expression` | espone EXPRESSION in information_schema.statistics |
| catalogo | `catalog.statistics_shape` | espone le colonne da cui il preflight Upsert ricostruisce gli indici |
| scrittura | `write.on_duplicate_key_rowcount` | quante righe dichiara un ON DUPLICATE KEY che aggiorna |
| scrittura | `write.on_duplicate_key_second_unique` | su quale indice unico scatta con due chiavi candidate |
| scrittura | `write.truncate_survives_rollback` | se TRUNCATE sopravvive a un rollback |
| scrittura | `write.delete_survives_rollback` | se DELETE FROM — la Replace di MySQL — torna indietro |
| spatial | `spatial.srid_column` | accetta l'attributo SRID di colonna, che la fixture MySQL usa |
| spatial | `spatial.geometrycollection` | accetta una colonna GEOMETRYCOLLECTION |
| prepared | `prepared.instances_table` | espone performance_schema.prepared_statements_instances |
| sequenze | `sequence.create` | accetta CREATE SEQUENCE |

## Sonde driver e provider

Il catalogo compilato contiene 102 sonde. Il ruolo e letto
dagli inventari del gate: una prova richiesta che cambia esito rende la
campagna rossa; una sonda osservativa registra invece una differenza.

| famiglia | sonda | ruolo nel gate |
| --- | --- | --- |
| raw | `raw.tls_cipher` | osservativa |
| raw | `raw.type_table` | osservativa |
| raw | `raw.type_row` | osservativa |
| raw | `raw.geometry_table` | osservativa |
| raw | `raw.prepare_metadata_geometry` | osservativa |
| raw | `raw.prepare_metadata` | osservativa |
| raw | `raw.prepare_parameters` | osservativa |
| raw | `raw.column_srid` | osservativa |
| raw | `raw.geometry_columns_registry` | osservativa |
| raw | `raw.declared_column_srid` | osservativa |
| raw | `raw.spatial_functions` | osservativa |
| raw | `raw.max_execution_time` | osservativa |
| raw | `raw.statistics_expression` | osservativa |
| raw | `raw.returning_forms` | osservativa |
| raw | `raw.spatial_write_forms` | osservativa |
| raw | `raw.spatial_index_forms` | osservativa |
| raw | `raw.scalar_function_forms` | osservativa |
| raw | `raw.geometry_result_forms` | osservativa |
| raw | `raw.crs_rule_check` | osservativa |
| raw | `raw.geometry_function_forms` | osservativa |
| raw | `raw.exact_geometry_column` | osservativa |
| raw | `raw.geometry_dimensions` | osservativa |
| provider | `provider.test_connection` | osservativa |
| provider | `provider.capabilities` | osservativa |
| provider | `provider.describe_object` | osservativa |
| provider | `provider.query_schema` | osservativa |
| provider | `provider.query_values` | osservativa |
| provider | `provider.read` | osservativa |
| provider | `provider.read_geometry` | osservativa |
| provider | `provider.transaction` | osservativa |
| provider | `provider.cancellation_inflight` | osservativa |
| provider | `provider.session_quarantine` | osservativa |
| provider | `provider.session_reuse` | osservativa |
| provider | `provider.ambiguous_commit` | richiesta: accepted |
| raw | `raw.error_unknown_column` | osservativa |
| raw | `raw.error_unknown_table` | osservativa |
| raw | `raw.error_unknown_database` | osservativa |
| raw | `raw.error_duplicate_key` | osservativa |
| raw | `raw.error_not_null` | osservativa |
| raw | `raw.error_foreign_key` | osservativa |
| raw | `raw.error_check_violation` | osservativa |
| raw | `raw.error_privilege` | osservativa |
| raw | `raw.error_statement_timeout` | osservativa |
| raw | `raw.error_lock_wait` | osservativa |
| raw | `raw.error_deadlock` | osservativa |
| raw | `raw.error_access_denied` | osservativa |
| provider | `provider.profile_probe` | richiesta: accepted |
| provider | `provider.profile_describe_object` | richiesta: accepted |
| provider | `provider.profile_describe_geometry` | richiesta: rejected |
| raw | `raw.functional_index_ddl` | osservativa |
| provider | `provider.profile_functional_index` | richiesta: accepted |
| provider | `provider.profile_read_schema` | richiesta: accepted |
| provider | `provider.profile_read_values` | richiesta: accepted |
| provider | `provider.profile_read_namespace` | richiesta: accepted |
| provider | `provider.profile_read_projection` | richiesta: accepted |
| provider | `provider.profile_read_filter_forms` | richiesta: accepted |
| provider | `provider.profile_read_filter_closed_like` | richiesta: rejected |
| provider | `provider.profile_read_filter_closed_spatial` | richiesta: rejected |
| provider | `provider.profile_read_ordering_asc` | richiesta: accepted |
| provider | `provider.profile_read_ordering_desc` | richiesta: accepted |
| provider | `provider.profile_read_streaming` | richiesta: accepted |
| provider | `provider.transaction_row_stream` | richiesta: accepted |
| provider | `provider.transaction_row_stream_abandoned` | richiesta: accepted |
| raw | `raw.generated_column_catalog` | osservativa |
| provider | `provider.profile_generated_index` | richiesta: accepted |
| provider | `provider.profile_upsert_on_primary_key` | richiesta: rejected |
| provider | `provider.profile_upsert_on_generated_key` | richiesta: rejected |
| provider | `provider.profile_upsert_generated_anchor` | richiesta: rejected |
| provider | `provider.profile_write_append` | richiesta: accepted |
| provider | `provider.profile_write_append_rollback` | richiesta: rejected |
| provider | `provider.profile_write_append_cancellation` | richiesta: rejected |
| provider | `provider.profile_write_create` | richiesta: accepted |
| provider | `provider.profile_write_create_rollback` | richiesta: rejected |
| provider | `provider.profile_write_create_cancellation` | richiesta: rejected |
| provider | `provider.profile_write_update` | richiesta: accepted |
| provider | `provider.profile_write_update_rollback` | richiesta: rejected |
| provider | `provider.profile_write_update_cancellation` | richiesta: rejected |
| provider | `provider.profile_write_upsert` | richiesta: accepted |
| provider | `provider.profile_write_upsert_rollback` | richiesta: rejected |
| provider | `provider.profile_write_upsert_cancellation` | richiesta: rejected |
| provider | `provider.profile_write_replace` | richiesta: accepted |
| provider | `provider.profile_write_replace_rollback` | richiesta: rejected |
| provider | `provider.profile_write_replace_cancellation` | richiesta: rejected |
| provider | `provider.profile_write_delete_by_keys` | richiesta: accepted |
| provider | `provider.profile_write_delete_by_keys_rollback` | richiesta: rejected |
| provider | `provider.profile_write_delete_by_keys_cancellation` | richiesta: rejected |
| provider | `provider.profile_timeout` | richiesta: rejected |
| provider | `provider.profile_portable_returning` | osservativa |
| provider | `provider.profile_crs_undeclared` | osservativa |
| provider | `provider.profile_crs_declared` | osservativa |
| provider | `provider.profile_crs_mismatched` | osservativa |
| provider | `provider.profile_savepoint_partial_rollback` | richiesta: accepted |
| provider | `provider.profile_savepoint_unknown_name` | richiesta: rejected |
| provider | `provider.profile_write_spatial_create` | richiesta: accepted |
| provider | `provider.profile_write_spatial_append` | richiesta: accepted |
| provider | `provider.profile_write_spatial_mixed` | richiesta: accepted |
| provider | `provider.profile_write_spatial_index` | richiesta: accepted |
| provider | `provider.profile_spatial_functions` | osservativa |
| provider | `provider.profile_concurrent_readers` | osservativa |
| provider | `provider.profile_concurrent_writers` | osservativa |
| provider | `provider.profile_pool_endurance` | osservativa |
| provider | `provider.profile_mixed_load` | osservativa |

## Prova critica: commit ambiguo

`provider.ambiguous_commit` usa il seam `DelayedCommitResponse`: il
server applica il commit e la risposta viene trattenuta. Il provider deve
dichiarare `OutcomeUnknown` e la sonda rilegge `commit_contents` da una
seconda connessione. Entrambe le meta sono necessarie: senza rilettura,
l'esito ignoto non dimostrerebbe che il commit e realmente atterrato.

## Cosa resta aperto

Il documento non mantiene una roadmap parallela al codice. Le capability
correnti sono generate in `docs/STATO.md`; le forme spatial non pubblicate
restano chiuse nelle dichiarazioni di profilo e nei relativi inventari di
prova. Il perche storico delle campagne precedenti resta in Git.
