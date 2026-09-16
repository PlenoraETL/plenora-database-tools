# Profili MySQL e MariaDB

## Confine di prodotto

`plenora-db-mysql` condivide trasporto, pool, transazioni e mapping Arrow fra
`MysqlProvider` e `MariadbProvider`. Il consumatore sceglie il prodotto prima
della connessione: `MysqlProvider` rifiuta MariaDB e `MariadbProvider` rifiuta
MySQL. Non c'e selezione automatica dal server raggiunto.

Il profilo di prodotto possiede riconoscimento, versioni ammesse, timeout,
query di catalogo, metadata nativi, regole spatial e classificazione degli
errori. Le differenze concrete sono implementate in
[`profile.rs`](../../crates/plenora-db-mysql/src/profile.rs); le guide non
mantengono una seconda lista di codici server o di capability.

## Prove

Versioni e digest delle fixture sono dichiarati in
[`references.json`](../../docker/mariadb/references.json). L'inventario delle
sonde e i comandi di riproduzione sono generati in [`EVIDENCE.md`](EVIDENCE.md).
Gli esiti live appartengono al verdetto della singola corsa.

Una capability resta chiusa senza una prova riproducibile. `not_measured`
non autorizza supporto e una versione non qualificata non viene considerata
compatibile per somiglianza con un altro riferimento.
