# ADR 0011 — Giudice canonico Arrow nel core

Stato: accettato e verificato internamente.

## Contesto

Il primo `FieldContract` viveva come modulo privato nel provider PostgreSQL e
mescolava due responsabilità:

- regole trasversali su versione, geometria, CRS e GeoArrow;
- metadati nativi PostgreSQL, inclusi range e composite.

SQL Server aveva quindi una seconda validazione manuale e parziale. Questa
asimmetria permetteva ai provider di accettare contratti diversi.

## Decisione

`plenora-database-core::field_contract::FieldContract` è l'unico giudice delle
chiavi canoniche e delle rappresentazioni GeoArrow/legacy. Verifica:

- `plenora.contract.version` a livello schema e rifiuto delle versioni future;
- enumerazioni chiuse, lista dei tipi unica e in ordine canonico;
- obbligatorietà dei metadati del protocollo corrente;
- coerenza fra rappresentazioni canoniche, legacy e GeoArrow;
- relazioni fra stato CRS, identificatore, SRID, definizione e ordine assi;
- conflitto riconoscibile `EPSG:<n>`/SRID con categoria `Crs`.

Il provider PostgreSQL conserva un adattatore privato per il solo
sotto-namespace `plenora.postgres.*`. SQL Server usa direttamente il giudice
del core e aggiunge soltanto i vincoli della propria operazione, per esempio il
profilo spatial XY verificato.

La CLI espone `inspect-dataset <file.arrow>`: legge Arrow IPC offline, applica
limiti hard e riporta il contratto osservato e l'esito del parser WKB/EWKB per
ogni cella geometrica.

## Conseguenze

- un nuovo provider deve riusare il giudice comune;
- nessun provider può reinterpretare autonomamente le chiavi canoniche;
- i metadati nativi restano separati e non vengono promossi a regole comuni;
- il comando IPC rende verificabile il terzo anello della catena senza un
  database.

La campagna sul corpus trasversale ha anche rilevato un disallineamento esterno:
la fixture positiva `crs_unresolved` omette `axis_order` pur portando `crs_id`,
mentre R4.3.3 corrente lo dichiara obbligatorio. Il componente segue il testo e
fallisce chiuso; la fixture non viene corretta in questo repository.

Questa disciplina è safety-critical, ma non costituisce certificazione
avionica o conformità indipendente.
