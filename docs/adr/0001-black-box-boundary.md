# ADR 0001 — Confine della black box

Stato: **accettato**  
Data: 2026-07-26

## Contesto

`plenora-database-tools` deve riprodurre in Rust le funzioni database del
backend Plenora secondo il modello di `plenora-data-tools`, senza dipendere
dagli altri progetti.

Il backend include anche ArcGIS Feature Service, che non è un database SQL ma
ha le stesse esigenze di introspezione, read/write geospaziale e streaming.

## Decisione

Il workspace è autoconsistente e possiede:

- core e contratti;
- engine;
- SQL AST;
- driver SQL;
- provider ArcGIS;
- testkit;
- CLI.

Interoperabilità esterna:

- `RecordBatch`;
- GeoArrow-WKB;
- piano JSON versionato;
- output/outcome versionato.

Non dipende da `plenora-data-tools`, `plenora-IO-tools` o dal backend Python.

L'astrazione radice è un provider dati capability-driven. I database SQL
implementano dialect/transazioni; ArcGIS implementa REST/service-layer/edit.
Non si forza ArcGIS dentro un dialect SQL.

## Conseguenze

- la libreria può essere distribuita come black box;
- nessuna deriva di core esterni la blocca;
- Arrow resta un contratto pubblico da versionare;
- alcune strutture possono essere concettualmente simili agli altri progetti
  ma hanno ownership/versionamento propri;
- provider diversi condividono garanzie, non fingono identica semantica.

## Alternative scartate

- core condiviso obbligatorio tra tutti i repository;
- chiamare il backend Python da Rust;
- modellare ArcGIS come database/dialect SQL;
- separare ArcGIS subito in un prodotto senza condividere engine/contratti.

