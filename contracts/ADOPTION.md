# Adozione e delta contrattuale

La selezione e le deviazioni sono in [adoption-source.json](adoption-source.json).
Il pin unico governa anche [le copie upstream](upstream/source.json) e i checkout
CI; `phase0_validate` e il generatore del manifest rifiutano pin divergenti.
Il manifest di distribuzione si genera soltanto dagli artefatti effettivi,
con `scripts/render_adoption_manifest.py`, schema e controlli semantici del pin.

## Riesame del 15 novembre 2026

Il confronto Git fra il pin precedente e quello adottato conserva il catalogo
`database-tools-v2` e il profilo database. La migrazione locale `database.write@2`
resta una deviazione SURF-007: cambiare pin non certifica il consumer runtime.

La decisione 0022 chiarisce identita dei campi fra operazioni, parita dei
metadati fra superfici e forma `mixed` senza lista enumerabile. I nuovi vettori
Arrow sono inclusi nelle copie con digest e nel test Rust che le esegue tutte.
Il runner `vettori_arrow_data.py` legge direttamente l'intero insieme del pin,
compresi gli ingressi `field-id-round-trip` e `store-field-without-id`.
Il solo controllo offline di questi ingressi non prova un round trip server.
La differenza dei provider sull'identita persistita e registrata come
ARROW-003, senza promuovere una prova statica a qualifica live.

Arrow Primary Geometry e File Interchange sono contratti opt-in aggiunti dal
pin, non requisiti del profilo database adottato. Non vengono reclamati:
la geometria primaria rimane metadato sconosciuto secondo ARROW-010; nessuna
normalizzazione o scelta della geometria viene introdotta in questo cambio.
L'eventuale adozione richiede le prove di composizione con IO/data/interop.
La mappa SDK aggiunge IO senza cambiare la voce database. L'oracolo CRS usa
case folding ASCII: il core usa gia confronti ASCII, verificati dai suoi test.

Le deviazioni gia registrate restano esplicite. L'allineamento non apre
capability, non elimina i protocolli ancora prodotti dai writer correnti e
non attesta nuove qualifiche provider. I risultati locali di questa modifica
sono nel referto di lavoro `target/COMPAT-FIX.md`.
