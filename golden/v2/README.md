# Golden suite v2

`cases.json` è il catalogo macchina dei casi che Python e Rust eseguono sugli
stessi target isolati. È la suite **attiva**: `golden/v1/` resta come la sua
major di contratto, ritirata e non più aggiornata.

`offline_ready` significa che input e oracle sono già deterministici senza un
server. `database_required` significa che il caso è specificato ma dovrà
essere materializzato e verificato sul provider reale.

Rispetto alla v1 cadono i tre casi della categoria `arcgis` — pagination per
finestre di objectId, applyEdits parziale, idempotenza per GlobalID — e la
voce `arcgis` dagli elenchi di provider. Niente altro cambia: qui si toglie un
dominio, non si rimisura la semantica.

Le categorie che la suite deve coprire non sono scritte nel gate: sono quelle
che `contracts/v2/golden-manifest.schema.json` dichiara. Togliere una categoria
dal contratto la toglie da entrambi i lati.

Il catalogo non contiene credenziali, endpoint o output vendor variabili.
Versioni, capability e risultati finiscono negli artefatti della singola run.
