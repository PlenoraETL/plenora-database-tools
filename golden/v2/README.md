# Golden suite v2

`cases.json` è il catalogo macchina dei casi che Python e Rust eseguono sugli
stessi target isolati. È l'unica suite nel worktree: le precedenti stanno in
Git.

`offline_ready` significa che input e oracle sono già deterministici senza un
server. `database_required` significa che il caso è specificato ma dovrà
essere materializzato e verificato sul provider reale.

Le categorie che la suite deve coprire non sono scritte nel gate: sono quelle
che `contracts/v2/golden-manifest.schema.json` dichiara. Togliere una categoria
dal contratto la toglie da entrambi i lati.

Il catalogo non contiene credenziali, endpoint o output vendor variabili.
Versioni, capability e risultati finiscono negli artefatti della singola run.
