# Golden suite v1 — ritirata

**Questa suite e ritirata**, come la major di contratto che la valida.
La suite attiva e `golden/v2/`. Qui non cambia piu niente.

`cases.json` è il catalogo macchina dei casi che Python e Rust dovranno
eseguire sugli stessi target isolati.

`offline_ready` significa che input e oracle sono già deterministici senza un
server. `database_required` significa che il caso è specificato ma dovrà
essere materializzato e verificato sul provider reale.

Il catalogo non contiene credenziali, endpoint o output vendor variabili.
Versioni, capability e risultati finiscono negli artefatti della singola run.
