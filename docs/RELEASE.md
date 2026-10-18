# Avvisi Rust nella distribuzione

La licenza del prodotto resta il testo di famiglia in [`LICENSE`](../LICENSE).
Gli avvisi dei terzi sono separati, per crate e versione, con i copyright e
i NOTICE del pacchetto originale; un identificatore SPDX non li sostituisce.

Il precedente adottato e `plenora-IO-tools@9795e79`. Qui si versionano anche
i testi disponibili nella cache, per poter verificare i wheel e gli archivi
scaricati in job senza Cargo. La fonte generata e
[`avvisi-crate.json`](../assurance/registries/avvisi-crate.json): contiene i
profili, le dipendenze, la provenienza dei testi e le impronte. I conteggi si
leggono dal gate, non da una tabella mantenuta a mano.

## Raccolta e gate

```sh
python scripts/avvisi_crate.py --aggiorna
python scripts/avvisi_crate.py
python scripts/test_avvisi_crate.py
```

La raccolta usa esclusivamente `cargo metadata/tree --offline --locked`, la
cache Cargo e i fork del workspace. `cargo tree` seleziona il prodotto e il
target, con archi `normal,no-proc-macro`: esclude sviluppo, build e macro host.
Il grafo e quello delle feature di release definite nello script, non la
risoluzione unificata di tutto il workspace. E una copertura conservativa
dei crate runtime: non pretende di misurare quali funzioni sopravvivono a LTO.
I crate del prodotto sono riconosciuti tramite `workspace_members`; il fork
Oracle escluso dal workspace conserva quindi i propri avvisi upstream.

La raccolta include LICENSE/LICENCE, NOTICE, COPYING, COPYRIGHT, UNLICENSE
anche nelle sottodirectory e il `license-file`. Le impronte SHA-256
normalizzano CRLF, affinche il checkout Windows non alteri il risultato.
Un aggiornamento non elimina automaticamente testi obsoleti: il gate li
segnala e richiede la revisione del delta. I testi supplementari devono avere
provenienza e impronta; quelli recuperati dal precedente locale conservano
anche commit e blob registrati li.

`missing_reason` registra una lacuna della cache, **non un'esenzione**: il
gate e il confezionamento dei profili interessati falliscono. Non viene
inventato un copyright e non si conclude che upstream non abbia una licenza
solo perche il pacchetto locale non la contiene. Per risolvere una lacuna
occorre acquisire il testo della versione precisa, registrare fonte e SHA-256
e rigenerare. Nessun comando di questi script scarica testi dalla rete.

## Confezionamento

```sh
python scripts/licenze_artefatto.py --profile cli-linux --binary "target/release/plenora-database" dist/plenora-database-linux-x86_64.zip
python scripts/licenze_artefatto.py --profile wheel-linux dist/example.whl
python scripts/check-licenze-artefatto.py --profile wheel-linux dist/example.whl
```

I nomi dei profili effettivi sono nel registro. I CLI vengono distribuiti in
zip, insieme a `LICENSE` e `LICENSES/crate-rust/`; i wheel portano gli stessi
file sotto `plenora_database/` e un RECORD ricalcolato. L'archivio sorgente
prodotto da `git archive` riceve l'unione dei profili (`--profile source`).
Il confezionamento verifica il risultato, preserva il payload nativo e
rifiuta input Cargo diversi da quelli registrati.

Il controllo legge il contenuto finale e lo confronta con il registro esterno
al pacchetto, incluse le impronte di ogni testo e la licenza del prodotto.
Eliminare insieme un crate e la sua voce di CRATE.json non aggira il gate.
Le regressioni esercitano anche NOTICE mancanti, testi alterati, RECORD,
licenza del prodotto e archivio sorgente. Le build e i gate di rilascio sono
in [`python-wheel.yml`](../.github/workflows/python-wheel.yml); la CI ordinaria
esegue lo stesso gate dopo avere popolato la cache con `cargo fetch --locked`.

Una build `maturin` invocata direttamente produce un wheel intermedio: prima
di distribuirlo va eseguito il confezionamento qui sopra. I runner SDK, Oracle
e Db2 lo eseguono prima di installare o qualificare il wheel.
