# CI locale: contratto comune della famiglia, schema 2

Questo documento definisce il contratto da condividere con IO. Le opzioni,
le dipendenze e i comandi effettivi si leggono da `scripts/ci_locale.py`,
`requirements-ci-locale.txt` e dai workflow; le prove di conformita sono in
`tests/test_ci_locale.py`. Le ricevute schema 1 non sono compatibili.

## Principi

Il piano conserva tutti i passi e i motivi di indisponibilita. Il profilo PR
e il sovrainsieme dei workflow pull_request: non deduce la branch protection.
Una precondizione assente significa NON ESEGUITO; un processo avviato che
fallisce significa FAIL. Nessun errore viene reinterpretato come successo.
Release e pubblicazioni restano vietate. I servizi live richiedono opt-in.

La rete e abilitata salvo richiesta offline esplicita. Offline non certifica
la completezza delle cache: download e risoluzione delle dipendenze senza
sorgenti locali dimostrate restano NON ESEGUITO. Non e un sandbox di rete.
La piattaforma e la shell dei workflow devono essere conservate.

## Checkout aggiuntivi

Un checkout con `repository`, `ref` e `path` deve usare una copia separata
nel worktree, al ref richiesto. La sorgente locale si configura attraverso
`CI_LOCALE_SORGENTE_<NOME>`: NOME e l'ultimo componente di `repository`,
maiuscolo, con i caratteri diversi da lettere e numeri sostituiti da `_`.
Per esempio `PlenoraETL/plenora-contracts` usa
`CI_LOCALE_SORGENTE_PLENORA_CONTRACTS`.

Se `repository` e omesso, la sorgente e il worktree corrente: il checkout
storico con `ref` e `path` resta separato. La copia mantiene tutta la storia,
anche quando `fetch-depth` richiede soltanto una storia parziale.

Senza sorgente locale, online usa HTTPS verso il repository dichiarato;
offline resta NON ESEGUITO con motivo. La sorgente non viene modificata.
La destinazione deve essere separata dalla radice e contenuta nel worktree,
anche dopo la risoluzione dei symlink. Una destinazione preesistente deve
essere pulita, della stessa sorgente e gia al commit richiesto: non viene
sovrascritta. Un ref non risolvibile o uno SHA differente produce FAIL.
Il log registra ref richiesto, SHA risolto e SHA effettivo.

Le espressioni `steps.<id>.outputs.<nome>` sono risolte solo dopo il PASS del
produttore. Il produttore puo leggere il pin dai file del repository e
scrivere `nome=valore` in `GITHUB_OUTPUT`. Un output non supportato fallisce;
un'espressione ancora ignota impedisce l'esecuzione. Non si inventano pin.

## Ricevute

Lo schema 2 conserva `steps/status/exit_code`, non
`results/outcome/returncode`. I campi comuni obbligatori sono:

| Livello | Campi |
| --- | --- |
| Root | schema, network_mode, services_opt_in, sha, dirty, started_at, host, python, selected_jobs, profile, sources, overrides, jobs, steps, counts |
| Root a fine esecuzione | finished_at, final_sha, final_dirty, exit_code |
| jobs[] | id, matrix, needs, reason, status |
| steps[] | job, source, raw, name, run, uses, with, shell, working_directory, env, if, continue_on_error, toolchain, status, reason, services, container, duration_seconds, exit_code, command, environment, log, condition_false |

Sono ammessi campi aggiuntivi. `sources` associa i sorgenti ai digest;
`overrides` registra le policy effettive. I campi `log` ed `exit_code` sono
null se il processo non parte. `counts` conta gli esiti presenti.
La ricevuta si aggiorna dopo ogni passo.

PASS richiede esecuzione riuscita. NON ESEGUITO non e PASS; condizioni false
sono distinte con `condition_false` e non bloccano i passi successivi.
`continue-on-error` non nasconde FAIL. SALTATO resta riservato.
L'uscita e 1 in presenza di FAIL, altrimenti 2 per copertura incompleta o piano
vuoto, 0 solo con tutti i job PASS. Il piano valido esce con 0.

L'aggiunta del mapping checkout e degli output non cambia i campi obbligatori
o il significato degli esiti dello schema 2. In IO questo contratto va inserito
nel documento ammesso dal suo docset; non autorizza un file fuori allowlist.
