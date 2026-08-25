# AGENTS.md

Il documento che si legge per primo. Non descrive il repository — quello lo fa
il repository — ma dice **cosa non e negoziabile** e **dove sta il resto**.

Era assente: la prassi lo dava per esistente, le review lo cercavano e non lo
trovavano, e i vincoli qui sotto vivevano sparsi fra `README.md` e la storia dei
commit. Un vincolo che nessuno puo citare non e un vincolo.

## Le regole che non cambiano

Sono policy, non fatti del codice: non si generano da nessuna parte, e questo e
l'unico posto in cui sono scritte.

1. **Una capability resta `false` finche non esiste una prova riproducibile che
   la sostiene.** `not_measured` non e un `no`, ma non apre niente lo stesso.
   Vale per il documento capability dei provider e per ogni default: un campo
   assente e una capability non dichiarata, quindi negata.
2. **Una modifica incompatibile del contratto richiede una nuova major**, non
   una nota di rilascio. Se un allineamento si puo ottenere rendendo il codice
   piu tollerante invece del contratto piu severo, si sceglie la prima strada.
3. **I documenti non ripetono fatti che vivono nel codice**: o li generano da
   li, oppure non li dicono. `docs/STATO.md` e generato e non si modifica a
   mano.
4. **Un messaggio d'errore pubblico non trasporta payload**: niente valori di
   cella, frammenti di riga, DSN, token o SQL bindato. Contesto operativo si,
   dato no. Il divieto e dichiarato su
   `plenora_database_core::error::DatabaseError::message` e vale per ogni
   superficie che lo costruisce, CLI e binding Python compresi.
5. **Trovato un difetto, si cerca la stessa classe altrove.** Quasi nessuno dei
   difetti di questo repository e stato unico: il valore sta nel chiuderli
   tutti, non nel chiudere quello segnalato.
6. **Un gate che nessuno esegue non e un gate.** Una guardia nuova entra in un
   workflow che gira, oppure non serve a niente — e proprio cosi che una suite
   e rimasta rossa per mesi senza che nessuno lo vedesse.

## Dove sta il resto

| serve | sta in |
| --- | --- |
| cosa il codice dichiara oggi | [`docs/STATO.md`](docs/STATO.md) — generato |
| il contratto | [`contracts/v2/`](contracts/v2/README.md) |
| far girare i fixture | [`docs/operativo.md`](docs/operativo.md) |
| come si costruisce e cosa si esegue | [`README.md`](README.md) |
| la qualifica MariaDB in corso | [`docs/mariadb/`](docs/mariadb/EVIDENCE.md) |
| perche una decisione e stata presa | `git log` |

## Prima di dire "fatto"

I comandi stanno in [`README.md`](README.md), sezione «Cosa fa girare le
prove», e nei workflow sotto `.github/workflows/`: sono la fonte, e ricopiarli
qui li farebbe divergere al primo cambiamento.

Cio che questo documento aggiunge e l'ordine di lettura del risultato:

- i gate **offline** (contratti, self-test, `cargo test`, `cargo deny`) devono
  essere verdi sempre, e girano senza server;
- i gate **live** avviano i propri fixture Compose e valgono per il provider
  che nominano: un gate MySQL verde non dice niente su PostgreSQL;
- se un gate non e stato eseguito, si dice che non e stato eseguito. Un gate
  saltato non e un gate passato.
