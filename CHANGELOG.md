# Changelog

## 12.0.0 — 2026-11-15

- Rimozioni incompatibili: eliminati `PostgresProvider::default_secure` e la
  facciata Rust `plenora_database_core::query`; usare `Default` e `relational`.
- Eliminate le note delle release anteriori alla 11.0.0. Le prove correnti
  dei provider e gli avvisi delle dipendenze restano parte della qualifica.
- Contratti adottati allineati al pin di famiglia `38d68b402389855edbcecb7ce57c31e91282cb4e`;
  copie, digest e vettori aggiornati insieme ai checkout CI.
- Decisione del titolare del 15/11/2026: nessuna compatibilita con release
  Plenora precedenti; i protocolli condivisi ancora correnti richiedono
  coordinamento con i consumatori prima del ritiro.
- Guardie dell'IR relazionale contro definizioni parallele; sonde PostgreSQL
  e inventari MariaDB allineati alle superfici correnti dopo le rimozioni (#158).
- Sicurezza temporale B1–B3 (#159): timeout, keepalive e retry condividono
  un dominio positivo limitato a un giorno e deadline con somma controllata;
  Db2 converte il timeout senza fallback.
- Decoder temporali: PostgreSQL rifiuta TIME 24:00 senza trasformarlo in
  mezzanotte; SQL Server valida i campi wire prima del decoder, evitando
  panic su smalldatetime, avvolgimenti degli orari e sostituzioni degli offset.
  Oracle rifiuta il byte di lunghezza assente invece di restituire NULL.
- MySQL: fork vendorizzati di mysql_common e mysql_async, con licenze
  upstream conservate; rifiuto InvalidData dei formati temporali non
  supportati e delle lunghezze non canoniche, senza panic o perdita di byte,
  e aritmetica controllata nella conversione delle durate. L'anno 0 resta
  valido nel dominio 0..9999; date zero e giorni inesistenti restano rifiutati.
- Guardia temporale in CI contro conversioni infallibili e bypass dei decoder;
  guardie dei moduli, registro Arrow e budget del codice riallineati al delta.

## 11.0.0 — 2026-11-14

- Qualifica giro 3: proxy MySQL raggiunto con host e porta separati sulle
  superfici SDK/CLI, provati offline col driver reale e TLS verificato;
  consenso `write` trasportato nel JSON e rapporto letto dall'envelope.
  Fixture dei target PostgreSQL esistenti validate offline prima delle
  campagne. Nessun cambio di firma SDK: `mapping_policy` resta obbligatoria.

- Giro 2 della corsa finale: MySQL `double(255,30)` rifiuta anche i valori
  non identici dopo la ricomposizione binaria del server; nessun consenso
  abilita questa perdita non qualificata. Strict anticipa anche il rifiuto
  dei non finiti. Corretta la normalizzazione della stringa `NULL` in Extra
  delle tabelle temporanee. ORM legge le annotazioni differite di Python 3.14
  e conserva i tipi dei bind. Le campagne verificano i contratti registrati
  sui target esistenti senza sostituirli implicitamente dopo TRUNCATE.
- Correzioni della corsa finale: gli input PostgreSQL scalari legacy senza
  metadati non richiedono ownership per COMMENT; staging MySQL creato prima
  della transazione e ricontrollato sotto lock; confronto Create normalizzato
  per l'assenza di Extra nelle tabelle temporanee. Il ledger usa il proprio
  indice per confermare le celle senza riscansionare l'intero batch.
- Campagne e wheel offline ricevono fixture, DSN e binari espliciti; builder
  Db2 coerente con curl-minimal di UBI e registro avvisi rigenerato.

- **Python minimo 3.14**: SDK, ABI stabile dei wheel, CI, rilascio e campagne
  usano il minimo dichiarato in `.python-version`. Rimossi i fallback precedenti.
- PostgreSQL: protocollo versionato nel commento di colonna per conservare le
  mappe Arrow di schema e campo, con digest e verifica del descrittore fisico.
  Il commento umano resta integro. Il digest rileva corruzione accidentale,
  non autentica un proprietario SQL; cancellare tutti i prefissi rende la
  colonna indistinguibile da una sorgente legacy.
- **Cambio incompatibile imposto dalla qualifica geometrica:** dimensions
  unknown con SRID rifiuta prima degli effetti; un SRID presente soltanto nei
  valori richiede prevalidazione dell'intero input entro il budget. La modalita
  che richiederebbe staging prima di quella verifica rifiuta preventivamente.
  Nessuna conversione implicita in XY. Strict rifiuta le rappresentazioni
  geometriche che non si possono attestare identiche; le permissive richiedono
  consenso alla regola `pg.geometry.representation`. La regola A resta attiva.
- `read_loss` v1 distingue lettura aperta, completata, fallita e interrotta;
  viene conservato anche negli errori. Gli stream Python espongono
  `stream_state` e gli adattatori `ArrowStreamReader`/`AsyncArrowStreamReader`
  mantengono gli errori strutturati senza interpretare il testo di ArrowInvalid.
- DB-c / B-13: reader sync e async condividono stato e rilascio; EOF, errore
  e close rilasciano lo stream conservando il primo errore e il read_loss.
  Gli adapter C registrano anche gli errori locali di decoding. `aclose()`
  attende il rilascio async. Nella major 11 la cancellazione dell'adapter
  async espone PlenoraCancelledError con assi, invece di perdere lo stato
  in asyncio.CancelledError; il reader nativo conserva lo stato anche quando
  viene annullato direttamente il suo awaitable. I consumer C esterni possono
  ancora ricevere ArrowInvalid: lo stato della sorgente resta interrogabile.
- DB-a / B-07: il piano delle 15 proprieta governa conservazione, verifiche,
  proiezioni e regole. I metadati opzionali oltre 1 MiB per colonna o 16 MiB
  per operazione vengono omessi come chiavi intere, con localizzazione redatta
  (scope, campo, proprieta, digest della chiave, motivo e hop). Strict rifiuta
  prima degli effetti; il consenso 10.0.0 viene verificato sul valore realmente
  perso, senza autorizzare sulla sola previsione. ID e vincoli GEO-009 restano
  non sacrificabili; NUL, contratto incoerente e impossibilita di rappresentare
  anche il rapporto rifiutano. Profondita massima JSON del protocollo: 16.
- Il rapporto esecutivo con soggetti metadata usa la versione 4; i rapporti
  dei soli valori mantengono la versione 3. Le omissioni persistite usano
  `read_loss` v2, anche su tabelle vuote; le letture senza omissioni puntuali
  mantengono v1. Il dry run PostgreSQL espone `execution_loss` con valutazione
  e senza perdite effettive. Queste aggiunte e i nuovi campi/varianti Rust
  appartengono alla major 11.0.0. Regola A e comportamento strict dei valori
  restano invariati salvo i nuovi rifiuti geometrici prima silenziosi descritti
  sopra. La matrice Rust/CLI/SDK sync+async/runtime e le campagne B-13 sono
  predisposte; le prove offline non attestano la qualifica live.

- Apertura del percorso MySQL non-strict con consenso per valore comune a
  Rust (`LossConsent`), CLI (`--accept-loss`), Python sync/async
  (`accept_loss=True`) e `database.write@2` (`accept_loss`). MariaDB resta
  rifiutato. Strict, regola A e capability non cambiano.
- Preparazione MySQL con rischio v3 senza consenso implicito; gli errori
  conservano evaluation/effective. Warning Python soltanto per perdite
  committed; warning-as-error conserva l'outcome e vieta retry automatici.
- CLI `write`/`database-write-ipc`: dry-run del solo piano con rapporto,
  senza effetti. **Cambio incompatibile:** il risultato puo essere anche
  `dry_run`; i reader precedenti limitati a WriteOutcome devono aggiornarsi
  alla major 11 prima di usare questa opzione.
- MySQL permissive: trasformazioni temporali e zero negativo nei percorsi
  ammessi; float/double a precisione fissa e underflow conservativo restano
  chiusi anche con consenso. Create prevalida prima del DDL; i rapporti
  distinguono rollback DML, residuo DDL ed esito incerto.
- Campagne: inventari derivati dai sorgenti e confronto con i casi eseguiti;
  la matrice PostgreSQL include anche i test ignorati di conservazione e
  perdita metadata. Raccolta completa dopo i fallimenti, senza promuovere
  skip o fixture mancanti a qualifica. Nessuna capability promossa.
- Vettori comuni offline, test live delle superfici collegati alle tre major
  MySQL. Qualifica live e rilascio restano subordinati alle campagne finali.


- **Cambio incompatibile:** estensione del vocabolario chiuso del rapporto
  esecutivo v3; i reader 10.0.0 non accettano le nuove regole. Producer e reader
  devono essere aggiornati insieme. Il rapporto legacy v2 resta invariato.
- Fondazione comune per valutazione, attribuzione e consenso; i percorsi
  esistenti PostgreSQL e SQL Server conservano la contabilita dei budget.
  L'estensione del registro non costituisce qualifica o apertura di un provider.
