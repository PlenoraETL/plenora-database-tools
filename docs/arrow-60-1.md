# Qualifica offline della patch Arrow

Revisione preparata su `7e74e8f6`, dopo il tag `py-v9.0.0`, per
`py-v9.0.1`. Nessun tag o artefatto pubblicato. Riferimenti confrontati:
data PR #152 (checkout locale `134af653`) e IO PR #89 (`2c67e72`).
Il precedente di preparazione `bd9f3cf0` (8.0.2) aggiungeva la sezione
«non pubblicata» insieme ai bump di workspace, binding, wheel e lock;
si segue lo stesso schema. `tests/test_changelog.py` verifica la coerenza
della prima sezione con i manifest; il prefisso di distribuzione resta `py-v`.

L'advisory [RUSTSEC-2026-0442](https://rustsec.org/advisories/RUSTSEC-2026-0442.html)
riguarda la copertura logica dei run, inclusi lunghezza e offset. Il
[changelog upstream](https://github.com/apache/arrow-rs/blob/60.1.0/CHANGELOG.md)
descrive anche le correzioni IPC. I pin e le dipendenze effettive si leggono
nei manifest e nei due lock, non da un inventario mantenuto qui a mano.

## Raggiungibilita e prove

I nomi delle prove sotto sono funzioni eseguite dalla suite workspace o
dalla suite SDK offline gia collegate a `rust-ci`. Le prove di lettura
database sono analisi dei costruttori e test offline dei codec: non sono
misure dei server. Nessuna nuova capability viene aperta.

| Superficie | Esito e riferimento alla prova |
| --- | --- |
| Lettura PostgreSQL verso Rust, IPC CLI, SDK | Nessun buffer REE esterno importato: `ColumnBuffer::finish` in `crates/plenora-db-postgres/src/arrow.rs` costruisce gli array dai valori del driver. I tipi nativi e i contenitori qualificati sono compilati dall'adapter. I golden live restano da rieseguire. |
| Lettura MySQL e MariaDB | Stessa conversione da valori del driver, nessun import REE: `crates/plenora-db-mysql/src/arrow.rs`, `finish`; prove offline in `arrow_tests.rs`. I profili dei due server restano distinti. |
| Lettura SQL Server | `crates/plenora-db-sqlserver/src/arrow.rs`, `finish`, e `arrow_tests.rs`: builder da valori TDS, nessun layout REE importato. |
| Lettura Oracle | `crates/plenora-db-oracle/src/read.rs::rows_to_batch`: colonne costruite da righe del driver, nessun import REE. |
| Lettura Db2 | `crates/plenora-db-db2/src/read.rs::decode_batch`: conversione dai buffer ODBC; nessun array C Arrow o IPC esterno. |
| Scrittura Rust PostgreSQL | `write_tests.rs::run_end_schema_is_rejected_before_buffers` esercita `WriteColumnPlan::compile`: rifiuto dello schema prima dei buffer. |
| Scrittura Rust MySQL/MariaDB | Stessa prova in `crates/plenora-db-mysql/src/write_tests.rs`, con entrambi i `ProductProfile`: rifiuto nel compilatore del piano. |
| Scrittura Rust SQL Server | `write/plan_tests.rs::run_end_schema_is_rejected_before_buffers`: rifiuto in `WritePlan::compile_create`. |
| Scrittura Rust Db2 | `write_tests.rs::run_end_schema_is_rejected_before_buffers`: rifiuto in `Db2WritePlan::compile`. |
| Scrittura Rust Oracle | `write_tests.rs::run_end_schema_is_rejected_before_buffers`: rifiuto nel compilatore `native_type`, chiamato da `compile_column` prima dell'I/O. |
| IPC CLI, file e stream | **Raggiunge Arrow**: il precontrollo verifica framing, budget e coerenza, non la copertura dei run. `ipc_input_tests.rs::run_end_coverage_is_checked_by_arrow_before_delivery` dimostra che il primo batch valido passa e il secondo malformato viene rifiutato dal decoder prima della consegna. `arrow_60_1.rs::public_inspect_rejects_run_ends_and_short_bitmaps_without_payload` verifica anche il processo CLI e il suo envelope pubblico. Vale anche per l'ingresso di `write`; il piano del provider puo rifiutare prima uno schema non supportato. |
| Binding Rust Python, bytes IPC | **Raggiunge Arrow**: `write_ipc_tests.rs::run_end_coverage_failure_is_redacted_before_provider_write` verifica framing valido e errore sul secondo batch. `decode_ipc_stream` non restituisce il buffer di batch al provider quando uno fallisce; il messaggio pubblico non contiene il payload. Sync e async, compresi gli adapter di famiglia, usano questo decoder. |
| SDK Python, Table/RecordBatch/iterabile/PyCapsule | Un oggetto PyArrow puo esistere senza validazione completa: il vecchio adattatore lo serializzava prima del controllo Rust. `_narrowed_batch` ora esegue `validate(full=True)` prima di cast e writer. `test_arrow_60_1.py::test_invalid_run_ends_rejected_before_serialization` copre i quattro ingressi, figli annidati e batch successivi. Il test fallisce togliendo la nuova guardia: nessuna eccezione nel vecchio percorso. Non si attribuisce alla patch Rust la sicurezza di un produttore C che viola il protocollo prima di consegnare il batch a PyArrow. |
| Costruttori nell'API Rust | **Raggiunge Arrow**: `plenora-database-core/tests/run_end_validation.rs::run_ends_cover_length_and_offset` prova i tre indici, offset e lunghezze oltre l'ultimo run, e un caso valido. Le API unsafe di un consumer restano responsabilita del consumer; il workspace vieta unsafe nel proprio codice. |

Le prove dei writer includono REE Int16/32/64, direttamente e dentro list e
struct. Non costruiscono array invalidi con unsafe e non leggono celle fuori
limite. Le fixture IPC del secondo batch con run insufficienti sono prodotte
da `tests/fixtures/arrow/generate.py` e consumate da CLI e binding; il framing
e il primo batch restano validi. Un rifiuto di tipo del provider non sarebbe
una prova sufficiente per dichiarare protetto il decoder IPC: le due prove
restano separate.

## Comportamenti osservabili

| Caso | Prova e significato per database |
| --- | --- |
| Map `keys_sorted` | `test_arrow_60_1.py::test_map_keys_sorted_survives_python_ipc` e `arrow_60_1.rs::map_keys_sorted_survives_rust_ipc`, false e true su due batch. I passaggi Python e Rust conservano il flag; database non usa `arrow-pyarrow`. Non significa che un writer SQL accetti Map. |
| Colonne senza null | `plenora-database-cli/tests/arrow_60_1.rs::ipc_senza_null_scrive_bitmap_vuota_e_rilegge_gli_stessi_valori`: descriptor IPC della bitmap con lunghezza zero, schema e valori identici. Byte, lunghezze e fingerprint di IPC prodotti possono cambiare. |
| Bitmap troppo corta | `ipc_input_tests.rs::struct_short_validity_bitmap_fails_during_batch_decode` e `write_ipc_tests.rs::struct_short_validity_bitmap_returns_invalid_plan_at_validate`: nove righe Struct, un solo byte di validita del padre, figlio valido. Framing accettato, rifiuto durante la decodifica. La CLI pubblica verifica `invalid_plan` / `validate`, messaggio redatto e assenza di panico; il binding verifica gli stessi assi sullo stream. `test_arrow_60_1.py::test_struct_short_bitmap_rejected_by_sdk_at_prepare` verifica file e stream attraverso un iterabile lazy: errore SDK `data_mapping` / `prepare`. |
| Liste affettate con union | `plenora-database-cli/tests/arrow_60_1.rs::liste_affettate_di_union_conservano_i_valori_attraverso_io`: dense e sparse, offset non zero e null nei figli. L'oracolo confronta valori espliciti, non solo uguaglianza ArrayData. Correzione della serializzazione IPC upstream; non nuova capability di lettura o scrittura SQL. |

Il generatore delle fixture PyArrow e stato rieseguito: i quattro binari
dictionary preesistenti sono byte-identici. Le nuove fixture sono
malformazioni intenzionali riproducibili. I test CLI dei budget e quelli
runtime che contano i byte restano vincolanti; le impronte della prima
passata IPC sono calcolate sui byte effettivi. Le baseline live dei benchmark
non sono state riscritte con numeri inventati: vanno rimisurate sulla VM.
Il cambiamento calamine osservato in IO non riguarda questo grafo.

## Supply chain e artefatti generati

`cargo-deny 0.20.2` controlla entrambi i grafi con tutte le categorie della
policy CI. La prima copia cache era vecchia e non costituisce evidenza finale.
Il controllo finale usa RustSec
`309392b18f6b17022f82c701f300433126901068` (6 novembre 2026), acquisito in
`target/arrow-60-1/advisory-db`. Il fetch Git ha richiesto il backend TLS
OpenSSL, impostato soltanto sul comando; nessuna configurazione globale.

Nessun altro advisory aperto nei due grafi. Rimossa l'eccezione a
RUSTSEC-2026-0381: il database aggiornato dichiara esplicitamente
`webpki-roots 0.26.11` non interessato e la 1.x risolta e corretta.
Restano i warning informativi sui duplicati ammessi dalla policy e
sull'eccezione di licenza libfuzzer non incontrata nel grafo workspace.
Il contenitore CI non e stato eseguito: Docker non e installato qui.

I manifest e i lock di `vendor/odbc-api` e `vendor/oracle-rs` non dipendono
da Arrow e restano invariati. Rigenerati `avvisi-crate.json`, relativi testi
di licenza e `docs/STATO.md` con gli script del progetto. Il contratto dei
conteggi SDK include le nuove regressioni. Il budget di codice cresce della
sola validazione Python e del suo commento, senza margine aggiunto.

## Ripetere la qualifica

Usare `CARGO_BUILD_JOBS=3` e `RUST_TEST_THREADS=4`. I comandi canonici sono
nel README e nei workflow; i log di questa corsa, inclusi tentativi falliti
e loro correzioni, sono sotto `target/arrow-60-1/`. La formattazione locale
seleziona esplicitamente i pacchetti workspace, senza attraversare vendor.

Le prove live non sono state eseguite. Eseguire sulla VM i workflow di
assurance PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, Db2 e AGE, le
matrici dei provider/sessioni, la campagna SDK e le sonde SRID, operandi e
scritture. I programmi `run`, gli ambienti e le matrici esatti sono estratti
dai workflow nel referto locale `target/arrow-60-1/live-workflows.md`.
Quel file e un'istantanea dei comandi da eseguire, non un verdetto live.

## Esito della corsa locale

Windows, Rust 1.98.1, Python 3.11.15, PyArrow 25.0.1, maturin 1.15.0.
Suite workspace: 1798 passati, 98 ignorati, 562 filtrati. Passano anche
fmt selettivo, Clippy `-D warnings` e anti-panic, le sei configurazioni CLI
del workflow, check fuzz, i 372 test del fork Oracle su copia temporanea,
il consumer esterno, contratti pubblici/runtime, vettori Arrow e typing SDK.
Self-test Python: 171 test in `tests/`, 474 in `scripts/` (uno saltato per
evidenza sessioni live assente). Gli inventari MySQL e i conteggi SDK sono
riallineati; i gate statici e degli avvisi passano.

SDK sulla wheel di debug appena costruita: 361 passati, 320 saltati secondo
gli scope senza server/benchmark. **Non e una riproduzione esatta dell'ambiente
CI**: i pin richiesti di numpy, packaging, pandas e Pygments non coincidono
con l'ambiente locale. Il tentativo di installare tutti i requisiti in una
directory isolata fallisce per `numpy==2.5.3`, non disponibile nell'indice
raggiungibile. I pin del progetto non sono stati abbassati. La sonda con la
sola nuova guardia rimossa fallisce come atteso: il vecchio codice non rifiuta
il REE prima della serializzazione. La validazione completa aggiunge una
scansione per batch; nessuna nuova misura di throughput viene dichiarata.

Non eseguiti: contenitore cargo-deny, celle Linux/altri Python, coverage
strumentata, campagne libFuzzer e benchmark prestazionali, packaging di
release e tutte le prove live. I gate sono stati invocati singolarmente:
non si dichiara una sweep integrale verde. Il referto locale
`target/arrow-60-1/REPORT.md` conserva comandi, log e limiti della corsa.

## Correzione delle prove dopo PR #148

Le fixture `struct-short-bitmap.file` e `.stream` sono prodotte da
`tests/fixtures/arrow/generate.py`: Struct di nove righe, un null nel padre,
figlio stringa valido con marker di redazione. Viene ridotta da due byte a
uno solo la lunghezza del primo buffer nel descriptor IPC, senza alterare
framing, nodi, figlio o corpo. Lo stesso generatore deposita le copie nei
fixture SDK, perche la CI esegue `python/tests` fuori dal source tree.
Due rigenerazioni sono byte-identiche, incluse le fixture preesistenti.

La fixture Int32 resta: esercita `ArrayDataBuilder::build`, distinto da
`create_struct_array`. Non basta a qualificare la correzione specifica
[upstream #11658](https://github.com/apache/arrow-rs/pull/11658).
La prova SDK usa il decoder PyArrow e la traduzione degli errori dello SDK;
non dimostra una correzione del decoder Rust. Il binding accetta soltanto
IPC stream: la prova della decodifica file appartiene alla CLI.

La controprova e stata eseguita con un consumer temporaneo in
`target/arrow-struct-fix/`, usando gli stessi byte e `catch_unwind` solo
nell'esperimento. Manifest, lock, sorgente e log sono conservati li:

| Grafo effettivo | Int32 file/stream | Struct file/stream |
| --- | --- | --- |
| Tutti i crate Arrow a 60.0.0 | panico | panico |
| Solo arrow-ipc a 60.0.0, resto a 60.1.0 | errore restituito | panico |
| Tutti i crate Arrow a 60.1.0 | errore restituito | errore restituito |

Quindi la premessa che Int32 fosse gia fallibile con l'intera famiglia
60.0.0 non e confermata. In `arrow-data-60.0.0/src/data.rs:2337`, `build`
chiama `BooleanBuffer::new` prima della validazione; per Struct la chiamata
separata e in `arrow-ipc-60.0.0/src/reader.rs:372`. Entrambe raggiungono
l'assert in `arrow-buffer-60.0.0/src/buffer/boolean.rs:128`. Abbassare soltanto
il pin IPC puo lasciare il resto alla patch e nascondere il primo percorso.
La nuova prova fallirebbe sulla 60.0.0 per panico durante `next()`, mentre
sulla 60.1.0 richiede un errore restituito e gli assi pubblici previsti.

### Ricerca della stessa classe in data e IO

Nessuna modifica ai due repository. I `main` locali sono arretrati (data
`190c4932`, IO `2c1561b`; il checkout IO e su un altro branch). Sono stati
letti con `git show` anche gli `origin/main` gia presenti: data
`6153454fdaf91d879a72a5aaaa6a93ec2f621247`, IO
`8026148cec904557ce8ea41b8fa95173eef30102`, contenenti le PR interessate.
Le righe seguenti si riferiscono a questi ref, non ai file del checkout.

| Repository, file e riga | Risultato della revisione |
| --- | --- |
| IO `crates/driver-ipc/src/tests/layout_ipc.rs:199` | `arrow_60_1_bitmap_corta_restituisce_errore_senza_panico` usa Int32, non Struct. Non copre #11658; passerebbe con IPC 60.0.0 e arrow-data 60.1.0. Con tutta la famiglia 60.0.0 il percorso del builder panicava: non va descritto come verde su quel grafo. |
| IO `crates/driver-ipc/src/tests/layout_ipc.rs:289` | `run_ends_devono_coprire_la_lunghezza_logica` si ferma alla prevalidazione propria (`verifica`, righe 156-190), prima di Arrow: passa anche con 60.0.0. E una prova valida della barriera IO, non della patch Arrow; `docs/RELEASE.md:1355` lo dichiara correttamente. |
| IO `crates/driver-ipc/src/tests.rs:316` | Dizionario con bitmap corta rifiutato prima di Arrow: prova della prevalidazione, indipendente dalla patch. |
| Data `crates/plenora-io/tests/round_trip.rs:696` | `ipc_run_end_malformato_rifiutato_prima_dei_blocchi` richiede `Unsupported` sullo schema, prima del decoder: passa anche con 60.0.0. Non misura la nuova validazione dei buffer. |
| Data `crates/plenora-data-py/python/tests/test_operazioni.py:258` | `test_sdk_rifiuta_run_end_malformato_letto_da_ipc` usa PyArrow per leggere/validare e chiede `PlenoraUnsupportedError` / `read` al confine SDK. Il decoder Rust non legge quei byte: passa anche con Arrow Rust 60.0.0. |
| Data `crates/plenora-data-py/python/tests/test_operazioni.py:231` | Rifiuto REE dal solo schema, senza richiedere il primo blocco: prova intenzionale del confine, indipendente dalla patch. |

Non trovata una prova bitmap corta analoga in data. Le prove dirette
`data:crates/plenora-core/tests/run_end_validation.rs:9` e
`IO:crates/driver-ipc/src/tests/arrow_60_1.rs:124` modificano lunghezza/offset
del padre e chiamano il builder Arrow: non hanno il bypass di schema delle
prove REE elencate sopra. Anche la prova GeoParquet IO
`crates/driver-geoparquet/tests/run_end_encoded.rs:12` verifica deliberatamente
che l'hint REE non costruisca REE, non una regressione del suo decoder.

### Esito della correzione

Log e comandi completi: `target/arrow-struct-fix/`. Test CLI e binding:
161 passati, 27 ignorati, 24 filtrati (`--skip live_`). Passati fmt con soli
pacchetti workspace, Clippy workspace/all-targets/all-features `-D warnings`,
`check_test_layout.py`, self-test dei contatori SDK/MySQL, suite `tests/`,
controlli documenti, mojibake, budget e cargo deny offline.
Anche `cargo test --workspace -- --skip live_` e verde: 1800 passati,
98 ignorati, 562 filtrati. Passano i self-test `scripts/`, lo stato
generato e i controlli dei contratti. Il gate pubblico e stato rieseguito
con successo su una copia temporanea del repository contratti al pin esatto;
la vecchia copia senza identita Git era stata correttamente rifiutata.

Wheel ricostruita e installata in un venv Python 3.13, con i pin esatti di
`requirements-sdk-tests.txt`: suite SDK copiata fuori dal package sorgente,
origine in site-packages verificata, 363 passati e 320 skip attesi; contratto
dello scope offline verificato. E il procedimento di `offline_suite.sh`,
eseguito con comandi Python equivalenti su Windows. Il precedente tentativo
su Python 3.11 non installava numpy 2.5.3 (richiede Python >=3.12); il log
resta conservato e non viene contato come qualifica dei pin.
Le prove live e la matrice CI su altri sistemi non sono state eseguite.
