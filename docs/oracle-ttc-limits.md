# Limite residuo del framing TTC Oracle

Il framing incrementale a livello di messaggio non costituisce una garanzia
lineare di CPU per una singola riga molto grande e frammentata. Il rischio
accettato in questo intervento e la ripetizione della decodifica del messaggio
incompleto: i valori intermedi vengono scartati, poi decodificati nuovamente
quando arrivano altri byte. Un peer che frammenta una riga in molti frame puo
quindi amplificare il lavoro, anche quadraticamente per quel messaggio.

Il contenimento deve restare fail-closed: timeout dell'operazione, limite
cumulativo dei byte sul canale e contabilizzazione dello spazio di lavoro del
framing non devono essere rimossi per aggirare un errore su righe grandi.
La riserva per i decoder e prudenziale, non una misura dell'RSS o dell'overhead
dell'allocatore. Non costituisce una qualifica di resistenza a ogni possibile
amplificazione dei decoder di tipi complessi. Gli errori pubblici devono
continuare a riportare solo contesto operativo, senza frammenti TTC o valori.

Il rientro richiede un lettore incrementale che salti i valori senza
materializzarli, mantenga lo stato anche dentro un singolo valore e contabilizzi
le allocazioni effettive dei metadati. Prima di rimuovere questa limitazione,
servono prove offline con righe larghe, valori complessi e frammentazione a
singolo byte, con misure riproducibili di lavoro e picco di memoria; vanno
eseguite nel job del fork gia presente in `rust-ci.yml`.

L'implementazione e la fonte dei dettagli, senza duplicarne soglie o layout:
[`PageFraming` e collettore](../vendor/oracle-rs/src/connection.rs).
Le prove di regressione sono nel
[file dei test dello scambio](../vendor/oracle-rs/src/connection_exchange_tests.rs).
