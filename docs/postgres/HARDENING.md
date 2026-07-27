# Hardening PostgreSQL/PostGIS

Stato: **profilo hardening v1 implementato sul riferimento PostgreSQL 16 /
PostGIS 3.4**.

Questo profilo non aggiunge nuove promesse SQL: rende verificabili lifecycle,
sicurezza e comportamento sotto errore del data path v3.

## Garanzie aggiunte

- Il pool è bounded e partizionato da fingerprint di credenziale, modalità TLS,
  opzioni di rete e timeout di sessione.
- Una connessione nuova riceve timeout e `application_name` nel pacchetto di
  startup, senza una query di configurazione. Ogni connessione riusata esegue
  un solo `DISCARD ALL`, che ripristina quegli stessi default ed elimina anche
  prepared statement e oggetti temporanei.
- Un errore di reset rende la sessione non riutilizzabile.
- La cache schema è LRU e bounded. Non considera mai una TTL come prova di
  validità: confronta OID e firma strutturale del catalogo prima di ogni hit.
- Il token include stato di colonne, tipi, typmod, nullability, default,
  identity/generated e campi composite. Un DDL esterno forza il refresh.
- Una write Plenora invalida il target dopo commit e anche quando l'outcome è
  incerto.
- Il fast path parametrizzato viene usato soltanto quando bind, colonna e tipo
  PostgreSQL built-in sono determinabili senza ambiguità. Enum, domini,
  composite e mismatch ricadono sul prepare con inferenza server-side.
- `QueryOperation` usa il one-shot con tipi canonici. Se PostgreSQL rifiuta la
  tipizzazione, il driver torna al prepare; la SELECT non viene eseguita due
  volte. Per un risultato vuoto il describe ricava lo schema senza una seconda
  esecuzione.
- Una sessione con errore di protocollo/stream, write fallita, commit incerto,
  cancellazione o stream abbandonato non rientra nel pool.
- Le cancellazioni raggiungono il backend PostgreSQL con timeout bounded.
- Dopo cancellazioni concorrenti il pool deve ristabilire una connessione sana.
- `PostgresTlsMode::Disabled` forza `sslmode=disable`;
  `PostgresTlsMode::Require` forza `sslmode=require` e verifica hostname e
  catena tramite Rustls/WebPKI. Il DSN non può indebolire la modalità scelta
  dall'API.
- `PostgresTlsConfig` accetta `WebPKI`, CA private PEM, catene client e chiavi
  PEM PKCS#1/PKCS#8/SEC1; il pool separa anche configurazioni TLS differenti.
- Il materiale mTLS viene compilato in un connector Rustls condiviso, non ha
  getter o serializzazione e appare sempre redatto in `Debug`.
- Il parser numeric interno rifiuta segni multipli, whitespace, esponenti e
  forme ambigue non prodotte dal mapping supportato.
- L'encoding binary numeric è verificato deterministicamente su zero, segno,
  scale positive/negative e limiti `i128`.
- Escaping di range e composite con quote, backslash, newline e Unicode ha un
  unico encoder condiviso.

## Metriche bounded

`PostgresProvider::metrics_snapshot()` restituisce soli contatori `u64`:

| Area | Contatori |
|---|---|
| pool | checkout, riusi, nuove connessioni, timeout |
| lifecycle | reset delle sessioni riusate, sessioni invalidate, cancellazioni |
| schema | controlli token, hit, miss, eviction e invalidazioni cache |
| read | introspezioni catalogo complete, fast path tipizzati `ReadOperation`/`QueryOperation`, fallback prepared, batch, righe, byte Arrow, batch chiusi dal target adattivo |
| write | commit, righe confermate, outcome unknown |

Non esistono label dinamiche. Snapshot e metriche non contengono DSN, SQL,
hostname, database, utente, nomi di oggetti o valori. I contatori usano atomiche
relaxed: servono per osservabilità operativa, non per sincronizzare il data
path.

## Prove automatiche

Il gate esegue:

1. rustfmt e Clippy con warning negati;
2. test deterministici dei codec senza database;
3. suite live read/write/PostGIS;
4. 120 letture concorrenti su pool massimo 4, con conteggi esatti;
5. quattro query lente cancellate simultaneamente, assenza di backend rimasti
   attivi e recovery del pool;
6. rollback prima del commit e `OutcomeUnknown` dopo commit;
7. reset di una sessione contaminata: GUC, tabella temporanea e prepared
   statement devono sparire conservando i default di startup;
8. cache schema: miss, hit validato, modifica DDL esterna, nuovo fingerprint,
   refresh e LRU eviction;
9. metriche di pool, reset, schema, introspezione, fast path, streaming, scrittura,
   cancellazione e invalidazione;
10. server con CA privata e `clientcert=verify-full`, rifiuto senza identità,
   cancellazione server-side mTLS e recovery.
11. pianificazione tipizzata dei bind built-in e spatial, con fallback
    deterministico per i tipi custom.
12. schema `QueryOperation` ricavato dalla prima riga e describe sicuro per
    result set vuoti.

Esecuzione:

```powershell
python scripts\check_postgres_hardening.py
```

## Limiti TLS dichiarati

CA privata e mTLS sono ora coperti live. Restano fuori dal profilo corrente
CRL/OCSP configurabili, chiavi cifrate con password, integrazione diretta con
HSM/PKCS#11 e rotazione live senza ricreare il provider/pool.

La fixture dedicata si avvia con:

```powershell
docker compose -f docker-compose.postgres-tls.yml up -d
```

Certificati e chiavi di test sono generati nel volume Docker
`plenora-database-tools_postgres_tls_certs`, mai nel repository.

## Matrice versioni ancora necessaria

Il riferimento PostgreSQL 16/PostGIS 3.4 è affiancato dal gate completo sulle
major PostgreSQL 14–18, PostGIS 3.5–3.6 e dal server mTLS con CA privata.
Risultati e policy sono in [COMPATIBILITY.md](COMPATIBILITY.md).

Restano campagne esterne per TLS con CA pubblica reale, Linux arm64 e servizi
PostgreSQL gestiti con i rispettivi vincoli di privilegi ed estensioni.
