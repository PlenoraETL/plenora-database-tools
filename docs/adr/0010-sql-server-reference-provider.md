# ADR 0010 — Provider di riferimento Microsoft SQL Server

Stato: **accettata per le fasi offline; verifica live obbligatoria**

Data: 2026-07-29

## Contesto

PostgreSQL/PostGIS resta il provider di riferimento già congelato. SQL Server
è il secondo provider e non può essere trattato come una variazione sintattica
di PostgreSQL: protocollo TDS, transazioni, paginazione, limiti dei parametri e
modello spatial hanno semantiche proprie.

Il componente è progettato con criteri safety-critical: un comportamento non
provato non viene pubblicizzato come capability e uno stato remoto ambiguo non
viene trasformato in successo.

## Decisione

- baseline iniziale: SQL Server 2022, compatibility level 160;
- client Rust diretto: `tiberius`, senza ORM e senza tipi driver nell'API
  pubblica;
- TLS richiesto e certificato verificato per default;
- `trust_server_certificate` è un'eccezione esplicita e diagnosticabile;
- MARS è disabilitato;
- bootstrap di ogni sessione:
  `SET XACT_ABORT ON; SET IMPLICIT_TRANSACTIONS OFF; SET NOCOUNT ON`;
- una connessione cancellata, con protocollo non drenato o con stato
  transazionale incerto viene quarantinata e mai restituita al pool;
- perdita della connessione durante `COMMIT` produce effetto remoto `Unknown`
  e richiede recovery;
- limite provider di 2.100 parametri e limite di 128 caratteri per ogni parte
  di un identificatore sono applicati prima dell'invio;
- paginazione: `TOP` senza offset; `OFFSET … ROWS/FETCH NEXT … ROWS ONLY` con
  offset e ordinamento deterministico;
- `MERGE` non è una strategia di scrittura predefinita;
- `geometry` e `geography` restano semantiche distinte;
- SRID ignoto resta ignoto: non viene sostituito implicitamente con 0 o 4326;
- nessuna riproiezione implicita e nessuna capability `ST_Transform`;
- non viene introdotto un fallback PROJ dentro il provider: una trasformazione
  client-side appartiene al livello centrale di elaborazione, deve ricevere
  CRS sorgente e destinazione risolti e sarà pubblicabile solo con dipendenza,
  griglie e failure policy versionate;
- `FullGlobe` è rifiutato nel profilo strict iniziale.

## Gate di evidenza

Le fasi offline 0–3 possono definire contratti, renderer, configurazione,
pool e macchina a stati. Restano `unverified` fino alla campagna live:

1. handshake TLS e autenticazione;
2. bootstrap e verifica delle opzioni sessione;
3. cancellazione durante result set e transazione;
4. perdita di rete prima, durante e dopo `COMMIT`;
5. errori TDS, deadlock e timeout reali;
6. round-trip dei tipi critici e di `geometry`/`geography`;
7. compatibilità SQL Server 2019, 2022, 2025 e Azure SQL.

Nessun esito offline soddisfa questi punti.

## Conseguenze

Il nuovo crate può essere costruito e testato senza database, ma pubblica solo
le proprietà dimostrate. L'abilitazione di lettura, scrittura e spatial
avverrà per capability atomiche dopo evidenza riproducibile.
