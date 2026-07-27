# ADR 0007 — Segreti, errori e costruzione SQL

Stato: **accettato**  
Data: 2026-07-27

## Contesto

Una black box multi-provider gestisce DSN, password, token ArcGIS, SQL e
payload potenzialmente sensibili. I messaggi dei driver possono contenere
questi valori.

## Decisione

- Il piano contiene un `connection_ref`, non credenziali persistite.
- I segreti entrano tramite resolver iniettato e vivono in wrapper redatti.
- Log, metriche, errori pubblici e report non contengono DSN, token, bind
  values o payload completi.
- Gli errori canonici hanno categoria, fase, provider, retryability,
  execution ID e cause redatte.
- Valori SQL passano esclusivamente da bind parameter.
- Identificatori passano da tipi validati e quoting del dialect; non vengono
  trattati come bind value né concatenati da input grezzo.
- SQL libero è fuori dal piano canonico v1. Un’eventuale escape hatch futura
  richiederà un ADR e una policy separata.
- Dopo outcome incerto, la connessione non torna nel pool.

Il testkit usa marker-segreto e fallisce se un artefatto pubblico li contiene.

## Conseguenze

- diagnosi e telemetria restano utili ma sicure;
- SQL injection e leakage diventano proprietà verificabili;
- gli errori vendor completi possono vivere solo in un sink protetto e
  opt-in, mai nell’API normale.

## Alternative scartate

- accettare DSN inline nel piano serializzato;
- pubblicare direttamente `str(error)` del driver;
- interpolare valori o identificatori in template SQL.
