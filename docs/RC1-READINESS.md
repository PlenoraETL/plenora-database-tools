# Readiness RC1 di `plenora-database-tools`

## Esito

La baseline tecnica
`5d644461c882e5f97efafc32e2af4f7d3552d70f` è congelata e il tag annotato,
non firmato, `v0.1.0-rc.1` è stato creato sul record pre-tag
`b541c61dd1c286cdf2e808e17eefd133d7c9ba20`. Il claim è
`verified_internally`; non sono dichiarate né una RC di sistema né una
certificazione avionica.

Il manifesto verificabile e autoritativo è
[`release/rc1-readiness.json`](../release/rc1-readiness.json). Il gate è
fail-closed: controlla la baseline, le evidenze, i claim, il blocco della review,
le dipendenze esterne e le riduzioni di copertura.

## Perimetro congelato

Il candidato comprende:

- PostgreSQL 16 con PostGIS 3.4;
- SQL Server 2022 Developer `16.0.4255.1`, compatibility level 160;
- `geometry` e `geography` SQL Server XY con SRID esplicito.

`0.1.0-rc.1` è l'etichetta del candidato nel manifesto e la versione dei sette
crate. La baseline precedente è stata sostituita deliberatamente dopo
l'allineamento SemVer e la ripetizione delle evidenze.

## Evidenze sulla baseline

| Gate | Esito | Esecuzione |
| --- | --- | --- |
| PostgreSQL/PostGIS reference | superato | [30548540038](https://github.com/PlenoraETL/plenora-database-tools/actions/runs/30548540038) |
| SQL Server reference | 28/28 live, 49 offline, Clippy pulito | [30548543561](https://github.com/PlenoraETL/plenora-database-tools/actions/runs/30548543561) |
| Coverage workspace | soglie linee 80%, funzioni 79%, regioni 77% superate | [30548539965](https://github.com/PlenoraETL/plenora-database-tools/actions/runs/30548539965) |
| Manifesto C1-C4 | superato senza errori | [30548541207](https://github.com/PlenoraETL/plenora-database-tools/actions/runs/30548541207) |

Ogni evidenza è registrata sulla stessa revisione completa. I link non sono
considerati prova sufficiente da soli: il manifesto associa anche identificativo,
stato e baseline, e il gate verifica la coerenza meccanicamente.

## Decisione e dipendenze

La revisione indipendente (`PLN-DB-REVIEW`) è un attributo di assurance aperto,
non un blocker della RC di componente. R0.4 limita il claim corrente a
`verified_internally`; C4.2 richiede di dichiarare separatamente che la review
non è avvenuta, ma non vieta il tag. Una futura promozione a
`verified_independently` dovrà registrare almeno:

1. identità e indipendenza del revisore;
2. revisione esatta esaminata;
3. comandi e ambienti usati;
4. rilievi, severità e chiusure;
5. esito finale esplicito.

Tre elementi restano dipendenze esterne, ma non sono trasformati impropriamente
in blocker della RC di componente:

- ratifica di R4.6, necessaria per separare rilevazione CRS e policy per ruolo;
- ratifica di §15.3, necessaria per il crate normativo condiviso;
- qualifica di sistema, di proprietà del corpus di conformità trasversale.

Queste dipendenze impediscono rispettivamente l'adozione trasversale o un claim
di sistema. Non annullano le evidenze del perimetro di componente secondo C3.

## Riduzioni di copertura dichiarate

Restano fuori dal candidato:

- SQL Server 2019, SQL Server 2025 e Azure SQL;
- geometrie SQL Server Z, M, ZM e `FullGlobe`;
- catena TLS positiva con CA privata e hostname matching;
- catalogo esteso temporal, graph, external table e partizioni.

Il manifesto conserva per ciascun punto la policy runtime, la condizione
d'uscita e una distinzione obbligatoria fra:

- `verified_live`: comportamento esercitato da un test nominato in una specifica
  evidenza live;
- `declared_not_verified_live`: requisito o riduzione dichiarata che la campagna
  live corrente non dimostra.

La copertura spaziale negativa è deliberatamente descritta come parziale. La
campagna live prova soltanto che una `geometry Z` viene rifiutata sul bordo di
lettura senza essere appiattita a XY. Il rifiuto di `geography Z`, degli altri
bordi Z, di M, ZM e `FullGlobe` resta dichiarato ma non verificato live. Allo
stesso modo, il riferimento prova SQL Server 2022/compatibility level 160, non
le piattaforme escluse; prova il rifiuto del certificato self-signed sotto
`Verify` e l'opt-out esplicito, non una catena positiva con CA privata.

Il catalogo esteso è `declared_only`: nessuna delle quattro famiglie escluse è
presentata come provata. L'assenza di supporto non deve diventare accettazione
silenziosa, ma la policy dichiarata non viene confusa con un'evidenza.

## Regola di congelamento

Qualunque modifica al codice di produzione o agli input dei gate dopo la
baseline invalida il pacchetto: il controllo comprende crate, contratti, catalogo,
fixture, script di assurance, workflow tecnici, ambienti Docker, test, fuzzing e
baseline prestazionali. Occorre scegliere una nuova revisione, ripetere i gate
applicabili e aggiornare tutte le evidenze. Modifiche soltanto documentali o al
gate di readiness non spostano automaticamente la baseline tecnica, ma devono
comunque passare la CI.

Nello stato `tagged` il gate verifica il target e l'oggetto del tag annotato,
oltre ai quattro run pre-tag sul record di rilascio. La review indipendente resta
aperta e potrà promuovere separatamente il claim, ma non è una condizione del
tag `verified_internally`.

## Esecuzione locale

```powershell
python scripts\check_release_manifest.py --repo . release\development.json release\rc1-readiness.json
python scripts\test_check_release_manifest.py
python scripts\check_rc1_readiness.py --repo . release\rc1-readiness.json
python scripts\test_check_rc1_readiness.py
```
