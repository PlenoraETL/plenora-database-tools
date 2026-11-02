# Provenienza dei profili

`baseline.json` identifica il contenuto storico prima della separazione dalle
sonde server. I suoi hash non vengono aggiornati durante una restrizione.
`writer_restrictions.json` conserva prima, dopo e nomi dei valori della batteria
per ogni restrizione derivata dal writer. La provenienza e il comando sono nel
referto: non si tratta di una nuova misura diretta del server.

In particolare, l'osservazione storica di CLOB proviene dalla sonda del server;
il rifiuto aggiunto deriva dal writer. I valori rimasti ammessi conservano
l'evidenza storica, senza attribuirle retroattivamente un replay della libreria.

La regressione `qualified_profile_pure_guards` visita ogni chiave e confronta
la batteria con le guardie di produzione invocate dai test dei provider. Il
confronto e unidirezionale: un rifiuto puro deve comparire nel profilo; un
successo locale non dimostra un successo del server. I test vivono fuori dai
moduli `live_` e vengono eseguiti dal job unit del workflow `rust-ci`.

`scripts/test_sonde_scritture_server.py` ricostruisce il baseline, ne controlla
gli hash e calcola le ammissioni prima/dopo per ogni valore della batteria,
con e senza perdita ammessa. Le guardie P/codec continuano a valere anche
quando una riscrittura storica diventa un rifiuto.

Il replay completo, comprese le guardie che dipendono dal catalogo e dal
server, resta nel workflow `write-admission-probes`. Il controllo offline
non lo sostituisce e non qualifica nuove coppie o nuovi valori.
