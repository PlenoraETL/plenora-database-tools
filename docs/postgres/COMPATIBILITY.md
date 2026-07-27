# Compatibilità PostgreSQL/PostGIS

## Politica

La release stabile supporta soltanto combinazioni presenti nel gate live.
Il supporto segue le major PostgreSQL mantenute upstream, ma l'ingresso o
l'uscita dalla matrice richiede comunque un'esecuzione verde e una modifica
esplicita di questo documento.

Matrice verificata il 27 luglio 2026:

| Major | Versione osservata | PostGIS osservato | Immagine gate |
|---:|---|---:|---|
| 14 | 14.18 | 3.5.2 | `postgis/postgis:14-3.5` |
| 15 | 15.13 | 3.5.2 | `postgis/postgis:15-3.5` |
| 16 | 16.9 | 3.5.2 | `postgis/postgis:16-3.5` |
| 17 | 17.5 | 3.5.2 | `postgis/postgis:17-3.5` |
| 18 | 18.4 | 3.6.4 | `postgis/postgis:18-3.6` |

PostgreSQL 16/PostGIS 3.4 rimane il riferimento storico e prestazionale v3.
La matrice aggiuntiva verifica l'intera suite funzionale su ogni combinazione,
non soltanto una connessione.

PostgreSQL 14 non accetta scale `numeric` negative, introdotte dalla major 15.
La fixture usa quindi `numeric(8,0)` su 14 e `numeric(6,-2)` su 15–18; entrambi
i mapping sono sottoposti allo stesso roundtrip Arrow/write. Il supporto alla
scala negativa viene dichiarato solo quando il server la espone.

## Esecuzione

```powershell
python scripts\check_postgres_matrix.py
```

Il runner:

- crea un container effimero per volta;
- inizializza la fixture completa;
- esegue tutti i test non ignorati del crate PostgreSQL;
- legge dal server le versioni effettive;
- arresta e rimuove il container anche in caso di errore;
- non crea volumi persistenti e non salva il DSN.

Le immagini possono creare volumi Docker anonimi dichiarati upstream; l'uso di
`docker run --rm` li associa al container effimero e ne impedisce il riuso.

## Confini

- Le immagini ufficiali usate dal gate sono Linux amd64.
- PostgreSQL 14 termina il supporto upstream nel novembre 2026: a quella data
  va rimosso oppure mantenuto come profilo legacy esplicito.
- Le versioni osservate sono quelle contenute nei tag al momento del gate.
  Un deployment deve comunque usare l'ultima minor corretta della propria
  major: il gate di protocollo non certifica lo stato delle patch server.
- La compatibilità con servizi gestiti (RDS, Aurora, Cloud SQL, Azure Database,
  AlloyDB e simili) richiede campagne dedicate per privilegi, TLS, estensioni
  e comportamento operativo.
- Le beta e le immagini `master` non fanno parte della matrice stabile.
