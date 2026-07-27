# ADR 0009 — AST spatial portabile e catalogo dichiarativo

Stato: **accettata**

## Decisione

Il core non espone centinaia di metodi PostGIS e non dipende da un ORM.
Espone invece:

- `QueryOperation` e `QueryExpression` senza valori interpolati;
- `SpatialFunction` con semantica provider-neutral;
- parametri tipizzati separati dall'AST;
- capability runtime per singola funzione;
- catalogo versionato `catalog/spatial-functions.v1.json`;
- renderer e adapter specifici per provider.

Raster, topology, SFCGAL e altre famiglie native restano estensioni opzionali.

## Motivo

PostGIS, Oracle Spatial, Db2 Spatial, SQL Server e ArcGIS hanno sintassi,
tipi, unità e capability differenti. Un wrapper PostGIS o Diesel non può
costituire il contratto comune della black box.

## Compatibilità

`ReadOperation` e i casi v1 esistenti restano validi. I nuovi predicati sono
additivi e falliscono in preparazione quando il provider non pubblicizza la
funzione richiesta.
