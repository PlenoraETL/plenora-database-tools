# ADR 0005 — Tipi canonici e `LossReport`

Stato: **accettato**  
Data: 2026-07-27

## Contesto

Il modello Python corrente comprime diversi tipi vendor in categorie troppo
larghe. Questo può perdere precisione decimal, timezone, unsigned, geometria,
dimensioni e SRID.

## Decisione

Il dato tabellare canonico è Arrow. Ogni campo conserva:

- tipo Arrow e nullability;
- precisione/scala e unità temporale;
- metadata nativi utili al round-trip;
- per la geometria: encoding GeoArrow-WKB, tipo, dimensioni, SRID/CRS e
  semantica `geometry`, `geography` o `feature_service`.

Ogni mapping usa una policy esplicita:

- `strict`: rifiuta qualsiasi perdita;
- `compatible`: permette conversioni semanticamente compatibili;
- `lossy`: permette perdite dichiarate;
- `native`: privilegia il round-trip del singolo provider.

La preparazione produce sempre un `LossReport`. In `strict`, una voce con
severity `data_loss` impedisce l’esecuzione. SRID sconosciuto rimane
sconosciuto e non diventa implicitamente 4326.

## Conseguenze

- le conversioni non sono più implicite;
- schema e mapping possono essere validati prima della mutazione;
- i test golden confrontano valori e geometrie semanticamente;
- il metadata nativo è versionato e non diventa una dipendenza tra provider.

## Alternative scartate

- ridurre tutto a stringa/float;
- usare soltanto i type name vendor;
- scegliere automaticamente il mapping “più comodo”.
