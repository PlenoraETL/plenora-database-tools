# Campagna SDK su VM

Questo documento registra un esito di campagna, non una capability dedotta a
mano. Il comando autorevole resta il runner del repository:

```bash
python scripts/check_sdk_campaign.py
```

Le prime corse sono state eseguite il 31 agosto 2026; la qualifica Db2 e stata
eseguita il 1 settembre 2026. Tutte usano una VM Linux e soltanto il relativo
daemon Docker. In tutte le corse riportate il checkout era pulito e non e
cambiato durante la misura.

## Bind PostgreSQL e lifecycle delle transazioni Python

| campo | valore |
| --- | --- |
| commit | `65b6b8151c2a5e63ca5f96c0e2fb0c607100ed83` |
| live | `332 passed, 6 skipped in 10.03s` |
| benchmark | `2 passed, 336 deselected in 1.07s` |
| offline | `101 passed, 237 skipped in 0.79s` |
| wheel live/benchmark SHA-256 | `8df1d8da6a8937c7d3648b345424f7c45b316946b856a8d355eee734ead6597b` |
| wheel offline SHA-256 | `318794a9df8de063b12452ff58cdfe1866a9a4d8c9e15276688f623e1725f30c` |
| modulo nativo SHA-256 | `e921ca6fe5de591e1d753fe9e66e6f633d5d3555bcebcd0f6105f27b6045fd03` |
| CLI SHA-256 | `f8d9c60ffaa8fb1b8dd06b3c553a1a749079e2134a7e8ec9d011a6708a7fffa00` |
| immagine build | `rust@sha256:271849e998ffce5776454bbf98c5dc21baafc854ff8e566197908d3aca9a881e8` |
| immagine test | `python@sha256:7ce4b6dfe35e55397b7cda544f8a13f191b7ae28dc5aad71fe664dbc9bcc2623f` |
| Python / Rust | `3.13.15` / `1.98.0` |
| verdetto | live, benchmark e offline `passed`, `authoritative=true` |

La campagna copre il widening di un intero Python piccolo verso `bigint`, gli
helper pubblici `int32()`/`int64()`, la diagnostica di bind senza payload e il
rilascio thread-affine della transazione prima del passaggio del traceback fra
thread. I sei skip live restano esclusivamente i casi Db2 dedicati.

## Qualifica Geometry ORM MySQL/MariaDB

| campo | valore |
| --- | --- |
| commit | `33c998d0c8e7e607ac59fc347b26b1e3a66bbb85` |
| live | `321 passed, 6 skipped in 10.01s` |
| benchmark | `2 passed, 325 deselected in 1.14s` |
| wheel SHA-256 | `980d113738a0aca364372fbc1c766e6d5b0b46de33d7afae2b9cf71f3aea1823` |
| modulo nativo SHA-256 | `8d65944d9c31396144c3e7095ee0bfa05339cd3f1eaa5fefbecdce948bbf75e94` |
| CLI SHA-256 | `4730916e384e838694048ffb56bb24bfd4f410aab42a15661abc363ad3c32f02` |
| immagine build | `rust@sha256:271849e998ffce5776454bbf98c5dc21baafc854ff8e566197908d3aca9a881e8` |
| immagine test | `python@sha256:7ce4b6dfe35e55397b7cda544f8a13f191b7ae28dc5aad71fe664dbc9bcc2623f` |
| Python / Rust | `3.13.15` / `1.98.0` |
| verdetto | `passed`, `authoritative=true` |

I sei skip live sono esclusivamente i casi Db2, che richiedono il runner e il
gate dedicati.

## Qualifica Geometry ORM Db2

| campo | valore |
| --- | --- |
| commit | `d737afc64ea2c90329ec46271f2cb57b6f1c458e` |
| generato | `2026-09-01T01:38:35.478419+00:00` |
| server | `DB2 v12.1.5.0` |
| unit offline | `44 passed, 12 ignored` |
| live Rust | `12 passed, 0 failed` |
| wheel Python live | `7 passed, 0 failed` |
| immagine fixture | `sha256:4a086fe8098851ec96332a039540e76ac20bd9663fe56b0dd4a9b3e097486802` |
| immagine Db2 | `icr.io/db2_community/db2@sha256:2de8151713c261843868c5c3411b57be6ae779d99d70a5b3022337836776bfda6` |
| immagine Rust | `rust@sha256:271849e998ffce5776454bbf98c5dc21baafc854ff8e566197908d3aca9a881e8` |
| piattaforma | `linux-x86_64` |
| verdetto | `passed` |

Il gate ha costruito e importato `plenora-database 0.13.0` dai sorgenti del
commit, poi ha esercitato Point, LineString e Polygon XY, Point XYZ, `NULL`,
SRID, input EWKB invalido, insert, lettura, update, predicato spatial e delete.
Il risultato Db2 e semanticamente stabile; `ST_GEOMETRY` puo normalizzare le
coordinate floating point di un ULP, quindi il gate verifica struttura esatta
e coordinate con tolleranza esplicita invece dell'identita accidentale dei
byte WKB.

## Chiusura Geometry ORM multi-provider

| campo | valore |
| --- | --- |
| commit | `a97a0c6a74b9dfd10a433f098e0691f4db7baee2` |
| live | `342 passed, 7 skipped in 10.16s` |
| benchmark | `2 passed, 347 deselected in 1.08s` |
| offline | `passed` |
| wheel SHA-256 | `1109585277220945247c6dd84d250c623decc1aec45ceebfcc1975032a167dd3e` |
| modulo nativo SHA-256 | `0f9c477adad60fab0c208207fd5685a6048166fd0928efa61086eb578a1304b52` |
| CLI SHA-256 | `e49c29366e4e0fa960b0fc839b245b5c08700052134589ac1985438585a5fa700` |
| immagine build | `rust@sha256:271849e998ffce5776454bbf98c5dc21baafc854ff8e566197908d3aca9a881e8` |
| immagine test | `python@sha256:7ce4b6dfe35e55397b7cda544f8a13f191b7ae28dc5aad71fe664dbc9bcc2623f` |
| Python / Rust | `3.13.15` / `1.98.0` |
| verdetto | live, benchmark e offline `passed`, `authoritative=true` |

I sette skip live sono esclusivamente i test Db2 che richiedono il fixture
dedicato; non sono conteggiati come copertura implicita e sono chiusi dal gate
Db2 riportato nella sezione precedente.

## Baseline precedente

La baseline immediatamente precedente, commit
`631e848fbf039b6bfc6a4e52d55fd0a4d88eff89`, era anch'essa autorevole: live
`319 passed, 6 skipped`, benchmark `2 passed, 323 deselected`. Il delta di due
casi live e due casi deselezionati corrisponde alla qualifica MySQL/MariaDB.

Un nuovo esito non si ottiene aggiornando questa pagina: si riesegue il comando
su un checkout pulito e si registra il nuovo verdetto con i suoi digest.
