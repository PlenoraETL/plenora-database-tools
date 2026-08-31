# Campagna SDK su VM

Questo documento registra un esito di campagna, non una capability dedotta a
mano. Il comando autorevole resta il runner del repository:

```bash
python scripts/check_sdk_campaign.py
```

Le corse sono state eseguite il 31 agosto 2026 in una VM Linux usando soltanto
il daemon Docker della VM. In tutte le corse riportate il checkout era pulito
e non e cambiato durante la misura.

## Bind PostgreSQL e lifecycle delle transazioni Python

| campo | valore |
| --- | --- |
| commit | `fc1b3315dd520f3a42e8a549422a1f7de1560813` |
| live | `332 passed, 6 skipped in 9.88s` |
| benchmark | `2 passed, 336 deselected in 1.03s` |
| offline | `101 passed, 237 skipped in 0.76s` |
| wheel live/benchmark SHA-256 | `97258f821b47199868093b5549df86796c184dc3dcc5bfdbc25664c7c8251cd4` |
| wheel offline SHA-256 | `4a063f646d75eec4f96d5a8dbb87dac0747b7de137597877060ed117a1e56cf2` |
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

## Baseline precedente

La baseline immediatamente precedente, commit
`631e848fbf039b6bfc6a4e52d55fd0a4d88eff89`, era anch'essa autorevole: live
`319 passed, 6 skipped`, benchmark `2 passed, 323 deselected`. Il delta di due
casi live e due casi deselezionati corrisponde alla qualifica MySQL/MariaDB.

Un nuovo esito non si ottiene aggiornando questa pagina: si riesegue il comando
su un checkout pulito e si registra il nuovo verdetto con i suoi digest.
