# Campagna SDK su VM

Questo documento registra un esito di campagna, non una capability dedotta a
mano. Il comando autorevole resta il runner del repository:

```bash
python scripts/check_sdk_campaign.py
```

La corsa e stata eseguita il 31 agosto 2026 in una VM Linux usando soltanto il
daemon Docker della VM. Il checkout era pulito e non e cambiato durante la
misura.

| campo | valore |
| --- | --- |
| commit | `631e848fbf039b6bfc6a4e52d55fd0a4d88eff89` |
| live | `319 passed, 6 skipped in 10.31s` |
| benchmark | `2 passed, 323 deselected in 1.09s` |
| wheel SHA-256 | `3eaf421221a091494ea7d8bad1024f07f5f6eb8b45e3750f01c986c349119c16f` |
| modulo nativo SHA-256 | `866b648ee82eabe0663ee3d03a2c0efec1dbfffd83204b06d26037c90cc58318a` |
| CLI SHA-256 | `ab85fd8414d76f792fceb837b02f975977f5bb569add8d492d1b23cc7ecfbfc99` |
| immagine build | `rust@sha256:271849e998ffce5776454bbf98c5dc21baafc854ff8e566197908d3aca9a881e8` |
| immagine test | `python@sha256:7ce4b6dfe35e55397b7cda544f8a13f191b7ae28dc5aad71fe664dbc9bcc2623f` |
| Python / Rust | `3.13.15` / `1.98.0` |
| verdetto | `passed`, `authoritative=true` |

I sei skip live sono esclusivamente i casi Db2, che richiedono il runner e il
gate dedicati. Un nuovo esito non si ottiene aggiornando questa tabella: si
riesegue il comando su un checkout pulito e si registra il nuovo verdetto con i
suoi digest.
