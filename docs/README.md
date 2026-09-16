# Documentazione

Le guide descrivono l'API e le procedure correnti. Versioni, capability e
inventari derivano dai sorgenti; gli esiti live sono qualificati dalla corsa
che li produce. La cronologia delle modifiche si consulta in Git.

| documento | contenuto |
| --- | --- |
| [`STATO.md`](STATO.md) | stato generato: crate, capability, comandi e inventario dei test |
| [`operativo.md`](operativo.md) | lifecycle e isolamento delle fixture Compose |
| [`README dello SDK`](../crates/plenora-database-py/README.md) | API Python, esempi e limiti |
| [`CLI_AND_SDK.md`](../crates/plenora-database-py/docs/CLI_AND_SDK.md) | uso dello SDK nelle applicazioni che invocano il CLI |
| [`mariadb/EVIDENCE.md`](mariadb/EVIDENCE.md) | inventario generato delle prove MariaDB |
| [`mariadb/SESSION-MATRIX.md`](mariadb/SESSION-MATRIX.md) | matrice di riferimento confrontata dal gate live di sessione |
| [`mariadb/ARCHITETTURA.md`](mariadb/ARCHITETTURA.md) | confine fra profili MySQL e MariaDB |

`scripts/render_state.py` genera `STATO.md`; `scripts/check_docs.py` verifica
documenti generati, link, comandi ed esempi. I documenti generati non si
modificano a mano.
