#!/usr/bin/env bash
# La suite SDK **offline**, contro il wheel installato.
#
# `verify_wheel.py` prova che il wheel si importi e dichiari la versione
# giusta. Non prova che faccia qualcosa: parametri tipizzati, normalizzazione
# Arrow, costruzione dell'AST portable e validazione spatial sono verificabili
# senza server e appartengono a questa suite.
#
# I test live si saltano da soli — leggono le variabili d'ambiente dei
# riferimenti e qui non ci sono — quindi non serve selezionarli a mano. Cio che
# resta e la parte che non ha bisogno di un database.
#
# La suite gira **fuori** dal checkout, e non e una comodita: `python/tests` e
# un package, quindi pytest metterebbe in `sys.path` la directory che lo
# contiene, e da li `plenora_database` si importerebbe dal source tree invece
# che dal wheel. E' la stessa precauzione di `scripts/check_sdk_tests.py`.
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
staging="${RUNNER_TEMP:-/tmp}/plenora-sdk-offline"

python -m pip install -q -r "$repository/requirements-sdk-tests.txt"

rm -rf "$staging"
mkdir -p "$staging"
cp -R "$repository/crates/plenora-database-py/python/tests" "$staging/tests"
rm -rf "$staging/tests/__pycache__"

cd "$staging"

# L'origine del package si verifica prima di misurare qualsiasi cosa: una
# suite che gira sul source tree darebbe un verdetto su un artefatto diverso
# da quello che sta per essere pubblicato.
python - <<'PROBE'
import pathlib
import plenora_database

origin = pathlib.Path(plenora_database.__file__).resolve()
if "site-packages" not in origin.parts:
    raise SystemExit(
        f"plenora_database importato da {origin}: non e il wheel installato"
    )
print(f"offline-suite: package da {origin}")
PROBE

# `-rs` elenca i motivi degli skip: senza, una suite interamente saltata e
# indistinguibile da una suite passata.
python -m pytest tests -q -rs --junitxml="$staging/offline-suite.xml"

# Il numero esatto e il contratto di `scripts/check_sdk_tests.py`, che e la
# fonte per quello: ripeterlo qui creerebbe una seconda verita da aggiornare.
# Qui basta il caso che rende il passo inutile — nessun test eseguito.
python - "$staging/offline-suite.xml" <<'COUNT'
import sys
import xml.etree.ElementTree as ET

suites = list(ET.parse(sys.argv[1]).getroot().iter("testsuite"))
total = sum(int(suite.get("tests", 0)) for suite in suites)
skipped = sum(int(suite.get("skipped", 0)) for suite in suites)
executed = total - skipped
print(f"offline-suite: {executed} eseguiti, {skipped} saltati, {total} raccolti")
if executed <= 0:
    raise SystemExit(
        "nessun test eseguito: la suite si e saltata per intero, ed e un verde "
        "che non dice niente sul wheel"
    )
COUNT
