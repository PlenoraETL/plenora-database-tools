#!/usr/bin/env python3
"""Scoperta della rete Compose di un container di riferimento.

I compose del repository dichiarano ciascuno il proprio progetto
(`docker-compose.*.yml`, campo `name:`), quindi la rete si chiama
`<progetto>_default` e cambia se il progetto cambia. Un runner che scrive quel
nome a mano si rompe in silenzio al primo rename: il container esiste, la rete
esiste, ma il contenitore del gate finisce su un'altra rete e ogni connessione
fallisce con un errore di trasporto che non nomina la causa.

La rete si chiede a Docker, come gia fanno i due gate di riferimento.
"""

from __future__ import annotations

import json
import subprocess


def _inspect(container: str, template: str) -> str:
    completed = subprocess.run(
        ["docker", "inspect", "--format", template, container],
        check=False,
        text=True,
        capture_output=True,
        timeout=30,
    )
    if completed.returncode:
        raise RuntimeError(
            f"container {container} non ispezionabile: e avviato? "
            f"({completed.stderr.strip()})"
        )
    return completed.stdout.strip()


def compose_network(container: str, *, required_alias: str | None = None) -> str:
    """Nome della rete Compose a cui il container di riferimento e attaccato.

    # Raises

    `RuntimeError` quando il container non esiste, non e stato avviato da
    Compose, o e attaccato a una rete diversa da quella del proprio progetto.
    Fallire qui e preferibile a passare un nome inventato a `docker run`, che
    produrrebbe un errore di connessione senza indizi sulla causa.
    """

    labels = json.loads(_inspect(container, "{{json .Config.Labels}}"))
    project = labels.get("com.docker.compose.project") if isinstance(labels, dict) else None
    if not isinstance(project, str) or not project:
        raise RuntimeError(
            f"container {container} senza label di progetto Compose: "
            "non e stato avviato da docker compose"
        )
    expected = f"{project}_default"
    networks = json.loads(_inspect(container, "{{json .NetworkSettings.Networks}}"))
    network = networks.get(expected) if isinstance(networks, dict) else None
    if network is None:
        available = sorted(networks) if isinstance(networks, dict) else []
        raise RuntimeError(
            f"container {container} non e sulla rete {expected} (trovate: {available})"
        )
    if required_alias is not None:
        aliases = network.get("Aliases")
        if not isinstance(aliases, list) or required_alias not in aliases:
            raise RuntimeError(
                f"alias {required_alias} assente dalla rete {expected} di {container}"
            )
    return expected


if __name__ == "__main__":
    import sys

    for name in sys.argv[1:]:
        print(f"{name}: {compose_network(name)}")
