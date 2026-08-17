#!/usr/bin/env python3
"""Cio che un container di riferimento sa di se: rete, volumi, ambiente.

I compose del repository dichiarano ciascuno il proprio progetto
(`docker-compose.*.yml`, campo `name:`), quindi la rete si chiama
`<progetto>_default` e cambia se il progetto cambia. Un runner che scrive quel
nome a mano si rompe in silenzio al primo rename: il container esiste, la rete
esiste, ma il contenitore del gate finisce su un'altra rete e ogni connessione
fallisce con un errore di trasporto che non nomina la causa.

Lo stesso vale per le credenziali della fixture. Ricopiarle nel runner crea
due fonti per un dato che ne ha una — il compose — e le due divergono senza
rumore: al primo cambio di password il gate fallisce in autenticazione, cioe
con l'errore di un server configurato male invece che di un runner stale. E
mette una password in chiaro in un file in piu, per ogni runner che la
ricopia.

Rete, volumi e ambiente si chiedono a Docker, con la stessa `docker inspect`.
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


def compose_network(
    container: str,
    *,
    required_alias: str | None = None,
    required_aliases: dict[str, str] | None = None,
) -> str:
    """Nome della rete Compose a cui il container di riferimento e attaccato.

    `required_aliases` mappa alias -> motivo per gli alias che servono oltre a
    quello del container. Il motivo finisce nel messaggio di errore perche un
    alias mancante non e mai un dettaglio di configurazione: nel riferimento
    MySQL, senza `mysql-hostname-mismatch` la prova TLS negativa diventerebbe
    un errore DNS invece di un rifiuto di identita, e passerebbe per un
    successo del gate.

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
    aliases = network.get("Aliases")
    if required_alias is not None:
        if not isinstance(aliases, list) or required_alias not in aliases:
            raise RuntimeError(
                f"alias {required_alias} assente dalla rete {expected} di {container}"
            )
    for alias, reason in (required_aliases or {}).items():
        if not isinstance(aliases, list) or alias not in aliases:
            raise RuntimeError(
                f"alias {alias} assente dalla rete {expected} di {container}: {reason}"
            )
    return expected


def compose_network_arguments(*containers: str) -> list[str]:
    """Argomenti `--network` per raggiungere **tutti** i container indicati.

    Riferimenti in progetti Compose distinti stanno su reti distinte, e un
    gate che ne interroga due — il caso dell'hardening PostgreSQL, che
    confronta il riferimento plaintext con quello TLS — deve attaccarsi a
    entrambe. `docker run` accetta piu `--network`, quindi la risposta e una
    lista, non un nome.

    Le reti si deduplicano: due container dello stesso progetto restituiscono
    un solo `--network`, e passarlo due volte sarebbe un errore di Docker.

    # Raises

    `RuntimeError` per gli stessi motivi di [`compose_network`].
    """

    arguments: list[str] = []
    seen: set[str] = set()
    for container in containers:
        network = compose_network(container, required_alias=container)
        if network in seen:
            continue
        seen.add(network)
        arguments += ["--network", network]
    return arguments


def compose_volume(container: str, destination: str) -> str:
    """Nome del volume montato dal container in `destination`.

    Come per la rete, il nome porta il prefisso del progetto Compose e cambia
    quando il progetto cambia: scriverlo a mano lo rende stale al primo
    rename, e l'errore che ne segue e un mount vuoto — il gate vede una
    directory senza certificati invece di un nome sbagliato.

    # Raises

    `RuntimeError` quando il container non e ispezionabile o non monta un
    volume **nominato** in quella destinazione.
    """

    mounts = json.loads(_inspect(container, "{{json .Mounts}}"))
    for mount in mounts if isinstance(mounts, list) else []:
        if mount.get("Destination") == destination and mount.get("Name"):
            return str(mount["Name"])
    raise RuntimeError(
        f"container {container} non monta un volume nominato in {destination}"
    )


def container_environment(container: str) -> dict[str, str]:
    """Le variabili d'ambiente con cui il container e stato avviato.

    Sono quelle del compose: e da li che arrivano utente, database e password
    della fixture.
    """

    entries = json.loads(_inspect(container, "{{json .Config.Env}}"))
    environment: dict[str, str] = {}
    for entry in entries if isinstance(entries, list) else []:
        name, separator, value = str(entry).partition("=")
        if separator:
            environment[name] = value
    return environment


def container_variable(container: str, variable: str) -> str:
    """Il valore di `variable` nel container **in esecuzione**.

    E l'unico modo di ottenere una credenziale della fixture senza scriverne
    una seconda copia: il compose la dichiara, il container la porta, il
    runner la legge. Se il compose cambia, il gate cambia con lui.

    # Raises

    `RuntimeError` quando la variabile manca o e vuota. Vuota e il caso
    insidioso: proseguire produrrebbe un tentativo di autenticazione senza
    password, e l'errore parlerebbe di credenziali sbagliate invece che di
    una fixture avviata in modo diverso da come il gate la assume.
    """

    value = container_environment(container).get(variable)
    if not value:
        raise RuntimeError(
            f"variabile {variable} assente o vuota nel container {container}: "
            "la fixture non e quella che il gate assume"
        )
    return value


if __name__ == "__main__":
    import sys

    for name in sys.argv[1:]:
        print(f"{name}: {compose_network(name)}")
