"""MISURA TEMPORANEA 8.0.4 (solo ramo misura/804): colonne omonime su MariaDB.

Stesso ambiente della campagna `check_mariadb_campaign.py` (fixture, rete,
CA, credenziali dal container), ma esegue soltanto `misura_804` con il
`MariadbProvider` su ciascun riferimento MariaDB.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.check_mariadb_driver import (  # noqa: E402
    RUST_IMAGE,
    compose_network_arguments,
    compose_volume,
    container_variable,
    servers,
)
from scripts.fixture_campaign import start_fixtures, stop_fixtures  # noqa: E402

TEST_COMMAND = (
    "cargo test --locked -p plenora-db-mysql --lib misura_804 "
    "-- --ignored --nocapture --test-threads=1"
)


def main() -> int:
    started: list[str] = []
    try:
        start_fixtures(("docker-compose.mysql.yml", "docker-compose.mariadb.yml"), started)
        for server in servers():
            if server.key == "mysql":
                continue
            print(f"MISURA ===== riferimento {server.label} ({server.container}) =====", flush=True)
            tls_volume = compose_volume(server.container, "/etc/mysql/tls")
            command = [
                "docker", "run", "--rm",
                *compose_network_arguments(server.container),
                "-v", f"{ROOT}:/workspace",
                "-v", f"{tls_volume}:/tls:ro",
                "-v", "plenora_cargo_registry:/usr/local/cargo/registry",
                "-v", "plenora_cargo_git:/usr/local/cargo/git",
                "-v", "plenora_rustup:/usr/local/rustup",
                "-v", "pln_target_docker:/workspace/target-docker",
                "-w", "/workspace",
                "-e", "CARGO_TARGET_DIR=/workspace/target-docker",
                "-e", f"PLENORA_MYSQL_HOST={server.container}",
                "-e", "PLENORA_MYSQL_PORT=3306",
                "-e", "PLENORA_MYSQL_CA=/tls/ca.pem",
                "-e", f"PLENORA_MYSQL_DATABASE={container_variable(server.container, 'MARIADB_DATABASE')}",
                "-e", f"PLENORA_MYSQL_USER={container_variable(server.container, 'MARIADB_USER')}",
                "-e", f"PLENORA_MYSQL_PASSWORD={container_variable(server.container, server.password_variable)}",
                "-e", "MISURA_PRODUCT=mariadb",
                "-e", f"MISURA_LABEL=mariadb[{server.label}]",
                RUST_IMAGE, "sh", "-c", TEST_COMMAND,
            ]
            completed = subprocess.run(command, cwd=ROOT, check=False)
            print(f"MISURA riferimento {server.label} exit={completed.returncode}", flush=True)
    finally:
        for failure in stop_fixtures(started):
            print(f"pulizia: {failure}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
