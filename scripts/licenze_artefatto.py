"""Confeziona e verifica CLI zip, wheel e sorgenti tar.gz senza rete.

Il controllo confronta l'archivio con il registro esterno versionato: eliminare
insieme una voce di CRATE.json e i suoi file non puo rendere verde il gate.
"""
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
from pathlib import Path
import sys
import tarfile
import zipfile

import avvisi_crate as notices


def read_archive(path: Path) -> dict[str, bytes]:
    if path.name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as archive:
            entries = [m for m in archive.getmembers() if m.isfile()]
            names = [m.name for m in entries]
            files = {m.name: archive.extractfile(m).read() for m in entries}
    else:
        with zipfile.ZipFile(path) as archive:
            names = [m.filename for m in archive.infolist() if not m.is_dir()]
            files = {name: archive.read(name) for name in names}
    if len(names) != len(set(names)) or any(not notices.safe_name(n) for n in names):
        raise ValueError("nomi duplicati o non sicuri nell'archivio")
    return files


def prefix_for(path: Path, files: dict[str, bytes]) -> str:
    if path.suffix == ".whl":
        return "plenora_database/"
    if path.name.endswith(".tar.gz"):
        roots = {name.split("/")[0] for name in files}
        if len(roots) != 1:
            raise ValueError("il sorgente deve avere una sola radice")
        return next(iter(roots)) + "/"
    return ""


def record_bytes(files: dict[str, bytes], record: str) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    for name, data in sorted(files.items()):
        if name != record:
            sha = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
            writer.writerow([name, "sha256=" + sha, len(data)])
    writer.writerow([record, "", ""])
    return output.getvalue().encode()


def wheel_record(files: dict[str, bytes]) -> str:
    records = [n for n in files if n.endswith(".dist-info/RECORD")]
    if len(records) != 1:
        raise ValueError("wheel senza RECORD univoco")
    return records[0]


def verify(path: Path, profile: str) -> None:
    files = read_archive(path)
    prefix = prefix_for(path, files)
    expected = {prefix + n: data for n, data in notices.bundle(profile).items()}
    for name, data in expected.items():
        if name not in files or notices.digest(files[name]) != notices.digest(data):
            raise ValueError(f"{path.name}: avviso assente o diverso: {name}")
    actual = {n for n in files if n.startswith(prefix + "LICENSES/crate-rust/")}
    if actual != {n for n in expected if n.startswith(prefix + "LICENSES/crate-rust/")}:
        raise ValueError("avvisi extra o mancanti nell'archivio")
    if path.suffix == ".whl":
        record = wheel_record(files)
        rows = list(csv.reader(io.StringIO(files[record].decode())))
        wanted = list(csv.reader(io.StringIO(record_bytes(files, record).decode())))
        if sorted(rows) != sorted(wanted):
            raise ValueError("RECORD wheel incompleto o impronte diverse")


def package(path: Path, profile: str, binary: Path | None = None) -> None:
    files = {binary.name: binary.read_bytes()} if binary else read_archive(path)
    prefix = prefix_for(path, files)
    files.update({prefix + n: data for n, data in notices.bundle(profile).items()})
    if path.suffix == ".whl":
        record = wheel_record(files)
        files[record] = record_bytes(files, record)
    temporary = path.with_name(path.name + ".tmp")
    try:
        if path.name.endswith(".tar.gz"):
            # Conserva modi e link del git archive, aggiungendo solo gli avvisi.
            with tarfile.open(path, "r:gz") as source, tarfile.open(temporary, "w:gz") as output:
                added = {prefix + n for n in notices.bundle(profile)}
                for member in source.getmembers():
                    if member.name not in added:
                        output.addfile(member, source.extractfile(member) if member.isfile() else None)
                for name in sorted(added):
                    member = tarfile.TarInfo(name)
                    member.size = len(files[name])
                    member.mode = 0o644
                    output.addfile(member, io.BytesIO(files[name]))
        else:
            with zipfile.ZipFile(temporary, "w", zipfile.ZIP_DEFLATED) as output:
                for name, data in sorted(files.items()):
                    info = zipfile.ZipInfo(name)
                    info.compress_type = zipfile.ZIP_DEFLATED
                    info.create_system = 3
                    info.external_attr = (0o100755 if binary and name == binary.name else 0o100644) << 16
                    output.writestr(info, data)
        temporary.replace(path)
        verify(path, profile)
    finally:
        temporary.unlink(missing_ok=True)


def main(check_only: bool = False) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args()
    try:
        for path in args.artifacts:
            if not check_only:
                package(path, args.profile, args.binary)
            verify(path, args.profile)
            print(f"OK avvisi: {path.name} ({args.profile})")
        return 0
    except (OSError, ValueError, KeyError, tarfile.TarError, zipfile.BadZipFile) as error:
        print(f"licenze artefatto: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
