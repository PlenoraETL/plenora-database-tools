#!/usr/bin/env python3
"""Importa soltanto differenze complete fixture/misura dai log CI delle sonde.

La sequenza delle major viene dai riepiloghi dello stesso log. I messaggi
diagnostici fra percorsi PostgreSQL non sono differenze rispetto al fixture.
Non modifica mai i profili qualificati di ammissione del prodotto.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLASSES = {
    "VuotoANull": "vuoto_a_null", "RiempieTesto": "riempie_testo",
    "RiempieBinario": "riempie_binario", "ToglieFinali": "toglie_finali",
    "SpaziOltre": "spazi_oltre", "ZeroNegativo": "zero_negativo",
    "NonAscii": "non_ascii", "JsonNumeri": "json_numeri", "Ogni": "ogni",
    "Underflow": "underflow", "FrazioneSecondi": "frazione_secondi",
}


def measured_key(line: str) -> dict:
    text = line.split("MeasuredKey ", 1)[1]
    text = re.sub(r'([{,] )(\w+):', r'\1"\2":', text)
    text = re.sub(r'Some\((\d+)\)', r'\1', text).replace('None', 'null')
    text = re.sub(r'\b(Esatto|Riscrive|Rifiuta|Illeggibile)\b',
                  lambda m: json.dumps(m[0].lower()), text)
    text = re.sub(r'(\w+) \{ ("(?:cifre|min_abs)": [^}]+) \}',
                  r'{"classe": "\1", \2}', text)
    names = '|'.join(CLASSES)
    text = re.sub(r'(?<=[\[, ])(' + names + r')(?=[,\]])',
                  r'{"classe": "\1"}', text)
    result = json.loads(text)
    if result['dimensione'] is None:
        del result['dimensione']
    for rewrite in result['riscritture']:
        rewrite['classe'] = CLASSES[rewrite['classe']]
        if 'min_abs' in rewrite:
            rewrite['min_abs'] = str(rewrite['min_abs'])
    return result


def identity(key: dict) -> tuple:
    return key['arrow'], key['nativo'], key.get('dimensione')


def rationale(provider: str, key: dict) -> str:
    """Motivo della classe, senza inferire nuovi valori per la fixture."""
    if key['arrow'].startswith('float'):
        return ('Bind binario del driver come in produzione, senza rifiuto anticipato: '
                'segno dello zero, underflow e non finiti sono osservati dal server. '
                f"Il round-trip {key['nativo']} su {provider} non prova le comparazioni SQL: resta il rifiuto del codec.")
    if key['arrow'].startswith(('timestamp', 'time')):
        return ('Precisione del target: la rimozione della guardia applicativa lascia '
                'osservare arrotondamento/troncamento del percorso server.')
    if 'jsonb' in key['nativo']:
        return ('Senza canonicalizzazione applicativa si osservano anche i membri '
                'duplicati; la classe misurata ogni e conservata dal log.')
    if provider == 'sqlserver':
        return ('Conversione del percorso writer senza guardia P: lunghezza, code page '
                'e ANSI_PADDING sono quelli del catalogo, non del DDL richiesto. '
                'Illeggibile include il troncamento di una coppia UTF-16; non e un rifiuto del server.')
    return ('Spazi oltre la lunghezza e padding osservati senza guardie P/S. '
            'Si conserva la classe del log, inclusa ogni quando il classificatore '
            'non dispone della dimensione necessaria; nessuna nuova ammissione.')


def log_changes(text: str, provider: str) -> list[dict]:
    summaries = re.findall(r'Z ' + provider + r' (.+?): major ([\d.]+) \(', text)
    blocks = text.split('la misura del server (non l\'ammissione della libreria) non coincide con il fixture:')[1:]
    if not summaries or len(blocks) != len(summaries):
        raise ValueError('log incompleto: ogni confronto deve avere il riepilogo della sua immagine')
    changes = []
    for block, (image, major) in zip(blocks, summaries, strict=True):
        lines = block.splitlines()
        for index, line in enumerate(lines):
            if re.search(r'Z diversa: ', line):
                before, after = measured_key(lines[index + 1]), measured_key(lines[index + 2])
                if identity(before) != identity(after):
                    raise ValueError('identita diversa fra fixture e misura')
                changes.append(dict(major=major, image=image, decision='a',
                                    reason=rationale(provider, after), before=before, after=after))
    return changes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--provider', required=True, choices=['db2', 'oracle', 'postgres', 'mysql', 'mariadb', 'sqlserver'])
    parser.add_argument('--log', required=True, type=Path)
    parser.add_argument('--evidence', required=True, type=Path)
    args = parser.parse_args()
    raw = args.log.read_bytes()
    changes = log_changes(raw.decode('utf-8-sig'), args.provider)
    path = ROOT / 'tests/fixtures' / f'scrittura_server_{args.provider}.json'
    document = json.loads(path.read_text(encoding='utf-8'))
    index = {(m['server_major'], identity(k)): k for m in document['misure'] for k in m['chiavi']}
    for change in changes:
        current = index[change['major'], identity(change['before'])]
        if current not in (change['before'], change['after']):
            raise ValueError('il fixture non coincide con la fonte del log')
        current.clear()
        current.update(change['after'])
    path.write_text(json.dumps(document, indent=1, ensure_ascii=False) + '\n', encoding='utf-8')
    args.evidence.write_text(json.dumps(dict(provider=args.provider, log=args.log.name,
        sha256=hashlib.sha256(raw).hexdigest(), changes=changes), indent=1) + '\n', encoding='utf-8')
    print(f'{args.provider}: {len(changes)} differenze importate dal log')


if __name__ == '__main__':
    main()
