#!/usr/bin/env python3
"""P1: inventario derivato, senza promuovere prove pianificate a qualifica."""
import argparse
import itertools
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
DIRECTORY = ROOT / "tests/fixtures/mysql_permissive"


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def signature(arrow, native, rewrite):
    return arrow, native, json.dumps(rewrite, sort_keys=True)


def validate(fixture, declarations):
    """Uguaglianza dei domini, non sola inclusione dei selector correnti."""
    if declarations["gate"] != "closed":
        raise ValueError("P1 gate must remain closed")
    majors = {m["server_major"] for m in fixture["misure"]}
    declared, ids, aliases, predicates = {}, set(), set(), set()
    for rule in declarations["rules"]:
        if rule["id"] in ids or rule["provider"] != "mysql":
            raise ValueError("duplicate rule or wrong provider")
        ids.add(rule["id"])
        if not rule.get("predicate") or not rule.get("targets"):
            raise ValueError("missing predicate or emitter")
        predicate = declarations["predicates"].get(rule["predicate"])
        if not predicate or not predicate.get("expression"):
            raise ValueError("unknown predicate")
        predicates.add(rule["predicate"])
        if rule["basis"] != predicate["basis"] or rule["action"] != predicate["action"]:
            raise ValueError("invalid basis")
        if rule["qualification"] != "not_measured":
            raise ValueError("planned proof is not qualification")
        if set(rule["proofs"]) != majors:
            raise ValueError("missing or orphan major proof")
        for proof in rule["proofs"].values():
            if proof.get("baseline") not in declarations["baseline_tests"] or not proof.get("planned"):
                raise ValueError("missing proof")
        for alias in rule["aliases"]:
            if alias in aliases or alias in ids:
                raise ValueError("duplicate alias")
            aliases.add(alias)
        for target in rule["targets"]:
            key = signature(rule["arrow"], target["native"], target["rewrite"])
            if key in declared:
                raise ValueError("ambiguous selector")
            declared[key] = rule["id"]
    if aliases & ids:
        raise ValueError("alias shadows rule")
    if predicates != set(declarations["predicates"]):
        raise ValueError("orphan predicate")
    for measurement in fixture["misure"]:
        measured = [signature(k["arrow"], k["nativo"], r)
                    for k in measurement["chiavi"] for r in k["riscritture"]]
        if set(measured) != set(declared) or len(measured) != len(declared):
            raise ValueError("fixture/declarations differ")
    return declared


def generate(fixture, declarations):
    selectors = validate(fixture, declarations)
    measurements = []
    for measurement in fixture["misure"]:
        pairs = []
        for key in measurement["chiavi"]:
            pairs.append({**key, "rules": [selectors[signature(key["arrow"], key["nativo"], r)]
                                          for r in key["riscritture"]]})
        measurements.append({**measurement, "chiavi": pairs})
    matrix = []
    for major, mode, profile, policy, consent, partial in itertools.product(
            [m["server_major"] for m in fixture["misure"]], declarations["modes"],
            declarations["profiles"], declarations["policies"], declarations["consents"], [False, True]):
        baseline = (mode != "truncate_insert" and profile == "single_transaction"
                    and policy == "strict" and not partial)
        matrix.append(dict(major=major, mode=mode, profile=profile, policy=policy,
                           consent=consent, allow_partial=partial,
                           baseline="operation_admitted" if baseline else "refused"))
    path_modes = {"update_accumulated": {"update"}, "append_diagnostics": {"append"},
                  "upsert_insert": {"upsert"}, "upsert_update": {"upsert"}, "upsert_noop": {"upsert"}}
    planned_paths = [dict(operation=index, path=path, status="not_executed" if (
        operation["mode"] != "truncate_insert" and operation["profile"] == "single_transaction"
        and not operation["allow_partial"] and operation["mode"] in path_modes.get(path, set(declarations["modes"]))
    ) else "negative_not_applicable") for index, operation in enumerate(matrix) for path in declarations["paths"]]
    contexts = declarations["selector_contexts"]
    selector_matrix = [dict(planner=planner, role=role, policy=policy)
                       for planner, role, policy in itertools.product(contexts["planner"], contexts["role"], contexts["policy"])]
    return dict(schema_version=1, generated_by="scripts/mysql_permissive_inventory.py",
                gate="closed", declarations=declarations, measurements=measurements,
                operation_matrix=matrix, planned_paths=planned_paths,
                selector_matrix=selector_matrix, live_status="not_executed",
                planned_matrix_axes=dict(paths=declarations["paths"], cases=declarations["planned_cases"],
                                         surfaces=["rust", "cli", "sdk_sync", "sdk_async", "runtime"]))


def verify_execution(declarations, output):
    """Un nome presente o filtrato non e una prova eseguita."""
    passed = set(re.findall(r"^test (\S+) \.\.\. ok$", output, re.MULTILINE))
    required = set(declarations["baseline_tests"])
    if not required <= passed:
        raise ValueError("baseline tests not executed successfully: " + ", ".join(sorted(required - passed)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--execution-log", type=Path)
    args = parser.parse_args()
    result = generate(read(ROOT / "tests/fixtures/ammissione_scrittura/mysql.json"),
                      read(DIRECTORY / "declarations.json"))
    # Stessa discovery usata dai runner MySQL; ignored/filtered non qualificano.
    from scripts.mysql_inventory import collect
    if not set(result["declarations"]["baseline_tests"]) <= collect()["unit"]:
        raise ValueError("baseline proof outside the unit runner")
    if args.execution_log:
        verify_execution(result["declarations"], args.execution_log.read_text(encoding="utf-8-sig"))
    path = DIRECTORY / "inventory.json"
    if args.write:
        path.write_text(json.dumps(result, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    elif read(path) != result:
        raise SystemExit("inventory stale: run scripts/mysql_permissive_inventory.py --write")
    print("MySQL P1 inventory: fixture/declarations bidirectional; P1 baseline immutable")


if __name__ == "__main__":
    main()
