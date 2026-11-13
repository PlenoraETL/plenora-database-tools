"""DB-b: every cell goes through actual public transports; live is never inferred."""
import itertools
import json
import warnings

import plenora_database as p

from ._harness import connect_postgres, postgres_dsn_or_skip
from ._postgres_surfaces import (
    SURFACES, POLICIES, CONSENTS, REFUSED, WRITERS, READERS, Harness,
    fixtures, cells, table, operation, read_operation, normalized,
    assert_original, loss_sources, expected_subject, retained_table,
)


def test_postgres_geometry_surface_enumeration():
    source = fixtures()
    assert set(WRITERS) == set(READERS) == set(SURFACES)
    assert len(source["properties"]) == len(set(source["properties"])) == 15
    identities = {(f["name"], f["variant"]) for f in source["fixtures"]}
    assert len(identities) == len(source["fixtures"]) == 28
    assert len({name for name, _ in identities}) == 14
    assert len(cells(source)) == 15 * 14 * 2 * 5 * 4 * 2
    # Negative mutation: dropping any adapter must leave missing cells.
    for surface in SURFACES:
        assert cells(source) - {c for c in cells(source) if c[2] != surface}


def test_postgres_geometry_offline_preflight_parity(tmp_path):
    harness = Harness(tmp_path, "host=unused.invalid dbname=offline")
    for fixture in fixtures()["fixtures"]:
        original = table(fixture)
        for policy, accept in itertools.product(POLICIES, CONSENTS):
            results = [harness.invoke(s, "preflight", operation("offline", policy), original, accept)
                       for s in ("rust", "cli")]
            assert results[0]["ok"] == results[1]["ok"]
            if results[0]["ok"]:
                assert fixture["name"] not in REFUSED
                assert results[0]["document"]["execution_loss"] == results[1]["document"]["execution_loss"]
            else:
                assert normalized(results[0]) == normalized(results[1])
                assert results[0]["document"]["category"] == REFUSED[fixture["name"]]
                assert results[0]["document"]["phase"] == ("prepare" if fixture["name"] == "uint64_ordering" else "validate")
                assert results[0]["document"]["remote_effect"] == "none"
    for scope, empty, original in loss_sources():
        for policy, accept in itertools.product(POLICIES, CONSENTS):
            results = [harness.invoke(s, "preflight", operation("offline", policy), original, accept)
                       for s in ("rust", "cli")]
            assert results[0]["ok"] == results[1]["ok"] == (policy != "strict" and accept)
            if results[0]["ok"]:
                report = results[0]["document"]["execution_loss"]
                assert report == results[1]["document"]["execution_loss"]
                assert report["risk"][0]["subject"] == expected_subject(scope)
                assert report["evaluation"]["entries"][0]["would_change"] == 1
                assert report["effective"] == [], "dry run cannot confirm a loss"
            else:
                assert normalized(results[0]) == normalized(results[1]), (scope, empty, policy, accept)


def test_live_postgres_geometry_surface_matrix(tmp_path):
    dsn = postgres_dsn_or_skip()
    source = fixtures()
    harness = Harness(tmp_path, dsn)
    session = connect_postgres()
    observed, failures, records = set(), [], []
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", p.MappingLossWarning)
            for index, fixture in enumerate(source["fixtures"]):
                original = table(fixture)
                for policy, accept in itertools.product(POLICIES, CONSENTS):
                    reference = None
                    read_reference = None
                    for surface in SURFACES:
                        name = f"db_b_{index}_{policy}_{int(accept)}_{surface}"
                        session.execute_ddl(f'DROP TABLE IF EXISTS public."{name}"')
                        try:
                            result = WRITERS[surface](harness, operation(name, policy), original, accept)
                            actual = normalized(result)
                            if fixture["name"] in REFUSED:
                                assert not actual["ok"]
                                assert actual["category"] == REFUSED[fixture["name"]]
                                assert actual["phase"] == ("prepare" if fixture["name"] == "uint64_ordering" else "validate")
                                assert actual["remote_effect"] == "none"
                            else:
                                assert actual["ok"], "nominal fixture unexpectedly refused"
                            if reference is None:
                                reference = actual
                            assert actual == reference, "write reports or refusal axes differ"
                            if actual["ok"]:
                                first = None
                                # Cross-read every writer with every public reader, twice.
                                for reader in SURFACES:
                                    for _ in range(2):
                                        report, restored = READERS[reader](harness, read_operation(name))
                                        assert_original(restored, original)
                                        assert report["status"] == "complete"
                                        assert report["entries"] == []
                                        if read_reference is None:
                                            read_reference = report
                                        assert report == read_reference
                                        if first is None:
                                            first = report
                                        assert report == first, "read_loss differs"
                            else:
                                assert session.execute_scalar(f"SELECT to_regclass('public.{name}') IS NULL")
                            for prop in source["properties"]:
                                observed.add(((fixture["name"], fixture["variant"]), prop, surface, policy, accept))
                            records.append([fixture["name"], fixture["variant"], surface, policy, accept,
                                            "preserved" if actual["ok"] else "refused"])
                        except (AssertionError, KeyError, TypeError, ValueError):
                            failures.append([fixture["name"], fixture["variant"], surface, policy, accept])
                        finally:
                            session.execute_ddl(f'DROP TABLE IF EXISTS public."{name}"')
    finally:
        session.close()
        evidence = {
            "expected": len(cells(source)), "asserted": len(observed), "failed": failures,
            "missing": sorted(cells(source) - observed), "live": True,
            "properties": source["properties"], "records": records}
        (tmp_path / "db-b-matrix.json").write_text(json.dumps(evidence), encoding="utf-8")
        print("PLENORA_DB_B_MATRIX " + json.dumps(evidence))
    assert not failures, f"matrix failed cells (no payload): {failures}"
    assert observed == cells(source), "missing surface/property/fixture/policy/consent cell"


def test_live_postgres_metadata_loss_surface_matrix(tmp_path):
    harness = Harness(tmp_path, postgres_dsn_or_skip())
    session = connect_postgres()
    records = []
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", p.MappingLossWarning)
            for scope, empty, original in loss_sources():
                for policy, accept in itertools.product(POLICIES, CONSENTS):
                    reference = None
                    read_reference = None
                    for surface in SURFACES:
                        name = "db_b_metadata"
                        session.execute_ddl(f"DROP TABLE IF EXISTS public.{name}")
                        result = WRITERS[surface](harness, operation(name, policy), original, accept)
                        actual = normalized(result)
                        allowed = policy != "strict" and accept
                        assert actual["ok"] == allowed
                        if reference is None:
                            reference = actual
                        assert actual == reference, (scope, empty, policy, accept, surface)
                        if allowed:
                            loss = actual["loss_report"]
                            assert loss["schema_version"] == 4
                            assert len(loss["risk"]) == len(loss["effective"]) == 1
                            subject = expected_subject(scope)
                            assert loss["risk"][0]["rule_id"] == "pg.metadata.key_not_preserved"
                            assert loss["risk"][0]["subject"] == subject
                            assert loss["evaluation"]["complete"] is True
                            assert loss["evaluation"]["entries"] == [{
                                "field_id": 0, "rule_id": "pg.metadata.key_not_preserved", "basis": "exact",
                                "checked": 1, "would_change": 1, "subject": 0}]
                            assert loss["evaluation"]["entries"][0]["would_change"] == 1
                            assert loss["effective"][0]["committed"] == 1
                            assert loss["effective"][0]["uncertain"] == 0
                            assert actual["rows"]["confirmed"] == (0 if empty else 1)
                            reports = []
                            for reader in SURFACES:
                                report, restored = READERS[reader](harness, read_operation(name))
                                assert report["schema_version"] == 2 and report["status"] == "complete"
                                assert len(report["entries"]) == 1
                                assert report["entries"][0] == {"position": 0,
                                    "field_id": subject["field_identity"], "property": "native_metadata",
                                    "rule": "pg.metadata.key_not_preserved", "subject": subject}
                                assert_original(restored, retained_table(original, scope))
                                if read_reference is None:
                                    read_reference = report
                                assert report == read_reference
                                reports.append(report)
                            assert all(r == reports[0] for r in reports)
                        else:
                            assert actual["remote_effect"] == "none"
                            assert session.execute_scalar(f"SELECT to_regclass('public.{name}') IS NULL")
                        records.append([scope, empty, surface, policy, accept, "reported" if allowed else "refused"])
                        session.execute_ddl(f"DROP TABLE IF EXISTS public.{name}")
    finally:
        session.execute_ddl("DROP TABLE IF EXISTS public.db_b_metadata")
        session.close()
        print("PLENORA_DB_B_LOSS_MATRIX " + json.dumps({"records": records}))
    expected = set(itertools.product(("field", "schema", "aggregate"), (False, True), SURFACES, POLICIES, CONSENTS))
    assert {tuple(row[:5]) for row in records} == expected
    assert len(records) == len(expected)
