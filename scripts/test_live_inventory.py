#!/usr/bin/env python3
"""Unit test dell'inventario condiviso dei test live.

Questo modulo decide, per due gate, quali test esistono e quali devono essere
stati eseguiti. Un suo falso negativo toglie un test dall'inventario in
silenzio — e allora il gate resta verde su una copertura che non c'e piu; un
suo falso positivo pretende l'esecuzione di qualcosa che non e un test, e
allora il gate non e piu superabile. Entrambi sono gia successi, e sono la
ragione per cui queste forme sintattiche stanno scritte qui una per una.
"""

from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path

from scripts import live_inventory as inventory


class AnnotatedTests(unittest.TestCase):
    def test_a_helper_is_not_a_test(self) -> None:
        """`fn live_dsn()` esiste in due moduli del provider PostgreSQL."""

        source = "    fn live_dsn() -> String {\n        String::new()\n    }\n"
        self.assertEqual(inventory.annotated_tests(source), [])

    def test_a_commented_attribute_does_not_make_a_test(self) -> None:
        source = "    // #[tokio::test]\n    async fn live_finto() {}\n"
        self.assertEqual(inventory.annotated_tests(source), [])

    def test_a_block_commented_test_does_not_count(self) -> None:
        source = (
            "    /*\n"
            "    #[tokio::test]\n"
            "    async fn live_archiviato() {}\n"
            "    */\n"
        )
        self.assertEqual(inventory.annotated_tests(source), [])

    def test_comments_between_the_attribute_and_the_signature_keep_the_test(self) -> None:
        """I commenti tra attributo e firma non nascondono il test."""

        source = (
            "    #[tokio::test]\n"
            "    #[allow(clippy::too_many_lines)]\n"
            "    // Una sessione live condivide setup e credenziali.\n"
            "    async fn live_con_commento() {}\n"
        )
        self.assertEqual(inventory.annotated_tests(source), ["live_con_commento"])

    def test_a_blank_line_before_the_signature_keeps_the_test(self) -> None:
        source = "    #[tokio::test]\n\n    async fn live_riga_vuota() {}\n"
        self.assertEqual(inventory.annotated_tests(source), ["live_riga_vuota"])

    def test_a_block_comment_before_the_signature_keeps_the_test(self) -> None:
        source = (
            "    #[tokio::test]\n"
            "    /* preparazione condivisa */\n"
            "    async fn live_blocco() {}\n"
        )
        self.assertEqual(inventory.annotated_tests(source), ["live_blocco"])

    def test_restricted_visibility_keeps_the_test(self) -> None:
        source = "    #[test]\n    pub(crate) fn live_visibile() {}\n"
        self.assertEqual(inventory.annotated_tests(source), ["live_visibile"])

    def test_any_attribute_ending_in_test_counts(self) -> None:
        for attribute in ("#[test]", "#[tokio::test]", "#[async_std::test]"):
            source = f"    {attribute}\n    fn live_x() {{}}\n"
            self.assertEqual(inventory.annotated_tests(source), ["live_x"], attribute)

    def test_an_attribute_that_only_starts_with_test_does_not_count(self) -> None:
        source = "    #[test_case(1)]\n    fn live_parametrico() {}\n"
        self.assertEqual(inventory.annotated_tests(source), [])

    def test_a_test_written_inside_a_raw_string_is_not_collected(self) -> None:
        """Una fixture che contiene codice non e codice.

        I sorgenti del repository usano raw string per SQL e per snippet: se
        una di esse contenesse un attributo di test, il gate ne avrebbe preteso
        l'esecuzione per sempre, e nessuna corsa avrebbe potuto soddisfarlo.
        """

        source = (
            '    const FIXTURE: &str = r#"\n'
            "    #[tokio::test]\n"
            "    async fn live_dentro_una_stringa() {}\n"
            '    "#;\n'
            "    #[tokio::test]\n"
            "    async fn live_vero() {}\n"
        )
        self.assertEqual(inventory.annotated_tests(source), ["live_vero"])

    def test_a_test_written_inside_a_normal_string_is_not_collected(self) -> None:
        source = (
            '    const S: &str = "#[test] fn live_finto() {}";\n'
            "    #[test]\n"
            "    fn live_vero() {}\n"
        )
        self.assertEqual(inventory.annotated_tests(source), ["live_vero"])

    def test_a_lifetime_does_not_open_a_char_literal(self) -> None:
        """`&'input T` non e l'inizio di un letterale: se lo fosse, tutto cio
        che segue verrebbe cancellato e i test dopo sparirebbero."""

        source = (
            "    fn helper<'input>(value: &'input str) -> &'input str {\n"
            "        value\n"
            "    }\n"
            "\n"
            "    #[tokio::test]\n"
            "    async fn live_dopo_un_lifetime() {}\n"
        )
        self.assertEqual(inventory.annotated_tests(source), ["live_dopo_un_lifetime"])

    def test_a_char_literal_is_stripped(self) -> None:
        source = (
            "    fn helper() -> char {\n"
            "        '\\\\''\n"
            "    }\n"
            "    #[test]\n"
            "    fn live_dopo_un_char() {}\n"
        )
        self.assertIn("live_dopo_un_char", inventory.annotated_tests(source))

    def test_cfg_attr_declaring_a_test_counts(self) -> None:
        source = (
            '    #[cfg_attr(feature = "live", tokio::test)]\n'
            "    async fn live_condizionale() {}\n"
        )
        self.assertEqual(inventory.annotated_tests(source), ["live_condizionale"])

    def test_an_attribute_on_the_signature_line_counts(self) -> None:
        """`#[test] fn x() {}` e Rust valido, e va inventariato.

        Pretendere che l'attributo stesse su una riga a se lo toglieva
        dall'inventario in silenzio.
        """

        self.assertEqual(
            inventory.annotated_tests("    #[test] fn live_stessa_riga() {}\n"),
            ["live_stessa_riga"],
        )

    def test_test_must_be_the_last_segment_of_the_path(self) -> None:
        """`#[foo::test::case]` non dichiara un test.

        `test` deve essere l'ultimo segmento del path; una semplice occorrenza
        nel mezzo non dichiara un test eseguibile.
        """

        source = "    #[foo::test::case]\n    fn live_finto() {}\n"
        self.assertEqual(inventory.annotated_tests(source), [])

    def test_a_parametrised_test_attribute_counts(self) -> None:
        source = (
            '    #[tokio::test(flavor = "multi_thread")]\n'
            "    async fn live_parametrizzato() {}\n"
        )
        self.assertEqual(inventory.annotated_tests(source), ["live_parametrizzato"])

    def test_cfg_attr_with_test_as_the_predicate_does_not_count(self) -> None:
        """`#[cfg_attr(test, derive(Debug))]` non dichiara un test.

        Quel `test` e la condizione di compilazione, non l'attributo emesso.
        """

        source = (
            "    #[cfg_attr(test, derive(Debug))]\n"
            "    fn live_non_e_un_test() {}\n"
        )
        self.assertEqual(inventory.annotated_tests(source), [])


    def test_two_attributes_on_one_line_keep_the_test(self) -> None:
        """`#[test] #[allow(dead_code)]` e la firma sulla riga dopo.

        Il blocco ammetteva un attributo per riga, e questa forma spariva.
        """

        source = (
            "    #[test] #[allow(dead_code)]\n    fn live_due_attributi() {}\n"
        )
        self.assertEqual(inventory.annotated_tests(source), ["live_due_attributi"])

    def test_spaces_around_the_path_separator_keep_the_test(self) -> None:
        """`#[tokio :: test]` e Rust valido."""

        source = "    #[tokio :: test]\n    async fn live_spazi() {}\n"
        self.assertEqual(inventory.annotated_tests(source), ["live_spazi"])

    def test_two_consecutive_cfg_attr_do_not_swallow_each_other(self) -> None:
        """La lettura greedy inglobava il secondo e perdeva entrambi.

        `.*` in modalita `DOTALL` arriva all'ultima parentesi del blocco: gli
        argomenti del primo `cfg_attr` finivano per contenere anche il secondo,
        e il test spariva dall'inventario.
        """

        source = (
            "    #[cfg_attr(all(), tokio::test)]\n"
            "    #[cfg_attr(all(), allow(dead_code))]\n"
            "    async fn live_due() {}\n"
        )
        self.assertEqual(inventory.annotated_tests(source), ["live_due"])

    def test_an_always_false_predicate_does_not_emit_a_test(self) -> None:
        """`any()` senza argomenti e falso: cio che emette non si compila.

        Pretenderne l'esecuzione avrebbe reso il gate rosso per sempre.
        """

        source = "    #[cfg_attr(any(), test)]\n    fn live_mai() {}\n"
        self.assertEqual(inventory.annotated_tests(source), [])

    def test_a_space_before_the_cfg_attr_parenthesis_is_allowed(self) -> None:
        source = "    #[cfg_attr (all(), test)]\n    fn live_spazio() {}\n"
        self.assertEqual(inventory.annotated_tests(source), ["live_spazio"])

    def test_a_raw_identifier_keeps_the_test(self) -> None:
        """`r#` e un prefisso, non parte del nome: `fn r#type()` si chiama `type`."""

        source = "    #[test]\n    fn r#live_raw() {}\n"
        self.assertEqual(inventory.annotated_tests(source), ["live_raw"])

    def test_a_multiline_attribute_keeps_the_test(self) -> None:
        source = "    #[tokio ::\n     test]\n    async fn live_multilinea() {}\n"
        self.assertEqual(inventory.annotated_tests(source), ["live_multilinea"])

    def test_a_large_source_is_scanned_without_regex_backtracking(self) -> None:
        helpers = "\n\n".join(f"fn helper_{index}() {{}}" for index in range(5_000))
        source = f"{helpers}\n\n#[test]\nfn live_in_fondo() {{}}\n"
        self.assertEqual(inventory.annotated_tests(source), ["live_in_fondo"])

    def test_a_composed_constant_predicate_is_evaluated(self) -> None:
        """`not(all())` e `all(any())` sono falsi quanto `any()`.

        Riconoscere i soli letterali lasciava fuori forme valide e altrettanto
        costanti, e il gate pretendeva l'esecuzione di codice non compilato.
        """

        for predicate in ("any()", "not(all())", "all(any())", "false"):
            with self.subTest(predicate):
                source = f"    #[cfg_attr({predicate}, test)]\n    fn live_mai() {{}}\n"
                self.assertEqual(inventory.annotated_tests(source), [])

        for predicate in ("all()", "true", 'feature = "x"', "not(any())"):
            with self.subTest(predicate):
                source = f"    #[cfg_attr({predicate}, test)]\n    fn live_si() {{}}\n"
                self.assertEqual(inventory.annotated_tests(source), ["live_si"])

    def test_a_unicode_identifier_is_seen_by_source_listing_and_run(self) -> None:
        """Rust ammette identificatori Unicode, e le tre espressioni li
        scartavano insieme: il confronto restava verde senza aver presidiato
        nulla."""

        name = "live_per" + chr(0xF2)
        self.assertEqual(
            inventory.annotated_tests(f"    #[test]\n    fn {name}() {{}}\n"),
            [name],
        )
        self.assertEqual(
            inventory.LISTED.findall(f"m::tests::{name}: test"), [f"m::tests::{name}"]
        )
        self.assertEqual(
            inventory.EXECUTED.findall(f"test m::tests::{name} ... ok"),
            [f"m::tests::{name}"],
        )

    def test_cfg_attr_also_requires_test_as_the_last_segment(self) -> None:
        """Il controllo dell'ultimo segmento valeva fuori da `cfg_attr` e non dentro.

        `#[cfg_attr(any(), foo::test::case)]` ne faceva un test, e nessuna
        corsa avrebbe potuto eseguirlo.
        """

        source = "    #[cfg_attr(any(), foo::test::case)]\n    fn live_falso() {}\n"
        self.assertEqual(inventory.annotated_tests(source), [])


class MysqlScanner(unittest.TestCase):
    """Lo scanner MySQL riconosce le stesse forme di quello condiviso."""

    def attribute(self, line: str) -> bool:
        from scripts import mysql_inventory

        return bool(mysql_inventory.TEST_ATTRIBUTE.match(line))

    def test_a_parametrised_attribute_counts(self) -> None:
        self.assertTrue(self.attribute('    #[tokio::test(flavor = "multi_thread")]'))
        self.assertTrue(self.attribute('    #[tokio::test (flavor = "multi_thread")]'))

    def test_spaces_around_the_path_separator_count(self) -> None:
        self.assertTrue(self.attribute("    #[tokio :: test]"))

    def test_test_must_be_the_last_segment(self) -> None:
        self.assertFalse(self.attribute("    #[foo::test::case]"))
        self.assertFalse(self.attribute("    #[test_case(1)]"))

    def test_a_nested_module_is_detected_whatever_its_visibility(self) -> None:
        """`pub mod inner {` e un `mod` come gli altri.

        La guardia riconosceva solo la forma nuda, quindi un test in
        `pub mod inner` riceveva un percorso con un segmento in meno — un nome
        che cargo non stampera mai.
        """

        from scripts import mysql_inventory

        for line, expected in (
            ("    mod inner {", "inner"),
            ("    pub mod inner {", "inner"),
            ("    pub(crate) mod inner {", "inner"),
            ("    mod r#type {", "type"),
            # Cio che non e una dichiarazione di modulo non deve diventarlo.
            ("    fn x() {", None),
            ("    pub mod inner;", None),
        ):
            with self.subTest(line.strip()):
                self.assertEqual(mysql_inventory.nested_module_name(line), expected)

    def test_a_raw_identifier_is_named_without_its_prefix(self) -> None:
        """La correzione degli identificatori raw valeva solo nell'inventario
        condiviso: qui `fn r#type()` non veniva riconosciuto affatto."""

        from scripts import mysql_inventory

        match = mysql_inventory.FUNCTION.match("    fn r#type() {")
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "type")

    def test_external_file_keeps_the_declared_rust_module_path(self) -> None:
        from scripts import mysql_inventory

        with tempfile.TemporaryDirectory() as directory:
            source_dir = Path(directory)
            library = source_dir / "lib.rs"
            checks = source_dir / "query_checks.rs"
            library.write_text(
                '#[cfg(test)]\n#[path = "query_checks.rs"]\nmod checks;\n',
                encoding="utf-8",
            )
            checks.write_text("#[test]\nfn accepts_external_layout() {}\n", encoding="utf-8")

            modules = mysql_inventory.external_test_modules(source_dir)
            scanned = mysql_inventory._scan(checks, modules[checks])

            self.assertEqual(modules, {checks: "checks"})
            self.assertEqual(
                [test.path for test in scanned],
                ["checks::accepts_external_layout"],
            )

    def test_unit_test_in_an_undeclared_file_is_rejected(self) -> None:
        from scripts import mysql_inventory

        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "orphan.rs"
            source.write_text("#[test]\nfn orphan() {}\n", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "non appartiene"):
                mysql_inventory._scan(source)


class StripNonCode(unittest.TestCase):
    def test_the_length_is_preserved(self) -> None:
        """`ignore_reasons` rilegge gli attributi dal sorgente originale usando
        le posizioni trovate su quello ripulito: se lo svuotamento cambiasse la
        lunghezza, leggerebbe il testo sbagliato."""

        source = (
            "    // commento\n"
            "    /* blocco\n       su piu righe */\n"
            '    const S: &str = r#"testo con "virgolette""#;\n'
            "    fn helper<'a>(x: &'a str) {}\n"
        )
        stripped = inventory.strip_noncode(source)
        self.assertEqual(len(stripped), len(source))
        self.assertEqual(stripped.count(chr(10)), source.count(chr(10)))
        self.assertNotIn("commento", stripped)
        self.assertNotIn("blocco", stripped)
        self.assertIn("fn helper", stripped)


class SourceInventory(unittest.TestCase):
    def test_homonymous_tests_are_an_error(self) -> None:
        """Due nomi uguali collasserebbero in uno, e uno solo li proverebbe."""

        definition = "    #[tokio::test]\n    async fn live_omonimo() {}\n"
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for module in ("primo.rs", "secondo.rs"):
                path = Path(directory) / module
                path.write_text(definition, encoding="utf-8")
                paths.append(path)
            with self.assertRaisesRegex(RuntimeError, "stesso nome"):
                inventory.source_inventory(paths)

    def test_an_empty_inventory_fails_closed(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "non puo essere vuoto"):
            inventory.source_inventory([], keep=lambda name: True)


class Listing(unittest.TestCase):
    def test_the_listing_keeps_the_full_name(self) -> None:
        listing = "test_suite::tests::live_uno: test\naltro::tests::live_due: test"
        self.assertEqual(
            inventory.listed_tests(listing),
            {"test_suite::tests::live_uno", "altro::tests::live_due"},
        )

    def test_an_empty_listing_fails_closed(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "non ha elencato"):
            inventory.listed_tests("", keep=lambda name: True)

    def test_only_passing_tests_are_executed(self) -> None:
        output = (
            "test m::tests::live_uno ... ok\n"
            "test m::tests::live_due ... FAILED\n"
            "test m::tests::live_tre ... ignored"
        )
        self.assertEqual(inventory.executed_tests(output), {"m::tests::live_uno"})


class IgnoreReasons(unittest.TestCase):
    def test_the_reason_comes_from_the_code(self) -> None:
        source = (
            "    #[tokio::test]\n"
            '    #[ignore = "richiede istanza PolyBase"]\n'
            "    async fn live_polybase() {}\n"
            "    #[tokio::test]\n"
            "    async fn live_senza_motivo() {}\n"
        )
        self.assertEqual(
            inventory.ignore_reasons(source), {"live_polybase": "richiede istanza PolyBase"}
        )


class RealSources(unittest.TestCase):
    """Le due suite reali, cosi che una regressione del parser si veda qui."""

    ROOT = Path(__file__).resolve().parents[1]

    def test_the_postgres_inventory_excludes_the_helpers(self) -> None:
        names = inventory.source_inventory(
            list((self.ROOT / "crates" / "plenora-db-postgres" / "src").rglob("*.rs")),
            keep=lambda name: name.startswith("live_"),
        )
        self.assertNotIn("live_dsn", names)
        self.assertIn("live_postgis_read_when_dsn_is_available", names)
        self.assertGreater(len(names), 100)

    def test_the_sqlserver_inventory_covers_the_live_module(self) -> None:
        names = inventory.source_inventory(
            [self.ROOT / "crates" / "plenora-db-sqlserver" / "src" / "live_tests.rs"]
        )
        self.assertIn(
            "live_provider_row_diagnostics_matches_confirmed_rollback_oracle", names
        )
        self.assertGreater(len(names), 40)


class CitedTests(unittest.TestCase):
    """Una prova nominata in un commento deve esistere.

    Le capability di questo repository non si spiegano da sole: accanto a
    `mixed_geometry_types: ...` c'e un commento che dice **quale** prova live
    la attraversa, ed e cosi che un lettore distingue una misura da una
    deduzione. Il commento e percio parte del contratto, e come il contratto
    puo mentire.

    La guardia verifica che ogni nome citato esista. Non può dimostrare
    staticamente che il corpo del test sostenga davvero la capability: quella
    resta responsabilità della prova e della review.

    # Perche i nomi si raccolgono da tutto il repository

    Le prove live non stanno in un solo file, quindi la scansione copre tutto
    il repository invece di assumere un nome di modulo.
    """

    #: Quello che sembra un nome e non lo e. `live_tests` e un modulo; una
    #: citazione con `*` e una famiglia di prove, e verificarla vorrebbe un
    #: glob — che si puo fare, ma che qui direbbe meno di quanto costa.
    NOT_A_NAME = frozenset({"live_tests"})

    def test_every_live_test_named_in_a_comment_exists(self) -> None:
        root = Path(__file__).resolve().parents[1]
        sources = sorted((root / "crates").rglob("*.rs"))
        self.assertGreater(len(sources), 50, "sorgenti Rust non trovate")

        # `source_inventory` rifiuta due prove con lo stesso nome in moduli
        # diversi, ed e giusto che lo faccia: confronta un inventario con
        # un'esecuzione, e un nome per due test renderebbe il confronto
        # ambiguo. Qui la domanda e piu piccola — «questo nome esiste?» — e i
        # crate MySQL e MariaDB hanno legittimamente prove omonime.
        known = {
            inventory.leaf(name)
            for source in sources
            for name in inventory.annotated_tests(
                source.read_text(encoding="utf-8")
            )
        }
        self.assertGreater(len(known), 100, "inventario delle prove live vuoto")

        cited: dict[str, list[str]] = {}
        for source in sources:
            for number, line in enumerate(
                source.read_text(encoding="utf-8").splitlines(), 1
            ):
                _, marker, comment = line.partition("//")
                if not marker:
                    continue
                for name in re.findall(r"\b(live_\w+)", comment):
                    # Il glob si riconosce dal carattere che **segue** il nome,
                    # non dal nome: `live_query_stream_*_is_reusable` arriva qui
                    # troncato a `live_query_stream_`.
                    tail = comment.split(name, 1)[1][:1]
                    # Un file che nomina **se stesso** non sta citando una
                    # prova: `tests/live_f4.rs` parla di `live_f4` in testa,
                    # ed e il proprio nome, non un identificatore.
                    if (
                        tail == "*"
                        or name == source.stem
                        or name in self.NOT_A_NAME
                        or name in known
                    ):
                        continue
                    cited.setdefault(name, []).append(
                        f"{source.relative_to(root)}:{number}"
                    )

        self.assertEqual(
            cited,
            {},
            "commenti che nominano prove live inesistenti: "
            + "; ".join(f"{name} ({', '.join(where)})" for name, where in cited.items()),
        )


if __name__ == "__main__":
    unittest.main()
