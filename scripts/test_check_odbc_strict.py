"""Mutazioni che devono fallire prima di poter perdere un avviso ODBC."""
import unittest
from check_odbc_strict import inspect, ROOT, EXCEPTION_SOURCES


class StrictTests(unittest.TestCase):
    def test_immediate_boundary_and_bypasses(self):
        for path in ('vendor/odbc-api/src/handles/statement.rs',
                     'crates/plenora-db-db2/src/read.rs'):
            good = 'unsafe { SQLFetch(self.as_sys()) }.into_sql_result_strict(self, "SQLFetch")'
            self.assertFalse(inspect(path, good)[0])
            for bad in (
                good.replace('into_sql_result_strict(self, ', 'into_sql_result('),
                'match SQLFetch(handle) { _ => Ok(()) }',
                'let result = SQLPrepare(h, sql); next_call();',
                'result.into_result(&statement)',
                'result.into_result_without_logging(&statement)',
                'match result { SqlResult::SuccessWithInfo(_) => Ok(()), _ => fail() }',
                'use odbc_sys::SQLFetch as fetch; fetch(handle);',
            ):
                self.assertTrue(inspect(path, bad)[0], bad)

    def test_exception_sources_are_closed(self):
        for name in EXCEPTION_SOURCES:
            source = (ROOT / name).read_text(encoding="utf-8")
            self.assertFalse(inspect(name, source)[0])
            self.assertTrue(inspect(name, source + "\nSQLFreeHandle(t, h);")[0])

    def test_comments_do_not_create_exceptions(self):
        self.assertTrue(inspect('new.rs', 'SQLFetch(h); // .into_sql_result_strict(h, "fetch")')[0])
        self.assertFalse(inspect('new.rs', '// SQLFetch(h);\nlet text = "SuccessWithInfo";')[0])
        self.assertTrue(inspect('new.rs', 'SQLFreeHandle(t, h);')[0])


if __name__ == '__main__':
    unittest.main()
