"""Regressioni della guardia temporale, senza Cargo o server."""
import unittest
from scripts.check_temporal import PATTERN


class TemporalGuard(unittest.TestCase):
    def test_infallible_and_unwrap_are_detected(self):
        for source in ["Duration::days(remote)", "x.timestamp_nanos()",
                       "NaiveDate::from_ymd(y, m, d)",
                       "DateTime::from_timestamp_micros(raw).unwrap()",
                       "Duration::from_secs_f64(remote)"]:
            self.assertIsNotNone(PATTERN.search(source), source)

    def test_checked_result_is_allowed(self):
        self.assertIsNone(PATTERN.search("DateTime::from_timestamp_micros(raw).ok_or_else(invalid)?"))


if __name__ == "__main__":
    unittest.main()
