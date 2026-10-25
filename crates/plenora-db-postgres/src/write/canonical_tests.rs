use super::{canonical, SessionLeaf};

#[test]
fn le_date_canoniche_sono_iso() {
    for text in [
        "2026-01-02",
        "12026-01-02",
        "0001-01-01 BC",
        "infinity",
        "-infinity",
    ] {
        assert!(canonical(SessionLeaf::Date, text), "{text}");
    }
    // S3 della rilettura del 25/10: `01/02/2026` dipende da `DateStyle`.
    for text in [
        "01/02/2026",
        "2026-1-2",
        "02.01.2026",
        "January 2, 2026",
        "today",
        "2026-01-02x",
    ] {
        assert!(!canonical(SessionLeaf::Date, text), "{text}");
    }
}

#[test]
fn i_timestamp_con_zona_la_portano_esplicita() {
    for text in [
        "2026-01-02 10:00:00Z",
        "2026-01-02T10:00:00+00:00",
        "2026-01-02 10:00:00.123456 +05:30",
        "2026-01-02T10:00+01",
        "2026-01-02 10:00:00-0800",
        "infinity",
    ] {
        assert!(canonical(SessionLeaf::TimestampTz, text), "{text}");
    }
    // S3: senza offset l'istante dipende dal `TimeZone` della sessione.
    for text in [
        "2026-01-02 10:00:00",
        "2026-01-02",
        "2026-01-02 10:00:00 Europe/Rome",
        "now",
        "01/02/2026 10:00:00+00",
    ] {
        assert!(!canonical(SessionLeaf::TimestampTz, text), "{text}");
    }
}

#[test]
fn i_timestamp_senza_zona_non_ne_portano() {
    assert!(canonical(SessionLeaf::Timestamp, "2026-01-02 10:00:00"));
    assert!(canonical(SessionLeaf::Timestamp, "2026-01-02T10:00:00.5"));
    assert!(!canonical(SessionLeaf::Timestamp, "2026-01-02 10:00:00+02"));
    assert!(!canonical(SessionLeaf::Timestamp, "01/02/2026 10:00"));
}

#[test]
fn timetz_con_zona() {
    assert!(canonical(SessionLeaf::TimeTz, "10:00:00+02"));
    assert!(!canonical(SessionLeaf::TimeTz, "10:00:00"));
}

#[test]
fn gli_intervalli_non_dipendono_da_interval_style() {
    for text in [
        "P1Y2M3DT4H5M6S",
        "P-1M2D",
        "PT0.5S",
        "1 day 02:00:00",
        "-1 mons +2 days +03:00:00",
        "-1 day",
        "1 year -2 months",
        "@ 1 day ago",
    ] {
        assert!(canonical(SessionLeaf::Interval, text), "{text}");
    }
    // Con `sql_standard` il segno del primo campo vale per i campi senza segno.
    for text in [
        "-1 mons 2 days 03:00:00",
        "-1 day 02:00:00",
        "- 1 day 2 hours",
        "-1 day,2 hours",
        "-1:00:00,2 days",
        "-1 day\u{b}2 hours",
        "P",
        "PT",
        "P1H",
        "P1D2Y",
    ] {
        assert!(!canonical(SessionLeaf::Interval, text), "{text}");
    }
}

#[test]
fn money_non_e_mai_canonico() {
    assert!(!canonical(SessionLeaf::Money, "12.34"));
}
