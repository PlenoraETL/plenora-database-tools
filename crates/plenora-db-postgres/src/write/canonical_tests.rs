use super::{
    canonical, classify, fraction_digits, json_duplicate_keys, reduced_temporal, LeafClass,
    SessionLeaf,
};

#[test]
fn le_date_canoniche_sono_iso() {
    for text in ["2026-01-02", "12026-01-02", "0001-01-01 BC"] {
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
        // F3 della terza lettura: i valori speciali si rifiutano ovunque.
        "infinity",
        "-infinity",
        "epoch",
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
    ] {
        assert!(canonical(SessionLeaf::TimestampTz, text), "{text}");
    }
    // S3: senza offset l'istante dipende dal `TimeZone` della sessione.
    for text in [
        "2026-01-02 10:00:00",
        "2026-01-02",
        "2026-01-02 10:00:00 Europe/Rome",
        "now",
        "infinity",
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

/// F3: `'now'` verso `time` si scriveva come l'ora del `TimeZone` della
/// sessione; `time` e ora una foglia temporale, e i valori speciali non
/// sono canonici.
#[test]
fn le_ore_sono_canoniche_senza_valori_speciali() {
    for text in ["10:00", "10:00:00", "23:59:59.999999", "24:00:00"] {
        assert!(canonical(SessionLeaf::Time, text), "{text}");
    }
    for text in [
        "now",
        "allballs",
        "10:00:00+02",
        "10:00 PM",
        "10.00",
        "",
        "infinity",
    ] {
        assert!(!canonical(SessionLeaf::Time, text), "{text}");
    }
    assert!(!canonical(SessionLeaf::Interval, "infinity"));
    assert!(!canonical(SessionLeaf::Interval, "@ ago"));
}

/// La regola J: le chiavi ripetute di un oggetto JSON, a ogni profondita e
/// dopo gli escape; i numeri non si leggono.
#[test]
fn le_chiavi_ripetute_si_trovano_a_ogni_profondita() {
    for (text, expected) in [
        (r#"{"b":1,"a":2,"a":3}"#, Some(true)),
        (r#"[{"x": {"k": 1, "k": 2}}]"#, Some(true)),
        (r#"{"a\u0062": 1, "ab": 2}"#, Some(true)),
        (r#"{"\ud83d\ude00": 1, "😀": 2}"#, Some(true)),
        (r#"{"a": {"a": 1}, "b": [{"a": 1}, {"a": 2}]}"#, Some(false)),
        (r#"{"a": "a", "b": "a"}"#, Some(false)),
        (r#"{"n": 1e400, "m": 12345678901234567890123}"#, Some(false)),
        (r#"{"a": 1, "x": {"a": 1}, "b": 2}"#, Some(false)),
        (r#"[1, "a", "a", {}]"#, Some(false)),
        (r#"{"\ud800": 1}"#, None),
        (r#"{"\q": 1}"#, None),
    ] {
        assert_eq!(json_duplicate_keys(text), expected, "{text}");
    }
    assert_eq!(classify(Some("jsonb_in"), -1), LeafClass::Jsonb);
}

#[test]
fn la_classe_viene_dalla_funzione_d_ingresso() {
    assert_eq!(
        classify(Some("textin"), -1),
        LeafClass::Text { max_chars: None }
    );
    // `citext` legge con `textin`; `varchar(3)` ha typmod 7.
    assert_eq!(
        classify(Some("varcharin"), 7),
        LeafClass::Text { max_chars: Some(3) }
    );
    assert_eq!(
        classify(Some("bpcharin"), -1),
        LeafClass::Text { max_chars: None }
    );
    // Mondo chiuso: una funzione in C, `xml_in`, `charin`, `regclassin`.
    for symbol in [
        None,
        Some("xml_in"),
        Some("charin"),
        Some("namein"),
        Some("regclassin"),
    ] {
        assert_eq!(classify(symbol, -1), LeafClass::Unknown, "{symbol:?}");
    }
    assert_eq!(
        classify(Some("time_in"), 0),
        LeafClass::Session {
            leaf: SessionLeaf::Time,
            fraction: 0,
            reduced: true
        }
    );
    assert_eq!(
        classify(Some("timestamp_in"), -1),
        LeafClass::Session {
            leaf: SessionLeaf::Timestamp,
            fraction: 6,
            reduced: false
        }
    );
    assert_eq!(
        classify(Some("timestamptz_in"), 6),
        LeafClass::Session {
            leaf: SessionLeaf::TimestampTz,
            fraction: 6,
            reduced: false
        }
    );
    // `interval` senza typmod; `interval day` (campi); `interval(3)`.
    assert!(matches!(
        classify(Some("interval_in"), -1),
        LeafClass::Session { reduced: false, .. }
    ));
    let day = (1 << 3) << 16 | 0xFFFF;
    assert!(matches!(
        classify(Some("interval_in"), day),
        LeafClass::Session { reduced: true, .. }
    ));
    let three = 0x7FFF << 16 | 3;
    assert_eq!(
        classify(Some("interval_in"), three),
        LeafClass::Session {
            leaf: SessionLeaf::Interval,
            fraction: 3,
            reduced: true
        }
    );
    assert!(matches!(
        classify(Some("cash_in"), -1),
        LeafClass::Session {
            leaf: SessionLeaf::Money,
            ..
        }
    ));
}

#[test]
fn le_cifre_frazionarie_si_contano() {
    assert_eq!(fraction_digits("10:00:00"), 0);
    assert_eq!(fraction_digits("10:00:00.999999"), 6);
    assert_eq!(fraction_digits("2026-01-02T10:00:00.1234567Z"), 7);
    assert_eq!(fraction_digits("1.5 days 00:00:00.12"), 2);
    assert_eq!(fraction_digits("PT0.1234567S"), 7);
}

#[test]
fn le_colonne_temporali_ridotte_si_riconoscono_dalla_dichiarazione() {
    for declaration in [
        "timestamp(0) without time zone",
        "timestamp(3) with time zone",
        "time(0) without time zone",
        "time(5) with time zone[]",
        "interval day",
        "interval year to month",
        "interval(0)",
        "interval day to second(3)",
    ] {
        assert!(reduced_temporal(declaration), "{declaration}");
    }
    for declaration in [
        "timestamp without time zone",
        "timestamp(6) with time zone",
        "time without time zone",
        "interval",
        "interval(6)",
        "date",
        "text",
        "timestamptz",
    ] {
        assert!(!reduced_temporal(declaration), "{declaration}");
    }
    // `interval(6)` conserva il microsecondo.
    assert!(matches!(
        classify(Some("interval_in"), 0x7FFF << 16 | 6),
        LeafClass::Session { reduced: false, .. }
    ));
}
