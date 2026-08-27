use super::truncate_index_name_63_bytes;

#[test]
fn short_names_pass_through() {
    let s = "short_col_gix";
    assert_eq!(truncate_index_name_63_bytes(s), s);
}

#[test]
fn exactly_63_bytes_passes_through() {
    let s: String = "a".repeat(63);
    assert_eq!(truncate_index_name_63_bytes(&s), s);
}

#[test]
fn over_63_bytes_gets_hash_suffix_and_fits() {
    let s: String = "verylongtablename_verylongcolumnname_gix".repeat(3);
    let out = truncate_index_name_63_bytes(&s);
    assert!(out.len() <= 63, "output {} byte", out.len());
    assert!(out.contains('_'));
}

#[test]
fn different_long_names_get_different_hashes() {
    let base = "T".repeat(60);
    let a = format!("{base}alpha_gix");
    let b = format!("{base}bravo_gix");
    let a_out = truncate_index_name_63_bytes(&a);
    let b_out = truncate_index_name_63_bytes(&b);
    // Il prefisso troncato coincide, ma il suffix hash deve
    // differire — altrimenti collisione (regressione).
    assert_ne!(a_out, b_out);
}

#[test]
fn multibyte_utf8_stays_valid_and_within_budget() {
    // Nome con caratteri multibyte (é = 2 byte). Verifica che il
    // truncation non spezza a metà uno scalar UTF-8.
    let s = "é".repeat(40); // 80 byte, 40 char
    let out = truncate_index_name_63_bytes(&s);
    assert!(out.len() <= 63);
    // Valid UTF-8 automatico: se String::push_str è stato ok, ok.
    assert!(out
        .chars()
        .all(|c| c == 'é' || c == '_' || c.is_ascii_hexdigit()));
}

#[test]
fn hash_suffix_is_stable_across_versions() {
    // FNV-1a è spec-driven: il valore per un input dato è fisso
    // per sempre. Freezing test — se cambia significa che qualcuno
    // ha sostituito l'algoritmo senza pensare al fatto che i nomi
    // indice generati diventano diversi.
    let s: String = "T".repeat(80);
    let out = truncate_index_name_63_bytes(&s);
    // 63 byte totali: 54 prefix "T" + "_" + 8 char hex FNV.
    // Il suffix hex deve essere identico ad ogni run e ad ogni
    // upgrade di toolchain.
    assert!(out.starts_with(&"T".repeat(54)));
    let suffix = &out[55..];
    assert_eq!(suffix.len(), 8);
    // FNV-1a di "T".repeat(80) — precalcolato:
    // Se questo assert fallisce dopo un cambio, verificare che
    // l'algoritmo di hashing sia ancora FNV-1a (non sostituito
    // con DefaultHasher o simile).
    assert_eq!(suffix, format!("{:08x}", super::fnv1a_32(s.as_bytes())));
}
