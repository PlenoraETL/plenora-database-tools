use super::qualify;

#[test]
fn qualifica_i_nomi_di_postgis_e_nient_altro() {
    let sql = "SELECT postgis_typmod_srid(a.atttypmod), ST_GeomFromEWKB($1)::geography \
               FROM spatial_ref_sys s, x.spatial_ref_sys, my_spatial_ref_sys";
    assert_eq!(
        qualify(sql, "\"gis\"."),
        "SELECT \"gis\".postgis_typmod_srid(a.atttypmod), \
         \"gis\".ST_GeomFromEWKB($1)::\"gis\".geography \
         FROM \"gis\".spatial_ref_sys s, x.spatial_ref_sys, my_spatial_ref_sys"
    );
    assert_eq!(qualify(sql, ""), sql);
    assert_eq!(qualify("$1::geometryx", "p."), "$1::geometryx");
}
