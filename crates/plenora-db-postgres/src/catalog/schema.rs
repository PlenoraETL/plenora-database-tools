use super::CatalogSchemaToken;
use crate::error::{classify_error, public_error};
use crate::types::ColumnSpec;
use plenora_database_core::plan::ObjectRef;
use plenora_database_core::{ErrorCategory, ErrorPhase, Result};
use std::sync::Arc;
use tokio_postgres::Client;

#[allow(clippy::too_many_lines)]
pub async fn load_columns_and_token(
    client: &Client,
    source: &ObjectRef,
) -> Result<(Arc<Vec<ColumnSpec>>, CatalogSchemaToken)> {
    let schema = source.schema.as_deref().unwrap_or("public");
    let rows = client
        .query(
            r"
            SELECT
                a.attname,
                t.typname,
                NOT a.attnotnull AS nullable,
                CASE
                  WHEN t.typname = 'numeric' AND a.atttypmod >= 0
                  THEN ((a.atttypmod - 4) >> 16) & 65535
                END AS numeric_precision,
                CASE
                  WHEN t.typname = 'numeric' AND a.atttypmod >= 0
                  THEN CASE
                    WHEN ((a.atttypmod - 4) & 2047) >= 1024
                    THEN ((a.atttypmod - 4) & 2047) - 2048
                    ELSE (a.atttypmod - 4) & 2047
                  END
                END AS numeric_scale,
                CASE
                  WHEN t.typname IN ('geometry', 'geography')
                       AND a.atttypmod >= 0
                  THEN postgis_typmod_srid(a.atttypmod)
                END AS spatial_srid,
                CASE
                  WHEN t.typname IN ('geometry', 'geography')
                       AND a.atttypmod >= 0
                  THEN postgis_typmod_dims(a.atttypmod)
                END AS spatial_dims,
                CASE
                  WHEN t.typname IN ('geometry', 'geography')
                       AND a.atttypmod >= 0
                  THEN postgis_typmod_type(a.atttypmod)
                END AS spatial_type,
                srs.auth_name AS spatial_crs_auth_name,
                srs.auth_srid AS spatial_crs_auth_srid,
                pg_get_expr(ad.adbin, ad.adrelid) AS default_expression,
                NULLIF(a.attidentity, '')::text AS identity_kind,
                NULLIF(a.attgenerated, '')::text AS generated_kind,
                format_type(a.atttypid, a.atttypmod) AS native_declaration,
                t.typtype::text AS type_kind,
                CASE WHEN t.typtype = 'c' THEN (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'name', ca.attname,
                            'declaration', format_type(ca.atttypid, ca.atttypmod)
                        )
                        ORDER BY ca.attnum
                    )::text
                    FROM pg_catalog.pg_attribute ca
                    WHERE ca.attrelid = t.typrelid
                      AND ca.attnum > 0
                      AND NOT ca.attisdropped
                ) END AS composite_fields,
                d.oid::bigint AS database_oid,
                n.oid::bigint AS namespace_oid,
                c.oid::bigint AS relation_oid,
                signature.structural_signature,
                CASE WHEN t.typtype = 'e' THEN (
                    SELECT jsonb_agg(e.enumlabel ORDER BY e.enumsortorder)::text
                    FROM pg_catalog.pg_enum e
                    WHERE e.enumtypid = t.oid
                ) END AS enum_labels,
                CASE WHEN t.typtype = 'd'
                     THEN format_type(t.typbasetype, t.typtypmod)
                END AS domain_base_type,
                CASE WHEN t.typtype = 'd' THEN (
                    SELECT jsonb_agg(
                        pg_get_constraintdef(dc.oid, true)
                        ORDER BY dc.conname
                    )::text
                    FROM pg_catalog.pg_constraint dc
                    WHERE dc.contypid = t.oid
                ) END AS domain_constraints,
                CASE WHEN a.attcollation <> t.typcollation
                     THEN coll.collname
                END AS collation
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_database d ON d.datname = current_database()
            JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
            LEFT JOIN pg_catalog.pg_attrdef ad
              ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
            LEFT JOIN pg_catalog.pg_collation coll ON coll.oid = a.attcollation
            LEFT JOIN spatial_ref_sys srs
              ON t.typname IN ('geometry', 'geography')
             AND a.atttypmod >= 0
             AND srs.srid = postgis_typmod_srid(a.atttypmod)
            CROSS JOIN LATERAL (
                SELECT jsonb_build_object(
                    'relation',
                    jsonb_build_array(c.relname, c.relkind, c.xmin::text),
                    'columns',
                    COALESCE((
                        SELECT jsonb_agg(
                            jsonb_build_array(
                                sa.attnum,
                                sa.attname,
                                sa.atttypid::text,
                                sa.atttypmod,
                                sa.attnotnull,
                                sa.attidentity,
                                sa.attgenerated,
                                sa.attcollation::text,
                                sa.xmin::text,
                                st.typname,
                                st.typtype,
                                st.xmin::text,
                                pg_get_expr(sad.adbin, sad.adrelid),
                                CASE WHEN st.typtype = 'e' THEN (
                                    SELECT jsonb_agg(
                                        jsonb_build_array(
                                            se.enumlabel,
                                            se.enumsortorder,
                                            se.xmin::text
                                        )
                                        ORDER BY se.enumsortorder
                                    )
                                    FROM pg_catalog.pg_enum se
                                    WHERE se.enumtypid = st.oid
                                ) END,
                                CASE WHEN st.typtype = 'd' THEN (
                                    SELECT jsonb_agg(
                                        jsonb_build_array(
                                            sdc.conname,
                                            sdc.xmin::text,
                                            pg_get_constraintdef(sdc.oid, true)
                                        )
                                        ORDER BY sdc.conname
                                    )
                                    FROM pg_catalog.pg_constraint sdc
                                    WHERE sdc.contypid = st.oid
                                ) END,
                                CASE WHEN st.typtype = 'c' THEN (
                                    SELECT jsonb_agg(
                                        jsonb_build_array(
                                            sca.attnum,
                                            sca.attname,
                                            sca.atttypid::text,
                                            sca.atttypmod,
                                            sca.xmin::text,
                                            format_type(
                                                sca.atttypid,
                                                sca.atttypmod
                                            )
                                        )
                                        ORDER BY sca.attnum
                                    )
                                    FROM pg_catalog.pg_attribute sca
                                    WHERE sca.attrelid = st.typrelid
                                      AND sca.attnum > 0
                                      AND NOT sca.attisdropped
                                ) END,
                                CASE
                                  WHEN st.typname IN ('geometry', 'geography')
                                       AND sa.atttypmod >= 0
                                  THEN (
                                    SELECT jsonb_build_array(
                                        ssrs.auth_name,
                                        ssrs.auth_srid,
                                        ssrs.xmin::text
                                    )
                                    FROM spatial_ref_sys ssrs
                                    WHERE ssrs.srid =
                                        postgis_typmod_srid(sa.atttypmod)
                                  )
                                END
                            )
                            ORDER BY sa.attnum
                        )
                        FROM pg_catalog.pg_attribute sa
                        JOIN pg_catalog.pg_type st ON st.oid = sa.atttypid
                        LEFT JOIN pg_catalog.pg_attrdef sad
                          ON sad.adrelid = sa.attrelid
                         AND sad.adnum = sa.attnum
                        WHERE sa.attrelid = c.oid
                          AND sa.attnum > 0
                          AND NOT sa.attisdropped
                    ), '[]'::jsonb)
                )::text AS structural_signature
            ) signature
            WHERE n.nspname = $1
              AND c.relname = $2
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            ",
            &[&schema, &source.object],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?;
    if rows.is_empty() {
        return Err(public_error(
            ErrorCategory::NotFound,
            ErrorPhase::Probe,
            false,
            "oggetto PostgreSQL non trovato",
        ));
    }
    let token = CatalogSchemaToken::from_catalog_row(&rows[0])?;
    let columns = Arc::new(
        rows.iter()
            .map(ColumnSpec::from_catalog_row)
            .collect::<Result<Vec<_>>>()?,
    );
    Ok((columns, token))
}

#[allow(clippy::too_many_lines)]
pub async fn schema_token(client: &Client, source: &ObjectRef) -> Result<CatalogSchemaToken> {
    let schema = source.schema.as_deref().unwrap_or("public");
    let row = client
        .query_opt(
            r"
            SELECT
                d.oid::bigint AS database_oid,
                n.oid::bigint AS namespace_oid,
                c.oid::bigint AS relation_oid,
                jsonb_build_object(
                    'relation',
                    jsonb_build_array(c.relname, c.relkind, c.xmin::text),
                    'columns',
                    COALESCE((
                        SELECT jsonb_agg(
                            jsonb_build_array(
                                a.attnum,
                                a.attname,
                                a.atttypid::text,
                                a.atttypmod,
                                a.attnotnull,
                                a.attidentity,
                                a.attgenerated,
                                a.attcollation::text,
                                a.xmin::text,
                                t.typname,
                                t.typtype,
                                t.xmin::text,
                                pg_get_expr(ad.adbin, ad.adrelid),
                                CASE WHEN t.typtype = 'e' THEN (
                                    SELECT jsonb_agg(
                                        jsonb_build_array(
                                            e.enumlabel,
                                            e.enumsortorder,
                                            e.xmin::text
                                        )
                                        ORDER BY e.enumsortorder
                                    )
                                    FROM pg_catalog.pg_enum e
                                    WHERE e.enumtypid = t.oid
                                ) END,
                                CASE WHEN t.typtype = 'd' THEN (
                                    SELECT jsonb_agg(
                                        jsonb_build_array(
                                            dc.conname,
                                            dc.xmin::text,
                                            pg_get_constraintdef(dc.oid, true)
                                        )
                                        ORDER BY dc.conname
                                    )
                                    FROM pg_catalog.pg_constraint dc
                                    WHERE dc.contypid = t.oid
                                ) END,
                                CASE WHEN t.typtype = 'c' THEN (
                                    SELECT jsonb_agg(
                                        jsonb_build_array(
                                            ca.attnum,
                                            ca.attname,
                                            ca.atttypid::text,
                                            ca.atttypmod,
                                            ca.xmin::text,
                                            format_type(ca.atttypid, ca.atttypmod)
                                        )
                                        ORDER BY ca.attnum
                                    )
                                    FROM pg_catalog.pg_attribute ca
                                    WHERE ca.attrelid = t.typrelid
                                      AND ca.attnum > 0
                                      AND NOT ca.attisdropped
                                ) END,
                                CASE
                                  WHEN t.typname IN ('geometry', 'geography')
                                       AND a.atttypmod >= 0
                                  THEN (
                                    SELECT jsonb_build_array(
                                        srs.auth_name,
                                        srs.auth_srid,
                                        srs.xmin::text
                                    )
                                    FROM spatial_ref_sys srs
                                    WHERE srs.srid =
                                        postgis_typmod_srid(a.atttypmod)
                                  )
                                END
                            )
                            ORDER BY a.attnum
                        )
                        FROM pg_catalog.pg_attribute a
                        JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
                        LEFT JOIN pg_catalog.pg_attrdef ad
                          ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
                        WHERE a.attrelid = c.oid
                          AND a.attnum > 0
                          AND NOT a.attisdropped
                    ), '[]'::jsonb)
                )::text AS structural_signature
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_database d ON d.datname = current_database()
            WHERE n.nspname = $1 AND c.relname = $2
            ",
            &[&schema, &source.object],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Probe, &error))?
        .ok_or_else(|| {
            public_error(
                ErrorCategory::NotFound,
                ErrorPhase::Probe,
                false,
                "oggetto PostgreSQL non trovato",
            )
        })?;
    CatalogSchemaToken::from_catalog_row(&row)
}
