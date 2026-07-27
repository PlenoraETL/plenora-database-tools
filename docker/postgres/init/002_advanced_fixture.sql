\set ON_ERROR_STOP on

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = 'plenora_fixture' AND t.typname = 'event_status'
    ) THEN
        CREATE TYPE plenora_fixture.event_status AS ENUM ('new', 'active', 'closed');
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = 'plenora_fixture' AND t.typname = 'positive_integer'
    ) THEN
        CREATE DOMAIN plenora_fixture.positive_integer AS integer CHECK (VALUE > 0);
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = 'plenora_fixture' AND t.typname = 'profile_value'
    ) THEN
        CREATE TYPE plenora_fixture.profile_value AS (
            label text,
            priority integer,
            enabled boolean
        );
    END IF;
END
$$;

DROP TABLE IF EXISTS plenora_fixture.advanced_types;

CREATE TABLE plenora_fixture.advanced_types (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    status plenora_fixture.event_status NOT NULL DEFAULT 'new',
    domain_value plenora_fixture.positive_integer NOT NULL,
    rounded_amount numeric NOT NULL,
    integer_values integer[] NOT NULL,
    text_values text[],
    integer_window int4range,
    timestamp_window tstzrange,
    duration interval,
    local_time time,
    zoned_time timetz,
    host inet,
    network cidr,
    external_id uuid NOT NULL,
    profile plenora_fixture.profile_value,
    doubled integer GENERATED ALWAYS AS (domain_value * 2) STORED
);

DO $$
BEGIN
    IF current_setting('server_version_num')::integer >= 150000 THEN
        ALTER TABLE plenora_fixture.advanced_types
            ALTER COLUMN rounded_amount TYPE numeric(6,-2);
    ELSE
        ALTER TABLE plenora_fixture.advanced_types
            ALTER COLUMN rounded_amount TYPE numeric(8,0);
    END IF;
END
$$;

INSERT INTO plenora_fixture.advanced_types (
    status,
    domain_value,
    rounded_amount,
    integer_values,
    text_values,
    integer_window,
    timestamp_window,
    duration,
    local_time,
    zoned_time,
    host,
    network,
    external_id,
    profile
)
VALUES (
    'active',
    7,
    12300,
    ARRAY[1, 2, 3],
    ARRAY['alpha', 'beta'],
    '[1,10)'::int4range,
    '[2025-01-01 00:00:00+00,2025-02-01 00:00:00+00)'::tstzrange,
    interval '2 days 03:04:05',
    time '12:34:56.123456',
    timetz '12:34:56+02',
    inet '192.0.2.10/24',
    cidr '192.0.2.0/24',
    uuid '123e4567-e89b-12d3-a456-426614174000',
    ROW('primary', 9, true)::plenora_fixture.profile_value
);

DROP TABLE IF EXISTS plenora_fixture.spatial_dimensions;

CREATE TABLE plenora_fixture.spatial_dimensions (
    id bigint PRIMARY KEY,
    point_xy geometry(Point, 4326) NOT NULL,
    point_z geometry(PointZ, 4326) NOT NULL,
    point_m geometry(PointM, 4326) NOT NULL,
    point_zm geometry(PointZM, 4326) NOT NULL,
    collection geometry(GeometryCollection, 4326) NOT NULL,
    curve geometry(CircularString, 4326) NOT NULL,
    tin geometry(TINZ, 4326) NOT NULL,
    geog geography(Point, 4326) NOT NULL
);

INSERT INTO plenora_fixture.spatial_dimensions
VALUES (
    1,
    ST_GeomFromEWKT('SRID=4326;POINT (9 45)'),
    ST_GeomFromEWKT('SRID=4326;POINT Z (9 45 100)'),
    ST_GeomFromEWKT('SRID=4326;POINT M (9 45 7)'),
    ST_GeomFromEWKT('SRID=4326;POINT ZM (9 45 100 7)'),
    ST_GeomFromEWKT(
        'SRID=4326;GEOMETRYCOLLECTION(POINT(9 45),LINESTRING(9 45,10 46))'
    ),
    ST_GeomFromEWKT('SRID=4326;CIRCULARSTRING(9 45,10 46,11 45)'),
    ST_GeomFromEWKT(
        'SRID=4326;TIN Z (((9 45 0,10 45 0,9 46 0,9 45 0)))'
    ),
    ST_GeomFromEWKT('SRID=4326;POINT (9 45)')::geography
);

DROP TABLE IF EXISTS plenora_fixture.secure_events CASCADE;

CREATE TABLE plenora_fixture.secure_events (
    id bigint GENERATED ALWAYS AS IDENTITY,
    tenant_id integer NOT NULL,
    payload text,
    payload_size integer GENERATED ALWAYS AS (length(payload)) STORED,
    geom geometry(Point, 4326),
    CONSTRAINT secure_events_pk PRIMARY KEY (id, tenant_id),
    CONSTRAINT secure_events_payload_check CHECK (payload_size <= 4096)
) PARTITION BY RANGE (tenant_id);

CREATE TABLE plenora_fixture.secure_events_tenant_0
    PARTITION OF plenora_fixture.secure_events
    FOR VALUES FROM (0) TO (100);

CREATE TABLE plenora_fixture.secure_events_tenant_100
    PARTITION OF plenora_fixture.secure_events
    FOR VALUES FROM (100) TO (200);

CREATE INDEX secure_events_geom_gix
    ON plenora_fixture.secure_events USING gist (geom);

CREATE INDEX secure_events_payload_partial_idx
    ON plenora_fixture.secure_events (payload_size)
    INCLUDE (tenant_id)
    WHERE payload IS NOT NULL;

ALTER TABLE plenora_fixture.secure_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE plenora_fixture.secure_events FORCE ROW LEVEL SECURITY;

CREATE POLICY secure_events_tenant_policy
    ON plenora_fixture.secure_events
    AS RESTRICTIVE
    FOR ALL
    TO PUBLIC
    USING (tenant_id = current_setting('plenora.tenant_id', true)::integer)
    WITH CHECK (tenant_id = current_setting('plenora.tenant_id', true)::integer);

DROP MATERIALIZED VIEW IF EXISTS plenora_fixture.event_region_summary;

CREATE MATERIALIZED VIEW plenora_fixture.event_region_summary AS
SELECT region_id, count(*) AS event_count
FROM plenora_fixture.events
GROUP BY region_id
WITH DATA;

CREATE UNIQUE INDEX event_region_summary_region_idx
    ON plenora_fixture.event_region_summary (region_id);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'plenora_reader') THEN
        CREATE ROLE plenora_reader NOLOGIN;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA plenora_fixture TO plenora_reader;
GRANT SELECT ON plenora_fixture.secure_events TO plenora_reader;
