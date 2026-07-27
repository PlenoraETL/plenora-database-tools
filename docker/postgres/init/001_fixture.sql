\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS plenora_fixture;

CREATE TABLE plenora_fixture.events (
    event_id bigint PRIMARY KEY,
    region_id integer NOT NULL,
    name text NOT NULL,
    amount numeric(38, 18),
    active boolean NOT NULL,
    occurred_at timestamptz NOT NULL,
    local_date date NOT NULL,
    payload jsonb,
    raw_bytes bytea,
    geom geometry(PointZ, 4326),
    geog geography(Point, 4326)
);

INSERT INTO plenora_fixture.events (
    event_id,
    region_id,
    name,
    amount,
    active,
    occurred_at,
    local_date,
    payload,
    raw_bytes,
    geom,
    geog
)
SELECT
    value,
    (value % 20)::integer,
    'evento-' || value::text,
    (value::numeric / 1000000)::numeric(38, 18),
    value % 2 = 0,
    timestamptz '2025-01-01 00:00:00+00'
        + value * interval '1 second',
    date '2025-01-01' + (value % 365)::integer,
    jsonb_build_object('id', value, 'kind', 'fixture'),
    decode(lpad(to_hex(value), 16, '0'), 'hex'),
    ST_SetSRID(
        ST_MakePoint(
            9.0 + (value % 100)::double precision / 1000,
            45.0 + (value % 100)::double precision / 1000,
            (value % 500)::double precision
        ),
        4326
    ),
    ST_SetSRID(
        ST_MakePoint(
            9.0 + (value % 100)::double precision / 1000,
            45.0 + (value % 100)::double precision / 1000
        ),
        4326
    )::geography
FROM generate_series(1, 10000) AS series(value);

CREATE INDEX events_geom_gix
    ON plenora_fixture.events
    USING gist (geom);

CREATE INDEX events_geog_gix
    ON plenora_fixture.events
    USING gist (geog);

CREATE TABLE plenora_fixture."Quoted Table" (
    "select" integer NOT NULL,
    "spaced column" text,
    "a""b" text
);

INSERT INTO plenora_fixture."Quoted Table"
VALUES (1, 'caffè 🗺️', 'quoted');
