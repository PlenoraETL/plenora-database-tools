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
