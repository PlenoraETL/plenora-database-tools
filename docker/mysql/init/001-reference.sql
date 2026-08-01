USE dataflow_test;

CREATE TABLE catalog_probe (
    id BIGINT NOT NULL,
    name VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    amount DECIMAL(18, 4) NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    event_date DATE NULL,
    event_ts DATETIME(6) NULL,
    payload JSON NULL,
    payload_bin VARBINARY(16) NULL,
    geom GEOMETRY NOT NULL SRID 4326,
    geom_point POINT NOT NULL SRID 4326,
    geom_collection GEOMETRYCOLLECTION NOT NULL SRID 4326,
    PRIMARY KEY (id),
    SPATIAL INDEX catalog_probe_geom_sidx (geom)
) ENGINE=InnoDB;

INSERT INTO catalog_probe (
    id, name, amount, active, event_date, event_ts, payload, payload_bin, geom, geom_point,
    geom_collection
) VALUES (
    1,
    'reference',
    1234.5000,
    TRUE,
    DATE '2026-01-02',
    TIMESTAMP '2026-01-02 03:04:05.123456',
    JSON_OBJECT('qualified', TRUE),
    X'00112233AABBCCDD',
    ST_GeomFromText('POINT(9 45)', 4326, 'axis-order=long-lat'),
    ST_GeomFromText('POINT(9 45)', 4326, 'axis-order=long-lat'),
    ST_GeomFromText('GEOMETRYCOLLECTION(POINT(9 45))', 4326, 'axis-order=long-lat')
);

CREATE VIEW catalog_probe_view AS
SELECT id, name, active FROM catalog_probe;

CREATE TABLE stream_probe (
    id BIGINT NOT NULL PRIMARY KEY,
    payload VARBINARY(1024) NOT NULL
) ENGINE=InnoDB;

SET SESSION cte_max_recursion_depth = 4096;
INSERT INTO stream_probe (id, payload)
WITH RECURSIVE sequence (id) AS (
    SELECT 1
    UNION ALL
    SELECT id + 1 FROM sequence WHERE id < 2048
)
SELECT id, REPEAT(X'5A', 1024) FROM sequence;
