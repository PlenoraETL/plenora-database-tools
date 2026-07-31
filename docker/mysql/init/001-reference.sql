USE dataflow_test;

CREATE TABLE catalog_probe (
    id BIGINT NOT NULL,
    name VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    amount DECIMAL(18, 4) NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    event_date DATE NULL,
    event_ts DATETIME(6) NULL,
    payload JSON NULL,
    geom GEOMETRY NOT NULL SRID 4326,
    PRIMARY KEY (id),
    SPATIAL INDEX catalog_probe_geom_sidx (geom)
) ENGINE=InnoDB;

INSERT INTO catalog_probe (
    id, name, amount, active, event_date, event_ts, payload, geom
) VALUES (
    1,
    'reference',
    1234.5000,
    TRUE,
    DATE '2026-01-02',
    TIMESTAMP '2026-01-02 03:04:05.123456',
    JSON_OBJECT('qualified', TRUE),
    ST_GeomFromText('POINT(9 45)', 4326, 'axis-order=long-lat')
);

CREATE VIEW catalog_probe_view AS
SELECT id, name, active FROM catalog_probe;
