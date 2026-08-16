USE dataflow_test;

-- Il gate QueryOperation osserva COUNT_EXECUTE senza esporre hook nel
-- provider di produzione. Il grant resta confinato all'account della fixture.
GRANT SELECT ON performance_schema.prepared_statements_instances TO 'dataflow'@'%';
GRANT SELECT ON performance_schema.events_statements_current TO 'dataflow'@'%';
GRANT SELECT ON performance_schema.threads TO 'dataflow'@'%';

-- Il provider apre ogni sessione con time_zone = '+00:00'. La fixture usa lo
-- stesso fuso, cosi il valore TIMESTAMP letto non dipende dal fuso di sistema
-- del container che esegue questo script di bootstrap.
SET SESSION time_zone = '+00:00';

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
    -- Una colonna per ogni famiglia di tipo wire che il path query accetta nei
    -- metadati di COM_STMT_PREPARE: senza una colonna reale il mapping
    -- resterebbe dimostrato solo dai test offline.
    tiny_signed TINYINT NOT NULL,
    tiny_unsigned TINYINT UNSIGNED NOT NULL,
    small_signed SMALLINT NOT NULL,
    small_unsigned SMALLINT UNSIGNED NOT NULL,
    year_value YEAR NOT NULL,
    medium_signed MEDIUMINT NOT NULL,
    medium_unsigned MEDIUMINT UNSIGNED NOT NULL,
    int_signed INT NOT NULL,
    int_unsigned INT UNSIGNED NOT NULL,
    big_unsigned BIGINT UNSIGNED NOT NULL,
    float_value FLOAT NOT NULL,
    double_value DOUBLE NOT NULL,
    decimal_scale_zero DECIMAL(12, 0) NOT NULL,
    decimal_unsigned DECIMAL(9, 2) UNSIGNED NOT NULL,
    time_value TIME(6) NOT NULL,
    stamp_value TIMESTAMP(6) NOT NULL,
    bit_value BIT(16) NOT NULL,
    enum_value ENUM('alpha', 'beta') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    set_value SET('read', 'write') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    char_value CHAR(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    binary_value BINARY(4) NOT NULL,
    tiny_text TINYTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    tiny_blob TINYBLOB NOT NULL,
    text_value TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    blob_value BLOB NOT NULL,
    medium_text MEDIUMTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    medium_blob MEDIUMBLOB NOT NULL,
    long_text LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    long_blob LONGBLOB NOT NULL,
    -- Nullable e davvero nulla: la nullability pubblicata viene dai metadati
    -- del prepare, non dal valore osservato nella riga.
    absent_note VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
    PRIMARY KEY (id),
    SPATIAL INDEX catalog_probe_geom_sidx (geom)
) ENGINE=InnoDB;

INSERT INTO catalog_probe (
    id, name, amount, active, event_date, event_ts, payload, payload_bin, geom, geom_point,
    geom_collection, tiny_signed, tiny_unsigned, small_signed, small_unsigned, year_value,
    medium_signed, medium_unsigned, int_signed, int_unsigned, big_unsigned, float_value,
    double_value, decimal_scale_zero, decimal_unsigned, time_value, stamp_value, bit_value,
    enum_value, set_value, char_value, binary_value, tiny_text, tiny_blob, text_value,
    blob_value, medium_text, medium_blob, long_text, long_blob, absent_note
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
    ST_GeomFromText('GEOMETRYCOLLECTION(POINT(9 45))', 4326, 'axis-order=long-lat'),
    -8,
    200,
    -300,
    40000,
    2026,
    -70000,
    16000000,
    -123456,
    4000000000,
    18446744073709551615,
    -- 1.5 e 2.25 sono esatti in binario: il valore letto non dipende
    -- dall'arrotondamento del motore.
    1.5,
    2.25,
    -42,
    7.25,
    TIME '01:02:03.456789',
    TIMESTAMP '2026-01-02 03:04:05.123456',
    b'0000000100000010',
    'beta',
    'read,write',
    'char',
    X'01020304',
    'tiny',
    X'0A0B',
    'text',
    X'0C0D',
    'medium',
    X'0E0F',
    'long',
    X'1011',
    NULL
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

-- Fixture del contratto Replace: AUTO_INCREMENT, default, CHECK, unique index,
-- foreign key e trigger, cioe tutto cio che una Replace implementata come
-- staging + RENAME perderebbe. Il trigger deve nascere qui, da root: l'utente
-- della fixture non ha SUPER e con il binlog attivo MySQL rifiuta CREATE
-- TRIGGER (errore 1419). I test resettano le righe, non la definizione.
CREATE TABLE replace_parent (parent_id BIGINT PRIMARY KEY) ENGINE=InnoDB;
INSERT INTO replace_parent VALUES (1), (2);

CREATE TABLE replace_audit (n BIGINT NOT NULL) ENGINE=InnoDB;
INSERT INTO replace_audit VALUES (0);

CREATE TABLE replace_target (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    label VARCHAR(64) NOT NULL DEFAULT 'etichetta-default',
    parent_id BIGINT NOT NULL,
    UNIQUE KEY replace_target_label_uk (label),
    CONSTRAINT replace_target_fk FOREIGN KEY (parent_id)
        REFERENCES replace_parent (parent_id),
    CONSTRAINT replace_target_ck CHECK (CHAR_LENGTH(label) > 0)
) ENGINE=InnoDB;

CREATE TRIGGER replace_target_audit AFTER INSERT ON replace_target
FOR EACH ROW UPDATE replace_audit SET n = n + 1;

-- View deliberatamente lenta per rendere riproducibili cancellation e timeout
-- in-flight del percorso QueryOperation. SLEEP e proiettato, quindi il motore
-- non puo eliminarlo come espressione inutilizzata.
CREATE ALGORITHM=MERGE VIEW slow_query_probe AS
SELECT id, SLEEP(1.0) AS delay_value
FROM stream_probe;
