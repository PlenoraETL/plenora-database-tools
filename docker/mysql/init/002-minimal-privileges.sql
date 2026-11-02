-- Account applicativo: nessun privilegio globale o sul catalogo performance_schema.
-- Il database deve essere gia stato creato dall'entrypoint; vale anche nel replay.
USE dataflow_test;
CREATE USER IF NOT EXISTS 'plenora_minimal'@'%' IDENTIFIED BY 'Minimal_Test_2026!';
-- CREATE non comprende le tabelle temporanee usate dal writer e dalla frontiera CHECK.
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE TEMPORARY TABLES
    ON dataflow_test.* TO 'plenora_minimal'@'%';
