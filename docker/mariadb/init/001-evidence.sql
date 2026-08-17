-- Fixture di evidenza MariaDB: il minimo che serve a osservare, non la
-- fixture di un riferimento qualificato.
--
-- Deliberatamente povera. La fixture MySQL dichiara collation, SRID di
-- colonna e grant su `performance_schema` perche i test live li esercitano;
-- qui ricopiarli significherebbe decidere in anticipo che MariaDB li
-- supporti allo stesso modo — che e esattamente la domanda a cui questo
-- ciclo deve rispondere con delle prove. Cio che diverge si misura, non si
-- assume: le colonne qui sotto usano solo costrutti che entrambi i motori
-- accettano, cosi una differenza osservata riguarda il motore e non il DDL.

USE dataflow_test;

SET SESSION time_zone = '+00:00';

CREATE TABLE evidence_probe (
    id BIGINT NOT NULL,
    name VARCHAR(100) NOT NULL,
    amount DECIMAL(18, 4) NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    event_date DATE NULL,
    event_ts DATETIME(6) NULL,
    payload_bin VARBINARY(16) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY evidence_probe_name_uk (name)
) ENGINE = InnoDB;

INSERT INTO evidence_probe (id, name, amount, active, event_date, event_ts)
VALUES
    (1, 'primo', 10.5000, TRUE, '2026-08-17', '2026-08-17 06:00:00.000000'),
    (2, 'secondo', NULL, FALSE, NULL, NULL);
