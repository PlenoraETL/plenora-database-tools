SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET NUMERIC_ROUNDABORT OFF;
GO

IF DB_ID(N'dataflow_test') IS NULL
BEGIN
    CREATE DATABASE [dataflow_test];
END;
GO

IF SUSER_ID(N'dataflow') IS NULL
BEGIN
    CREATE LOGIN [dataflow]
        WITH PASSWORD = N'DataFlow_Test_2026!',
             CHECK_POLICY = OFF,
             CHECK_EXPIRATION = OFF;
END;
GO

USE [dataflow_test];
GO

IF DATABASE_PRINCIPAL_ID(N'dataflow') IS NULL
BEGIN
    CREATE USER [dataflow] FOR LOGIN [dataflow];
END;
GO

IF IS_ROLEMEMBER(N'db_owner', N'dataflow') <> 1
BEGIN
    ALTER ROLE [db_owner] ADD MEMBER [dataflow];
END;
GO

IF SCHEMA_ID(N'plenora_test') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA [plenora_test] AUTHORIZATION [dbo]');
END;
GO

IF OBJECT_ID(N'plenora_test.stream_probe', N'U') IS NULL
BEGIN
    CREATE TABLE [plenora_test].[stream_probe]
    (
        [id] int NOT NULL CONSTRAINT [PK_stream_probe] PRIMARY KEY,
        [flag] bit NULL,
        [unsigned_small] tinyint NULL,
        [signed_small] smallint NULL,
        [signed_big] bigint NULL,
        [single_value] real NULL,
        [double_value] float(53) NULL,
        [exact_value] decimal(20, 6) NULL,
        [money_value] money NULL,
        [calendar_date] date NULL,
        [clock_time] time(7) NULL,
        [local_timestamp] datetime2(7) NULL,
        [offset_timestamp] datetimeoffset(7) NULL,
        [label] nvarchar(100) NULL,
        [payload] varbinary(32) NULL,
        [external_id] uniqueidentifier NULL,
        [document] xml NULL,
        [shape] geometry NULL,
        [position] geography NULL
    );
END;
GO

INSERT INTO [plenora_test].[stream_probe]
(
    [id],
    [flag],
    [unsigned_small],
    [signed_small],
    [signed_big],
    [single_value],
    [double_value],
    [exact_value],
    [money_value],
    [calendar_date],
    [clock_time],
    [local_timestamp],
    [offset_timestamp],
    [label],
    [payload],
    [external_id],
    [document],
    [shape],
    [position]
)
SELECT
    source.[id],
    CONVERT(bit, source.[id] % 2),
    CONVERT(tinyint, source.[id] * 10),
    CONVERT(smallint, source.[id] * -100),
    CONVERT(bigint, source.[id]) * 10000000000,
    CONVERT(real, source.[id]) / 10,
    CONVERT(float(53), source.[id]) / 3,
    CONVERT(decimal(20, 6), source.[id] * 123.456789),
    CONVERT(money, source.[id] * 10.25),
    DATEFROMPARTS(2026, 1, source.[id]),
    TIMEFROMPARTS(1, 2, source.[id], 1234560, 7),
    DATETIME2FROMPARTS(2026, 1, source.[id], 3, 4, 5, 1234560, 7),
    TODATETIMEOFFSET(
        DATETIME2FROMPARTS(2026, 1, source.[id], 3, 4, 5, 1234560, 7),
        N'+01:00'
    ),
    CONCAT(N'row-', source.[id]),
    CONVERT(varbinary(32), CONCAT(N'payload-', source.[id])),
    CONVERT(uniqueidentifier, CONCAT(N'00000000-0000-0000-0000-', RIGHT(
        CONCAT(N'000000000000', source.[id]), 12
    ))),
    CONVERT(xml, CONCAT(N'<row id="', source.[id], N'"/>')),
    geometry::Point(CONVERT(float, source.[id]), CONVERT(float, source.[id]), 4326),
    geography::Point(40.0 + source.[id], 10.0 + source.[id], 4326)
FROM (VALUES (1), (2), (3), (4), (5)) AS source([id])
WHERE NOT EXISTS
(
    SELECT 1
    FROM [plenora_test].[stream_probe] AS target
    WHERE target.[id] = source.[id]
);
GO

UPDATE [plenora_test].[stream_probe]
SET
    [clock_time] = TIMEFROMPARTS(1, 2, [id], 1234560, 7),
    [local_timestamp] = DATETIME2FROMPARTS(2026, 1, [id], 3, 4, 5, 1234560, 7),
    [offset_timestamp] = TODATETIMEOFFSET(
        DATETIME2FROMPARTS(2026, 1, [id], 3, 4, 5, 1234560, 7),
        N'+01:00'
    )
WHERE [id] BETWEEN 1 AND 5;
GO

IF OBJECT_ID(N'plenora_test.write_probe', N'U') IS NULL
BEGIN
    CREATE TABLE [plenora_test].[write_probe]
    (
        [id] int NOT NULL CONSTRAINT [PK_write_probe] PRIMARY KEY,
        [flag] bit NULL,
        [unsigned_small] tinyint NULL,
        [signed_small] smallint NULL,
        [signed_big] bigint NULL,
        [single_value] real NULL,
        [double_value] float(53) NULL,
        [exact_value] decimal(20, 6) NULL,
        [money_value] money NULL,
        [calendar_date] date NULL,
        [clock_time] time(7) NULL,
        [local_timestamp] datetime2(7) NULL,
        [offset_timestamp] datetimeoffset(7) NULL,
        [label] nvarchar(100) NULL,
        [payload] varbinary(32) NULL,
        [external_id] uniqueidentifier NULL,
        [document] xml NULL,
        [shape] geometry NULL,
        [position] geography NULL
    );
END;
GO

IF OBJECT_ID(N'plenora_test.write_guard_probe', N'U') IS NULL
BEGIN
    CREATE TABLE [plenora_test].[write_guard_probe]
    (
        [id] int NOT NULL CONSTRAINT [PK_write_guard_probe] PRIMARY KEY,
        [label] nvarchar(100) NOT NULL
    );
END;
GO

IF NOT EXISTS
(
    SELECT 1 FROM [plenora_test].[write_guard_probe] WHERE [id] = 99
)
BEGIN
    INSERT INTO [plenora_test].[write_guard_probe] ([id], [label])
    VALUES (99, N'sentinel');
END;
GO

IF OBJECT_ID(N'plenora_test.catalog_probe', N'U') IS NULL
BEGIN
    CREATE TABLE [plenora_test].[catalog_probe]
    (
        [id] bigint IDENTITY(1, 1) NOT NULL,
        [external_id] uniqueidentifier NOT NULL
            CONSTRAINT [DF_catalog_probe_external_id] DEFAULT NEWSEQUENTIALID(),
        [name] nvarchar(200) COLLATE Latin1_General_100_CI_AS_SC NOT NULL,
        [code] varchar(32) NULL,
        [amount] decimal(38, 12) NULL,
        [measured_at] datetime2(7) NULL,
        [measured_offset] datetimeoffset(7) NULL,
        [payload] varbinary(max) NULL,
        [document] xml NULL,
        [shape] geometry NULL,
        [position] geography NULL,
        [computed_name] AS UPPER([name]) PERSISTED,
        [version] rowversion NOT NULL,
        CONSTRAINT [PK_catalog_probe] PRIMARY KEY CLUSTERED ([id]),
        CONSTRAINT [UQ_catalog_probe_external_id] UNIQUE ([external_id]),
        CONSTRAINT [CK_catalog_probe_amount] CHECK ([amount] IS NULL OR [amount] >= 0)
    );

    CREATE INDEX [IX_catalog_probe_measured_at]
        ON [plenora_test].[catalog_probe] ([measured_at])
        INCLUDE ([name]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM [plenora_test].[catalog_probe]
)
BEGIN
    INSERT INTO [plenora_test].[catalog_probe]
    (
        [name],
        [code],
        [amount],
        [measured_at],
        [measured_offset],
        [payload],
        [document],
        [shape],
        [position]
    )
    VALUES
    (
        N'reference',
        'REF',
        123.450000000000,
        '2026-01-02T03:04:05.1234567',
        '2026-01-02T03:04:05.1234567+01:00',
        0x00010203,
        N'<reference version="1"/>',
        geometry::STGeomFromText('POINT (12.5 41.9)', 4326),
        geography::STGeomFromText('POINT (12.5 41.9)', 4326)
    );
END;
GO
