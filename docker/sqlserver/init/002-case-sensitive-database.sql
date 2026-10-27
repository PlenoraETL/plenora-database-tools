SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

-- Un database dove le maiuscole contano anche nei nomi di colonna
-- (`PLENORA_SQLSERVER_CS_DATABASE`): le regole di nome dei test live
-- (`live_name_shapes_and_the_transaction_catalog_on_a_case_sensitive_database`,
-- `live_read_column_names_resolve_like_the_server_on_a_case_sensitive_database`)
-- si provano soltanto qui. Il login e quello di 001.
IF DB_ID(N'dataflow_cs') IS NULL
BEGIN
    CREATE DATABASE [dataflow_cs] COLLATE Latin1_General_CS_AS;
END;
GO

USE [dataflow_cs];
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
