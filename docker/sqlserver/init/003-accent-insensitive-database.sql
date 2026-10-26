SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

-- Un database che non distingue gli accenti nei nomi di colonna
-- (`PLENORA_SQLSERVER_AI_DATABASE`): il legame dei nomi del server (regola A
-- di `spatial_operand`) si prova qui con `[Ä]` legata da `t.a`
-- (`live_i_nomi_li_lega_il_server_con_la_collation`). Il login e quello di 001.
IF DB_ID(N'dataflow_ai') IS NULL
BEGIN
    CREATE DATABASE [dataflow_ai] COLLATE Latin1_General_CI_AI;
END;
GO

USE [dataflow_ai];
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
