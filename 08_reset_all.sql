-- ============================================================
-- SCRIPT 08 : Reset complet — Supprime les 3 bases
-- Projet     : Business Intelligence — ESTG
-- ⚠️  ATTENTION : À utiliser UNIQUEMENT en développement !
--                 Toutes les données seront perdues.
-- ============================================================
USE master;
GO

PRINT '⚠️  ════════════════════════════════════════';
PRINT '⚠️  RESET COMPLET EN COURS — DEV UNIQUEMENT';
PRINT '⚠️  ════════════════════════════════════════';
GO

-- ── Supprimer ESTG_DW ─────────────────────────────
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'ESTG_DW')
BEGIN
    DECLARE @k1 VARCHAR(8000) = '';
    SELECT @k1 = @k1 + 'KILL ' + CONVERT(VARCHAR(5), session_id) + ';'
    FROM sys.dm_exec_sessions WHERE database_id = DB_ID('ESTG_DW');
    IF LEN(@k1) > 0 EXEC(@k1);
    ALTER DATABASE ESTG_DW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ESTG_DW;
    PRINT '  ✔ ESTG_DW supprimée';
END
ELSE PRINT '  -- ESTG_DW inexistante, ignorée';
GO

-- ── Supprimer ESTG_Staging ────────────────────────
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'ESTG_Staging')
BEGIN
    DECLARE @k2 VARCHAR(8000) = '';
    SELECT @k2 = @k2 + 'KILL ' + CONVERT(VARCHAR(5), session_id) + ';'
    FROM sys.dm_exec_sessions WHERE database_id = DB_ID('ESTG_Staging');
    IF LEN(@k2) > 0 EXEC(@k2);
    ALTER DATABASE ESTG_Staging SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ESTG_Staging;
    PRINT '  ✔ ESTG_Staging supprimée';
END
ELSE PRINT '  -- ESTG_Staging inexistante, ignorée';
GO

-- ── Supprimer ESTG_Source ─────────────────────────
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'ESTG_Source')
BEGIN
    DECLARE @k3 VARCHAR(8000) = '';
    SELECT @k3 = @k3 + 'KILL ' + CONVERT(VARCHAR(5), session_id) + ';'
    FROM sys.dm_exec_sessions WHERE database_id = DB_ID('ESTG_Source');
    IF LEN(@k3) > 0 EXEC(@k3);
    ALTER DATABASE ESTG_Source SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ESTG_Source;
    PRINT '  ✔ ESTG_Source supprimée';
END
ELSE PRINT '  -- ESTG_Source inexistante, ignorée';
GO

PRINT '';
PRINT '✔  Reset terminé.';
PRINT '   Relancer les scripts dans l''ordre :';
PRINT '   01 → 02 → 03 → 04 (ou 05) → 06 → 07';
GO
