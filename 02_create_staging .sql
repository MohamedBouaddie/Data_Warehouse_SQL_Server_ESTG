-- ============================================================
-- SCRIPT 02 CORRIGÉ : Création et chargement de la Staging Area
--             ESTG_Staging — Zone tampon entre Source et DW
-- Correction : \r (CHAR 13) dans Duree_Heures + statuts VALIDE
-- ============================================================
USE master;
GO

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'ESTG_Staging')
BEGIN
    DECLARE @kill VARCHAR(8000) = '';
    SELECT @kill = @kill + 'KILL ' + CONVERT(VARCHAR(5), session_id) + ';'
    FROM sys.dm_exec_sessions
    WHERE database_id = DB_ID('ESTG_Staging');
    IF LEN(@kill) > 0 EXEC(@kill);
    ALTER DATABASE ESTG_Staging SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ESTG_Staging;
END
GO

CREATE DATABASE ESTG_Staging COLLATE French_CI_AS;
GO
USE ESTG_Staging;
GO

-- ════════════════════════════════════════════════════
-- TABLES STAGING
-- ════════════════════════════════════════════════════
CREATE TABLE STG_Filieres (
    ID_Filiere    INT,
    Code_Filiere  VARCHAR(10),
    Nom_Filiere   NVARCHAR(100),
    Type_Diplome  VARCHAR(20),
    Duree         INT,
    Departement   NVARCHAR(60)
);

CREATE TABLE STG_Etudiants (
    ID_Etudiant       INT,
    CNE               VARCHAR(20),
    Nom               NVARCHAR(50),
    Prenom            NVARCHAR(50),
    Date_Naissance    VARCHAR(20),
    Sexe              CHAR(1),
    ID_Filiere        INT,
    Niveau            NVARCHAR(20),
    Statut            NVARCHAR(20),
    Annee_Inscription INT
);

CREATE TABLE STG_Enseignants (
    ID_Enseignant  INT,
    Nom            NVARCHAR(50),
    Prenom         NVARCHAR(50),
    Grade          NVARCHAR(20),
    Specialite     NVARCHAR(80),
    Departement    NVARCHAR(60)
);

CREATE TABLE STG_Modules (
    ID_Module      INT,
    Code_Module    VARCHAR(15),
    Nom_Module     NVARCHAR(100),
    ID_Filiere     INT,
    Semestre       INT,
    Coefficient    INT,
    Volume_Horaire INT,
    Type_Module    NVARCHAR(20)
);

CREATE TABLE STG_Enseignement (
    ID_Enseignement       INT,
    ID_Enseignant         INT,
    ID_Module             INT,
    Annee_Universitaire   VARCHAR(10),
    Semestre              INT,
    Nb_Heures_Effectuees  INT,
    Observations          NVARCHAR(200)
);

CREATE TABLE STG_Notes (
    ID_Note             INT,
    ID_Etudiant         INT,
    ID_Module           INT,
    ID_Enseignant       INT,
    Annee_Universitaire VARCHAR(10),
    Semestre            INT,
    Type_Examen         NVARCHAR(20),
    Note                DECIMAL(5,2),
    Date_Examen         VARCHAR(20),
    Session             NVARCHAR(15)
);

CREATE TABLE STG_Absences (
    ID_Absence    INT,
    ID_Etudiant   INT,
    ID_Module     INT,
    Date_Absence  VARCHAR(20),
    Type_Seance   NVARCHAR(10),
    Justifiee     CHAR(3),
    Duree_Heures  VARCHAR(10)   -- VARCHAR pour absorber le \r du CSV
);

CREATE TABLE STG_Calendrier (
    Date                VARCHAR(20),
    Jour                INT,
    Mois                INT,
    Annee               INT,
    Trimestre           INT,
    Semestre_Academique CHAR(2),
    Annee_Universitaire VARCHAR(10),
    Est_Vacances        VARCHAR(5)
);
GO

-- ════════════════════════════════════════════════════
-- BULK INSERT CSV → STAGING
-- ════════════════════════════════════════════════════
BULK INSERT STG_Filieres
FROM 'C:\Users\dell\Documents\PFE\DB new\format csv\filieres.csv'
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);
PRINT '  ✔ STG_Filieres — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

BULK INSERT STG_Etudiants
FROM 'C:\Users\dell\Documents\PFE\DB new\format csv\etudiants.csv'
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);
PRINT '  ✔ STG_Etudiants — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

BULK INSERT STG_Enseignants
FROM 'C:\Users\dell\Documents\PFE\DB new\format csv\enseignants.csv'
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);
PRINT '  ✔ STG_Enseignants — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

BULK INSERT STG_Modules
FROM 'C:\Users\dell\Documents\PFE\DB new\format csv\modules.csv'
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);
PRINT '  ✔ STG_Modules — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

BULK INSERT STG_Enseignement
FROM 'C:\Users\dell\Documents\PFE\DB new\format csv\enseignement.csv'
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);
PRINT '  ✔ STG_Enseignement — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

BULK INSERT STG_Notes
FROM 'C:\Users\dell\Documents\PFE\DB new\format csv\notes.csv'
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);
PRINT '  ✔ STG_Notes — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

BULK INSERT STG_Absences
FROM 'C:\Users\dell\Documents\PFE\DB new\format csv\absences.csv'
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);
PRINT '  ✔ STG_Absences — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

BULK INSERT STG_Calendrier
FROM 'C:\Users\dell\Documents\PFE\DB new\format csv\calendrier.csv'
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a', CODEPAGE='65001', TABLOCK);
PRINT '  ✔ STG_Calendrier — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ════════════════════════════════════════════════════
-- NETTOYAGE \r SUR TOUTES LES COLONNES VARCHAR
-- (dernière colonne de chaque CSV contient CHAR(13))
-- ════════════════════════════════════════════════════
-- STG_Filieres : dernière colonne = Departement
UPDATE STG_Filieres
SET Departement = REPLACE(Departement, CHAR(13), '');

-- STG_Etudiants : dernière colonne = Annee_Inscription (INT, pas affecté)
-- mais Statut peut l'être si ordre différent
UPDATE STG_Etudiants
SET Statut    = REPLACE(Statut,    CHAR(13), ''),
    Niveau    = REPLACE(Niveau,    CHAR(13), ''),
    Date_Naissance = REPLACE(Date_Naissance, CHAR(13), '');

-- STG_Enseignants : dernière colonne = Departement
UPDATE STG_Enseignants
SET Departement = REPLACE(Departement, CHAR(13), '');

-- STG_Modules : dernière colonne = Type_Module
UPDATE STG_Modules
SET Type_Module = REPLACE(Type_Module, CHAR(13), '');

-- STG_Enseignement : dernière colonne = Observations
UPDATE STG_Enseignement
SET Observations = REPLACE(ISNULL(Observations,''), CHAR(13), '');

-- STG_Notes : dernière colonne = Session
UPDATE STG_Notes
SET Session     = REPLACE(Session,     CHAR(13), ''),
    Date_Examen = REPLACE(Date_Examen, CHAR(13), ''),
    Type_Examen = REPLACE(Type_Examen, CHAR(13), '');

-- ✅ STG_Absences : dernière colonne = Duree_Heures (cause principale)
UPDATE STG_Absences
SET Duree_Heures = REPLACE(Duree_Heures, CHAR(13), ''),
    Justifiee    = REPLACE(Justifiee,    CHAR(13), ''),
    Type_Seance  = REPLACE(Type_Seance,  CHAR(13), ''),
    Date_Absence = REPLACE(Date_Absence, CHAR(13), '');

-- STG_Calendrier : dernière colonne = Est_Vacances
UPDATE STG_Calendrier
SET Est_Vacances        = REPLACE(Est_Vacances,        CHAR(13), ''),
    Annee_Universitaire = REPLACE(Annee_Universitaire, CHAR(13), '');

PRINT '  ✔ Nettoyage CHAR(13) terminé sur toutes les tables';
GO

-- ════════════════════════════════════════════════════
-- AJOUT COLONNES TECHNIQUES
-- ════════════════════════════════════════════════════
ALTER TABLE STG_Filieres     ADD STG_Date_Chargement DATETIME DEFAULT GETDATE(), STG_Statut VARCHAR(10);
ALTER TABLE STG_Etudiants    ADD STG_Date_Chargement DATETIME DEFAULT GETDATE(), STG_Statut VARCHAR(10);
ALTER TABLE STG_Enseignants  ADD STG_Date_Chargement DATETIME DEFAULT GETDATE(), STG_Statut VARCHAR(10);
ALTER TABLE STG_Modules      ADD STG_Date_Chargement DATETIME DEFAULT GETDATE(), STG_Statut VARCHAR(10);
ALTER TABLE STG_Enseignement ADD STG_Date_Chargement DATETIME DEFAULT GETDATE(), STG_Statut VARCHAR(10);
ALTER TABLE STG_Notes        ADD STG_Date_Chargement DATETIME DEFAULT GETDATE(), STG_Statut VARCHAR(10);
ALTER TABLE STG_Absences     ADD STG_Date_Chargement DATETIME DEFAULT GETDATE(), STG_Statut VARCHAR(10);
ALTER TABLE STG_Calendrier   ADD STG_Date_Chargement DATETIME DEFAULT GETDATE(), STG_Statut VARCHAR(10);
GO

UPDATE STG_Filieres     SET STG_Date_Chargement = GETDATE(), STG_Statut = 'NOUVEAU';
UPDATE STG_Etudiants    SET STG_Date_Chargement = GETDATE(), STG_Statut = 'NOUVEAU';
UPDATE STG_Enseignants  SET STG_Date_Chargement = GETDATE(), STG_Statut = 'NOUVEAU';
UPDATE STG_Modules      SET STG_Date_Chargement = GETDATE(), STG_Statut = 'NOUVEAU';
UPDATE STG_Enseignement SET STG_Date_Chargement = GETDATE(), STG_Statut = 'NOUVEAU';
UPDATE STG_Notes        SET STG_Date_Chargement = GETDATE(), STG_Statut = 'NOUVEAU';
UPDATE STG_Absences     SET STG_Date_Chargement = GETDATE(), STG_Statut = 'NOUVEAU';
UPDATE STG_Calendrier   SET STG_Date_Chargement = GETDATE(), STG_Statut = 'NOUVEAU';
GO

-- ════════════════════════════════════════════════════
-- VALIDATION QUALITÉ — MARQUAGE ERREURS
-- ════════════════════════════════════════════════════

-- Notes hors plage [0-20]
UPDATE STG_Notes
SET STG_Statut = 'ERREUR'
WHERE Note < 0 OR Note > 20;

-- Sexe invalide
UPDATE STG_Etudiants
SET STG_Statut = 'ERREUR'
WHERE Sexe NOT IN ('M','F');

-- ✅ Duree_Heures : après nettoyage \r, TRY_CAST fonctionne correctement
UPDATE STG_Absences
SET STG_Statut = 'ERREUR'
WHERE TRY_CAST(Duree_Heures AS DECIMAL(4,1)) IS NULL
   OR TRY_CAST(Duree_Heures AS DECIMAL(4,1)) <= 0;

-- Coefficient invalide
UPDATE STG_Modules
SET STG_Statut = 'ERREUR'
WHERE Coefficient IS NULL OR Coefficient <= 0;

-- Dates invalides dans les absences
UPDATE STG_Absences
SET STG_Statut = 'ERREUR'
WHERE STG_Statut != 'ERREUR'
  AND TRY_CAST(Date_Absence AS DATE) IS NULL;

-- Dates invalides dans les notes
UPDATE STG_Notes
SET STG_Statut = 'ERREUR'
WHERE STG_Statut != 'ERREUR'
  AND Date_Examen IS NOT NULL
  AND TRY_CAST(Date_Examen AS DATE) IS NULL;
GO

-- ════════════════════════════════════════════════════
-- MARQUAGE VALIDE — tout ce qui n'est pas ERREUR
-- ════════════════════════════════════════════════════
UPDATE STG_Filieres     SET STG_Statut = 'VALIDE' WHERE STG_Statut = 'NOUVEAU';
UPDATE STG_Etudiants    SET STG_Statut = 'VALIDE' WHERE STG_Statut = 'NOUVEAU';
UPDATE STG_Enseignants  SET STG_Statut = 'VALIDE' WHERE STG_Statut = 'NOUVEAU';
UPDATE STG_Modules      SET STG_Statut = 'VALIDE' WHERE STG_Statut = 'NOUVEAU';
UPDATE STG_Enseignement SET STG_Statut = 'VALIDE' WHERE STG_Statut = 'NOUVEAU';
UPDATE STG_Notes        SET STG_Statut = 'VALIDE' WHERE STG_Statut = 'NOUVEAU';
UPDATE STG_Absences     SET STG_Statut = 'VALIDE' WHERE STG_Statut = 'NOUVEAU';
UPDATE STG_Calendrier   SET STG_Statut = 'VALIDE' WHERE STG_Statut = 'NOUVEAU';
GO

-- ════════════════════════════════════════════════════
-- RAPPORT FINAL DE VALIDATION
-- ════════════════════════════════════════════════════
SELECT
    'STG_Filieres' AS [Table],
    COUNT(*)       AS Total,
    SUM(CASE WHEN STG_Statut = 'ERREUR' THEN 1 ELSE 0 END) AS Erreurs,
    SUM(CASE WHEN STG_Statut = 'VALIDE' THEN 1 ELSE 0 END) AS Valides
FROM STG_Filieres
UNION ALL
SELECT 'STG_Etudiants',    COUNT(*),
    SUM(CASE WHEN STG_Statut='ERREUR' THEN 1 ELSE 0 END),
    SUM(CASE WHEN STG_Statut='VALIDE' THEN 1 ELSE 0 END)
FROM STG_Etudiants
UNION ALL
SELECT 'STG_Enseignants',  COUNT(*),
    SUM(CASE WHEN STG_Statut='ERREUR' THEN 1 ELSE 0 END),
    SUM(CASE WHEN STG_Statut='VALIDE' THEN 1 ELSE 0 END)
FROM STG_Enseignants
UNION ALL
SELECT 'STG_Modules',      COUNT(*),
    SUM(CASE WHEN STG_Statut='ERREUR' THEN 1 ELSE 0 END),
    SUM(CASE WHEN STG_Statut='VALIDE' THEN 1 ELSE 0 END)
FROM STG_Modules
UNION ALL
SELECT 'STG_Enseignement', COUNT(*),
    SUM(CASE WHEN STG_Statut='ERREUR' THEN 1 ELSE 0 END),
    SUM(CASE WHEN STG_Statut='VALIDE' THEN 1 ELSE 0 END)
FROM STG_Enseignement
UNION ALL
SELECT 'STG_Notes',        COUNT(*),
    SUM(CASE WHEN STG_Statut='ERREUR' THEN 1 ELSE 0 END),
    SUM(CASE WHEN STG_Statut='VALIDE' THEN 1 ELSE 0 END)
FROM STG_Notes
UNION ALL
SELECT 'STG_Absences',     COUNT(*),
    SUM(CASE WHEN STG_Statut='ERREUR' THEN 1 ELSE 0 END),
    SUM(CASE WHEN STG_Statut='VALIDE' THEN 1 ELSE 0 END)
FROM STG_Absences
UNION ALL
SELECT 'STG_Calendrier',   COUNT(*),
    SUM(CASE WHEN STG_Statut='ERREUR' THEN 1 ELSE 0 END),
    SUM(CASE WHEN STG_Statut='VALIDE' THEN 1 ELSE 0 END)
FROM STG_Calendrier;
GO

PRINT '✔  ESTG_Staging créée, nettoyée et validée avec succès.';
GO