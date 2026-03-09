-- ============================================================
-- SCRIPT 03 CORRIGÉ : Data Warehouse ESTG_DW
-- Correction : KILL sessions actives avant DROP
-- ============================================================
USE master;
GO

-- ✅ CORRECTION : Tuer les sessions actives avant DROP
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'ESTG_DW')
BEGIN
    DECLARE @kill VARCHAR(8000) = '';
    SELECT @kill = @kill + 'KILL ' + CONVERT(VARCHAR(5), session_id) + ';'
    FROM sys.dm_exec_sessions
    WHERE database_id = DB_ID('ESTG_DW');
    IF LEN(@kill) > 0 EXEC(@kill);
    ALTER DATABASE ESTG_DW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ESTG_DW;
END
GO

CREATE DATABASE ESTG_DW COLLATE French_CI_AS;
GO
USE ESTG_DW;
GO

-- ════════════════════════════════════════════════════
-- DIMENSIONS
-- ════════════════════════════════════════════════════
CREATE TABLE Dim_Temps (
    ID_Temps              INT           NOT NULL PRIMARY KEY,
    Date                  DATE          NOT NULL UNIQUE,
    Jour                  INT           NOT NULL,
    Mois                  INT           NOT NULL,
    Nom_Mois              NVARCHAR(20)  NOT NULL,
    Annee                 INT           NOT NULL,
    Trimestre             INT           NOT NULL,
    Semestre_Academique   CHAR(2)       NOT NULL,
    Annee_Universitaire   VARCHAR(10)   NOT NULL,
    Est_Vacances          BIT           NOT NULL,
    Jour_Semaine          NVARCHAR(10)  NOT NULL
);
GO

CREATE TABLE Dim_Filiere (
    ID_Filiere   INT           NOT NULL PRIMARY KEY,
    Code_Filiere VARCHAR(10)   NOT NULL,
    Nom_Filiere  NVARCHAR(100) NOT NULL,
    Type_Diplome VARCHAR(20)   NOT NULL,
    Duree        INT           NOT NULL,
    Departement  NVARCHAR(60)  NOT NULL
);
GO

CREATE TABLE Dim_Etudiant (
    ID_Etudiant       INT           NOT NULL PRIMARY KEY,
    CNE               VARCHAR(20)   NOT NULL,
    Nom               NVARCHAR(50)  NOT NULL,
    Prenom            NVARCHAR(50)  NOT NULL,
    Date_Naissance    DATE          NULL,
    Age               INT           NULL,
    Sexe              CHAR(1)       NOT NULL,
    Niveau            NVARCHAR(20)  NOT NULL,
    Statut            NVARCHAR(20)  NOT NULL,
    Annee_Inscription INT           NOT NULL,
    ID_Filiere        INT           NOT NULL REFERENCES Dim_Filiere(ID_Filiere)
);
GO

CREATE TABLE Dim_Module (
    ID_Module      INT           NOT NULL PRIMARY KEY,
    Code_Module    VARCHAR(15)   NOT NULL,
    Nom_Module     NVARCHAR(100) NOT NULL,
    ID_Filiere     INT           NOT NULL REFERENCES Dim_Filiere(ID_Filiere),
    Semestre       INT           NOT NULL,
    Coefficient    INT           NOT NULL,
    Volume_Horaire INT           NOT NULL,
    Type_Module    NVARCHAR(20)  NOT NULL
);
GO

CREATE TABLE Dim_Enseignant (
    ID_Enseignant INT           NOT NULL PRIMARY KEY,
    Nom           NVARCHAR(50)  NOT NULL,
    Prenom        NVARCHAR(50)  NOT NULL,
    Grade         NVARCHAR(20)  NOT NULL,
    Specialite    NVARCHAR(80)  NOT NULL,
    Departement   NVARCHAR(60)  NOT NULL
);
GO

CREATE TABLE Dim_Examen (
    ID_Examen   INT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    Type_Examen NVARCHAR(20) NOT NULL,
    Session     NVARCHAR(15) NOT NULL,
    CONSTRAINT UQ_Examen UNIQUE (Type_Examen, Session)
);
GO

CREATE TABLE Dim_Type_Seance (
    ID_Type_Seance INT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    Type_Seance    NVARCHAR(10) NOT NULL UNIQUE
);
GO

-- ════════════════════════════════════════════════════
-- TABLES DE FAITS
-- ════════════════════════════════════════════════════
CREATE TABLE Fait_Notes (
    ID_Fait_Note        INT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    ID_Temps            INT          NOT NULL REFERENCES Dim_Temps(ID_Temps),
    ID_Etudiant         INT          NOT NULL REFERENCES Dim_Etudiant(ID_Etudiant),
    ID_Module           INT          NOT NULL REFERENCES Dim_Module(ID_Module),
    ID_Enseignant       INT          NOT NULL REFERENCES Dim_Enseignant(ID_Enseignant),
    ID_Examen           INT          NOT NULL REFERENCES Dim_Examen(ID_Examen),
    Annee_Universitaire VARCHAR(10)  NOT NULL,
    Semestre            INT          NOT NULL,
    Note                DECIMAL(5,2) NOT NULL
);
GO

CREATE TABLE Fait_Absences (
    ID_Fait_Absence     INT          NOT NULL PRIMARY KEY IDENTITY(1,1),
    ID_Temps            INT          NOT NULL REFERENCES Dim_Temps(ID_Temps),
    ID_Etudiant         INT          NOT NULL REFERENCES Dim_Etudiant(ID_Etudiant),
    ID_Module           INT          NOT NULL REFERENCES Dim_Module(ID_Module),
    ID_Type_Seance      INT          NOT NULL REFERENCES Dim_Type_Seance(ID_Type_Seance),
    Annee_Universitaire VARCHAR(10)  NOT NULL,
    Duree_Heures        DECIMAL(4,1) NOT NULL,
    Justifiee           CHAR(3)      NOT NULL
);
GO

-- ════════════════════════════════════════════════════
-- INDEX SUR LES CLÉs ÉTRANGÈRES
-- ════════════════════════════════════════════════════
CREATE INDEX IX_FaitNotes_Etudiant  ON Fait_Notes(ID_Etudiant);
CREATE INDEX IX_FaitNotes_Module    ON Fait_Notes(ID_Module);
CREATE INDEX IX_FaitNotes_Temps     ON Fait_Notes(ID_Temps);
CREATE INDEX IX_FaitNotes_Annee     ON Fait_Notes(Annee_Universitaire);
CREATE INDEX IX_FaitAbs_Etudiant    ON Fait_Absences(ID_Etudiant);
CREATE INDEX IX_FaitAbs_Module      ON Fait_Absences(ID_Module);
CREATE INDEX IX_FaitAbs_Temps       ON Fait_Absences(ID_Temps);
GO

PRINT '✔  Entrepôt ESTG_DW créé avec succès.';
GO