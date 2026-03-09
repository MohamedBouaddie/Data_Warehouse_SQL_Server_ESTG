-- ============================================================
-- SCRIPT 01 : Création de la base de données source ESTG_Source
-- Auteur    : Projet BI - ESTG
-- SGBD      : SQL Server 2019+
-- ============================================================

USE master;
GO

-- Supprime la base si elle existe déjà (dev uniquement)
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'ESTG_Source')
BEGIN
    ALTER DATABASE ESTG_Source SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ESTG_Source;
END
GO

CREATE DATABASE ESTG_Source
    COLLATE French_CI_AS;
GO

USE ESTG_Source;
GO

-- ─────────────────────────────────────────────
-- TABLE : filieres
-- ─────────────────────────────────────────────
CREATE TABLE filieres (
    ID_Filiere       INT           PRIMARY KEY,
    Code_Filiere     VARCHAR(10)   NOT NULL,
    Nom_Filiere      NVARCHAR(100) NOT NULL,
    Type_Diplome     VARCHAR(20)   NOT NULL,   -- DUT / Bachelor
    Duree            INT           NOT NULL,
    Departement      NVARCHAR(60)  NOT NULL
);
GO

-- ─────────────────────────────────────────────
-- TABLE : etudiants
-- ─────────────────────────────────────────────
CREATE TABLE etudiants (
    ID_Etudiant      INT           PRIMARY KEY,
    CNE              VARCHAR(20)   NOT NULL UNIQUE,
    Nom              NVARCHAR(50)  NOT NULL,
    Prenom           NVARCHAR(50)  NOT NULL,
    Date_Naissance   DATE          NULL,
    Sexe             CHAR(1)       NOT NULL CHECK (Sexe IN ('M','F')),
    ID_Filiere       INT           NOT NULL REFERENCES filieres(ID_Filiere),
    Niveau           NVARCHAR(20)  NOT NULL,   -- 1ère année / 2ème année / 3ème année
    Statut           NVARCHAR(20)  NOT NULL,   -- Actif / Redoublant / Diplômé / Abandon
    Annee_Inscription INT          NOT NULL
);
GO

-- ─────────────────────────────────────────────
-- TABLE : enseignants
-- ─────────────────────────────────────────────
CREATE TABLE enseignants (
    ID_Enseignant    INT           PRIMARY KEY,
    Nom              NVARCHAR(50)  NOT NULL,
    Prenom           NVARCHAR(50)  NOT NULL,
    Grade            NVARCHAR(20)  NOT NULL,   -- Professeur / PA / PH / Vacataire
    Specialite       NVARCHAR(80)  NOT NULL,
    Departement      NVARCHAR(60)  NOT NULL
);
GO

-- ─────────────────────────────────────────────
-- TABLE : modules
-- ─────────────────────────────────────────────
CREATE TABLE modules (
    ID_Module        INT           PRIMARY KEY,
    Code_Module      VARCHAR(15)   NOT NULL UNIQUE,
    Nom_Module       NVARCHAR(100) NOT NULL,
    ID_Filiere       INT           NOT NULL REFERENCES filieres(ID_Filiere),
    Semestre         INT           NOT NULL CHECK (Semestre BETWEEN 1 AND 6),
    Coefficient      INT           NOT NULL CHECK (Coefficient > 0),
    Volume_Horaire   INT           NOT NULL,
    Type_Module      NVARCHAR(20)  NOT NULL    -- Majeur / Complémentaire / Transversal
);
GO

-- ─────────────────────────────────────────────
-- TABLE : enseignement  (affectation enseignant-module)
-- ─────────────────────────────────────────────
CREATE TABLE enseignement (
    ID_Enseignement       INT           PRIMARY KEY,
    ID_Enseignant         INT           NOT NULL REFERENCES enseignants(ID_Enseignant),
    ID_Module             INT           NOT NULL REFERENCES modules(ID_Module),
    Annee_Universitaire   VARCHAR(10)   NOT NULL,  -- ex: 2024-2025
    Semestre              INT           NOT NULL,
    Nb_Heures_Effectuees  INT           NULL,
    Observations          NVARCHAR(200) NULL,
    CONSTRAINT UQ_Ens_Mod_Annee UNIQUE (ID_Enseignant, ID_Module, Annee_Universitaire)
);
GO

-- ─────────────────────────────────────────────
-- TABLE : notes
-- ─────────────────────────────────────────────
CREATE TABLE notes (
    ID_Note               INT           PRIMARY KEY,
    ID_Etudiant           INT           NOT NULL REFERENCES etudiants(ID_Etudiant),
    ID_Module             INT           NOT NULL REFERENCES modules(ID_Module),
    ID_Enseignant         INT           NOT NULL REFERENCES enseignants(ID_Enseignant),
    Annee_Universitaire   VARCHAR(10)   NOT NULL,
    Semestre              INT           NOT NULL,
    Type_Examen           NVARCHAR(20)  NOT NULL,  -- CC / TP / Examen Final
    Note                  DECIMAL(5,2)  NOT NULL CHECK (Note BETWEEN 0 AND 20),
    Date_Examen           DATE          NULL,
    Session               NVARCHAR(15)  NOT NULL   -- Normale / Rattrapage
);
GO

-- ─────────────────────────────────────────────
-- TABLE : absences
-- ─────────────────────────────────────────────
CREATE TABLE absences (
    ID_Absence    INT           PRIMARY KEY,
    ID_Etudiant   INT           NOT NULL REFERENCES etudiants(ID_Etudiant),
    ID_Module     INT           NOT NULL REFERENCES modules(ID_Module),
    Date_Absence  DATE          NOT NULL,
    Type_Seance   NVARCHAR(10)  NOT NULL,   -- Cours / TD / TP
    Justifiee     CHAR(3)       NOT NULL CHECK (Justifiee IN ('Oui','Non')),
    Duree_Heures  DECIMAL(4,1)  NOT NULL
);
GO

-- ─────────────────────────────────────────────
-- TABLE : calendrier
-- ─────────────────────────────────────────────
CREATE TABLE calendrier (
    Date                  DATE          PRIMARY KEY,
    Jour                  INT           NOT NULL,
    Mois                  INT           NOT NULL,
    Annee                 INT           NOT NULL,
    Trimestre             INT           NOT NULL,
    Semestre_Academique   CHAR(2)       NOT NULL,
    Annee_Universitaire   VARCHAR(10)   NOT NULL,
    Est_Vacances          BIT           NOT NULL
);
GO

PRINT '✔  Base ESTG_Source créée avec succès.';
GO
