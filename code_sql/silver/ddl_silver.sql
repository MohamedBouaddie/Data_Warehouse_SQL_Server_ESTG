/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

IF OBJECT_ID('silver.ens_calendrier', 'U') IS NOT NULL
    DROP TABLE silver.ens_calendrier;
GO

CREATE TABLE silver.ens_calendrier (
    ens_date                    DATE,
    ens_jour                    INT,
    ens_mois                    INT,
    ens_annee                   INT,
    ens_trimestre               INT,
    ens_semestre_academique     VARCHAR(20),
    ens_annee_universitaire     VARCHAR(20),
    ens_est_vacances            VARCHAR(10),
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.ens_enseignants', 'U') IS NOT NULL
    DROP TABLE silver.ens_enseignants;
GO

CREATE TABLE silver.ens_enseignants (
    ens_id_enseignant           INT,
    ens_nom                     VARCHAR(100),
    ens_prenom                  VARCHAR(100),
    ens_grade                   VARCHAR(100),
    ens_specialite              VARCHAR(150),
    ens_departement             VARCHAR(100),
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.ens_enseignements', 'U') IS NOT NULL
    DROP TABLE silver.ens_enseignements;
GO

CREATE TABLE silver.ens_enseignements (
    ens_id_enseignement         INT,
    ens_id_enseignant           INT,
    ens_id_module               INT,
    ens_annee_universitaire     VARCHAR(20),
    ens_semestre                VARCHAR(20),
    ens_groupe                  VARCHAR(50),
    ens_type_enseignement       VARCHAR(100),
    ens_nb_heures_assure        FLOAT,
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.etd_absences', 'U') IS NOT NULL
    DROP TABLE silver.etd_absences;
GO

CREATE TABLE silver.etd_absences (
    etd_id_absence              INT,
    etd_id_etudiant             INT,
    etd_id_module               INT,
    etd_date_absence            DATE,
    etd_type_seance             VARCHAR(50),
    etd_justifiee               VARCHAR(10),
    etd_duree_heures            FLOAT,
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.etd_etudiants', 'U') IS NOT NULL
    DROP TABLE silver.etd_etudiants;
GO

CREATE TABLE silver.etd_etudiants (
    etd_id_etudiant             INT,
    etd_cne                     VARCHAR(50),
    etd_nom                     VARCHAR(100),
    etd_prenom                  VARCHAR(100),
    etd_date_naissance          DATE,
    etd_sexe                    VARCHAR(20),
    etd_id_filiere              INT,
    etd_niveau                  VARCHAR(50),
    etd_statut                  VARCHAR(50),
    etd_annee_inscription       INT,
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.etd_filieres', 'U') IS NOT NULL
    DROP TABLE silver.etd_filieres;
GO

CREATE TABLE silver.etd_filieres (
    etd_id_filiere              INT,
    etd_code_filiere            VARCHAR(50),
    etd_nom_filiere             VARCHAR(150),
    etd_type_diplome            VARCHAR(100),
    etd_duree                   INT,
    etd_departement             VARCHAR(100),
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.etd_modules', 'U') IS NOT NULL
    DROP TABLE silver.etd_modules;
GO

CREATE TABLE silver.etd_modules (
    etd_id_module               INT,
    etd_code_module             VARCHAR(50),
    etd_nom_module              VARCHAR(150),
    etd_id_filiere              INT,
    etd_semestre                VARCHAR(20),
    etd_coefficient             FLOAT,
    etd_volume_horaire          FLOAT,
    etd_type_module             VARCHAR(100),
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.etd_notes', 'U') IS NOT NULL
    DROP TABLE silver.etd_notes;
GO

CREATE TABLE silver.etd_notes (
    etd_id_note                 INT,
    etd_id_etudiant             INT,
    etd_id_module               INT,
    etd_id_enseignant           INT,
    etd_annee_universitaire     VARCHAR(20),
    etd_semestre                VARCHAR(20),
    etd_type_examen             VARCHAR(50),
    etd_note                    FLOAT,
    etd_date_examen             DATE,
    etd_session                 VARCHAR(50),
    dwh_create_date             DATETIME2 DEFAULT GETDATE()
);
GO