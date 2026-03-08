/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

/*
1
enseignants table >> calendrier 

columns list :
ens_date,ens_jour,ens_mois,ens_annee,ens_trimestre,ens_semestre_academique,ens_annee_universitaire,ens_est_vacances

*/
IF OBJECT_ID('bronze.ens_calendrier', 'U') IS NOT NULL
    DROP TABLE bronze.ens_calendrier;
GO

CREATE TABLE bronze.ens_calendrier (
    ens_date                    DATE,
    ens_jour                    INT,
    ens_mois                    INT,
    ens_annee                   INT,
    ens_trimestre               INT,
    ens_semestre_academique     VARCHAR(30),
    ens_annee_universitaire     VARCHAR(30),
    ens_est_vacances            VARCHAR(30)
);
GO
/*
2
ens_id_enseignant,ens_nom,ens_prenom,ens_grade,ens_specialite,ens_departement

*/
IF OBJECT_ID('bronze.ens_enseignants', 'U') IS NOT NULL
    DROP TABLE bronze.ens_enseignants;
GO

CREATE TABLE bronze.ens_enseignants (
    ens_id_enseignant           INT,
    ens_nom                     VARCHAR(100),
    ens_prenom                  VARCHAR(100),
    ens_grade                   VARCHAR(100),
    ens_specialite              VARCHAR(150),
    ens_departement             VARCHAR(100)
);
GO
/*
3
ens_id_enseignement,ens_id_enseignant,ens_id_module,ens_annee_universitaire,ens_semestre,ens_groupe,ens_type_enseignement,ens_nb_heures_assure

*/
IF OBJECT_ID('bronze.ens_enseignements', 'U') IS NOT NULL
    DROP TABLE bronze.ens_enseignements;
GO

CREATE TABLE bronze.ens_enseignements (
    ens_id_enseignement         INT,
    ens_id_enseignant           INT,
    ens_id_module               INT,
    ens_annee_universitaire     VARCHAR(20),
    ens_semestre                VARCHAR(20),
    ens_groupe                  VARCHAR(50),
    ens_type_enseignement       VARCHAR(100),
    ens_nb_heures_assure        FLOAT
);
GO
/*
4
etd_id_absence,etd_id_etudiant,etd_id_module,etd_date_absence,etd_type_seance,etd_justifiee,etd_duree_heures

*/
IF OBJECT_ID('bronze.etd_absences', 'U') IS NOT NULL
    DROP TABLE bronze.etd_absences;
GO

CREATE TABLE bronze.etd_absences (
    etd_id_absence              INT,
    etd_id_etudiant             INT,
    etd_id_module               INT,
    etd_date_absence            DATE,
    etd_type_seance             VARCHAR(50),
    etd_justifiee               VARCHAR(30),
    etd_duree_heures            FLOAT
);
GO
/*
5
etd_id_etudiant,etd_cne,etd_nom,etd_prenom,etd_date_naissance,etd_sexe,etd_id_filiere,etd_niveau,etd_statut,etd_annee_inscription

*/
IF OBJECT_ID('bronze.etd_etudiants', 'U') IS NOT NULL
    DROP TABLE bronze.etd_etudiants;
GO

CREATE TABLE bronze.etd_etudiants (
    etd_id_etudiant             INT,
    etd_cne                     VARCHAR(50),
    etd_nom                     VARCHAR(100),
    etd_prenom                  VARCHAR(100),
    etd_date_naissance          DATE,
    etd_sexe                    VARCHAR(30),
    etd_id_filiere              INT,
    etd_niveau                  VARCHAR(50),
    etd_statut                  VARCHAR(50),
    etd_annee_inscription       INT
);
GO
/*
6
etd_id_filiere,etd_code_filiere,etd_nom_filiere,etd_type_diplome,etd_duree,etd_departement

*/
IF OBJECT_ID('bronze.etd_filieres', 'U') IS NOT NULL
    DROP TABLE bronze.etd_filieres;
GO

CREATE TABLE bronze.etd_filieres (
    etd_id_filiere              INT,
    etd_code_filiere            VARCHAR(50),
    etd_nom_filiere             VARCHAR(150),
    etd_type_diplome            VARCHAR(100),
    etd_duree                   INT,
    etd_departement             VARCHAR(100)
);
GO
/*
7
etd_id_module,etd_code_module,etd_nom_module,etd_id_filiere,etd_semestre,etd_coefficient,etd_volume_horaire,etd_type_module

*/
IF OBJECT_ID('bronze.etd_modules', 'U') IS NOT NULL
    DROP TABLE bronze.etd_modules;
GO

CREATE TABLE bronze.etd_modules (
    etd_id_module               INT,
    etd_code_module             VARCHAR(50),
    etd_nom_module              VARCHAR(150),
    etd_id_filiere              INT,
    etd_semestre                VARCHAR(30),
    etd_coefficient             FLOAT,
    etd_volume_horaire          FLOAT,
    etd_type_module             VARCHAR(100)
);
GO
/*
8
etd_id_note,etd_id_etudiant,etd_id_module,etd_id_enseignant,etd_annee_universitaire,etd_semestre,etd_type_examen,etd_note,etd_date_examen,etd_session

*/
IF OBJECT_ID('bronze.etd_notes', 'U') IS NOT NULL
    DROP TABLE bronze.etd_notes;
GO

CREATE TABLE bronze.etd_notes (
    etd_id_note                 INT,
    etd_id_etudiant             INT,
    etd_id_module               INT,
    etd_id_enseignant           INT,
    etd_annee_universitaire     VARCHAR(30),
    etd_semestre                VARCHAR(30),
    etd_type_examen             VARCHAR(50),
    etd_note                    FLOAT,
    etd_date_examen             DATE,
    etd_session                 VARCHAR(50)
);
GO