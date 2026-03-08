/*
===============================================================================
Script DDL Gold : Schema en constellation simple (SQL Server)
===============================================================================
But :
    Ce script cree des vues simples pour un schema en constellation.

    Dimensions :
    - gold.dim_dates
    - gold.dim_filiere
    - gold.dim_etudiants
    - gold.dim_modules
    - gold.dim_enseignants
    - gold.dim_enseignements

    Tables de faits :
    - gold.fait_notes
    - gold.fait_absences

Remarques :
    - Syntaxe SQL Server (T-SQL)
    - INNER JOIN dans les faits pour eviter les NULL
    - Noms en francais sans accents pour garder une bonne compatibilite SQL
===============================================================================
Dim_Temps 
Dim_Étudiant
Dim_Module
Dim_Enseignant
Dim_Filière
dim_enseignements


Fait_Absences
Fait_Notes
*/

-- =============================================================================
-- Creer le schema gold s'il n'existe pas
-- =============================================================================
IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'gold'
)
BEGIN
    EXEC('CREATE SCHEMA gold');
END;
GO

-- =============================================================================
-- Supprimer les vues si elles existent
-- Commencer par les faits, puis les dimensions
-- =============================================================================
IF OBJECT_ID('gold.fait_notes', 'V') IS NOT NULL
    DROP VIEW gold.fait_notes;
GO

IF OBJECT_ID('gold.fait_absences', 'V') IS NOT NULL
    DROP VIEW gold.fait_absences;
GO

IF OBJECT_ID('gold.dim_enseignements', 'V') IS NOT NULL
    DROP VIEW gold.dim_enseignements;
GO

IF OBJECT_ID('gold.dim_enseignants', 'V') IS NOT NULL
    DROP VIEW gold.dim_enseignants;
GO

IF OBJECT_ID('gold.dim_modules', 'V') IS NOT NULL
    DROP VIEW gold.dim_modules;
GO

IF OBJECT_ID('gold.dim_etudiants', 'V') IS NOT NULL
    DROP VIEW gold.dim_etudiants;
GO

IF OBJECT_ID('gold.dim_filiere', 'V') IS NOT NULL
    DROP VIEW gold.dim_filiere;
GO

IF OBJECT_ID('gold.dim_dates', 'V') IS NOT NULL
    DROP VIEW gold.dim_dates;
GO

-- =============================================================================
-- Dimension : gold.dim_dates
-- =============================================================================
CREATE VIEW gold.dim_dates AS
WITH toutes_les_dates AS (
    SELECT CAST(etd_date_examen AS date) AS date_complete
    FROM silver.etd_notes
    WHERE etd_date_examen IS NOT NULL

    UNION

    SELECT CAST(etd_date_absence AS date) AS date_complete
    FROM silver.etd_absences
    WHERE etd_date_absence IS NOT NULL
)
SELECT
    ROW_NUMBER() OVER (ORDER BY date_complete) AS cle_date,
    date_complete                              AS date_complete,
    DAY(date_complete)                         AS jour,
    MONTH(date_complete)                       AS mois,
    YEAR(date_complete)                        AS annee,
    DATEPART(QUARTER, date_complete)           AS trimestre
FROM toutes_les_dates;
GO

-- =============================================================================
-- Dimension : gold.dim_filiere
-- =============================================================================
CREATE VIEW gold.dim_filiere AS
SELECT
    ROW_NUMBER() OVER (ORDER BY f.etd_id_filiere) AS cle_filiere,
    f.etd_id_filiere                              AS id_filiere,
    f.etd_code_filiere                            AS code_filiere,
    LTRIM(RTRIM(f.etd_nom_filiere))               AS nom_filiere,
    f.etd_type_diplome                            AS type_diplome,
    f.etd_duree                                   AS duree_filiere,
    f.etd_departement                             AS departement
FROM silver.etd_filieres f
WHERE f.etd_id_filiere IS NOT NULL;
GO

-- =============================================================================
-- Dimension : gold.dim_etudiants
-- =============================================================================
CREATE VIEW gold.dim_etudiants AS
SELECT
    ROW_NUMBER() OVER (ORDER BY e.etd_id_etudiant) AS cle_etudiant,
    e.etd_id_etudiant                              AS id_etudiant,
    e.etd_cne                                      AS cne,
    LTRIM(RTRIM(e.etd_nom))                        AS nom,
    LTRIM(RTRIM(e.etd_prenom))                     AS prenom,
    CAST(e.etd_date_naissance AS date)             AS date_naissance,
    e.etd_sexe                                     AS sexe,
    e.etd_niveau                                   AS niveau,
    e.etd_statut                                   AS statut_etudiant,
    e.etd_annee_inscription                        AS annee_inscription,
    e.etd_id_filiere                               AS id_filiere
FROM silver.etd_etudiants e
WHERE e.etd_id_etudiant IS NOT NULL;
GO

-- =============================================================================
-- Dimension : gold.dim_modules
-- =============================================================================
CREATE VIEW gold.dim_modules AS
SELECT
    ROW_NUMBER() OVER (ORDER BY m.etd_id_module) AS cle_module,
    m.etd_id_module                              AS id_module,
    m.etd_code_module                            AS code_module,
    LTRIM(RTRIM(m.etd_nom_module))               AS nom_module,
    m.etd_semestre                               AS semestre,
    m.etd_coefficient                            AS coefficient,
    m.etd_volume_horaire                         AS volume_horaire,
    m.etd_type_module                            AS type_module,
    m.etd_id_filiere                             AS id_filiere
FROM silver.etd_modules m
WHERE m.etd_id_module IS NOT NULL;
GO

-- =============================================================================
-- Dimension : gold.dim_enseignants
-- =============================================================================
CREATE VIEW gold.dim_enseignants AS
SELECT
    ROW_NUMBER() OVER (ORDER BY t.ens_id_enseignant) AS cle_enseignant,
    t.ens_id_enseignant                              AS id_enseignant,
    LTRIM(RTRIM(t.ens_nom))                          AS nom,
    LTRIM(RTRIM(t.ens_prenom))                       AS prenom,
    t.ens_grade                                      AS grade,
    t.ens_specialite                                 AS specialite,
    t.ens_departement                                AS departement
FROM silver.ens_enseignants t
WHERE t.ens_id_enseignant IS NOT NULL;
GO

-- =============================================================================
-- Dimension : gold.dim_enseignements
-- Simple : une ligne par module
-- =============================================================================
CREATE VIEW gold.dim_enseignements AS
WITH source_enseignements AS (
    SELECT
        e.ens_id_module              AS id_module,
        MIN(e.ens_id_enseignant)     AS id_enseignant,
        MIN(m.etd_id_filiere)        AS id_filiere,
        MIN(m.etd_semestre)          AS semestre,
        MIN(e.ens_type_enseignement) AS type_enseignement
    FROM silver.ens_enseignements e
    INNER JOIN silver.etd_modules m
        ON e.ens_id_module = m.etd_id_module
    WHERE e.ens_id_module IS NOT NULL
    GROUP BY e.ens_id_module
)
SELECT
    ROW_NUMBER() OVER (ORDER BY s.id_module) AS cle_enseignement,
    s.id_module                              AS id_module,
    s.id_enseignant                          AS id_enseignant,
    s.id_filiere                             AS id_filiere,
    s.semestre                               AS semestre,
    s.type_enseignement                      AS type_enseignement
FROM source_enseignements s;
GO

-- =============================================================================
-- Table de faits : gold.fait_notes
-- type_examen et session restent dans la table de faits
-- =============================================================================
CREATE VIEW gold.fait_notes AS
SELECT
    n.etd_id_note             AS id_note,
    de.cle_etudiant           AS cle_etudiant,
    dm.cle_module             AS cle_module,
    den.cle_enseignant        AS cle_enseignant,
    df.cle_filiere            AS cle_filiere,
    densi.cle_enseignement    AS cle_enseignement,
    dd.cle_date               AS cle_date,
    n.etd_type_examen         AS type_examen,
    n.etd_session             AS session_examen,
    n.etd_note                AS valeur_note,
    n.etd_semestre            AS semestre,
    n.etd_annee_universitaire AS annee_universitaire,
    1                         AS nombre_notes
FROM silver.etd_notes n
INNER JOIN gold.dim_etudiants de
    ON n.etd_id_etudiant = de.id_etudiant
INNER JOIN gold.dim_modules dm
    ON n.etd_id_module = dm.id_module
INNER JOIN gold.dim_enseignants den
    ON n.etd_id_enseignant = den.id_enseignant
INNER JOIN gold.dim_filiere df
    ON de.id_filiere = df.id_filiere
INNER JOIN gold.dim_enseignements densi
    ON n.etd_id_module = densi.id_module
INNER JOIN gold.dim_dates dd
    ON CAST(n.etd_date_examen AS date) = dd.date_complete
WHERE n.etd_id_etudiant IS NOT NULL
  AND n.etd_id_module IS NOT NULL
  AND n.etd_id_enseignant IS NOT NULL
  AND n.etd_date_examen IS NOT NULL;
GO

-- =============================================================================
-- Table de faits : gold.fait_absences
-- type_seance reste dans la table de faits
-- =============================================================================
CREATE VIEW gold.fait_absences AS
SELECT
    a.etd_id_absence          AS id_absence,
    de.cle_etudiant           AS cle_etudiant,
    dm.cle_module             AS cle_module,
    df.cle_filiere            AS cle_filiere,
    densi.cle_enseignement    AS cle_enseignement,
    dd.cle_date               AS cle_date,
    a.etd_type_seance         AS type_seance,
    a.etd_duree_heures        AS duree_heures,
    a.etd_justifiee           AS statut_justification,
    1                         AS nombre_absences
FROM silver.etd_absences a
INNER JOIN gold.dim_etudiants de
    ON a.etd_id_etudiant = de.id_etudiant
INNER JOIN gold.dim_modules dm
    ON a.etd_id_module = dm.id_module
INNER JOIN gold.dim_filiere df
    ON de.id_filiere = df.id_filiere
INNER JOIN gold.dim_enseignements densi
    ON a.etd_id_module = densi.id_module
INNER JOIN gold.dim_dates dd
    ON CAST(a.etd_date_absence AS date) = dd.date_complete
WHERE a.etd_id_etudiant IS NOT NULL
  AND a.etd_id_module IS NOT NULL
  AND a.etd_date_absence IS NOT NULL;
GO
