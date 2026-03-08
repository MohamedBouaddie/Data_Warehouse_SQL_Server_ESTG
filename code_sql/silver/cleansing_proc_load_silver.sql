/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Purpose:
    Cleanse dirty bronze data and load standardized data into silver tables.

Important note:
    This script handles rule-based cleansing:
    - trim spaces
    - normalize text values
    - remove negative signs from numeric fields where relevant
    - expand coded values (M/F, TD/TP, DUT, S1 -> Semestre1 in modules)
    - split etd_cne into letter part and numeric part

    If some dirty rows contain swapped or randomly altered values, SQL cleansing
    rules alone cannot reconstruct the exact original clean file without using
    the clean CSV files as a reference source.
===============================================================================
*/
    IF COL_LENGTH('silver.etd_etudiants', 'etd_cne_lettre') IS NULL
        ALTER TABLE silver.etd_etudiants ADD etd_cne_lettre VARCHAR(10);
    GO

    IF COL_LENGTH('silver.etd_etudiants', 'etd_cne_numero') IS NULL
        ALTER TABLE silver.etd_etudiants ADD etd_cne_numero VARCHAR(20);
    GO

    CREATE OR ALTER PROCEDURE silver.load_silver
    AS
    BEGIN
        SET NOCOUNT ON;

        DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

        BEGIN TRY
            SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

        TRUNCATE TABLE silver.ens_calendrier;
        TRUNCATE TABLE silver.ens_enseignants;
        TRUNCATE TABLE silver.ens_enseignements;
        TRUNCATE TABLE silver.etd_absences;
        TRUNCATE TABLE silver.etd_etudiants;
        TRUNCATE TABLE silver.etd_filieres;
        TRUNCATE TABLE silver.etd_modules;
        TRUNCATE TABLE silver.etd_notes;

        /* ============================================================
           1) ens_calendrier
        ============================================================ */
        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.ens_calendrier';

        INSERT INTO silver.ens_calendrier (
            ens_date,
            ens_jour,
            ens_mois,
            ens_annee,
            ens_trimestre,
            ens_semestre_academique,
            ens_annee_universitaire,
            ens_est_vacances
        )
        SELECT
            c.date_clean AS ens_date,
            DATEPART(DAY, c.date_clean) AS ens_jour,
            DATEPART(MONTH, c.date_clean) AS ens_mois,
            DATEPART(YEAR, c.date_clean) AS ens_annee,
            DATEPART(QUARTER, c.date_clean) AS ens_trimestre,
            CASE
                WHEN DATEPART(MONTH, c.date_clean) BETWEEN 9 AND 12 THEN 'S1'
                ELSE 'S2'
            END AS ens_semestre_academique,
            CASE
                WHEN DATEPART(MONTH, c.date_clean) >= 9
                    THEN CONCAT(DATEPART(YEAR, c.date_clean), '-', DATEPART(YEAR, c.date_clean) + 1)
                ELSE CONCAT(DATEPART(YEAR, c.date_clean) - 1, '-', DATEPART(YEAR, c.date_clean))
            END AS ens_annee_universitaire,
            CASE
                WHEN ((DATEDIFF(DAY, '19000101', c.date_clean) % 7 + 7) % 7) IN (5, 6) THEN 'Oui'
                WHEN UPPER(LTRIM(RTRIM(ISNULL(b.ens_est_vacances, '')))) = 'OUI' THEN 'Oui'
                ELSE 'Non'
            END AS ens_est_vacances
        FROM bronze.ens_calendrier b
        CROSS APPLY (
            SELECT TRY_CONVERT(DATE, LTRIM(RTRIM(b.ens_date))) AS date_clean
        ) c
        WHERE c.date_clean IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        /* ============================================================
           2) ens_enseignants
        ============================================================ */
        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.ens_enseignants';

        INSERT INTO silver.ens_enseignants (
            ens_id_enseignant,
            ens_nom,
            ens_prenom,
            ens_grade,
            ens_specialite,
            ens_departement
        )
        SELECT
            ABS(TRY_CAST(LTRIM(RTRIM(ens_id_enseignant)) AS INT)) AS ens_id_enseignant,
            NULLIF(LTRIM(RTRIM(ens_nom)), '') AS ens_nom,
            NULLIF(LTRIM(RTRIM(ens_prenom)), '') AS ens_prenom,
            NULLIF(LTRIM(RTRIM(ens_grade)), '') AS ens_grade,
            NULLIF(LTRIM(RTRIM(ens_specialite)), '') AS ens_specialite,
            NULLIF(LTRIM(RTRIM(ens_departement)), '') AS ens_departement
        FROM bronze.ens_enseignants
        WHERE TRY_CAST(REPLACE(LTRIM(RTRIM(ens_id_enseignant)), '-', '') AS INT) IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        /* ============================================================
           3) ens_enseignements
        ============================================================ */
        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.ens_enseignements';

        INSERT INTO silver.ens_enseignements (
            ens_id_enseignement,
            ens_id_enseignant,
            ens_id_module,
            ens_annee_universitaire,
            ens_semestre,
            ens_groupe,
            ens_type_enseignement,
            ens_nb_heures_assure
        )
        SELECT
            ABS(TRY_CAST(LTRIM(RTRIM(ens_id_enseignement)) AS INT)) AS ens_id_enseignement,
            ABS(TRY_CAST(LTRIM(RTRIM(ens_id_enseignant)) AS INT)) AS ens_id_enseignant,
            ABS(TRY_CAST(LTRIM(RTRIM(ens_id_module)) AS INT)) AS ens_id_module,
            NULLIF(LTRIM(RTRIM(ens_annee_universitaire)), '') AS ens_annee_universitaire,
            NULLIF(UPPER(LTRIM(RTRIM(ens_semestre))), '') AS ens_semestre,
            NULLIF(UPPER(LTRIM(RTRIM(ens_groupe))), '') AS ens_groupe,
            CASE
                WHEN UPPER(LTRIM(RTRIM(ens_type_enseignement))) = 'TD' THEN 'Travaux Dirigés'
                WHEN UPPER(LTRIM(RTRIM(ens_type_enseignement))) = 'TP' THEN 'Travaux Pratiques'
                ELSE NULLIF(LTRIM(RTRIM(ens_type_enseignement)), '')
            END AS ens_type_enseignement,
            ABS(TRY_CAST(LTRIM(RTRIM(ens_nb_heures_assure)) AS FLOAT)) AS ens_nb_heures_assure
        FROM bronze.ens_enseignements
        WHERE TRY_CAST(REPLACE(LTRIM(RTRIM(ens_id_enseignement)), '-', '') AS INT) IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        /* ============================================================
           4) etd_absences
        ============================================================ */
        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.etd_absences';

        INSERT INTO silver.etd_absences (
            etd_id_absence,
            etd_id_etudiant,
            etd_id_module,
            etd_date_absence,
            etd_type_seance,
            etd_justifiee,
            etd_duree_heures
        )
        SELECT
            ABS(TRY_CAST(LTRIM(RTRIM(etd_id_absence)) AS INT)) AS etd_id_absence,
            ABS(TRY_CAST(LTRIM(RTRIM(etd_id_etudiant)) AS INT)) AS etd_id_etudiant,
            ABS(TRY_CAST(LTRIM(RTRIM(etd_id_module)) AS INT)) AS etd_id_module,
            TRY_CONVERT(DATE, LTRIM(RTRIM(etd_date_absence))) AS etd_date_absence,
            CASE
                WHEN UPPER(LTRIM(RTRIM(etd_type_seance))) = 'TD' THEN 'Travaux Dirigés'
                WHEN UPPER(LTRIM(RTRIM(etd_type_seance))) = 'TP' THEN 'Travaux Pratiques'
                ELSE NULLIF(LTRIM(RTRIM(etd_type_seance)), '')
            END AS etd_type_seance,
            CASE
                WHEN UPPER(LTRIM(RTRIM(etd_justifiee))) = 'OUI' THEN 'Oui'
                WHEN UPPER(LTRIM(RTRIM(etd_justifiee))) = 'NON' THEN 'Non'
                ELSE NULLIF(LTRIM(RTRIM(etd_justifiee)), '')
            END AS etd_justifiee,
            ABS(TRY_CAST(LTRIM(RTRIM(etd_duree_heures)) AS FLOAT)) AS etd_duree_heures
        FROM bronze.etd_absences
        WHERE TRY_CAST(REPLACE(LTRIM(RTRIM(etd_id_absence)), '-', '') AS INT) IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        /* ============================================================
           5) etd_etudiants
        ============================================================ */
        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.etd_etudiants';

        INSERT INTO silver.etd_etudiants (
            etd_id_etudiant,
            etd_cne,
            etd_nom,
            etd_prenom,
            etd_date_naissance,
            etd_sexe,
            etd_id_filiere,
            etd_niveau,
            etd_statut,
            etd_annee_inscription,
            etd_cne_lettre,
            etd_cne_numero
        )
        SELECT
            ABS(TRY_CAST(LTRIM(RTRIM(b.etd_id_etudiant)) AS INT)) AS etd_id_etudiant,
            c.cne_clean AS etd_cne,
            NULLIF(LTRIM(RTRIM(b.etd_nom)), '') AS etd_nom,
            NULLIF(LTRIM(RTRIM(b.etd_prenom)), '') AS etd_prenom,
            TRY_CONVERT(DATE, LTRIM(RTRIM(b.etd_date_naissance))) AS etd_date_naissance,
            CASE
                WHEN UPPER(LTRIM(RTRIM(b.etd_sexe))) = 'M' THEN 'Masculin'
                WHEN UPPER(LTRIM(RTRIM(b.etd_sexe))) = 'F' THEN 'Femelle'
                ELSE NULLIF(LTRIM(RTRIM(b.etd_sexe)), '')
            END AS etd_sexe,
            ABS(TRY_CAST(LTRIM(RTRIM(b.etd_id_filiere)) AS INT)) AS etd_id_filiere,
            NULLIF(LTRIM(RTRIM(b.etd_niveau)), '') AS etd_niveau,
            NULLIF(LTRIM(RTRIM(b.etd_statut)), '') AS etd_statut,
            ABS(TRY_CAST(LTRIM(RTRIM(b.etd_annee_inscription)) AS INT)) AS etd_annee_inscription,
            CASE
                WHEN PATINDEX('%[0-9]%', c.cne_clean) > 1
                    THEN LEFT(c.cne_clean, PATINDEX('%[0-9]%', c.cne_clean) - 1)
                ELSE NULL
            END AS etd_cne_lettre,
            CASE
                WHEN PATINDEX('%[0-9]%', c.cne_clean) > 0
                    THEN SUBSTRING(c.cne_clean, PATINDEX('%[0-9]%', c.cne_clean), LEN(c.cne_clean))
                ELSE NULL
            END AS etd_cne_numero
        FROM bronze.etd_etudiants b
        CROSS APPLY (
            SELECT UPPER(REPLACE(LTRIM(RTRIM(ISNULL(b.etd_cne, ''))), ' ', '')) AS cne_clean
        ) c
        WHERE TRY_CAST(REPLACE(LTRIM(RTRIM(b.etd_id_etudiant)), '-', '') AS INT) IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        /* ============================================================
           6) etd_filieres
        ============================================================ */
        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.etd_filieres';

        ;WITH filieres_clean AS (
            SELECT
                ABS(TRY_CAST(LTRIM(RTRIM(etd_id_filiere)) AS INT)) AS etd_id_filiere,
                NULLIF(UPPER(LTRIM(RTRIM(etd_code_filiere))), '') AS etd_code_filiere,
                NULLIF(LTRIM(RTRIM(etd_nom_filiere)), '') AS etd_nom_filiere,
                CASE
                    WHEN UPPER(LTRIM(RTRIM(etd_type_diplome))) = 'DUT'
                        THEN 'Diplome Universitaire de Technologie'
                    ELSE NULLIF(LTRIM(RTRIM(etd_type_diplome)), '')
                END AS etd_type_diplome,
                ABS(TRY_CAST(LTRIM(RTRIM(etd_duree)) AS INT)) AS etd_duree,
                NULLIF(LTRIM(RTRIM(etd_departement)), '') AS etd_departement,
                ROW_NUMBER() OVER (
                    PARTITION BY ABS(TRY_CAST(LTRIM(RTRIM(etd_id_filiere)) AS INT))
                    ORDER BY LEN(LTRIM(RTRIM(ISNULL(etd_code_filiere, '')))),
                             LEN(LTRIM(RTRIM(ISNULL(etd_nom_filiere, ''))))
                ) AS rn
            FROM bronze.etd_filieres
            WHERE TRY_CAST(REPLACE(LTRIM(RTRIM(etd_id_filiere)), '-', '') AS INT) IS NOT NULL
        )
        INSERT INTO silver.etd_filieres (
            etd_id_filiere,
            etd_code_filiere,
            etd_nom_filiere,
            etd_type_diplome,
            etd_duree,
            etd_departement
        )
        SELECT
            etd_id_filiere,
            etd_code_filiere,
            etd_nom_filiere,
            etd_type_diplome,
            etd_duree,
            etd_departement
        FROM filieres_clean
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        /* ============================================================
           7) etd_modules
        ============================================================ */
        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.etd_modules';

        INSERT INTO silver.etd_modules (
            etd_id_module,
            etd_code_module,
            etd_nom_module,
            etd_id_filiere,
            etd_semestre,
            etd_coefficient,
            etd_volume_horaire,
            etd_type_module
        )
        SELECT
            ABS(TRY_CAST(LTRIM(RTRIM(etd_id_module)) AS INT)) AS etd_id_module,
            NULLIF(UPPER(LTRIM(RTRIM(etd_code_module))), '') AS etd_code_module,
            NULLIF(LTRIM(RTRIM(etd_nom_module)), '') AS etd_nom_module,
            ABS(TRY_CAST(LTRIM(RTRIM(etd_id_filiere)) AS INT)) AS etd_id_filiere,
            CASE
                WHEN UPPER(LTRIM(RTRIM(etd_semestre))) = 'S1' THEN 'Semestre1'
                WHEN UPPER(LTRIM(RTRIM(etd_semestre))) = 'S2' THEN 'Semestre2'
                WHEN UPPER(LTRIM(RTRIM(etd_semestre))) = 'S3' THEN 'Semestre3'
                WHEN UPPER(LTRIM(RTRIM(etd_semestre))) = 'S4' THEN 'Semestre4'
                WHEN UPPER(LTRIM(RTRIM(etd_semestre))) = 'S5' THEN 'Semestre5'
                WHEN UPPER(LTRIM(RTRIM(etd_semestre))) = 'S6' THEN 'Semestre6'
                ELSE NULLIF(LTRIM(RTRIM(etd_semestre)), '')
            END AS etd_semestre,
            ABS(TRY_CAST(LTRIM(RTRIM(etd_coefficient)) AS FLOAT)) AS etd_coefficient,
            ABS(TRY_CAST(LTRIM(RTRIM(etd_volume_horaire)) AS FLOAT)) AS etd_volume_horaire,
            NULLIF(LTRIM(RTRIM(etd_type_module)), '') AS etd_type_module
        FROM bronze.etd_modules
        WHERE TRY_CAST(REPLACE(LTRIM(RTRIM(etd_id_module)), '-', '') AS INT) IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        /* ============================================================
           8) etd_notes
        ============================================================ */
        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.etd_notes';

        INSERT INTO silver.etd_notes (
            etd_id_note,
            etd_id_etudiant,
            etd_id_module,
            etd_id_enseignant,
            etd_annee_universitaire,
            etd_semestre,
            etd_type_examen,
            etd_note,
            etd_date_examen,
            etd_session
        )
        SELECT
            ABS(TRY_CAST(LTRIM(RTRIM(etd_id_note)) AS INT)) AS etd_id_note,
            ABS(TRY_CAST(LTRIM(RTRIM(etd_id_etudiant)) AS INT)) AS etd_id_etudiant,
            ABS(TRY_CAST(LTRIM(RTRIM(etd_id_module)) AS INT)) AS etd_id_module,
            ABS(TRY_CAST(LTRIM(RTRIM(etd_id_enseignant)) AS INT)) AS etd_id_enseignant,
            NULLIF(LTRIM(RTRIM(etd_annee_universitaire)), '') AS etd_annee_universitaire,
            CAST(ABS(TRY_CAST(LTRIM(RTRIM(etd_semestre)) AS INT)) AS VARCHAR(20)) AS etd_semestre,
            CASE
                WHEN UPPER(LTRIM(RTRIM(etd_type_examen))) = 'TD' THEN 'Travaux Dirigés'
                WHEN UPPER(LTRIM(RTRIM(etd_type_examen))) = 'TP' THEN 'Travaux Pratiques'
                ELSE NULLIF(LTRIM(RTRIM(etd_type_examen)), '')
            END AS etd_type_examen,
            ABS(TRY_CAST(LTRIM(RTRIM(etd_note)) AS FLOAT)) AS etd_note,
            TRY_CONVERT(DATE, LTRIM(RTRIM(etd_date_examen))) AS etd_date_examen,
            NULLIF(LTRIM(RTRIM(etd_session)), '') AS etd_session
        FROM bronze.etd_notes
        WHERE TRY_CAST(REPLACE(LTRIM(RTRIM(etd_id_note)), '-', '') AS INT) IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        SET @batch_end_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';
    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';
    END CATCH
END
GO

