/*/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1; -- Select the most recent record per customer
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading crm_sales_details
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading erp_cust_az12
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

        -- Loading erp_loc_a101
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
		-- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
*/

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

