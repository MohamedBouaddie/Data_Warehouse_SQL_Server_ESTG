-- ============================================================
-- SCRIPT 05 : ETL Incrémental — Staging → Data Warehouse
--             Méthode MERGE (upsert) + NOT EXISTS
-- Projet     : Business Intelligence — ESTG
-- Usage      : Production — rechargement sans perte de données
-- ============================================================
USE ESTG_DW;
GO

PRINT '── ETL Incrémental démarré : ' + CONVERT(VARCHAR, GETDATE(), 120);
GO

-- ════════════════════════════════════════════════════
-- 1. Dim_Filiere
-- ════════════════════════════════════════════════════
MERGE Dim_Filiere AS cible
USING (
    SELECT ID_Filiere, Code_Filiere, Nom_Filiere, Type_Diplome, Duree, Departement
    FROM ESTG_Staging.dbo.STG_Filieres WHERE STG_Statut = 'VALIDE'
) AS source ON cible.ID_Filiere = source.ID_Filiere
WHEN MATCHED THEN
    UPDATE SET Nom_Filiere  = source.Nom_Filiere,
               Type_Diplome = source.Type_Diplome,
               Departement  = source.Departement
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ID_Filiere, Code_Filiere, Nom_Filiere, Type_Diplome, Duree, Departement)
    VALUES (source.ID_Filiere, source.Code_Filiere, source.Nom_Filiere,
            source.Type_Diplome, source.Duree, source.Departement);
PRINT '  ✔ Dim_Filiere — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ════════════════════════════════════════════════════
-- 2. Dim_Enseignant
-- ════════════════════════════════════════════════════
MERGE Dim_Enseignant AS cible
USING (
    SELECT ID_Enseignant, Nom, Prenom, Grade, Specialite, Departement
    FROM ESTG_Staging.dbo.STG_Enseignants WHERE STG_Statut = 'VALIDE'
) AS source ON cible.ID_Enseignant = source.ID_Enseignant
WHEN MATCHED THEN
    UPDATE SET Grade      = source.Grade,
               Specialite = source.Specialite
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ID_Enseignant, Nom, Prenom, Grade, Specialite, Departement)
    VALUES (source.ID_Enseignant, source.Nom, source.Prenom,
            source.Grade, source.Specialite, source.Departement);
PRINT '  ✔ Dim_Enseignant — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ════════════════════════════════════════════════════
-- 3. Dim_Module
-- ════════════════════════════════════════════════════
MERGE Dim_Module AS cible
USING (
    SELECT ID_Module, Code_Module, Nom_Module, ID_Filiere,
           Semestre, Coefficient, Volume_Horaire, Type_Module
    FROM ESTG_Staging.dbo.STG_Modules WHERE STG_Statut = 'VALIDE'
) AS source ON cible.ID_Module = source.ID_Module
WHEN MATCHED THEN
    UPDATE SET Coefficient    = source.Coefficient,
               Volume_Horaire = source.Volume_Horaire,
               Type_Module    = source.Type_Module
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ID_Module, Code_Module, Nom_Module, ID_Filiere,
            Semestre, Coefficient, Volume_Horaire, Type_Module)
    VALUES (source.ID_Module, source.Code_Module, source.Nom_Module,
            source.ID_Filiere, source.Semestre, source.Coefficient,
            source.Volume_Horaire, source.Type_Module);
PRINT '  ✔ Dim_Module — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ════════════════════════════════════════════════════
-- 4. Dim_Etudiant
-- ════════════════════════════════════════════════════
MERGE Dim_Etudiant AS cible
USING (
    SELECT
        ID_Etudiant, CNE, Nom, Prenom,
        TRY_CAST(Date_Naissance AS DATE) AS Date_Naissance,
        DATEDIFF(YEAR, TRY_CAST(Date_Naissance AS DATE), GETDATE())
        - CASE WHEN MONTH(TRY_CAST(Date_Naissance AS DATE))*100
                    + DAY(TRY_CAST(Date_Naissance AS DATE))
                    > MONTH(GETDATE())*100 + DAY(GETDATE()) THEN 1 ELSE 0 END AS Age,
        Sexe, Niveau, Statut, Annee_Inscription, ID_Filiere
    FROM ESTG_Staging.dbo.STG_Etudiants WHERE STG_Statut = 'VALIDE'
) AS source ON cible.ID_Etudiant = source.ID_Etudiant
WHEN MATCHED THEN
    UPDATE SET Niveau  = source.Niveau,
               Statut  = source.Statut,
               Age     = source.Age
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ID_Etudiant, CNE, Nom, Prenom, Date_Naissance, Age,
            Sexe, Niveau, Statut, Annee_Inscription, ID_Filiere)
    VALUES (source.ID_Etudiant, source.CNE, source.Nom, source.Prenom,
            source.Date_Naissance, source.Age, source.Sexe, source.Niveau,
            source.Statut, source.Annee_Inscription, source.ID_Filiere);
PRINT '  ✔ Dim_Etudiant — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ════════════════════════════════════════════════════
-- 5. Dim_Temps
-- ════════════════════════════════════════════════════
MERGE Dim_Temps AS cible
USING (
    SELECT
        CAST(FORMAT(TRY_CAST(Date AS DATE), 'yyyyMMdd') AS INT) AS ID_Temps,
        TRY_CAST(Date AS DATE) AS Date,
        Jour, Mois,
        CASE Mois
            WHEN 1  THEN N'Janvier'   WHEN 2  THEN N'Février'
            WHEN 3  THEN N'Mars'      WHEN 4  THEN N'Avril'
            WHEN 5  THEN N'Mai'       WHEN 6  THEN N'Juin'
            WHEN 7  THEN N'Juillet'   WHEN 8  THEN N'Août'
            WHEN 9  THEN N'Septembre' WHEN 10 THEN N'Octobre'
            WHEN 11 THEN N'Novembre'  WHEN 12 THEN N'Décembre'
        END AS Nom_Mois,
        Annee, Trimestre, Semestre_Academique, Annee_Universitaire,
        CAST(CASE WHEN Est_Vacances IN ('1','Oui','oui','true','True') THEN 1 ELSE 0 END AS BIT) AS Est_Vacances,
        CASE DATEPART(WEEKDAY, TRY_CAST(Date AS DATE))
            WHEN 1 THEN N'Dimanche' WHEN 2 THEN N'Lundi'
            WHEN 3 THEN N'Mardi'    WHEN 4 THEN N'Mercredi'
            WHEN 5 THEN N'Jeudi'    WHEN 6 THEN N'Vendredi'
            WHEN 7 THEN N'Samedi'
        END AS Jour_Semaine
    FROM ESTG_Staging.dbo.STG_Calendrier
    WHERE STG_Statut = 'VALIDE' AND TRY_CAST(Date AS DATE) IS NOT NULL
) AS source ON cible.ID_Temps = source.ID_Temps
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ID_Temps, Date, Jour, Mois, Nom_Mois, Annee, Trimestre,
            Semestre_Academique, Annee_Universitaire, Est_Vacances, Jour_Semaine)
    VALUES (source.ID_Temps, source.Date, source.Jour, source.Mois,
            source.Nom_Mois, source.Annee, source.Trimestre,
            source.Semestre_Academique, source.Annee_Universitaire,
            source.Est_Vacances, source.Jour_Semaine);
PRINT '  ✔ Dim_Temps — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ════════════════════════════════════════════════════
-- 6. Dim_Examen
-- ════════════════════════════════════════════════════
INSERT INTO Dim_Examen (Type_Examen, Session)
SELECT DISTINCT
    TRIM(REPLACE(Type_Examen, CHAR(13), '')),
    TRIM(REPLACE(Session,     CHAR(13), ''))
FROM ESTG_Staging.dbo.STG_Notes
WHERE STG_Statut = 'VALIDE'
  AND NOT EXISTS (
    SELECT 1 FROM Dim_Examen dx
    WHERE dx.Type_Examen = TRIM(REPLACE(STG_Notes.Type_Examen, CHAR(13), ''))
      AND dx.Session     = TRIM(REPLACE(STG_Notes.Session,     CHAR(13), ''))
  );
PRINT '  ✔ Dim_Examen — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ════════════════════════════════════════════════════
-- 7. Dim_Type_Seance
-- ════════════════════════════════════════════════════
INSERT INTO Dim_Type_Seance (Type_Seance)
SELECT DISTINCT TRIM(REPLACE(Type_Seance, CHAR(13), ''))
FROM ESTG_Staging.dbo.STG_Absences
WHERE STG_Statut = 'VALIDE'
  AND NOT EXISTS (
    SELECT 1 FROM Dim_Type_Seance ds
    WHERE ds.Type_Seance = TRIM(REPLACE(STG_Absences.Type_Seance, CHAR(13), ''))
  );
PRINT '  ✔ Dim_Type_Seance — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ════════════════════════════════════════════════════
-- 8. Fait_Notes (avec Annee_Universitaire + Semestre)
-- ════════════════════════════════════════════════════
INSERT INTO Fait_Notes
    (ID_Temps, ID_Etudiant, ID_Module, ID_Enseignant,
     ID_Examen, Annee_Universitaire, Semestre, Note)
SELECT
    CAST(FORMAT(TRY_CAST(n.Date_Examen AS DATE), 'yyyyMMdd') AS INT),
    n.ID_Etudiant, n.ID_Module, n.ID_Enseignant,
    x.ID_Examen,
    n.Annee_Universitaire,
    n.Semestre,
    n.Note
FROM ESTG_Staging.dbo.STG_Notes n
JOIN Dim_Examen x
    ON x.Type_Examen = TRIM(REPLACE(n.Type_Examen, CHAR(13), ''))
   AND x.Session     = TRIM(REPLACE(n.Session,     CHAR(13), ''))
WHERE n.STG_Statut = 'VALIDE'
  AND TRY_CAST(n.Date_Examen AS DATE) IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM Fait_Notes fn
    WHERE fn.ID_Etudiant = n.ID_Etudiant
      AND fn.ID_Module   = n.ID_Module
      AND fn.ID_Examen   = x.ID_Examen
      AND fn.ID_Temps    = CAST(FORMAT(TRY_CAST(n.Date_Examen AS DATE), 'yyyyMMdd') AS INT)
  );
PRINT '  ✔ Fait_Notes — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ════════════════════════════════════════════════════
-- 9. Fait_Absences (avec Annee_Universitaire calculée)
-- ════════════════════════════════════════════════════
INSERT INTO Fait_Absences
    (ID_Temps, ID_Etudiant, ID_Module, ID_Type_Seance,
     Annee_Universitaire, Duree_Heures, Justifiee)
SELECT
    CAST(FORMAT(TRY_CAST(a.Date_Absence AS DATE), 'yyyyMMdd') AS INT),
    a.ID_Etudiant, a.ID_Module,
    s.ID_Type_Seance,
    CASE
        WHEN MONTH(TRY_CAST(a.Date_Absence AS DATE)) >= 9
        THEN CAST(YEAR(TRY_CAST(a.Date_Absence AS DATE))   AS VARCHAR)
           + '-' + CAST(YEAR(TRY_CAST(a.Date_Absence AS DATE))+1 AS VARCHAR)
        ELSE CAST(YEAR(TRY_CAST(a.Date_Absence AS DATE))-1 AS VARCHAR)
           + '-' + CAST(YEAR(TRY_CAST(a.Date_Absence AS DATE))   AS VARCHAR)
    END,
    TRY_CAST(REPLACE(TRIM(a.Duree_Heures), CHAR(13), '') AS DECIMAL(4,1)),
    TRIM(REPLACE(a.Justifiee, CHAR(13), ''))
FROM ESTG_Staging.dbo.STG_Absences a
JOIN Dim_Type_Seance s ON s.Type_Seance = TRIM(REPLACE(a.Type_Seance, CHAR(13), ''))
WHERE a.STG_Statut = 'VALIDE'
  AND TRY_CAST(a.Date_Absence AS DATE) IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM Fait_Absences fa
    WHERE fa.ID_Etudiant    = a.ID_Etudiant
      AND fa.ID_Module      = a.ID_Module
      AND fa.ID_Type_Seance = s.ID_Type_Seance
      AND fa.ID_Temps       = CAST(FORMAT(TRY_CAST(a.Date_Absence AS DATE), 'yyyyMMdd') AS INT)
  );
PRINT '  ✔ Fait_Absences — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

PRINT '── ETL Incrémental terminé : ' + CONVERT(VARCHAR, GETDATE(), 120);
GO
