-- ============================================================
-- SCRIPT 04 CORRIGÉ : ETL Full Load — Staging → Data Warehouse
-- Correction : ID_Temps unifié via FORMAT(date,'yyyyMMdd')
--              dans Dim_Temps ET dans les tables de faits
-- ============================================================
USE ESTG_DW;
GO

PRINT '── ETL Full Load démarré : ' + CONVERT(VARCHAR, GETDATE(), 120);
GO

-- ════════════════════════════════════════════════════
-- ÉTAPE 0 : Nettoyage du DW (ordre inverse des FK)
-- ════════════════════════════════════════════════════
TRUNCATE TABLE Fait_Notes;
TRUNCATE TABLE Fait_Absences;
DELETE FROM Dim_Etudiant;
DELETE FROM Dim_Module;
DELETE FROM Dim_Examen;
DELETE FROM Dim_Type_Seance;
DELETE FROM Dim_Enseignant;
DELETE FROM Dim_Temps;
DELETE FROM Dim_Filiere;
PRINT '  ✔ DW nettoyé — prêt pour rechargement';
GO

-- ════════════════════════════════════════════════════
-- ÉTAPE 1 : Chargement des Dimensions
-- ════════════════════════════════════════════════════

-- ── Dim_Filiere
INSERT INTO Dim_Filiere (ID_Filiere, Code_Filiere, Nom_Filiere, Type_Diplome, Duree, Departement)
SELECT ID_Filiere, Code_Filiere, Nom_Filiere, Type_Diplome, Duree, Departement
FROM ESTG_Staging.dbo.STG_Filieres WHERE STG_Statut = 'VALIDE';
PRINT '  ✔ Dim_Filiere — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ── Dim_Enseignant
INSERT INTO Dim_Enseignant (ID_Enseignant, Nom, Prenom, Grade, Specialite, Departement)
SELECT ID_Enseignant, Nom, Prenom, Grade, Specialite, Departement
FROM ESTG_Staging.dbo.STG_Enseignants WHERE STG_Statut = 'VALIDE';
PRINT '  ✔ Dim_Enseignant — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ── Dim_Module
INSERT INTO Dim_Module (ID_Module, Code_Module, Nom_Module, ID_Filiere, Semestre, Coefficient, Volume_Horaire, Type_Module)
SELECT ID_Module, Code_Module, Nom_Module, ID_Filiere, Semestre, Coefficient, Volume_Horaire, Type_Module
FROM ESTG_Staging.dbo.STG_Modules WHERE STG_Statut = 'VALIDE';
PRINT '  ✔ Dim_Module — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ── Dim_Etudiant (calcul de l'âge)
INSERT INTO Dim_Etudiant
    (ID_Etudiant, CNE, Nom, Prenom, Date_Naissance, Age, Sexe, Niveau, Statut, Annee_Inscription, ID_Filiere)
SELECT
    ID_Etudiant, CNE, Nom, Prenom,
    TRY_CAST(Date_Naissance AS DATE),
    CASE
        WHEN TRY_CAST(Date_Naissance AS DATE) IS NOT NULL
        THEN DATEDIFF(YEAR, TRY_CAST(Date_Naissance AS DATE), GETDATE())
             - CASE WHEN MONTH(TRY_CAST(Date_Naissance AS DATE))*100
                         + DAY(TRY_CAST(Date_Naissance AS DATE))
                         > MONTH(GETDATE())*100 + DAY(GETDATE()) THEN 1 ELSE 0 END
        ELSE NULL
    END,
    Sexe, Niveau, Statut, Annee_Inscription, ID_Filiere
FROM ESTG_Staging.dbo.STG_Etudiants WHERE STG_Statut = 'VALIDE';
PRINT '  ✔ Dim_Etudiant — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ── Dim_Temps
-- ✅ CORRECTION : ID_Temps = FORMAT(date,'yyyyMMdd') pour cohérence avec les faits
INSERT INTO Dim_Temps
    (ID_Temps, Date, Jour, Mois, Nom_Mois, Annee, Trimestre,
     Semestre_Academique, Annee_Universitaire, Est_Vacances, Jour_Semaine)
SELECT
    CAST(FORMAT(TRY_CAST(Date AS DATE), 'yyyyMMdd') AS INT),  -- ← ex: 20240115
    TRY_CAST(Date AS DATE),
    Jour, Mois,
    CASE Mois
        WHEN 1  THEN N'Janvier'   WHEN 2  THEN N'Février'
        WHEN 3  THEN N'Mars'      WHEN 4  THEN N'Avril'
        WHEN 5  THEN N'Mai'       WHEN 6  THEN N'Juin'
        WHEN 7  THEN N'Juillet'   WHEN 8  THEN N'Août'
        WHEN 9  THEN N'Septembre' WHEN 10 THEN N'Octobre'
        WHEN 11 THEN N'Novembre'  WHEN 12 THEN N'Décembre'
    END,
    Annee, Trimestre, Semestre_Academique, Annee_Universitaire,
    CAST(CASE WHEN Est_Vacances IN ('1','Oui','oui','true','True') THEN 1 ELSE 0 END AS BIT),
    CASE DATEPART(WEEKDAY, TRY_CAST(Date AS DATE))
        WHEN 1 THEN N'Dimanche' WHEN 2 THEN N'Lundi'
        WHEN 3 THEN N'Mardi'    WHEN 4 THEN N'Mercredi'
        WHEN 5 THEN N'Jeudi'    WHEN 6 THEN N'Vendredi'
        WHEN 7 THEN N'Samedi'
    END
FROM ESTG_Staging.dbo.STG_Calendrier
WHERE STG_Statut = 'VALIDE'
  AND TRY_CAST(Date AS DATE) IS NOT NULL;
PRINT '  ✔ Dim_Temps — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ── Dim_Examen
INSERT INTO Dim_Examen (Type_Examen, Session)
SELECT DISTINCT
    TRIM(REPLACE(Type_Examen, CHAR(13), '')),
    TRIM(REPLACE(Session,     CHAR(13), ''))
FROM ESTG_Staging.dbo.STG_Notes WHERE STG_Statut = 'VALIDE';
PRINT '  ✔ Dim_Examen — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ── Dim_Type_Seance
INSERT INTO Dim_Type_Seance (Type_Seance)
SELECT DISTINCT TRIM(REPLACE(Type_Seance, CHAR(13), ''))
FROM ESTG_Staging.dbo.STG_Absences WHERE STG_Statut = 'VALIDE';
PRINT '  ✔ Dim_Type_Seance — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ════════════════════════════════════════════════════
-- ÉTAPE 1.5 : DIAGNOSTIC — vérifier couverture Dim_Temps
-- (optionnel, commenter après validation)
-- ════════════════════════════════════════════════════

-- Dates dans STG_Notes absentes de Dim_Temps
SELECT COUNT(*) AS Notes_Sans_Temps
FROM ESTG_Staging.dbo.STG_Notes n
WHERE n.STG_Statut = 'VALIDE'
  AND TRY_CAST(n.Date_Examen AS DATE) IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM ESTG_DW.dbo.Dim_Temps t
    WHERE t.ID_Temps = CAST(FORMAT(TRY_CAST(n.Date_Examen AS DATE), 'yyyyMMdd') AS INT)
  );
GO

-- Dates dans STG_Absences absentes de Dim_Temps
SELECT COUNT(*) AS Absences_Sans_Temps
FROM ESTG_Staging.dbo.STG_Absences a
WHERE a.STG_Statut = 'VALIDE'
  AND TRY_CAST(a.Date_Absence AS DATE) IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM ESTG_DW.dbo.Dim_Temps t
    WHERE t.ID_Temps = CAST(FORMAT(TRY_CAST(a.Date_Absence AS DATE), 'yyyyMMdd') AS INT)
  );
GO

-- ════════════════════════════════════════════════════
-- ÉTAPE 1.6 : Compléter Dim_Temps avec les dates
--             manquantes (dates d'examens/absences
--             hors calendrier académique)
-- ════════════════════════════════════════════════════

-- Insérer les dates d'examens manquantes dans Dim_Temps
INSERT INTO Dim_Temps
    (ID_Temps, Date, Jour, Mois, Nom_Mois, Annee, Trimestre,
     Semestre_Academique, Annee_Universitaire, Est_Vacances, Jour_Semaine)
SELECT DISTINCT
    CAST(FORMAT(TRY_CAST(n.Date_Examen AS DATE), 'yyyyMMdd') AS INT),
    TRY_CAST(n.Date_Examen AS DATE),
    DAY(TRY_CAST(n.Date_Examen AS DATE)),
    MONTH(TRY_CAST(n.Date_Examen AS DATE)),
    CASE MONTH(TRY_CAST(n.Date_Examen AS DATE))
        WHEN 1  THEN N'Janvier'   WHEN 2  THEN N'Février'
        WHEN 3  THEN N'Mars'      WHEN 4  THEN N'Avril'
        WHEN 5  THEN N'Mai'       WHEN 6  THEN N'Juin'
        WHEN 7  THEN N'Juillet'   WHEN 8  THEN N'Août'
        WHEN 9  THEN N'Septembre' WHEN 10 THEN N'Octobre'
        WHEN 11 THEN N'Novembre'  WHEN 12 THEN N'Décembre'
    END,
    YEAR(TRY_CAST(n.Date_Examen AS DATE)),
    DATEPART(QUARTER, TRY_CAST(n.Date_Examen AS DATE)),
    CASE WHEN MONTH(TRY_CAST(n.Date_Examen AS DATE)) BETWEEN 9 AND 12 THEN 'S1'
         WHEN MONTH(TRY_CAST(n.Date_Examen AS DATE)) BETWEEN 1 AND  6 THEN 'S2'
         ELSE 'S1' END,
    CASE WHEN MONTH(TRY_CAST(n.Date_Examen AS DATE)) >= 9
         THEN CAST(YEAR(TRY_CAST(n.Date_Examen AS DATE))   AS VARCHAR)
            + '-' + CAST(YEAR(TRY_CAST(n.Date_Examen AS DATE))+1 AS VARCHAR)
         ELSE CAST(YEAR(TRY_CAST(n.Date_Examen AS DATE))-1 AS VARCHAR)
            + '-' + CAST(YEAR(TRY_CAST(n.Date_Examen AS DATE))   AS VARCHAR)
    END,
    0,  -- Est_Vacances = 0 par défaut pour les dates d'examen
    CASE DATEPART(WEEKDAY, TRY_CAST(n.Date_Examen AS DATE))
        WHEN 1 THEN N'Dimanche' WHEN 2 THEN N'Lundi'
        WHEN 3 THEN N'Mardi'    WHEN 4 THEN N'Mercredi'
        WHEN 5 THEN N'Jeudi'    WHEN 6 THEN N'Vendredi'
        WHEN 7 THEN N'Samedi'
    END
FROM ESTG_Staging.dbo.STG_Notes n
WHERE n.STG_Statut = 'VALIDE'
  AND TRY_CAST(n.Date_Examen AS DATE) IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM Dim_Temps t
    WHERE t.ID_Temps = CAST(FORMAT(TRY_CAST(n.Date_Examen AS DATE), 'yyyyMMdd') AS INT)
  );
PRINT '  ✔ Dim_Temps complété (dates examens) — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes ajoutées';
GO

-- Insérer les dates d'absences manquantes dans Dim_Temps
INSERT INTO Dim_Temps
    (ID_Temps, Date, Jour, Mois, Nom_Mois, Annee, Trimestre,
     Semestre_Academique, Annee_Universitaire, Est_Vacances, Jour_Semaine)
SELECT DISTINCT
    CAST(FORMAT(TRY_CAST(a.Date_Absence AS DATE), 'yyyyMMdd') AS INT),
    TRY_CAST(a.Date_Absence AS DATE),
    DAY(TRY_CAST(a.Date_Absence AS DATE)),
    MONTH(TRY_CAST(a.Date_Absence AS DATE)),
    CASE MONTH(TRY_CAST(a.Date_Absence AS DATE))
        WHEN 1  THEN N'Janvier'   WHEN 2  THEN N'Février'
        WHEN 3  THEN N'Mars'      WHEN 4  THEN N'Avril'
        WHEN 5  THEN N'Mai'       WHEN 6  THEN N'Juin'
        WHEN 7  THEN N'Juillet'   WHEN 8  THEN N'Août'
        WHEN 9  THEN N'Septembre' WHEN 10 THEN N'Octobre'
        WHEN 11 THEN N'Novembre'  WHEN 12 THEN N'Décembre'
    END,
    YEAR(TRY_CAST(a.Date_Absence AS DATE)),
    DATEPART(QUARTER, TRY_CAST(a.Date_Absence AS DATE)),
    CASE WHEN MONTH(TRY_CAST(a.Date_Absence AS DATE)) BETWEEN 9 AND 12 THEN 'S1'
         WHEN MONTH(TRY_CAST(a.Date_Absence AS DATE)) BETWEEN 1 AND  6 THEN 'S2'
         ELSE 'S1' END,
    CASE WHEN MONTH(TRY_CAST(a.Date_Absence AS DATE)) >= 9
         THEN CAST(YEAR(TRY_CAST(a.Date_Absence AS DATE))   AS VARCHAR)
            + '-' + CAST(YEAR(TRY_CAST(a.Date_Absence AS DATE))+1 AS VARCHAR)
         ELSE CAST(YEAR(TRY_CAST(a.Date_Absence AS DATE))-1 AS VARCHAR)
            + '-' + CAST(YEAR(TRY_CAST(a.Date_Absence AS DATE))   AS VARCHAR)
    END,
    0,  -- Est_Vacances = 0 par défaut
    CASE DATEPART(WEEKDAY, TRY_CAST(a.Date_Absence AS DATE))
        WHEN 1 THEN N'Dimanche' WHEN 2 THEN N'Lundi'
        WHEN 3 THEN N'Mardi'    WHEN 4 THEN N'Mercredi'
        WHEN 5 THEN N'Jeudi'    WHEN 6 THEN N'Vendredi'
        WHEN 7 THEN N'Samedi'
    END
FROM ESTG_Staging.dbo.STG_Absences a
WHERE a.STG_Statut = 'VALIDE'
  AND TRY_CAST(a.Date_Absence AS DATE) IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM Dim_Temps t
    WHERE t.ID_Temps = CAST(FORMAT(TRY_CAST(a.Date_Absence AS DATE), 'yyyyMMdd') AS INT)
  );
PRINT '  ✔ Dim_Temps complété (dates absences) — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes ajoutées';
GO

-- ════════════════════════════════════════════════════
-- ÉTAPE 2 : Chargement des Tables de Faits
-- ════════════════════════════════════════════════════

-- ── Fait_Notes
INSERT INTO Fait_Notes
    (ID_Temps, ID_Etudiant, ID_Module, ID_Enseignant, ID_Examen, Annee_Universitaire, Semestre, Note)
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
  AND TRY_CAST(n.Date_Examen AS DATE) IS NOT NULL;
PRINT '  ✔ Fait_Notes — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ── Fait_Absences
INSERT INTO Fait_Absences
    (ID_Temps, ID_Etudiant, ID_Module, ID_Type_Seance, Annee_Universitaire, Duree_Heures, Justifiee)
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
  AND TRY_CAST(a.Date_Absence AS DATE) IS NOT NULL;
PRINT '  ✔ Fait_Absences — ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ════════════════════════════════════════════════════
-- ÉTAPE 3 : Vérification finale
-- ════════════════════════════════════════════════════
SELECT 'Dim_Filiere'      AS [Table], COUNT(*) AS Lignes FROM Dim_Filiere      UNION ALL
SELECT 'Dim_Etudiant',    COUNT(*) FROM Dim_Etudiant     UNION ALL
SELECT 'Dim_Enseignant',  COUNT(*) FROM Dim_Enseignant   UNION ALL
SELECT 'Dim_Module',      COUNT(*) FROM Dim_Module       UNION ALL
SELECT 'Dim_Temps',       COUNT(*) FROM Dim_Temps        UNION ALL
SELECT 'Dim_Examen',      COUNT(*) FROM Dim_Examen       UNION ALL
SELECT 'Dim_Type_Seance', COUNT(*) FROM Dim_Type_Seance  UNION ALL
SELECT 'Fait_Notes',      COUNT(*) FROM Fait_Notes       UNION ALL
SELECT 'Fait_Absences',   COUNT(*) FROM Fait_Absences;
GO

PRINT '── ETL Full Load terminé : ' + CONVERT(VARCHAR, GETDATE(), 120);
GO