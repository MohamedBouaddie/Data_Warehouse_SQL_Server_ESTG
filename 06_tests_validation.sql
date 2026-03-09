-- ============================================================
-- SCRIPT 06 : Tests de validation du Data Warehouse
-- Projet     : Business Intelligence — ESTG
-- Usage      : Exécuter après chaque ETL pour vérifier l'intégrité
-- ============================================================
USE ESTG_DW;
GO

PRINT '════════════════════════════════════════';
PRINT '   TESTS DE VALIDATION — ESTG_DW';
PRINT '   ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '════════════════════════════════════════';
GO

-- ════════════════════════════════════════════════════
-- T01 : Comptage global de toutes les tables
-- ════════════════════════════════════════════════════
PRINT '── T01 : Comptage des lignes par table';
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

-- ════════════════════════════════════════════════════
-- T02 : Notes hors plage [0-20]
-- ════════════════════════════════════════════════════
PRINT '── T02 : Notes hors plage [0-20]';
SELECT COUNT(*) AS Notes_Invalides,
       CASE WHEN COUNT(*) = 0 THEN '✔ OK' ELSE '✘ ERREUR' END AS Statut
FROM Fait_Notes WHERE Note < 0 OR Note > 20;
GO

-- ════════════════════════════════════════════════════
-- T03 : Durées d'absence invalides (<= 0)
-- ════════════════════════════════════════════════════
PRINT '── T03 : Durées absences invalides';
SELECT COUNT(*) AS Absences_Invalides,
       CASE WHEN COUNT(*) = 0 THEN '✔ OK' ELSE '✘ ERREUR' END AS Statut
FROM Fait_Absences WHERE Duree_Heures <= 0;
GO

-- ════════════════════════════════════════════════════
-- T04 : Clés étrangères orphelines dans Fait_Notes
-- ════════════════════════════════════════════════════
PRINT '── T04 : FK orphelines dans Fait_Notes';
SELECT
    SUM(CASE WHEN t.ID_Temps       IS NULL THEN 1 ELSE 0 END) AS Temps_Orphelins,
    SUM(CASE WHEN e.ID_Etudiant    IS NULL THEN 1 ELSE 0 END) AS Etudiants_Orphelins,
    SUM(CASE WHEN m.ID_Module      IS NULL THEN 1 ELSE 0 END) AS Modules_Orphelins,
    SUM(CASE WHEN en.ID_Enseignant IS NULL THEN 1 ELSE 0 END) AS Enseignants_Orphelins,
    SUM(CASE WHEN x.ID_Examen      IS NULL THEN 1 ELSE 0 END) AS Examens_Orphelins
FROM Fait_Notes fn
LEFT JOIN Dim_Temps      t  ON t.ID_Temps      = fn.ID_Temps
LEFT JOIN Dim_Etudiant   e  ON e.ID_Etudiant   = fn.ID_Etudiant
LEFT JOIN Dim_Module     m  ON m.ID_Module     = fn.ID_Module
LEFT JOIN Dim_Enseignant en ON en.ID_Enseignant = fn.ID_Enseignant
LEFT JOIN Dim_Examen     x  ON x.ID_Examen     = fn.ID_Examen;
GO

-- ════════════════════════════════════════════════════
-- T05 : Clés étrangères orphelines dans Fait_Absences
-- ════════════════════════════════════════════════════
PRINT '── T05 : FK orphelines dans Fait_Absences';
SELECT
    SUM(CASE WHEN t.ID_Temps       IS NULL THEN 1 ELSE 0 END) AS Temps_Orphelins,
    SUM(CASE WHEN e.ID_Etudiant    IS NULL THEN 1 ELSE 0 END) AS Etudiants_Orphelins,
    SUM(CASE WHEN m.ID_Module      IS NULL THEN 1 ELSE 0 END) AS Modules_Orphelins,
    SUM(CASE WHEN s.ID_Type_Seance IS NULL THEN 1 ELSE 0 END) AS Seances_Orphelines
FROM Fait_Absences fa
LEFT JOIN Dim_Temps       t ON t.ID_Temps       = fa.ID_Temps
LEFT JOIN Dim_Etudiant    e ON e.ID_Etudiant    = fa.ID_Etudiant
LEFT JOIN Dim_Module      m ON m.ID_Module      = fa.ID_Module
LEFT JOIN Dim_Type_Seance s ON s.ID_Type_Seance = fa.ID_Type_Seance;
GO

-- ════════════════════════════════════════════════════
-- T06 : Répartition étudiants par filière et sexe
-- ════════════════════════════════════════════════════
PRINT '── T06 : Répartition étudiants par filière';
SELECT
    f.Nom_Filiere,
    f.Type_Diplome,
    COUNT(e.ID_Etudiant)                                       AS Total,
    SUM(CASE WHEN e.Sexe = 'F' THEN 1 ELSE 0 END)             AS Femmes,
    SUM(CASE WHEN e.Sexe = 'M' THEN 1 ELSE 0 END)             AS Hommes,
    CAST(SUM(CASE WHEN e.Sexe = 'F' THEN 1.0 ELSE 0 END)
         / NULLIF(COUNT(e.ID_Etudiant),0) * 100 AS DECIMAL(5,1)) AS Pct_Femmes
FROM Dim_Etudiant e
JOIN Dim_Filiere f ON f.ID_Filiere = e.ID_Filiere
GROUP BY f.Nom_Filiere, f.Type_Diplome
ORDER BY Total DESC;
GO

-- ════════════════════════════════════════════════════
-- T07 : Moyenne des notes par filière et semestre
-- ════════════════════════════════════════════════════
PRINT '── T07 : Moyennes par filière et semestre';
SELECT
    f.Nom_Filiere,
    fn.Semestre,
    fn.Annee_Universitaire,
    COUNT(*)                              AS Nb_Notes,
    CAST(AVG(fn.Note) AS DECIMAL(5,2))    AS Moyenne,
    CAST(MIN(fn.Note) AS DECIMAL(5,2))    AS Min_Note,
    CAST(MAX(fn.Note) AS DECIMAL(5,2))    AS Max_Note
FROM Fait_Notes fn
JOIN Dim_Etudiant e ON e.ID_Etudiant = fn.ID_Etudiant
JOIN Dim_Filiere  f ON f.ID_Filiere  = e.ID_Filiere
GROUP BY f.Nom_Filiere, fn.Semestre, fn.Annee_Universitaire
ORDER BY f.Nom_Filiere, fn.Annee_Universitaire, fn.Semestre;
GO

-- ════════════════════════════════════════════════════
-- T08 : Top 10 étudiants les plus absents
-- ════════════════════════════════════════════════════
PRINT '── T08 : Top 10 absences';
SELECT TOP 10
    e.CNE,
    e.Nom + ' ' + e.Prenom                                    AS Etudiant,
    f.Nom_Filiere,
    COUNT(*)                                                   AS Nb_Absences,
    SUM(fa.Duree_Heures)                                       AS Total_Heures,
    SUM(CASE WHEN fa.Justifiee = 'Non' THEN 1 ELSE 0 END)     AS Non_Justifiees
FROM Fait_Absences fa
JOIN Dim_Etudiant e ON e.ID_Etudiant = fa.ID_Etudiant
JOIN Dim_Filiere  f ON f.ID_Filiere  = e.ID_Filiere
GROUP BY e.CNE, e.Nom, e.Prenom, f.Nom_Filiere
ORDER BY Total_Heures DESC;
GO

PRINT '════ TESTS DE VALIDATION TERMINÉS ════';
GO
