-- ============================================================
-- SCRIPT 07 : Vues analytiques pour Power BI — KPIs ESTG
-- Projet     : Business Intelligence — ESTG
-- Usage      : Connecter Power BI directement sur ces vues
-- ============================================================
USE ESTG_DW;
GO

-- ════════════════════════════════════════════════════
-- VUE 1 : KPI_Taux_Reussite
-- Taux de réussite par filière, semestre, session, année
-- → KPI : Taux de réussite global / par filière / par année
-- ════════════════════════════════════════════════════
CREATE OR ALTER VIEW vw_KPI_Taux_Reussite AS
SELECT
    f.Nom_Filiere,
    f.Type_Diplome,
    f.Departement,
    fn.Annee_Universitaire,
    fn.Semestre,
    x.Type_Examen,
    x.Session,
    COUNT(*)                                                         AS Nb_Evaluations,
    SUM(CASE WHEN fn.Note >= 10 THEN 1 ELSE 0 END)                  AS Nb_Reussis,
    SUM(CASE WHEN fn.Note < 10  THEN 1 ELSE 0 END)                  AS Nb_Echoues,
    CAST(AVG(fn.Note) AS DECIMAL(5,2))                               AS Moyenne_Generale,
    CAST(SUM(CASE WHEN fn.Note >= 10 THEN 1.0 ELSE 0 END)
         / NULLIF(COUNT(*), 0) * 100 AS DECIMAL(5,1))               AS Taux_Reussite_Pct,
    CAST(SUM(CASE WHEN fn.Note >= 16 THEN 1.0 ELSE 0 END)
         / NULLIF(COUNT(*), 0) * 100 AS DECIMAL(5,1))               AS Taux_Mention_TB_Pct,
    CAST(SUM(CASE WHEN fn.Note >= 14 AND fn.Note < 16 THEN 1.0 ELSE 0 END)
         / NULLIF(COUNT(*), 0) * 100 AS DECIMAL(5,1))               AS Taux_Mention_B_Pct
FROM Fait_Notes fn
JOIN Dim_Etudiant e ON e.ID_Etudiant = fn.ID_Etudiant
JOIN Dim_Filiere  f ON f.ID_Filiere  = e.ID_Filiere
JOIN Dim_Examen   x ON x.ID_Examen   = fn.ID_Examen
GROUP BY f.Nom_Filiere, f.Type_Diplome, f.Departement,
         fn.Annee_Universitaire, fn.Semestre, x.Type_Examen, x.Session;
GO

-- ════════════════════════════════════════════════════
-- VUE 2 : KPI_Absences
-- Analyse des absences par étudiant / filière / module / mois
-- → KPI : Taux d'absentéisme / Heures perdues / Justification
-- ════════════════════════════════════════════════════
CREATE OR ALTER VIEW vw_KPI_Absences AS
SELECT
    e.CNE,
    e.Nom + ' ' + e.Prenom                                          AS Etudiant,
    e.Sexe,
    e.Niveau,
    e.Statut,
    f.Nom_Filiere,
    f.Type_Diplome,
    f.Departement,
    m.Nom_Module,
    m.Type_Module,
    s.Type_Seance,
    t.Annee_Universitaire,
    t.Semestre_Academique,
    t.Nom_Mois,
    t.Mois,
    t.Annee,
    t.Trimestre,
    t.Jour_Semaine,
    t.Est_Vacances,
    fa.Duree_Heures,
    fa.Justifiee,
    CASE WHEN fa.Justifiee = 'Oui' THEN fa.Duree_Heures ELSE 0 END  AS Heures_Justifiees,
    CASE WHEN fa.Justifiee = 'Non' THEN fa.Duree_Heures ELSE 0 END  AS Heures_Non_Justifiees
FROM Fait_Absences fa
JOIN Dim_Etudiant    e ON e.ID_Etudiant    = fa.ID_Etudiant
JOIN Dim_Filiere     f ON f.ID_Filiere     = e.ID_Filiere
JOIN Dim_Module      m ON m.ID_Module      = fa.ID_Module
JOIN Dim_Type_Seance s ON s.ID_Type_Seance = fa.ID_Type_Seance
JOIN Dim_Temps       t ON t.ID_Temps       = fa.ID_Temps;
GO

-- ════════════════════════════════════════════════════
-- VUE 3 : KPI_Performance_Etudiants
-- Performance individuelle par étudiant
-- → KPI : Moyenne par étudiant / Classement / Profil de risque
-- ════════════════════════════════════════════════════
CREATE OR ALTER VIEW vw_KPI_Performance_Etudiants AS
SELECT
    e.ID_Etudiant,
    e.CNE,
    e.Nom + ' ' + e.Prenom                                          AS Etudiant,
    e.Sexe,
    e.Niveau,
    e.Statut,
    e.Age,
    e.Annee_Inscription,
    f.Nom_Filiere,
    f.Type_Diplome,
    f.Departement,
    fn.Annee_Universitaire,
    fn.Semestre,
    COUNT(fn.ID_Fait_Note)                                          AS Nb_Notes,
    CAST(AVG(fn.Note) AS DECIMAL(5,2))                              AS Moyenne,
    CAST(MIN(fn.Note) AS DECIMAL(5,2))                              AS Note_Min,
    CAST(MAX(fn.Note) AS DECIMAL(5,2))                              AS Note_Max,
    SUM(CASE WHEN fn.Note >= 10 THEN 1 ELSE 0 END)                 AS Modules_Valides,
    SUM(CASE WHEN fn.Note < 10  THEN 1 ELSE 0 END)                 AS Modules_Echoues,
    -- Total heures d'absence
    ISNULL((
        SELECT SUM(fa2.Duree_Heures)
        FROM Fait_Absences fa2
        WHERE fa2.ID_Etudiant = e.ID_Etudiant
          AND fa2.Annee_Universitaire = fn.Annee_Universitaire
    ), 0)                                                           AS Total_H_Absences,
    -- Profil de risque
    CASE
        WHEN AVG(fn.Note) >= 14 THEN 'Excellent'
        WHEN AVG(fn.Note) >= 12 THEN 'Bien'
        WHEN AVG(fn.Note) >= 10 THEN 'Passable'
        WHEN AVG(fn.Note) >= 8  THEN 'A risque'
        ELSE 'En difficulte'
    END                                                             AS Profil_Academique
FROM Fait_Notes fn
JOIN Dim_Etudiant e ON e.ID_Etudiant = fn.ID_Etudiant
JOIN Dim_Filiere  f ON f.ID_Filiere  = e.ID_Filiere
GROUP BY e.ID_Etudiant, e.CNE, e.Nom, e.Prenom, e.Sexe,
         e.Niveau, e.Statut, e.Age, e.Annee_Inscription,
         f.Nom_Filiere, f.Type_Diplome, f.Departement,
         fn.Annee_Universitaire, fn.Semestre;
GO

-- ════════════════════════════════════════════════════
-- VUE 4 : KPI_Charge_Enseignants
-- Charge pédagogique par enseignant
-- → KPI : Modules enseignés / Nb étudiants / Moyenne obtenue
-- ════════════════════════════════════════════════════
CREATE OR ALTER VIEW vw_KPI_Charge_Enseignants AS
SELECT
    en.ID_Enseignant,
    en.Nom + ' ' + en.Prenom                                        AS Enseignant,
    en.Grade,
    en.Specialite,
    en.Departement,
    m.Nom_Module,
    m.Type_Module,
    m.Volume_Horaire,
    m.Coefficient,
    f.Nom_Filiere,
    fn.Annee_Universitaire,
    fn.Semestre,
    COUNT(DISTINCT fn.ID_Etudiant)                                  AS Nb_Etudiants,
    COUNT(fn.ID_Fait_Note)                                          AS Nb_Evaluations,
    CAST(AVG(fn.Note) AS DECIMAL(5,2))                              AS Moyenne_Classe,
    CAST(SUM(CASE WHEN fn.Note >= 10 THEN 1.0 ELSE 0 END)
         / NULLIF(COUNT(*), 0) * 100 AS DECIMAL(5,1))              AS Taux_Reussite_Pct
FROM Fait_Notes fn
JOIN Dim_Enseignant en ON en.ID_Enseignant = fn.ID_Enseignant
JOIN Dim_Module     m  ON m.ID_Module      = fn.ID_Module
JOIN Dim_Etudiant   e  ON e.ID_Etudiant    = fn.ID_Etudiant
JOIN Dim_Filiere    f  ON f.ID_Filiere     = e.ID_Filiere
GROUP BY en.ID_Enseignant, en.Nom, en.Prenom, en.Grade,
         en.Specialite, en.Departement, m.Nom_Module,
         m.Type_Module, m.Volume_Horaire, m.Coefficient,
         f.Nom_Filiere, fn.Annee_Universitaire, fn.Semestre;
GO

-- ════════════════════════════════════════════════════
-- VUE 5 : KPI_Suivi_Mensuel
-- Évolution mensuelle des notes et absences
-- → KPI : Tendances temporelles pour graphiques Power BI
-- ════════════════════════════════════════════════════
CREATE OR ALTER VIEW vw_KPI_Suivi_Mensuel AS
SELECT
    t.Annee_Universitaire,
    t.Annee,
    t.Mois,
    t.Nom_Mois,
    t.Trimestre,
    t.Semestre_Academique,
    f.Nom_Filiere,
    f.Type_Diplome,
    -- Métriques notes
    COUNT(DISTINCT fn.ID_Fait_Note)                                 AS Nb_Evaluations,
    CAST(AVG(fn.Note) AS DECIMAL(5,2))                              AS Moyenne_Notes,
    CAST(SUM(CASE WHEN fn.Note >= 10 THEN 1.0 ELSE 0 END)
         / NULLIF(COUNT(fn.ID_Fait_Note), 0) * 100 AS DECIMAL(5,1)) AS Taux_Reussite_Pct,
    -- Métriques absences
    COUNT(DISTINCT fa.ID_Fait_Absence)                              AS Nb_Absences,
    ISNULL(SUM(fa.Duree_Heures), 0)                                 AS Total_H_Absences,
    ISNULL(SUM(CASE WHEN fa.Justifiee = 'Non' THEN fa.Duree_Heures ELSE 0 END), 0) AS H_Non_Justifiees
FROM Dim_Temps t
LEFT JOIN Fait_Notes fn     ON fn.ID_Temps    = t.ID_Temps
LEFT JOIN Fait_Absences fa  ON fa.ID_Temps    = t.ID_Temps
LEFT JOIN Dim_Etudiant e    ON e.ID_Etudiant  = fn.ID_Etudiant
LEFT JOIN Dim_Filiere  f    ON f.ID_Filiere   = e.ID_Filiere
WHERE t.Est_Vacances = 0
GROUP BY t.Annee_Universitaire, t.Annee, t.Mois, t.Nom_Mois,
         t.Trimestre, t.Semestre_Academique, f.Nom_Filiere, f.Type_Diplome;
GO

-- ════════════════════════════════════════════════════
-- VUE 6 : KPI_Comparaison_Sessions
-- Normale vs Rattrapage par filière/année
-- → KPI : Impact du rattrapage
-- ════════════════════════════════════════════════════
CREATE OR ALTER VIEW vw_KPI_Comparaison_Sessions AS
SELECT
    f.Nom_Filiere,
    f.Type_Diplome,
    fn.Annee_Universitaire,
    fn.Semestre,
    -- Session Normale
    SUM(CASE WHEN x.Session = 'Normale'    THEN 1 ELSE 0 END)      AS Nb_Normale,
    CAST(AVG(CASE WHEN x.Session = 'Normale'
                  THEN fn.Note END) AS DECIMAL(5,2))                AS Moy_Normale,
    CAST(SUM(CASE WHEN x.Session = 'Normale' AND fn.Note >= 10
                  THEN 1.0 ELSE 0 END)
         / NULLIF(SUM(CASE WHEN x.Session = 'Normale' THEN 1 ELSE 0 END), 0)
         * 100 AS DECIMAL(5,1))                                     AS Taux_Reussite_Normale,
    -- Session Rattrapage
    SUM(CASE WHEN x.Session = 'Rattrapage' THEN 1 ELSE 0 END)      AS Nb_Rattrapage,
    CAST(AVG(CASE WHEN x.Session = 'Rattrapage'
                  THEN fn.Note END) AS DECIMAL(5,2))                AS Moy_Rattrapage,
    CAST(SUM(CASE WHEN x.Session = 'Rattrapage' AND fn.Note >= 10
                  THEN 1.0 ELSE 0 END)
         / NULLIF(SUM(CASE WHEN x.Session = 'Rattrapage' THEN 1 ELSE 0 END), 0)
         * 100 AS DECIMAL(5,1))                                     AS Taux_Reussite_Rattrapage
FROM Fait_Notes fn
JOIN Dim_Etudiant e ON e.ID_Etudiant = fn.ID_Etudiant
JOIN Dim_Filiere  f ON f.ID_Filiere  = e.ID_Filiere
JOIN Dim_Examen   x ON x.ID_Examen   = fn.ID_Examen
GROUP BY f.Nom_Filiere, f.Type_Diplome, fn.Annee_Universitaire, fn.Semestre;
GO

-- ════════════════════════════════════════════════════
-- VUE 7 : KPI_Correlation_Absences_Notes
-- Corrélation entre absences et performance académique
-- → KPI : Profil de risque combiné
-- ════════════════════════════════════════════════════
CREATE OR ALTER VIEW vw_KPI_Correlation_Absences_Notes AS
SELECT
    e.CNE,
    e.Nom + ' ' + e.Prenom                                          AS Etudiant,
    e.Sexe,
    e.Niveau,
    f.Nom_Filiere,
    notes_agg.Annee_Universitaire,
    notes_agg.Semestre,
    notes_agg.Moyenne,
    notes_agg.Nb_Notes,
    ISNULL(abs_agg.Total_H_Absences, 0)                             AS Total_H_Absences,
    ISNULL(abs_agg.Nb_Absences, 0)                                  AS Nb_Absences,
    ISNULL(abs_agg.H_Non_Justifiees, 0)                             AS H_Non_Justifiees,
    -- Catégorie d'absence
    CASE
        WHEN ISNULL(abs_agg.Total_H_Absences, 0) = 0    THEN 'Aucune absence'
        WHEN ISNULL(abs_agg.Total_H_Absences, 0) < 10   THEN 'Faible (< 10h)'
        WHEN ISNULL(abs_agg.Total_H_Absences, 0) < 30   THEN 'Moyen (10-30h)'
        ELSE                                                  'Elevé (> 30h)'
    END                                                             AS Categorie_Absence,
    -- Catégorie de performance
    CASE
        WHEN notes_agg.Moyenne >= 14 THEN 'Excellent (≥14)'
        WHEN notes_agg.Moyenne >= 12 THEN 'Bien (12-14)'
        WHEN notes_agg.Moyenne >= 10 THEN 'Passable (10-12)'
        WHEN notes_agg.Moyenne >= 8  THEN 'A risque (8-10)'
        ELSE                              'En difficulté (<8)'
    END                                                             AS Categorie_Performance
FROM Dim_Etudiant e
JOIN Dim_Filiere f ON f.ID_Filiere = e.ID_Filiere
JOIN (
    SELECT ID_Etudiant, Annee_Universitaire, Semestre,
           CAST(AVG(Note) AS DECIMAL(5,2)) AS Moyenne,
           COUNT(*) AS Nb_Notes
    FROM Fait_Notes
    GROUP BY ID_Etudiant, Annee_Universitaire, Semestre
) notes_agg ON notes_agg.ID_Etudiant = e.ID_Etudiant
LEFT JOIN (
    SELECT ID_Etudiant, Annee_Universitaire,
           SUM(Duree_Heures) AS Total_H_Absences,
           COUNT(*)          AS Nb_Absences,
           SUM(CASE WHEN Justifiee = 'Non' THEN Duree_Heures ELSE 0 END) AS H_Non_Justifiees
    FROM Fait_Absences
    GROUP BY ID_Etudiant, Annee_Universitaire
) abs_agg ON abs_agg.ID_Etudiant = e.ID_Etudiant
         AND abs_agg.Annee_Universitaire = notes_agg.Annee_Universitaire;
GO

-- ════════════════════════════════════════════════════
-- CONFIRMATION
-- ════════════════════════════════════════════════════
SELECT TABLE_NAME AS Vue_Creee
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'dbo'
ORDER BY TABLE_NAME;
GO

PRINT '✔  Toutes les vues Power BI créées avec succès.';
GO
