# Catalogue de données de la couche Bronze

## Vue d'ensemble
La couche Bronze constitue la couche d'ingestion brute de l'entrepôt de données académique. Elle stocke les données sources avec un minimum de transformation, en conservant la structure d'origine des jeux de données entrants afin d'assurer la traçabilité et de faciliter les étapes de nettoyage en aval.

Ce catalogue documente les tables Bronze définies dans le script DDL et décrit le rôle de chaque table ainsi que de ses colonnes.

---

### 1. **bronze.ens_calendrier**
- **Rôle :** stocke les données du calendrier académique utilisées pour décrire les dates, les semestres académiques, les années universitaires et les périodes de vacances.
- **Colonnes :**

| Nom de colonne            | Type de données | Description |
|--------------------------|-----------------|-------------|
| ens_date                 | DATE            | Date complète du calendrier. |
| ens_jour                 | INT             | Numéro du jour dans le mois. |
| ens_mois                 | INT             | Numéro du mois dans l'année. |
| ens_annee                | INT             | Année civile. |
| ens_trimestre            | INT             | Numéro du trimestre dans l'année. |
| ens_semestre_academique  | VARCHAR(30)     | Libellé du semestre académique, par exemple S1 ou S2. |
| ens_annee_universitaire  | VARCHAR(30)     | Libellé de l'année universitaire, par exemple 2024-2025. |
| ens_est_vacances         | VARCHAR(30)     | Indique si la date appartient à une période de vacances. |

---

### 2. **bronze.ens_enseignants**
- **Rôle :** stocke les informations descriptives sur les enseignants et le personnel académique.
- **Colonnes :**

| Nom de colonne      | Type de données | Description |
|--------------------|-----------------|-------------|
| ens_id_enseignant  | INT             | Identifiant unique de l'enseignant. |
| ens_nom            | VARCHAR(100)    | Nom de famille de l'enseignant. |
| ens_prenom         | VARCHAR(100)    | Prénom de l'enseignant. |
| ens_grade          | VARCHAR(100)    | Grade académique ou professionnel de l'enseignant. |
| ens_specialite     | VARCHAR(150)    | Spécialité ou domaine d'enseignement de l'enseignant. |
| ens_departement    | VARCHAR(100)    | Département auquel appartient l'enseignant. |

---

### 3. **bronze.ens_enseignements**
- **Rôle :** stocke les affectations pédagogiques reliant les enseignants aux modules, aux semestres, aux groupes et aux années universitaires.
- **Colonnes :**

| Nom de colonne             | Type de données | Description |
|---------------------------|-----------------|-------------|
| ens_id_enseignement       | INT             | Identifiant unique de l'affectation d'enseignement. |
| ens_id_enseignant         | INT             | Identifiant de l'enseignant affecté à l'activité pédagogique. |
| ens_id_module             | INT             | Identifiant du module enseigné. |
| ens_annee_universitaire   | VARCHAR(20)     | Année universitaire de l'affectation. |
| ens_semestre              | VARCHAR(20)     | Semestre durant lequel l'enseignement a lieu. |
| ens_groupe                | VARCHAR(50)     | Groupe d'étudiants concerné par l'enseignement. |
| ens_type_enseignement     | VARCHAR(100)    | Type d'enseignement, par exemple cours, TD ou TP. |
| ens_nb_heures_assure      | FLOAT           | Nombre d'heures d'enseignement assurées. |

---

### 4. **bronze.etd_absences**
- **Rôle :** stocke les événements d'absence des étudiants par module, date, type de séance, statut de justification et durée.
- **Colonnes :**

| Nom de colonne      | Type de données | Description |
|--------------------|-----------------|-------------|
| etd_id_absence     | INT             | Identifiant unique de l'enregistrement d'absence. |
| etd_id_etudiant    | INT             | Identifiant de l'étudiant concerné par l'absence. |
| etd_id_module      | INT             | Identifiant du module lié à l'absence. |
| etd_date_absence   | DATE            | Date de l'absence. |
| etd_type_seance    | VARCHAR(50)     | Type de séance manquée, par exemple TD ou TP. |
| etd_justifiee      | VARCHAR(30)     | Indique si l'absence est justifiée. |
| etd_duree_heures   | FLOAT           | Durée de l'absence en heures. |

---

### 5. **bronze.etd_etudiants**
- **Rôle :** stocke les informations descriptives sur les étudiants et leur rattachement académique.
- **Colonnes :**

| Nom de colonne           | Type de données | Description |
|-------------------------|-----------------|-------------|
| etd_id_etudiant         | INT             | Identifiant unique de l'étudiant. |
| etd_cne                 | VARCHAR(50)     | Code national de l'étudiant ou identifiant d'inscription. |
| etd_nom                 | VARCHAR(100)    | Nom de famille de l'étudiant. |
| etd_prenom              | VARCHAR(100)    | Prénom de l'étudiant. |
| etd_date_naissance      | DATE            | Date de naissance de l'étudiant. |
| etd_sexe                | VARCHAR(30)     | Sexe de l'étudiant. |
| etd_id_filiere          | INT             | Identifiant de la filière à laquelle appartient l'étudiant. |
| etd_niveau              | VARCHAR(50)     | Niveau académique de l'étudiant. |
| etd_statut              | VARCHAR(50)     | Statut académique actuel de l'étudiant. |
| etd_annee_inscription   | INT             | Année d'inscription de l'étudiant. |

---

### 6. **bronze.etd_filieres**
- **Rôle :** stocke les informations descriptives sur les filières et les formations diplômantes.
- **Colonnes :**

| Nom de colonne       | Type de données | Description |
|---------------------|-----------------|-------------|
| etd_id_filiere      | INT             | Identifiant unique de la filière. |
| etd_code_filiere    | VARCHAR(50)     | Code de la filière utilisé pour l'identification et le reporting. |
| etd_nom_filiere     | VARCHAR(150)    | Nom de la filière. |
| etd_type_diplome    | VARCHAR(100)    | Type de diplôme associé à la filière. |
| etd_duree           | INT             | Durée de la filière, généralement en années. |
| etd_departement     | VARCHAR(100)    | Département responsable de la filière. |

---

### 7. **bronze.etd_modules**
- **Rôle :** stocke les informations descriptives sur les modules académiques et leurs attributs pédagogiques.
- **Colonnes :**

| Nom de colonne       | Type de données | Description |
|---------------------|-----------------|-------------|
| etd_id_module       | INT             | Identifiant unique du module. |
| etd_code_module     | VARCHAR(50)     | Code du module utilisé pour le suivi académique. |
| etd_nom_module      | VARCHAR(150)    | Nom du module. |
| etd_id_filiere      | INT             | Identifiant de la filière à laquelle le module appartient. |
| etd_semestre        | VARCHAR(30)     | Semestre durant lequel le module est enseigné. |
| etd_coefficient     | FLOAT           | Poids ou coefficient du module dans l'évaluation. |
| etd_volume_horaire  | FLOAT           | Volume horaire total du module. |
| etd_type_module     | VARCHAR(100)    | Type ou catégorie du module. |

---

### 8. **bronze.etd_notes**
- **Rôle :** stocke les résultats d'examen des étudiants par module, enseignant, semestre, session et date d'examen.
- **Colonnes :**

| Nom de colonne             | Type de données | Description |
|---------------------------|-----------------|-------------|
| etd_id_note               | INT             | Identifiant unique de l'enregistrement de note. |
| etd_id_etudiant           | INT             | Identifiant de l'étudiant recevant la note. |
| etd_id_module             | INT             | Identifiant du module concerné par la note. |
| etd_id_enseignant         | INT             | Identifiant de l'enseignant lié à l'examen ou à la note. |
| etd_annee_universitaire   | VARCHAR(30)     | Année universitaire du résultat d'examen. |
| etd_semestre              | VARCHAR(30)     | Semestre lié au résultat d'examen. |
| etd_type_examen           | VARCHAR(50)     | Type d'examen, par exemple contrôle ou rattrapage. |
| etd_note                  | FLOAT           | Valeur numérique de la note de l'étudiant. |
| etd_date_examen           | DATE            | Date à laquelle l'examen a eu lieu. |
| etd_session               | VARCHAR(50)     | Libellé de la session d'examen. |

---

## Résumé
La couche Bronze capture les données académiques brutes relatives au calendrier, aux enseignants, aux affectations pédagogiques, aux étudiants, aux filières, aux modules, aux notes et aux absences. Ces tables constituent la base des étapes de nettoyage dans la couche Silver et de la modélisation analytique dans la couche Gold.
