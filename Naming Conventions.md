# Conventions de nommage

Ce document décrit les conventions de nommage utilisées pour les schémas, tables, vues, colonnes et autres objets dans l’entrepôt de données.

## Table des matières

- [Conventions de nommage](#conventions-de-nommage)
  - [Table des matières](#table-des-matières)
  - [Principes généraux](#principes-généraux)
  - [Conventions de nommage des tables](#conventions-de-nommage-des-tables)
    - [Règles Bronze](#règles-bronze)
    - [Règles Silver](#règles-silver)
    - [Règles Gold](#règles-gold)
      - [Glossaire des catégories](#glossaire-des-catégories)
  - [Conventions de nommage des colonnes](#conventions-de-nommage-des-colonnes)
    - [Colonnes techniques](#colonnes-techniques)
  - [Procédures stockées](#procédures-stockées)

## Principes généraux

- **Convention de nommage** : utiliser le format `snake_case`, avec des lettres minuscules et des underscores (`_`) pour séparer les mots.
- **Langue** : utiliser la langue francais pour tous les noms.
- **Éviter les mots réservés** : ne pas utiliser de mots réservés SQL comme noms d’objets.

## Conventions de nommage des tables

### Règles Bronze

- Tous les noms doivent commencer par le nom du système source, et les noms des tables doivent correspondre exactement à leurs noms d’origine sans renommage.
- **`<systemesource>_<entite>`**
  - `<systemesource>` : nom du système source (par exemple : `etd`, `ens`)
  - `<entite>` : nom exact de la table provenant du système source
  - Exemple : `etd_absences` → informations de l'absences de etudiants .

### Règles Silver

- Tous les noms doivent commencer par le nom du système source, et les noms des tables doivent correspondre exactement à leurs noms d’origine sans renommage.
- **`<systemesource>_<entite>`**
  - `<systemesource>` : nom du système source (par exemple : `etd`, `ens`)
  - `<entite>` : nom exact de la table provenant du système source
  - Exemple : `etd_absences` → informations de l'absences de etudiants .

### Règles Gold

- Tous les noms doivent être significatifs, alignés sur le métier, et commencer par un préfixe de catégorie.
- **`<categorie>_<entite>`**
  - `<categorie>` : décrit le rôle de la table, comme `dim` (dimension) ou `fact` (table de faits)
  - `<entite>` : nom descriptif de la table, aligné sur le domaine métier 
  - Exemples :
    - `dim_etudiants` → table de dimension pour les données clients
    - `fact_notes` → table de faits contenant les transactions de vente

#### Glossaire des catégories

| Modèle | Signification | Exemple(s) |
|---|---|---|
| `dim_` | Table de dimension | `dim_etudiants`, `dim_Filière` |
| `fact_` | Table de faits | `fact_notes` |
| `report_` | Table de reporting | `report_etudiants`, `report_absences` |

## Conventions de nommage des colonnes


### Colonnes techniques

- Toutes les colonnes techniques doivent commencer par le préfixe `dwh_`, suivi d’un nom descriptif indiquant le rôle de la colonne.
- **`dwh_<nom_colonne>`**
  - `dwh` : préfixe réservé exclusivement aux métadonnées générées par le système
  - `<nom_colonne>` : nom descriptif précisant le rôle de la colonne
  - Exemple : `dwh_load_date` → colonne système utilisée pour stocker la date de chargement de l’enregistrement

## Procédures stockées

- Toutes les procédures stockées utilisées pour charger les données doivent suivre le modèle suivant :
- **`load_<layer>`**
  - `<layer>` : représente la couche chargée, par exemple `bronze`, `silver` ou `gold`
  - Exemples :
    - `load_bronze` → procédure stockée pour charger les données dans la couche Bronze
    - `load_silver` → procédure stockée pour charger les données dans la couche Silver
