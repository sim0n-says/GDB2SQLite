# GDB2SQL - Conversion GDB vers Spatialite

**POC (Proof of Concept)** - Outil de conversion de géodatabase ESRI (.gdb) vers Spatialite (.sqlite) avec préservation des métadonnées et optimisations de performance.

> ⚠️ **Note** : Ce projet est un prototype de démonstration. Il peut contenir des bugs et des limitations. Utilisez-le à vos propres risques.

## 🎯 Fonctionnalités (POC)

> **Note** : Cette section décrit les fonctionnalités implémentées dans ce POC. Certaines peuvent avoir des limitations ou nécessiter des améliorations.

### Conversion
- Conversion de géodatabase ESRI vers Spatialite
- Préservation des couches, géométries et attributs
- Support de la conversion d'une couche spécifique ou de toutes les couches
- Validation basique des entrées

### 📊 Préservation des métadonnées
- **Alias de champs** : Conservation des noms alternatifs des colonnes (expérimental)
- **Domaines codés** : Préservation des alias de valeurs via parsing XML (expérimental)
- **Clés primaires** : Recréation basique des contraintes de clés primaires
- **Triggers** : Support limité (les triggers ESRI ne sont généralement pas accessibles via GDAL)

### 🚀 Optimisations de performance
- **Index spatiaux** : Création automatique (toujours activés)
- **Mode optimisé par défaut** : Optimisations modérées (sécurité + performance)
- **Mode fast-mode** : Optimisations agressives (expérimental, risque de corruption)
- **Multi-threading** : Support basique pour conversion parallèle

### 📈 Monitoring et logs
- Logs avec horodatage et niveaux (INFO, DEBUG, WARNING, ERROR)
- Barres de progression avec tqdm (globale et par couche)
- Statistiques de conversion (temps, taille, succès/échecs)
- Monitoring basique des processus ogr2ogr

## 📋 Prérequis

### Installation de GDAL

GDAL (Geospatial Data Abstraction Library) est requis pour la conversion.

#### Ubuntu/Debian
```bash
sudo apt-get update
sudo apt-get install python3-gdal gdal-bin
```

#### macOS (avec Homebrew)
```bash
brew install gdal
```

#### Windows
Télécharger les binaires depuis [OSGeo4W](https://trac.osgeo.org/osgeo4w/) ou utiliser conda:
```bash
conda install -c conda-forge gdal
```

### Installation des dépendances Python

```bash
pip install -r requirements.txt
```

Les dépendances incluent :
- `gdal` : Bibliothèque géospatiale
- `tqdm` : Barres de progression

## 🚀 Utilisation rapide

### Conversion minimale

```bash
python gdb_to_spatialite.py Role_2024.gdb output.sqlite
```

Cette commande convertit toutes les couches avec les optimisations par défaut et la préservation des métadonnées (si disponibles).

## 📖 Exemples d'utilisation détaillés

### Conversion basique

```bash
# Conversion de toutes les couches
python gdb_to_spatialite.py Role_2024.gdb output.sqlite

# Conversion avec écrasement du fichier existant
python gdb_to_spatialite.py Role_2024.gdb output.sqlite --overwrite
```

### Conversion d'une couche spécifique

```bash
python gdb_to_spatialite.py Role_2024.gdb output.sqlite --layer "NomCouche"
```

### Conversion avec multi-threading

```bash
# Conversion avec 4 threads en parallèle
python gdb_to_spatialite.py Role_2024.gdb output.sqlite --workers 4

# Note: Pour plusieurs couches vers un seul fichier SQLite, 
# le mode séquentiel est automatiquement forcé (max_workers=1)
```

### Conversion optimisée (fast-mode)

```bash
# Mode performance maximale (sécurité réduite)
python gdb_to_spatialite.py Role_2024.gdb output.sqlite --fast-mode

# Fast-mode avec multi-threading
python gdb_to_spatialite.py Role_2024.gdb output.sqlite --fast-mode --workers 4
```

**⚠️ Avertissement** : Le mode `--fast-mode` désactive certaines protections SQLite (synchronous=OFF, journal_mode=OFF) pour maximiser les performances. À utiliser uniquement si vous avez des sauvegardes et que la perte de données n'est pas critique.

### Contrôle de la préservation des métadonnées

```bash
# Désactiver toutes les métadonnées
python gdb_to_spatialite.py Role_2024.gdb output.sqlite --no-metadata

# Préserver seulement les alias, pas les domaines
python gdb_to_spatialite.py Role_2024.gdb output.sqlite --skip-domains

# Préserver seulement les domaines, pas les alias
python gdb_to_spatialite.py Role_2024.gdb output.sqlite --skip-aliases

# Ignorer les clés primaires
python gdb_to_spatialite.py Role_2024.gdb output.sqlite --skip-primary-keys
```

### Lister les couches disponibles

```bash
python gdb_to_spatialite.py Role_2024.gdb output.sqlite --list-layers
```

Sortie :
```
Couches disponibles dans Role_2024.gdb:
  1. B05EX1_B05V_REPAR_FISC (1598005 entités)
  2. rol_unite_p (3711045 entités)
  3. B05EX1_B05V_ADR_UNITE_EVALN (3788405 entités)
  4. B05EX1_B05V_UNITE_EVALN (3711218 entités)
```

### Mode silencieux

```bash
python gdb_to_spatialite.py Role_2024.gdb output.sqlite --quiet
```

### Forcer l'utilisation de l'API Python

```bash
python gdb_to_spatialite.py Role_2024.gdb output.sqlite --no-ogr2ogr
```

**Note** : ogr2ogr est recommandé pour de meilleures performances et fiabilité.

## 📚 Référence complète des options CLI

### Arguments positionnels

- `gdb_path` : Chemin vers le dossier .gdb (géodatabase ESRI) - **obligatoire**
- `output_path` : Chemin vers le fichier Spatialite de sortie (.sqlite ou .db) - **obligatoire**

### Options de conversion

| Option | Description |
|--------|-------------|
| `--layer NOM` | Convertir uniquement la couche spécifiée (par défaut: toutes les couches) |
| `--overwrite` | Écraser le fichier de sortie s'il existe déjà |
| `--workers N` | Nombre de threads pour la conversion parallèle (défaut: 1, séquentiel) |
| `--no-ogr2ogr` | Forcer l'utilisation de l'API Python au lieu d'ogr2ogr |

### Options de métadonnées

| Option | Description |
|--------|-------------|
| `--no-metadata` | Désactiver toutes les métadonnées (alias, domaines, clés primaires, triggers) |
| `--skip-aliases` | Ne pas préserver les alias de champs |
| `--skip-domains` | Ne pas préserver les domaines codés (alias de valeurs) |
| `--skip-primary-keys` | Ne pas recréer les clés primaires |
| `--skip-triggers` | Ne pas recréer les triggers |

### Options de performance

| Option | Description |
|--------|-------------|
| `--fast-mode` | Activer les optimisations agressives (sécurité réduite, performance maximale) |

### Options de sortie

| Option | Description |
|--------|-------------|
| `--list-layers` | Lister les couches disponibles dans la géodatabase et quitter |
| `--quiet` | Mode silencieux (moins de logs détaillés) |

## 📊 Préservation des métadonnées

> **POC** : Cette fonctionnalité est expérimentale. L'extraction des métadonnées peut ne pas fonctionner pour toutes les géodatabases et peut avoir des limitations.

L'outil tente de préserver les métadonnées disponibles dans la géodatabase ESRI source.

### Alias de champs

Les alias de champs sont extraits depuis la géodatabase et stockés dans la table `metadata_field_aliases`.

**Structure** :
```sql
CREATE TABLE metadata_field_aliases (
    table_name TEXT NOT NULL,
    field_name TEXT NOT NULL,
    alias TEXT NOT NULL,
    PRIMARY KEY (table_name, field_name)
)
```

**Exemple d'utilisation** :
```sql
SELECT alias FROM metadata_field_aliases 
WHERE table_name = 'b05ex1_b05v_repar_fisc' 
  AND field_name = 'anrole';
-- Résultat: "ANNEE DU ROLE"
```

### Domaines codés (alias de valeurs)

Les domaines codés (associations code → description) sont extraits depuis le catalogue XML de la géodatabase et stockés dans la table `metadata_domain_values`.

**Structure** :
```sql
CREATE TABLE metadata_domain_values (
    table_name TEXT NOT NULL,
    field_name TEXT NOT NULL,
    code INTEGER NOT NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (table_name, field_name, code)
)
```

**Exemple d'utilisation** :
```sql
SELECT code, description FROM metadata_domain_values 
WHERE table_name = 'b05ex1_b05v_repar_fisc' 
  AND field_name = 'rl0504e'
ORDER BY code;
```

**Note (POC)** : L'extraction des domaines utilise le parsing automatique du fichier catalogue XML de la géodatabase (`a00000004.gdbtable` ou similaire). Cette méthode est expérimentale et peut ne pas fonctionner pour toutes les structures de géodatabase.

### Clés primaires

Les clés primaires sont détectées et recréées comme index uniques dans Spatialite.

**Exemple** :
```sql
-- Index unique créé automatiquement
CREATE UNIQUE INDEX pk_table_name_field1_field2 
ON "table_name" ("field1", "field2");
```

### Triggers

Les triggers SQL sont préservés si disponibles (limitation : les triggers ESRI ne sont généralement pas accessibles via GDAL/OGR).

## ⚡ Optimisations de performance

### Mode par défaut (optimisations modérées)

Par défaut, l'outil applique des optimisations équilibrées entre sécurité et performance :

- **SQLite PRAGMA** :
  - `synchronous = NORMAL` : Équilibre sécurité/performance
  - `journal_mode = WAL` : Write-Ahead Logging (plus rapide que DELETE)
  - `cache_size = 256MB` : Cache modéré
  - `temp_store = MEMORY` : Tables temporaires en mémoire

- **Index spatiaux** : Créés automatiquement pour toutes les couches avec géométrie (MUST HAVE)

- **Optimisations ogr2ogr** :
  - `SPATIAL_INDEX=YES` : Création immédiate des index spatiaux
  - `INIT_WITH_EPSG=NO` : Évite la réinitialisation si déjà fait

### Mode fast-mode (optimisations agressives)

Le mode `--fast-mode` active des optimisations agressives pour maximiser les performances :

- **SQLite PRAGMA** :
  - `synchronous = OFF` : Désactive complètement la synchronisation
  - `journal_mode = OFF` : Désactive le journal (risque de corruption en cas de crash)
  - `cache_size = 512MB` : Cache maximal
  - `temp_store = MEMORY` : Tables temporaires en mémoire

- **Analyse SQLite** : Exécution automatique de `ANALYZE` pour optimiser les statistiques

**⚠️ Important** : Le mode fast-mode réduit la sécurité des données. Utilisez-le uniquement si :
- Vous avez des sauvegardes régulières
- La perte de données n'est pas critique
- Vous recherchez des performances maximales

### Index spatiaux (MUST HAVE)

Les index spatiaux sont **toujours créés automatiquement** pour toutes les couches contenant des géométries. Ils ne peuvent pas être désactivés car ils sont essentiels pour les performances des requêtes spatiales.

Les index spatiaux sont créés via l'option ogr2ogr `SPATIAL_INDEX=YES` et sont visibles dans la table `geometry_columns` avec `spatial_index_enabled = 1`.

### Optimisations supplémentaires

- **Réutilisation de la datasource GDAL** : La datasource est mise en cache pour éviter les ouvertures/fermetures répétées lors de l'extraction des métadonnées
- **Polling adaptatif** : Le monitoring des processus utilise un polling adaptatif (100ms → 1s) pour réduire la latence
- **Cache des domaines** : Les domaines sont chargés une seule fois depuis le catalogue XML et mis en cache

## 🔧 Architecture et conception

> **POC** : Cette section décrit l'architecture cible du projet. L'implémentation peut avoir des limitations.

Le script tente de respecter les principes SOLID et les bonnes pratiques de développement :

### Principes SOLID

- **Single Responsibility** : Chaque classe a une responsabilité unique
  - `GDBToSpatialiteConverter` : Orchestration de la conversion
  - `GDBMetadataExtractor` : Extraction des métadonnées depuis la GDB
  - `SpatialiteMetadataApplier` : Application des métadonnées dans Spatialite
  - `ProgressLogger` : Gestion des logs et progression

- **Open/Closed** : Extensible sans modification grâce à l'interface CLI et l'injection de dépendances

- **Dependency Inversion** : Utilise les abstractions GDAL/OGR plutôt que des implémentations spécifiques

### Classes principales

- **`GDBToSpatialiteConverter`** : Classe principale orchestrant la conversion
- **`GDBMetadataExtractor`** : Extraction des métadonnées (alias, domaines, clés primaires, triggers)
- **`SpatialiteMetadataApplier`** : Application des métadonnées dans la base Spatialite
- **`ProgressLogger`** : Logging avec niveaux et formatage
- **`ProcessMonitor`** : Monitoring non-bloquant des processus ogr2ogr

## 📝 Exemple de sortie

```
2025-11-05 23:54:50 - INFO - ============================================================
2025-11-05 23:54:50 - INFO - Début de la conversion GDB vers Spatialite
2025-11-05 23:54:50 - INFO - ============================================================
2025-11-05 23:54:50 - INFO - Source: Role_2024.gdb
2025-11-05 23:54:50 - INFO - Destination: output.sqlite
2025-11-05 23:54:50 - INFO - ogr2ogr disponible: GDAL 3.10.2, released 2025/02/11
2025-11-05 23:54:50 - INFO - Nombre de couches trouvées: 4
2025-11-05 23:54:50 - INFO - Couches à convertir: 1
2025-11-05 23:54:50 - INFO -   1. B05EX1_B05V_REPAR_FISC: 1598005 entités
2025-11-05 23:54:50 - INFO - Conversion avec ogr2ogr (max_workers=1)
2025-11-05 23:54:50 - INFO - [1/1] Conversion de 'B05EX1_B05V_REPAR_FISC'...
Conversion: 100%|██████████| 1/1 [00:17<00:00, 17.45s/couche]
2025-11-05 23:55:07 - INFO - ✓ Couche 'B05EX1_B05V_REPAR_FISC' convertie avec succès
2025-11-05 23:55:07 - INFO - Application des métadonnées pour 'B05EX1_B05V_REPAR_FISC'...
2025-11-05 23:55:07 - INFO - ✓ 19 alias de champs appliqués pour 'b05ex1_b05v_repar_fisc'
2025-11-05 23:55:07 - INFO - ✓ 12 valeurs de domaine appliquées pour 'b05ex1_b05v_repar_fisc' (4 champs)
2025-11-05 23:55:07 - INFO - ✓ Métadonnées appliquées pour 'B05EX1_B05V_REPAR_FISC'
2025-11-05 23:55:07 - INFO - ============================================================
2025-11-05 23:55:07 - INFO - Résumé de la conversion
2025-11-05 23:55:07 - INFO - ============================================================
2025-11-05 23:55:07 - INFO - Temps total: 17s
2025-11-05 23:55:07 - INFO - Couches réussies: 1/1
2025-11-05 23:55:07 - INFO - ✓ Fichier créé: output.sqlite (135.16 MB)
2025-11-05 23:55:07 - INFO - ✓ Base SQLite optimisée pour les performances
```

## 🐛 Dépannage

### Erreur: "Le driver OpenFileGDB n'est pas disponible"

**Solution** :
- Vérifiez que GDAL est correctement installé : `ogr2ogr --version`
- Vérifiez que le driver OpenFileGDB est compilé dans votre version de GDAL
- Sur Ubuntu/Debian, installez `gdal-bin` et `python3-gdal`

### Erreur: "Impossible d'ouvrir la géodatabase"

**Solutions** :
1. Vérifiez que le chemin vers le .gdb est correct et absolu
2. Vérifiez que le dossier .gdb n'est pas corrompu
3. Vérifiez les permissions d'accès au dossier
4. Assurez-vous que la géodatabase n'est pas verrouillée par un autre processus

### Erreur lors de l'initialisation de Spatialite

**Solutions** :
- Le script essaie d'initialiser Spatialite automatiquement
- Si cela échoue, vous pouvez l'initialiser manuellement avec `spatialite_gui` ou un client SQLite :
  ```sql
  SELECT load_extension('mod_spatialite');
  SELECT InitSpatialMetadata(1);
  ```

### Erreur: "Conversion avec ogr2ogr (max_workers=1) alors que j'ai défini 4"

**Explication** : Pour plusieurs couches vers un seul fichier SQLite, le mode séquentiel est automatiquement forcé pour éviter les conflits de verrous SQLite. C'est normal et nécessaire.

### Performance lente

**Optimisations possibles** :
1. Utilisez `--fast-mode` pour des optimisations agressives (attention aux risques)
2. Augmentez `--workers` si vous convertissez plusieurs couches vers des fichiers séparés
3. Vérifiez que les index spatiaux sont bien créés (toujours activés par défaut)
4. Assurez-vous d'avoir assez de RAM (cache SQLite)

### Métadonnées manquantes

**Vérifications** :
1. Vérifiez que les métadonnées existent dans la géodatabase source
2. Consultez les tables `metadata_field_aliases` et `metadata_domain_values` dans la base Spatialite
3. Activez les logs détaillés (sans `--quiet`) pour voir les messages d'extraction
4. Les domaines sont extraits depuis le catalogue XML de la GDB (`a00000004.gdbtable`)

## 📚 Ressources et documentation

### Documentation technique

- [Documentation GDAL](https://gdal.org/)
- [Documentation Spatialite](https://www.gaia-gis.it/fossil/libspatialite/index)
- [Format Geodatabase ESRI](https://desktop.arcgis.com/en/arcmap/latest/manage-data/administer-file-gdbs/file-geodatabase-format.htm)
- [Architecture des géodatabases ESRI](https://pro.arcgis.com/en/pro-app/latest/help/data/geodatabases/overview/the-architecture-of-a-geodatabase.htm)

### Outils connexes

- **spatialite_gui** : Interface graphique pour Spatialite
- **QGIS** : Logiciel SIG supportant les géodatabases ESRI et Spatialite
- **ogr2ogr** : Outil en ligne de commande pour conversions de formats

## 📄 Licence

Ce projet est distribué sous la licence **Unlicense** (domaine public).

Voir le fichier [LICENSE](LICENSE) pour plus de détails.

### Licences des dépendances

Ce projet utilise les bibliothèques suivantes :

- **GDAL/OGR** : Licence MIT ([source](https://gdal.org/))
- **SQLite** : Domaine public ([source](https://www.sqlite.org/))
- **Spatialite** : MPL/GPL/LGPL ([source](https://www.gaia-gis.it/fossil/libspatialite/))
- **tqdm** : Licence MIT ([source](https://github.com/tqdm/tqdm))

### Pourquoi Unlicense ?

La licence Unlicense a été choisie car :
- ✅ **Maximum de liberté** : Domaine public, aucune restriction d'utilisation
- ✅ **Compatible** : Compatible avec toutes les licences (MIT, GPL, MPL, etc.)
- ✅ **Simple** : Aucune attribution requise (bien que recommandée)
- ✅ **Idéal pour un POC** : Permet la réutilisation totale sans contrainte
- ✅ **Aligné avec SQLite** : Même philosophie que SQLite (domaine public)

**Note** : L'Unlicense place le code dans le domaine public. Certaines juridictions peuvent avoir des règles différentes concernant le "domaine public" par déclaration. Pour un POC, c'est une excellente option pour maximiser la liberté d'utilisation.

---

**GDB2SQL** - POC de conversion de géodatabases ESRI vers Spatialite.

**Author** : Simon Bédard  
**Contact** : software@servicesforestiers.tech  
**License** : Unlicense (Public Domain)

> ⚠️ **Avertissement** : Ce projet est un prototype de démonstration. Il peut contenir des bugs, des limitations et des fonctionnalités non testées. Utilisez-le à vos propres risques et vérifiez toujours les résultats de conversion.
