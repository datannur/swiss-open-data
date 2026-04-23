# Mapping opendata.swiss (CKAN) → datannur

Document de référence pour la construction du catalogue datannur à partir des
métadonnées CKAN récupérées dans `out/<format>/datasets.jsonl` et des fichiers
téléchargés listés dans `out/<format>/manifest.jsonl`.

Corpus cible : ressources **françaises** au format **Parquet / CSV / Excel**
(.xls + .xlsx) d'opendata.swiss.

---

## 1. Rappel du modèle datannur

Référence : [structure.md](https://github.com/datannur/datannur/blob/main/src/assets/markdown/about-page/structure.md).

8 entités, 2 groupes :

**Intérieur du dataset** (extraits automatiquement par `datannurpy` en scannant
les fichiers — nous n'avons pas à les produire) :

- `dataset` — la table (1 fichier = 1 dataset, sauf time-series groupées)
- `variable` — les colonnes
- `modality` + `value` — ensembles de valeurs catégorielles
- `freq` — distribution de fréquence d'une variable

**Extérieur du dataset** (ce que nous fournissons à `datannurpy` via
`metadata_path`) :

- `institution` — fournisseur (`owner_id`) ou gestionnaire (`manager_id`),
  récursive
- `folder` — arborescence éditoriale, récursive
- `tag` — mots-clés, récursifs, liables à tout
- `concept` — glossaire métier (non utilisé dans cette première version)
- `doc` — documentations Markdown/PDF

---

## 2. Rappel du modèle CKAN

```
opendata.swiss (CKAN)
└── package         ← unité éditoriale publiée (fiche)
    ├── organization      (qui publie — une seule par package)
    ├── groups[]          (thématiques DCAT-AP-CH : econ, soci, envi…)
    ├── keywords.fr[]     (mots-clés libres)
    ├── resources[]       (fichiers téléchargeables — parquet/csv/xlsx/…)
    └── métadonnées       (titre FR, description FR, temporals, licence,
                           accrual_periodicity, organization.political_level…)
```

Un package CKAN est une **fiche de publication**, pas une table. Ses
`resources` peuvent être :

- **Cas 1** — mêmes données exposées en plusieurs formats (parquet + csv +
  xlsx) ;
- **Cas 2** — série temporelle éclatée (fichier par année) ;
- **Cas 3** — tables différentes regroupées éditorialement (plus rare).

Il n'existe aucune garantie de schéma commun entre resources d'un même
package.

---

## 3. Mapping global

| datannur       | ← CKAN                                               |
| -------------- | ---------------------------------------------------- |
| `institution`  | `package.organization` (+ hiérarchie reconstruite)   |
| `folder` (racine thématique) | un par `group` DCAT + `multi` + `other`   |
| `folder` (package)           | un par `package` (enfant d'une racine)    |
| `dataset`      | un par `resource` téléchargée                        |
| `tag`          | `package.keywords.fr` ∪ `thematique---<group>`       |

### 3.1 Granularité : resource = dataset

**1 resource CKAN = 1 dataset datannur.** Justification :

- `datannurpy` scanne les fichiers et crée de toute façon un dataset par
  fichier ; nos métadonnées ne font que l'enrichir.
- Les schémas ne sont pas garantis identiques entre resources d'un même
  package → fusionner serait risqué.
- Le lien éditorial entre les N resources d'un package est conservé via le
  `folder_id` commun (le folder-package).

Volume attendu : ≈ **6 665 datasets** (841 parquet + 897 csv + 4 927 excel,
d'après les manifests actuels).

### 3.2 Folders : arborescence éditoriale

Chaque package devient un folder. Son `parent_id` dépend du nombre de
groupes DCAT qu'il porte :

| Nb groupes du package | Folder racine             | Packages (≈) |
| ---------------------- | ------------------------- | ------------ |
| 1                      | le groupe (`econ`, etc.)  | 2 138        |
| ≥ 2                    | `multi`                   | 420          |
| 0                      | `other`                   | 1            |

**Folders racines** (14 au total) :

- 12 thématiques DCAT-AP-CH : `econ`, `soci`, `envi`, `regi`, `just`, `educ`,
  `heal`, `agri`, `tran`, `gove`, `ener`, `tech`
- `multi` : packages multi-thématiques
- `other` : packages sans groupe

Arbre résultant :

```
folders
├── econ/
│   ├── <package_uuid_1>/           (name = package.title.fr)
│   │   ├── dataset <rid>.parquet
│   │   └── dataset <rid>.csv
│   └── <package_uuid_2>/…
├── soci/…
├── …
├── multi/
│   └── <package_uuid>/              (tags : thematique---soci, thematique---educ…)
└── other/
    └── <package_uuid>/
```

### 3.3 Tags : thématiques + mots-clés libres

Pour **chaque** package (y compris mono-thématique), on émet :

- un tag `thematique---<group>` **par groupe** → `folder.tag_ids` du
  folder-package ;
- un tag par mot-clé libre `package.keywords.fr`.

Les tags `thematique---<group>` ont `parent_id = "thematique"` (tag racine)
pour former une hiérarchie propre dans l'UI datannur.

Justification du doublon "tag + folder" sur mono-thématique : l'utilisateur
peut alors filtrer indifféremment par tag ou naviguer par folder, et l'info
thématique reste systématiquement portée par le tag.

### 3.4 Institutions : hiérarchie 5 niveaux

**34 organisations uniques** dans le corpus. Arbre reconstruit :

```
suisse                                 (virtuel, racine)
│
├── confederation                      (virtuel, conteneur)
│   ├── agroscope
│   ├── bundesamt-fur-statistik-bfs
│   ├── bundesamt-fur-gesundheit-bag
│   │   └── abteilung-uebertragbare-krankheiten   (parent CKAN natif)
│   ├── bundesamt-fur-umwelt-bafu
│   ├── … (14 autres offices fédéraux)
│   └── schweizerisches-bundesarchiv-bar
│
├── cantons                            (virtuel, conteneur)
│   ├── kanton-bern-2                  (org CKAN publiante)
│   │   ├── amt-fuer-geoinformation-des-kantons-bern
│   │   └── communes                   (virtuel, conteneur)
│   │       └── biel-bienne            (virtuel, ville)
│   │           ├── basisdaten-biel-bienne
│   │           ├── infrastruktur-mobilitaet-biel-bienne
│   │           ├── leben-in-biel-bienne
│   │           └── planung-umwelt-biel-bienne
│   │
│   ├── kanton-freiburg                (virtuel)
│   │   ├── ssd
│   │   └── geoinformation_kanton_freiburg
│   │
│   └── kanton-vaud                    (virtuel)
│       └── communes                   (virtuel)
│           └── lausanne
│
└── autres                             (virtuel, conteneur)
    ├── fondation-modus
    ├── snf (FNS)
    ├── swissmedic
    ├── sik-isea
    ├── zb_zuerich
    ├── uek-administrative-versorgungen
    ├── eth-zuerich                    (virtuel, préservé)
    │   └── kof-konjunkturforschungsstelle
    └── schweizerische-bundesbahnen-sbb (virtuel, préservé)
        └── oevch
```

**Institutions virtuelles** (créées par le script, absentes de CKAN comme
orgs publiantes) :

- `suisse`, `confederation`, `cantons`, `autres` — conteneurs de niveau 1
- `communes` — conteneur répété sous chaque canton ayant des communes
- `kanton-freiburg`, `kanton-vaud` — cantons sans org publiante "canton" dans
  CKAN
- `biel-bienne` — ville conteneur de 4 services publiants
- `eth-zuerich`, `schweizerische-bundesbahnen-sbb` — parents CKAN non
  publiants, préservés par fidélité à la source

**Mapping commune → canton codé en dur** (CKAN ne fournit pas le canton
d'appartenance d'une commune) :

```python
COMMUNE_TO_CANTON = {
    "biel-bienne": "kanton-bern-2",
    "lausanne":    "kanton-vaud",
}
```

Table extensible : une commune inconnue tombe dans un canton `kanton-inconnu`
avec warning.

### 3.5 owner_id vs manager_id

Dans CKAN, `organization` est à la fois le fournisseur et le gestionnaire
(l'entité qui publie sur le portail). Aucun champ CKAN ne permet de
distinguer les deux rôles.

**Décision : `owner_id` seul, `manager_id` laissé vide.**

- Le sens CKAN (« qui publie / partage les données ») colle mieux à
  `owner_id` (fournisseur).
- Laisser `manager_id` vide reste honnête ; le champ peut être enrichi plus
  tard si une info de gestion distincte apparaît.

Les datasets et folders-packages reçoivent tous les deux le même
`owner_id = organization.name`.

---

## 4. Mapping détaillé champ par champ

Les tables ci-dessous listent tous les champs datannur peuplés. Les champs
non mentionnés sont laissés `null` (datannur les considère optionnels).

### 4.1 `institution.csv`

| Champ          | Source CKAN                                       | Notes                                      |
| -------------- | ------------------------------------------------- | ------------------------------------------ |
| `id`           | `organization.name` (ou identifiant virtuel)      | slug                                       |
| `parent_id`    | déduit : niveau politique + mapping hardcodé      | voir 3.4                                   |
| `name`         | `organization.display_name.fr` (ou DE en fallback) | noms des virtuels : "Suisse", "Cantons"…  |
| `description`  | `organization.description.fr`                     |                                            |
| `email`        | (à partir du premier `package.contact_points[0].email` de l'org si disponible, sinon vide) | optionnel |
| `tag_ids`      | `organization.political_level` comme tag `level---<niveau>` | optionnel |

### 4.2 `folder.csv`

**Folders racines** (thématiques + `multi` + `other`) :

| Champ        | Valeur                                                      |
| ------------ | ----------------------------------------------------------- |
| `id`         | `econ`, `soci`, …, `multi`, `other`                         |
| `parent_id`  | `null`                                                      |
| `name`       | libellé FR DCAT : "Économie et finances", "Multi-thématiques", "Autre" |
| `description`| libellé DCAT si dispo                                       |
| `type`       | `"thematique"`                                              |

**Folders packages** (un par package) :

| Champ              | Source CKAN                                     |
| ------------------ | ----------------------------------------------- |
| `id`               | `package.id` (UUID)                             |
| `parent_id`        | voir 3.2                                        |
| `owner_id`         | `package.organization.name`                     |
| `manager_id`       | `null` (voir 3.5)                               |
| `tag_ids`          | `thematique---<group>` pour chaque groupe + `keywords.fr` |
| `name`             | `package.title.fr`                              |
| `description`      | `package.description.fr`                        |
| `link`             | `package.url`                                   |
| `localisation`     | `package.organization.political_level`          |
| `start_date`       | `package.temporals[0].start_date`               |
| `end_date`         | `package.temporals[0].end_date`                 |
| `last_update_date` | `package.modified`                              |
| `updating_each`    | `package.accrual_periodicity` normalisé FR       |
| `type`             | `"package"`                                     |
| `data_path`        | `null` (les fichiers sont dans les datasets)    |

### 4.3 `dataset.csv`

| Champ              | Source CKAN / manifest                                 |
| ------------------ | ------------------------------------------------------ |
| `id`               | `resource.id` (UUID CKAN)                              |
| `folder_id`        | `resource.package_id` (= `package.id`)                 |
| `owner_id`         | `package.organization.name`                            |
| `manager_id`       | `null`                                                 |
| `tag_ids`          | hérite du folder-package (optionnel, sinon null)       |
| `name`             | `resource.title.fr` ou fallback `resource.format`      |
| `description`      | `resource.description.fr`                              |
| `data_path`        | `data/<fmt>/<rid>.<ext>` (chemin local réel)           |
| `link`             | `resource.url` (URL amont opendata.swiss)              |
| `delivery_format`  | `resource.format` (`PARQUET`, `CSV`, `XLS`, `XLSX`)    |
| `data_size`        | `manifest.downloaded_bytes` (taille réelle fichier)    |
| `last_update_date` | `resource.modified`                                    |
| `start_date`       | `package.temporals[0].start_date` (hérité)             |
| `end_date`         | `package.temporals[0].end_date`                        |
| `updating_each`    | `package.accrual_periodicity`                          |
| `type`             | `"resource"`                                           |

### 4.4 `tag.csv`

Deux sous-ensembles :

**Tags thématiques** (12, avec parent commun) :

| Champ       | Valeur                                        |
| ----------- | --------------------------------------------- |
| `id`        | `thematique---<group>`                        |
| `parent_id` | `thematique`                                  |
| `name`      | libellé FR du groupe                          |

Tag racine `thematique` : `id = "thematique"`, `parent_id = null`, `name = "Thématique"`.

**Tags libres** (mots-clés CKAN dédupliqués) :

| Champ       | Valeur                                        |
| ----------- | --------------------------------------------- |
| `id`        | slug du mot-clé                               |
| `parent_id` | `null`                                        |
| `name`      | mot-clé tel qu'écrit dans CKAN                |

### 4.5 `doc.csv`

Une entrée optionnelle par package pointant vers sa page sur le portail
d'origine, plus les URLs listées dans `package.documentation[]` quand elles
existent.

| Champ  | Source                            |
| ------ | --------------------------------- |
| `id`   | `doc---<package_id>---source`     |
| `name` | "Fiche opendata.swiss"            |
| `path` | `package.url`                     |
| `type` | `"url"`                           |

---

## 5. Volumes estimés

| Entité      | Nombre |
| ----------- | ------ |
| institution | ≈ 45 (34 réelles + 11 virtuelles)  |
| folder      | ≈ 2 574 (14 racines + 2 560 packages) |
| dataset     | ≈ 6 665 (resources téléchargées OK) |
| tag         | ≈ 5-10k (12 thématiques + keywords dédupliqués) |
| doc         | ≈ 2 560 (un par package)          |

---

## 6. Livrables attendus

Le script `build_metadata.py` produit un dossier `metadata/` contenant :

```
metadata/
├── institution.csv
├── folder.csv
├── dataset.csv
├── tag.csv
└── doc.csv
```

Puis `catalog.yml` à la racine du projet pointe `datannurpy` vers ce dossier
et vers les fichiers téléchargés :

```yaml
app_path: ./catalog
metadata_path: ./metadata
add:
  - folder: ./data/parquet
  - folder: ./data/csv
  - folder: ./data/excel
```

Exécution :

```bash
uv run python -m datannurpy catalog.yml
```

`datannurpy` scanne les fichiers (dataset + variable + stats), puis merge nos
CSV de métadonnées (les champs manuels priment), et exporte le catalogue
prêt à servir.
