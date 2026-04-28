# datannur-opench

Pipeline de collecte et de préparation des métadonnées opendata.swiss pour datannur.

Le workflow est maintenant organisé autour de dossiers partagés `staging/` et `data/`. Les métadonnées intermédiaires ne sont plus séparées par format, et les fichiers téléchargés sont écrits directement dans `data/`.

## Prérequis

- `uv`
- Python défini par le projet via `pyproject.toml`

Installation de l'environnement:

```bash
uv sync
```

## En bref

Les 4 commandes principales, dans l'ordre:

```bash
uv run python crawl.py                   # crawl CKAN -> staging/
uv run python download.py                # telecharge dans data/ + maj download_state
uv run python build_metadata.py          # genere metadata/
uv run python -m datannurpy catalog.yml  # construit le catalogue final
```

## Structure utile

- `staging/organizations.jsonl`: organisations CKAN dédupliquées
- `staging/packages.jsonl`: packages CKAN dédupliqués, limités aux métadonnées package
- `staging/resources.jsonl`: ressources CKAN filtrées par format, fusionnées, avec leurs métadonnées propres
- `staging/download_state.jsonl`: état technique des téléchargements, sans duplication des métadonnées CKAN
- `data/`: fichiers téléchargés
- `metadata/`: CSV finaux pour datannur

## Vocabulaire CKAN

- `package` CKAN: la fiche de publication
- `dataset` CKAN: en pratique, presque synonyme de `package`
- `resource` CKAN: un fichier ou une URL à l'intérieur d'un `package`

Règle de lecture dans ce projet:

- 1 `package` CKAN = 1 fiche éditoriale source
- 1 `package` CKAN peut contenir plusieurs `resources`
- 1 `resource` tabulaire téléchargée = 1 dataset datannur

Autrement dit, dans datannur, le mot `dataset` ne désigne pas la fiche CKAN, mais l'unité tabulaire finale réellement exploitable.

## Lancer le pipeline complet

### 1. Crawler les packages CKAN

```bash
uv run python crawl.py
```

Effet:

- alimente `staging/organizations.jsonl`
- alimente `staging/packages.jsonl`
- alimente `staging/resources.jsonl`
- met à jour `staging/crawl_summary.json`

### 2. Télécharger les fichiers retenus

```bash
uv run python download.py
```

Effet:

- télécharge les fichiers dans `data/`
- met à jour `staging/download_state.jsonl`

### 3. Construire les métadonnées finales

```bash
uv run python build_metadata.py
```

Effet:

- écrit `metadata/institution.csv`
- écrit `metadata/folder.csv`
- écrit `metadata/dataset.csv`
- écrit `metadata/tag.csv`
- écrit `metadata/doc.csv`

### 4. Construire le catalogue avec datannurpy

```bash
uv run python -m datannurpy catalog.yml
```

Effet:

- lit `metadata/` et `data/`
- scanne les fichiers tabulaires
- produit / met à jour le catalogue dans `catalog/`
- écrit un log `datannurpy*.log`

## Boucle optionnelle après datannurpy

`mark_excluded.py` n'est pas une étape normale du pipeline de collecte.
Il sert uniquement après un passage de `datannurpy`, pour réinjecter dans
`excluded_datasets.csv` les ressources qui ont produit `0 vars` et qu'on veut
ensuite ignorer lors des prochains téléchargements / builds.

```bash
uv run python mark_excluded.py [datannurpy*.log ...]
```

Sans argument, le script scanne les logs `datannurpy*.log` trouvés à la racine
du projet ou dans `staging/`.

## Rejouer uniquement la fin du pipeline

Si `staging/` et `data/` sont déjà remplis, il suffit souvent de relancer:

```bash
uv run python build_metadata.py
uv run python -m datannurpy catalog.yml
```

## Vérifications

Vérification statique du code:

```bash
make check
```

Ou séparément:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

## Remarques

- `staging/` est un état de travail partagé entre formats.
- `download.py` lit directement `staging/resources.jsonl`; l'étape de probe séparée a été supprimée.
- Le champ `format_key` dans les JSONL reste utilisé en interne pour appliquer les règles propres à chaque format.
- Le mapping métier vers datannur est documenté dans `MAPPING.md`.