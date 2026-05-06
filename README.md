# swiss-open-data

Production pipeline for the public datannur demo catalog at suisse.datannur.com, built from French-language tabular datasets published on opendata.swiss.

## Status

This repository is published for transparency and as a concrete datannur integration example. It is not intended to be a generic template, a stable API, or a reusable product as-is.

## Scope

The pipeline targets opendata.swiss CKAN resources that are:

- French-language or language-neutral;
- tabular files in Parquet, CSV, or Excel format;
- processed as part of the demo catalog build.

PDF documentation referenced by CKAN packages or resources is downloaded separately and exposed as datannur documents.

## CKAN to datannur Mapping

In CKAN, a `package` is a publication record. It contains metadata, a publishing organization, thematic groups, keywords, and one or more `resources`. A resource is an individual file or URL attached to the package.

Resources in the same package may represent the same data in different formats, a time series split across files, or different tables grouped under the same publication record. The pipeline therefore does not assume that resources in the same package share a schema.

| datannur entity | Source |
| --- | --- |
| `organization` | CKAN organizations plus a small reconstructed hierarchy |
| `folder` | CKAN packages and top-level thematic folders |
| `dataset` | Successfully downloaded CKAN resources |
| `tag` | CKAN thematic groups and French keywords |
| `doc` | Deduplicated PDF documentation URLs |

Each downloaded CKAN resource becomes one datannur `dataset`. This matches how `datannurpy` scans files and avoids merging resources that may have different schemas.

Each CKAN package becomes one datannur `folder`. The folder preserves the editorial grouping between resources from the same publication record.

Top-level folders are created from DCAT-AP-CH thematic groups. Packages with one group are placed under that group. Packages with several groups are placed under `multi`; packages without a group are placed under `other`. The same thematic information is also exposed as tags, so users can either browse by folder or filter by tag.

CKAN `organization` is mapped to datannur `owner_id`, because it represents the publisher of the source package. CKAN `contact_points` are mapped to datannur `manager_id` only when a valid email is available. These manager organizations are synthetic, deduplicated globally by email, and attached to both the package folder and its datasets.

The organization hierarchy is lightly reconstructed from CKAN organization metadata and a few project-specific containers, such as national, cantonal, communal, and other institutions. This improves navigation in the demo catalog, but is not meant to be a complete institutional authority file.

The pipeline emits two tag families: thematic tags from CKAN groups and free keyword tags from `keywords.fr`. Thematic tags are grouped under a common root, and free keywords are grouped under a separate root to avoid mixing controlled themes with free-text keywords.

PDF URLs found in package and resource metadata are downloaded into the documentation cache and exported as datannur `doc` rows. Documents are deduplicated globally by source URL, so the same PDF can be referenced by multiple folders or datasets.

## Repository Structure

- `src/`: pipeline scripts
- `staging/`: intermediate crawl, download, and PDF documentation state
- `data/`: downloaded tabular files
- `metadata/`: CSV files generated for datannur
- `public/`: manually maintained assets and catalog configuration
- `app_conf/`: app configuration copied into the generated catalog; sensitive local files are ignored
- `catalog/`: datannurpy output, ignored by Git

## Requirements

- `uv`
- The Python version defined in `pyproject.toml`
- Node.js and npm for the final static build and deployment step

Install the environment:

```bash
uv sync
```

Create the private app configuration files from the examples before building or deploying the catalog:

```bash
cp app_conf/deploy.config.example.json app_conf/deploy.config.json
cp app_conf/llm-web.config.example.json app_conf/llm-web.config.json
```

`app_conf/deploy.config.json` and `app_conf/llm-web.config.json` are ignored by Git because they may contain deployment credentials, API keys, and other environment-specific values. Review and replace the placeholders before running the final catalog build or deployment steps.

## Running the Pipeline

Run commands from the repository root unless noted otherwise.

### 1. Crawl CKAN Packages

```bash
uv run python src/crawl.py
```

Writes the selected organizations, packages, and resources to `staging/`.

### 2. Download Tabular Files

```bash
uv run python src/download.py
```

Downloads tabular files to `data/` and updates `staging/download_state.jsonl`. The command is idempotent: existing files are skipped. It can be rerun to retry failed downloads.

### 3. Download Documentation PDFs

```bash
uv run python src/download_docs.py
```

Downloads documentation PDFs to `staging/docs/` and updates `staging/doc_download_state.jsonl`.

### 4. Build Metadata CSVs

```bash
uv run python src/build_metadata.py
```

Generates the metadata files consumed by datannurpy in `metadata/`: `organization.csv`, `folder.csv`, `dataset.csv`, `tag.csv`, `doc.csv`, and `config.json`.

### 5. Build the datannur Catalog

```bash
uv run python -m datannurpy catalog.yml
```

Builds the datannur catalog from `metadata/` and `data/`, copies private app configuration from `app_conf/`, then writes the result to `catalog/`.

### 6. Build and Deploy Static Pages

```bash
cd catalog
npm install
npm run static-deploy
```

Builds and publishes the static version of the catalog.

## Optional Exclusion Loop

`mark_excluded.py` is not part of the normal pipeline. After a `datannurpy` run, it can add resources that produced `0 vars` to `staging/excluded_datasets.csv` so they are ignored by future downloads and builds.

```bash
uv run python src/mark_excluded.py [datannurpy*.log ...]
```

Without arguments, the script scans `staging/logs/datannurpy*.log`, then also accepts `datannurpy*.log` files at the repository root or in `staging/`.

## Rebuild Only the Final Stages

If `staging/` and `data/` already exist, the final stages can be rerun with:

```bash
uv run python src/build_metadata.py
uv run python -m datannurpy catalog.yml
```

## Checks

Run all static checks:

```bash
make check
```

Or run them separately:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

## License

The pipeline code in this repository is released under the MIT License. Source datasets, metadata, and documentation fetched from opendata.swiss remain governed by their original publisher terms.