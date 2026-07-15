![datannur banner](https://raw.githubusercontent.com/datannur/swiss-open-data/main/public/img/main-banner-dark.png#gh-dark-mode-only)
![datannur banner](https://raw.githubusercontent.com/datannur/swiss-open-data/main/public/img/main-banner.png#gh-light-mode-only)

[![MIT License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Demo](https://img.shields.io/badge/demo-live-success)](https://swiss-demo.datannur.com/)

# swiss-open-data

Production pipeline for the public datannur demo catalog at swiss-demo.datannur.com, built from the [i14y interoperability platform](https://www.i14y.admin.ch/) of the Swiss Confederation.

## Status

This repository is published for transparency and as a concrete datannur integration example. It is not intended to be a generic template, a stable API, or a reusable product as-is.

## Why i14y

datannur's value is documentation at the **variable** level — labels, descriptions and links to controlled code lists. A file scan only recovers column names and types, so a plain open-data file portal cannot feed that layer.

i14y is the only Swiss source that publishes, per variable:

- a human, multilingual **label** and **description**;
- a link (`dct:conformsTo`) to a controlled **code list** (code → meaning).

The catalog is therefore built on the **native** i14y datasets — curated directly on i14y (their `identifier` has no `@`), as opposed to the datasets i14y harvests from opendata.swiss, which are auto-generated, thin, and redundant with the file scan. Native datasets with a published data structure are the well-documented, showcase-quality subset (~180 datasets across federal offices and the cantons of Basel-Landschaft and Bern).

Data files come from each dataset's own i14y distribution, not from opendata.swiss.

## i14y to datannur Mapping

| datannur entity | Source (i14y) |
| --- | --- |
| `organization` | Publishers, grouped by `classification` (federal / cantonal / other public-law / association) under a single national root — a 3-level institutional tree |
| `folder` | A root `i14y` folder, one sub-folder per publisher, and a `codelist` folder for the shared code lists |
| `dataset` | Native datasets that have a published data structure |
| `variable` | Scanned from the data file, then enriched with the i14y multilingual label, description and code-list link |
| `enumeration` + `value` | i14y code lists (code → multilingual label) |
| `tag` | i14y controlled thematic vocabulary |
| `doc` | PDF / Markdown documentation referenced by the dataset (`documentation`, `relations`) |

The join that attaches the variable overlays: i14y's `sh:path` equals the file's column header (lowercased), and datannur's scanned variable id is `{dataset_id}---{header}`. The pipeline reads the real header from the downloaded file to recover the exact case, so `{dataset_id}---{header}` lands on the scanned variable. Code lists are supplied through metadata rather than derived from the data (`auto_enumerations` is off in `catalog.yml`).

## Repository Structure

- `src/i14y.py`: the whole pipeline — fetch i14y, download files and docs, emit metadata CSVs
- `staging/i14y/`: cached i14y API responses (records, structures, code lists) — reruns are offline
- `staging/docs/`: downloaded documentation files
- `data/i14y/`: downloaded tabular files
- `metadata/`: CSV files generated for datannurpy, plus the manually maintained `config.json` / `configFilter.json`
- `public/`: manually maintained assets and catalog configuration
- `app_conf/`: app configuration copied into the generated catalog; sensitive local files are ignored
- `catalog/`: datannurpy output, ignored by Git

## Requirements

- `uv`
- The Python version defined in `pyproject.toml`
- Playwright for the optional static page build

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

Run commands from the repository root.

### 1. Build the i14y metadata and download the files

```bash
uv run python src/i14y.py
```

Fetches the native i14y catalog, downloads the data files into `data/i14y/` and the documentation into `staging/docs/`, and writes the metadata CSVs into `metadata/` (`organization.csv`, `tag.csv`, `folder.csv`, `dataset.csv`, `variable.csv`, `enumeration.csv`, `value.csv`, `doc.csv`). API responses are cached under `staging/i14y/`, so reruns are offline and only re-emit metadata. Useful flags: `--limit N` (first N datasets), `--publisher <id>`, `--no-download` (metadata only), `--out <dir>` (isolated test catalog).

### 2. Build the datannur Catalog

```bash
uv run python -m datannurpy catalog.yml
```

Scans `data/i14y/` at `depth: value`, applies the `metadata/` overlays, copies private app configuration from `app_conf/` and documentation from `staging/docs/`, then writes the result to `catalog/`.

### 3. Optionally Generate API Documentation

```bash
python3 catalog/datannur.py openapi
```

Generates the static OpenAPI files served under `/api/` from the exported catalog database in `catalog/data/db`.

### 4. Optionally Export DCAT Artifacts

```bash
python3 catalog/datannur.py dcat
```

Exports DCAT interoperability artifacts from the exported catalog database, configured by `app_conf/dcat-export.config.json`.

### 5. Optionally Build Static Pages

```bash
python3 catalog/datannur.py static
```

Builds the static version of the catalog. Requires Playwright.

### 6. Deploy the Catalog

```bash
python3 catalog/datannur.py deploy
```

Publishes the catalog.

## Checks

```bash
make check
```

Or separately:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

## Continuous Deployment

`.github/workflows/build.yml` runs the whole pipeline on GitHub Actions,
adapted from [datannur/datannur-template](https://github.com/datannur/datannur-template):

- **Pull requests** run only the `check` job — `ruff`, `pyright`, and a
  metadata-only smoke build (`src/i14y.py --limit 12 --no-download`). Nothing is
  fetched at scale or deployed.
- **Pushes to `main`, the weekly schedule, and manual runs** (`workflow_dispatch`)
  fetch i14y, build the catalog, export the DCAT and OpenAPI artifacts, build the
  static pages, and deploy to swiss-demo.datannur.com by rsync over SSH. The weekly
  cron keeps the catalog in sync with i14y automatically.

The i14y API responses and downloaded files are cached between runs, so push and
manual reruns stay fast and gentle on the API. The weekly scheduled run drops that
cache first so it picks up new and changed i14y datasets; the incremental scan
cache is kept and re-scans only the files that actually changed.

Deployment requires these repository secrets (**Settings → Secrets and variables
→ Actions**):

| Secret | Value |
| --- | --- |
| `DEPLOY_SSH_KEY` | Private SSH key authorized on the target server |
| `DEPLOY_HOST` | Server hostname |
| `DEPLOY_USER` | SSH user |
| `DEPLOY_PORT` | SSH port (optional, defaults to 22) |
| `DEPLOY_REMOTE_PATH` | Absolute path of the catalog root on the server |
| `INFOMANIAK_API_KEY` | Infomaniak AI key for the LLM widget (optional) |
| `INFOMANIAK_PRODUCT_ID` | Infomaniak AI product id (optional) |
| `TURNSTILE_SITE_KEY` | Cloudflare Turnstile site key (optional) |
| `TURNSTILE_SECRET_KEY` | Cloudflare Turnstile secret key (optional) |

The workflow generates `app_conf/deploy.config.json` and, when the Infomaniak
secrets are set, `app_conf/llm-web.config.json` from these secrets at build time;
they never leave the runner. The LLM credentials are read only server-side by the
deployed PHP proxy (`app/api/llm`) and denied over HTTP by `.htaccess`, so they
are never exposed to the browser. Without the deploy secrets the build and
artifact steps still succeed and only the final rsync fails; without the LLM
secrets the catalog builds normally with the LLM widget disabled.

## License

The pipeline code in this repository is released under the MIT License. Source datasets, metadata, code lists, and documentation fetched from the i14y interoperability platform remain governed by their original publisher terms.
