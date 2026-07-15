#!/usr/bin/env python3
"""Build a datannur catalog from the i14y interoperability platform.

Why i14y and not opendata.swiss for this corpus
------------------------------------------------
datannurpy already derives variable *names* and *types* by scanning the data
files, so a plain opendata.swiss resource adds nothing at the variable level.
i14y is the only Swiss source that publishes, per variable, the three things a
scan can never infer:

  * a human, multilingual *label*  (sh:name in de/fr/it/en)
  * a *description*                (sh:description / dct:description)
  * a *code-list link*             (dct:conformsTo -> a controlled code list)

That documentation is concentrated in the **native** i14y datasets — those
curated directly on i14y, whose ``identifier`` has no ``@`` (harvested
opendata.swiss datasets carry ``<n>@<org>`` identifiers and are auto-generated,
thin, and redundant with our scan). This module keeps only native datasets that
have a published structure.

What it emits (into ``--out``/metadata, scanned files into ``--out``/data)
-------------------------------------------------------------------------
  organization.csv  — one per i14y publisher
  folder.csv        — a root "i14y" folder, one sub-folder per publisher,
                      plus a "codelist" folder holding the shared code lists
  dataset.csv       — one per native dataset (``_match_path`` links the file)
  variable.csv      — overlays keyed by ``{dataset_id}---{column}`` carrying the
                      i14y label, description and ``enumeration_ids``
  enumeration.csv   — one per referenced code list
  value.csv         — code -> multilingual label rows for each code list

datannurpy then scans the downloaded files (depth ``value``,
``auto_enumerations`` OFF — the code lists come from i14y, not from the scan)
and merges these overlays by id.

The join that makes it work: i14y ``sh:path`` equals the file's column header,
and datannur's scanned variable id is ``{dataset_id}---{header}``. We choose the
dataset id in dataset.csv (metadata wins over the scan via ``_match_path``), so
``{dataset_id}---{sh:path}`` lands exactly on the scanned variable.

Network responses are cached under ``staging/i14y/`` so re-runs are offline and
cheap. Run ``python src/i14y.py --limit 8`` for a quick end-to-end slice.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.parse import unquote

# datannurpy's own column-name sanitizer, used to build scanned variable ids
# ({dataset_id}---{sanitize_id(col)}). Importing it guarantees our overlay ids
# track datannurpy's exactly (``.`` -> ``_``, BOM -> ``_``, ...).
from datannurpy.utils.ids import sanitize_id

MAX_WORKERS = 16

ROOT = Path(__file__).resolve().parent.parent
API = "https://api.i14y.admin.ch/api/public/v1"
CACHE = ROOT / "staging" / "i14y"
# Production catalog layout: metadata/ + data/i14y/ at the repo root, scanned by
# catalog.yml. Override with --out for an isolated test build.
DEFAULT_OUT = ROOT

ID_SEP = "---"
# Display language priority: base field gets the first available; localized
# ``field:lang`` columns are emitted for the rest. Matches the en/de/fr/it app.
LANG_PRIORITY = ("en", "de", "fr", "it")
LOC_LANGS = ("de", "fr", "it", "en")

# JSON-LD / SHACL predicates used in the structure export.
SH = "http://www.w3.org/ns/shacl#"
DCT = "http://purl.org/dc/terms/"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"

_NON_ID = re.compile(r"[^a-z0-9]+")

# File formats datannurpy can scan, best first.
FORMAT_EXT = {
    "CSV": ".csv",
    "XLSX": ".xlsx",
    "XLS": ".xls",
    "PARQUET": ".parquet",
    "JSON": ".json",
}
FORMAT_PREFERENCE = ("CSV", "XLSX", "XLS", "PARQUET")


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------
def sid(value: str) -> str:
    """Stable slug for our own ids (folder/dataset/enumeration)."""
    return _NON_ID.sub("_", str(value).lower()).strip("_")


def langmap(value: Any) -> dict[str, str]:
    """Turn a JSON-LD ``[{"@language","@value"}]`` list into ``{lang: value}``."""
    out: dict[str, str] = {}
    for item in value if isinstance(value, list) else [value]:
        if isinstance(item, dict) and item.get("@language") and item.get("@value"):
            out[item["@language"]] = item["@value"]
    return out


def name_map(value: Any) -> dict[str, str]:
    """i14y REST ``name``/``title`` objects are plain ``{lang: value}`` dicts."""
    if isinstance(value, dict):
        return {k: v for k, v in value.items() if isinstance(v, str) and v}
    return {}


def loc_cols(prefix: str, m: dict[str, str]) -> dict[str, str]:
    """Expand a ``{lang: value}`` map into base + ``prefix:lang`` CSV columns."""
    row: dict[str, str] = {}
    if not m:
        return row
    for lang in LANG_PRIORITY:
        if m.get(lang):
            row[prefix] = m[lang]
            break
    for lang in LOC_LANGS:
        if m.get(lang):
            row[f"{prefix}:{lang}"] = m[lang]
    return row


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    """Write UTF-8 / LF CSV, dropping columns empty across all rows (keeps ``id``).

    Mirrors src/build_metadata.py: all-empty columns would be read as float64 by
    the polars ingestion and break ``str | None`` fields.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    kept = [
        c
        for c in columns
        if c in ("id", "enumeration_id")
        or any(row.get(c) not in (None, "") for row in rows)
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=kept,
            extrasaction="ignore",
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )
        w.writeheader()
        for row in rows:
            w.writerow({c: ("" if row.get(c) is None else row.get(c)) for c in kept})
    print(f"  wrote {path.relative_to(ROOT)}  ({len(rows)} rows)")


# ---------------------------------------------------------------------------
# HTTP + cache
# ---------------------------------------------------------------------------
# Some servers (e.g. www.zivi.admin.ch) reset the connection for the default
# Python-urllib agent; send a browser-like User-Agent.
_USER_AGENT = "Mozilla/5.0 (compatible; datannur-i14y/1.0; +https://datannur.com)"


def _fetch(url: str) -> tuple[bytes, dict[str, str]]:
    req = urllib.request.Request(
        url, headers={"Accept": "*/*", "User-Agent": _USER_AGENT}
    )
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                return r.read(), {k.lower(): v for k, v in r.headers.items()}
        except urllib.error.HTTPError:
            raise  # HTTP status errors are not transient; caller handles them
        except urllib.error.URLError, OSError, TimeoutError:
            if attempt == 3:
                raise
            time.sleep(2 * (attempt + 1))
    raise RuntimeError("unreachable")


def cached_json(cache_path: Path, url: str) -> Any:
    """Fetch ``url`` as JSON, caching the raw body at ``cache_path``."""
    if cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8"))
    body, _ = _fetch(url)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(body)
    return json.loads(body)


def search_all(params: str, cache_name: str) -> list[dict]:
    """Page through ``/search`` (paging total is in ``x-paging-totalrows``)."""
    cache_path = CACHE / cache_name
    if cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8"))
    rows: list[dict] = []
    page = 1
    while True:
        url = f"{API}/search?{params}&page={page}&pageSize=1000"
        body, headers = _fetch(url)
        data = json.loads(body).get("data", [])
        rows.extend(data)
        total_pages = int(headers.get("x-paging-totalpages", "1") or "1")
        if page >= total_pages or not data:
            break
        page += 1
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    return rows


# ---------------------------------------------------------------------------
# i14y fetch layer
# ---------------------------------------------------------------------------
def native_datasets() -> list[dict]:
    """Native (non-harvested) datasets that have a published structure."""
    rows = search_all(
        "types=Dataset&structure=WithStructure", "search_withstructure.json"
    )
    return [r for r in rows if "@" not in str(r.get("identifier", ""))]


def dataset_record(dataset_id: str) -> dict:
    d: Any = cached_json(
        CACHE / "record" / f"{dataset_id}.json", f"{API}/datasets/{dataset_id}"
    )
    if isinstance(d, dict):
        d = d.get("data", d)
    if isinstance(d, list):
        d = d[0] if d else {}
    return d if isinstance(d, dict) else {}


def dataset_structure(dataset_id: str) -> list[dict]:
    d = cached_json(
        CACHE / "structure" / f"{dataset_id}.json",
        f"{API}/datasets/{dataset_id}/structures/exports/JsonLd",
    )
    return d if isinstance(d, list) else d.get("@graph", [])


def concept_index() -> dict[str, dict]:
    """Map every concept ``identifier`` -> ``{uuid, type, title, description}``.

    ``conformsTo`` can point at a ``CodeList`` (has code -> label entries, becomes
    a datannur enumeration) or at a plain ``Numeric`` / ``String`` / ``Date``
    concept (a semantic type with no enumerated values, becomes a datannur
    concept). We need the ``conceptType`` to route each one correctly.
    """
    rows = search_all("types=Concept", "search_concepts.json")
    return {
        r["identifier"]: {
            "uuid": r["id"],
            "type": r.get("conceptType"),
            "title": name_map(r.get("title")),
            "description": name_map(r.get("description")),
        }
        for r in rows
        if r.get("identifier") and r.get("id")
    }


def codelist_entries(uuid: str) -> list[dict]:
    """Code -> label rows for a code list. Returns ``[]`` for concepts that have
    no exportable code list (the endpoint 400s on non-code-list concepts)."""
    cache_path = CACHE / "codelist" / f"{uuid}.json"
    try:
        d = cached_json(
            cache_path, f"{API}/concepts/{uuid}/codelist-entries/exports/json"
        )
    except urllib.error.HTTPError as e:
        if e.code in (400, 404):
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text('{"data": []}', encoding="utf-8")
            return []
        raise
    return d.get("data", d) if isinstance(d, dict) else d


# ---------------------------------------------------------------------------
# structure parsing
# ---------------------------------------------------------------------------
def _types(node: dict) -> list[Any]:
    t = node.get("@type")
    return t if isinstance(t, list) else [t]


def _path_column(node: dict) -> str | None:
    """The technical column name = last segment of the sh:path IRI."""
    path = node.get(f"{SH}path")
    if isinstance(path, list) and path and isinstance(path[0], dict):
        iri = path[0].get("@id", "")
        # the IRI segment may be percent-encoded (e.g. franz%C3%B6sisch)
        return unquote(iri.rsplit("/", 1)[-1]) or None
    return None


def _conforms_identifier(node: dict) -> str | None:
    """Extract the code-list identifier from a dct:conformsTo IRI."""
    conf = node.get(f"{DCT}conformsTo")
    for item in conf if isinstance(conf, list) else [conf]:
        iri = item.get("@id", "") if isinstance(item, dict) else ""
        if "/concept/" in iri:
            return iri.split("/concept/")[1].split("/version")[0]
    return None


def parse_variables(nodes: list[dict]) -> list[dict]:
    """Return ``[{column, label{lang}, description{lang}, conforms}]`` per column."""
    out: list[dict] = []
    for node in nodes:
        if not any("PropertyShape" in str(t) for t in _types(node)):
            continue
        column = _path_column(node)
        if not column:
            continue
        label = langmap(node.get(f"{SH}name"))
        description = langmap(node.get(f"{SH}description")) or langmap(
            node.get(f"{DCT}description")
        )
        out.append(
            {
                "column": column,
                "label": label,
                "description": description,
                "conforms": _conforms_identifier(node),
            }
        )
    return out


# ---------------------------------------------------------------------------
# column layouts (subset of datannurpy schema fields we populate)
# ---------------------------------------------------------------------------
ORG_COLS = ["id", "parent_id", "name", "name:de", "name:fr", "name:it", "email", "link"]
FOLDER_COLS = [
    "id",
    "parent_id",
    "name",
    "name:de",
    "name:fr",
    "name:it",
    "description",
    "description:de",
    "description:fr",
    "description:it",
]
TAG_COLS = ["id", "parent_id", "name", "name:de", "name:fr", "name:it"]
DATASET_COLS = [
    "id",
    "folder_id",
    "owner_organization_id",
    "manager_organization_id",
    "tag_ids",
    "doc_ids",
    "name",
    "name:de",
    "name:fr",
    "name:it",
    "description",
    "description:de",
    "description:fr",
    "description:it",
    "data_path",
    "_match_path",
    "link",
    "license",
    "license:de",
    "license:fr",
    "license:it",
    "delivery_format",
    "localisation",
    "start_date",
    "end_date",
    "updating_each",
    "updating_each:de",
    "updating_each:fr",
    "updating_each:it",
    "last_update_date",
]
VARIABLE_COLS = [
    "id",
    "dataset_id",
    "name",
    "name:de",
    "name:fr",
    "name:it",
    "description",
    "description:de",
    "description:fr",
    "description:it",
    "enumeration_ids",
    "concept_id",
]
CONCEPT_COLS = [
    "id",
    "name",
    "name:de",
    "name:fr",
    "name:it",
    "description",
    "description:de",
    "description:fr",
    "description:it",
]
ENUM_COLS = [
    "id",
    "folder_id",
    "name",
    "name:de",
    "name:fr",
    "name:it",
    "description",
    "description:de",
    "description:fr",
    "description:it",
]
VALUE_COLS = [
    "enumeration_id",
    "value",
    "description",
    "description:de",
    "description:fr",
    "description:it",
]
DOC_COLS = [
    "id",
    "name",
    "description",
    "description:de",
    "description:fr",
    "description:it",
    "path",
    "type",
    "last_update",
]

# Multilingual display labels for the generated nomenclature columns (the raw
# headers are Code / ParentCode / Name_<lang>). A ``Name_de`` column holds the
# German label, so it is labelled "Label (German)" in every UI language.
_LANG_NAME = {
    "de": {"en": "German", "de": "Deutsch", "fr": "allemand", "it": "tedesco"},
    "fr": {"en": "French", "de": "Französisch", "fr": "français", "it": "francese"},
    "it": {"en": "Italian", "de": "Italienisch", "fr": "italien", "it": "italiano"},
    "rm": {"en": "Romansh", "de": "Rätoromanisch", "fr": "romanche", "it": "romancio"},
    "en": {"en": "English", "de": "Englisch", "fr": "anglais", "it": "inglese"},
}
_LABEL_WORD = {
    "en": "Label",
    "de": "Bezeichnung",
    "fr": "Libellé",
    "it": "Denominazione",
}
_DESC_WORD = {
    "en": "Description",
    "de": "Beschreibung",
    "fr": "Description",
    "it": "Descrizione",
}
NOMEN_COLUMN_LABELS: dict[str, dict[str, str]] = {
    "Code": {"en": "Code", "de": "Code", "fr": "Code", "it": "Codice"},
    "ParentCode": {
        "en": "Parent code",
        "de": "Übergeordneter Code",
        "fr": "Code parent",
        "it": "Codice superiore",
    },
    **{
        f"Name_{lang}": {
            ui: f"{_LABEL_WORD[ui]} ({_LANG_NAME[lang][ui]})" for ui in _LABEL_WORD
        }
        for lang in _LANG_NAME
    },
    **{
        f"Description_{lang}": {
            ui: f"{_DESC_WORD[ui]} ({_LANG_NAME[lang][ui]})" for ui in _DESC_WORD
        }
        for lang in _LANG_NAME
    },
}

CODELIST_FOLDER = "i14y" + ID_SEP + "codelist"
NOMEN_FOLDER = "i14y" + ID_SEP + "nomenclatures"
NATIONAL_ROOT = "ch"
THEME_ROOT = "theme"
KEYWORD_ROOT = "keyword"
# i14y has no license, only an accessRights code. Show it as a readable
# multilingual access label; the DCAT export maps these to licence IRIs
# (see app_conf/dcat-export.config.json: Open -> terms_open, On request -> terms_ask).
ACCESS_LABELS = {
    "PUBLIC": {"en": "Open", "fr": "Libre", "de": "Frei", "it": "Libero"},
    "RESTRICTED": {
        "en": "On request",
        "fr": "Sur demande",
        "de": "Auf Anfrage",
        "it": "Su richiesta",
    },
}
# Code lists with at least this many entries are also exposed as browsable
# nomenclature datasets (NOGA, ISCO, CHOP, ICD-10, ...); smaller ones stay
# enumerations only.
CLASSIFICATION_MIN_ENTRIES = 100
# Fields on an i14y record that may reference documentation files.
# Only PDFs: they are actual files. Other extensions (.md, ...) in these fields
# usually point at web pages (e.g. a GitHub blob view), which download as HTML.
DOC_FIELDS = ("documentation", "relations")
DOC_EXTS = (".pdf",)


def doc_id_for(url: str, ext: str) -> str:
    h = hashlib.blake2b(url.encode("utf-8"), digest_size=8).hexdigest()
    return f"doc{ID_SEP}{ext.lstrip('.')}{ID_SEP}{h}"


def collect_docs(record: dict, docs: dict[str, dict]) -> list[str]:
    """Register a record's documentation files (pdf/md) globally by URL and
    return the doc ids attached to this dataset. Mirrors the datannur ``doc``
    convention: files land in staging/docs and are copied to data/doc."""
    ids: list[str] = []
    for field in DOC_FIELDS:
        for item in record.get(field) or []:
            uri = item.get("uri") if isinstance(item, dict) else item
            if not uri:
                continue
            low = str(uri).lower()
            ext = next((e for e in DOC_EXTS if low.endswith(e)), None)
            if not ext:
                continue
            doc_id = doc_id_for(uri, ext)
            if doc_id not in ids:
                ids.append(doc_id)
            if doc_id not in docs:
                label = (
                    name_map((item or {}).get("label"))
                    if isinstance(item, dict)
                    else {}
                )
                src = {lang: f"Source: {uri}" for lang in ("en", "de", "fr", "it")}
                docs[doc_id] = {
                    "id": doc_id,
                    "path": f"data/doc/{doc_id}{ext}",
                    "type": ext.lstrip("."),
                    "_url": uri,
                    **(
                        loc_cols("name", label)
                        if label
                        else {"name": uri.rsplit("/", 1)[-1]}
                    ),
                    **loc_cols("description", src),
                }
    return ids


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------
def real_columns(path: Path) -> list[str]:
    """Actual column headers of a downloaded file, as datannurpy will scan them.

    i14y lowercases ``sh:path`` (``bfs_gemeindenummer``) and uses underscores
    where the file header keeps case and spaces (``BFS_Gemeindenummer``,
    ``Projekttitel deutsch``); matching overlays to scanned variables needs the
    real names. Handles CSV/TSV and XLSX; returns ``[]`` otherwise."""
    if not path.exists() or path.stat().st_size == 0:
        return []
    ext = path.suffix.lower()
    if ext in (".csv", ".tsv", ".txt"):
        # datannurpy strips leading BOM(s) from column names; match that.
        # utf-8-sig drops one BOM at decode, lstrip removes extras (double BOM).
        with path.open(encoding="utf-8-sig", newline="") as fh:
            first = fh.readline().lstrip("\ufeff")
        if not first:
            return []
        delim = max((",", ";", "\t", "|"), key=first.count)
        return next(csv.reader([first], delimiter=delim), [])
    if ext in (".xlsx", ".xls"):
        try:
            import openpyxl  # datannurpy scans Excel via pandas + openpyxl

            wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
            ws = wb.active
            if ws is None:
                wb.close()
                return []
            row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), ())
            wb.close()
            return [str(c) for c in row if c is not None and str(c).strip()]
        except Exception:  # noqa: BLE001 - fall back to no header match
            return []
    return []


def col_key(name: str) -> str:
    """Separator-insensitive key for matching an i14y ``sh:path`` slug to a real
    header. i14y lowercases and normalizes separators (``E-Mail`` -> ``e_mail``),
    so drop every non-alphanumeric char on both sides before comparing."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def download_file(url: str, local: Path) -> bool:
    if local.exists() and local.stat().st_size > 0:
        return True
    try:
        body, _ = _fetch(url)
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(body)
        return True
    except Exception as e:  # noqa: BLE001 - keep the run going
        print(f"  ! download failed {url}: {e}")
        return False


def candidate_distributions(record: dict) -> list[tuple[str, str]]:
    """Ordered ``(download_url, ext)`` candidates, best scannable format first.

    Returns several so the caller can fall through to the next format when a
    distribution downloads empty (e.g. aramis publishes a 0-byte CSV but a full
    XLSX)."""
    dists = record.get("distributions") or []
    by_fmt: dict[str, str] = {}
    for dist in dists:
        du = dist.get("downloadUrl") or {}
        uri = du.get("uri") if isinstance(du, dict) else du
        if not uri:
            continue
        fmt = str(dist.get("format") or dist.get("mediaType") or "").upper()
        for known in FORMAT_EXT:
            if known in fmt or str(uri).lower().endswith(FORMAT_EXT[known]):
                by_fmt.setdefault(known, uri)
    return [(by_fmt[f], FORMAT_EXT[f]) for f in FORMAT_PREFERENCE if f in by_fmt]


def ensure_org(pub: dict, orgs: dict) -> str:
    """Register a publisher as an organization (under its classification group,
    below the national root) and return its id. Idempotent."""
    org_id = sid(pub.get("identifier") or pub.get("id") or "unknown")
    if org_id not in orgs:
        cls = pub.get("classification") or {}
        ccode = cls.get("code")
        if ccode:
            grp_id = f"org{ID_SEP}class{ID_SEP}{sid(ccode)}"
            if grp_id not in orgs:
                orgs[grp_id] = {
                    "id": grp_id,
                    "parent_id": NATIONAL_ROOT,
                    **loc_cols("name", name_map(cls.get("name"))),
                }
            parent = grp_id
        else:
            parent = NATIONAL_ROOT
        orgs[org_id] = {
            "id": org_id,
            "parent_id": parent,
            "link": pub.get("homePage"),
            **loc_cols(
                "name",
                name_map(pub.get("name") or pub.get("prefLabel"))
                or {"en": pub.get("identifier", org_id)},
            ),
        }
    return org_id


def _first(value: Any) -> dict:
    return (
        value[0]
        if isinstance(value, list) and value and isinstance(value[0], dict)
        else {}
    )


def dataset_extra(rec: dict) -> dict:
    """Scalar dataset fields i14y fills but we would otherwise drop: update
    frequency, temporal coverage, spatial coverage, landing page."""
    row: dict[str, Any] = {}
    freq = rec.get("frequency") or {}
    if freq.get("name"):
        row.update(loc_cols("updating_each", name_map(freq.get("name"))))
    tc = _first(rec.get("temporalCoverage"))
    if tc.get("start"):
        row["start_date"] = str(tc["start"])[:10]
    if tc.get("end"):
        row["end_date"] = str(tc["end"])[:10]
    spatial = [str(s) for s in (rec.get("spatial") or []) if s]
    if spatial:
        row["localisation"] = "; ".join(spatial)[:250]
    lp = _first(rec.get("landingPages"))
    if lp.get("uri"):
        row["link"] = lp["uri"]
    return row


def ensure_manager(rec: dict, orgs: dict, owner_id: str) -> str:
    """Register the dataset's contact point as a manager organization nested
    under its publisher (``owner_id``) -> a finer level of the org tree (e.g. the
    39 cantonal units under Basel-Landschaft). Returns its id, or '' when there
    is no contact. Deduped globally by email."""
    cp = _first(rec.get("contactPoints"))
    if not cp:
        return ""
    email = cp.get("hasEmail")
    fn = name_map(cp.get("fn"))
    if not (email or fn):
        return ""
    org_id = "contact" + ID_SEP + sid(email or "|".join(sorted(fn.values())))
    if org_id == owner_id:  # contact is the publisher itself: no extra level
        return owner_id
    if org_id not in orgs:
        orgs[org_id] = {
            "id": org_id,
            "parent_id": owner_id,
            "email": email,
            **(loc_cols("name", fn) or {"name": email}),
        }
    return org_id


def codelist_csv(uuid: str) -> Path:
    """Download (and cache) a code list's CSV export: Code, ParentCode, Name_*,
    Description_* — the hierarchical, multilingual table."""
    cache = CACHE / "codelist_csv" / f"{uuid}.csv"
    if not cache.exists():
        body, _ = _fetch(f"{API}/concepts/{uuid}/codelist-entries/exports/csv")
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_bytes(body)
    return cache


def _csv_row_count(path: Path) -> int:
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return max(sum(1 for _ in fh) - 1, 0)  # minus header


def select_classifications() -> list[dict]:
    """Latest version of each CodeList with >= CLASSIFICATION_MIN_ENTRIES entries.

    A classification (NOGA, ISCO, CHOP, ...) is worth exposing as a browsable
    dataset; tiny code lists (yes/no, sex) stay enumerations only."""
    rows = search_all("types=Concept", "search_concepts.json")
    codelists = [
        r
        for r in rows
        if r.get("conceptType") == "CodeList" and r.get("id") and r.get("identifier")
    ]
    # dedup by identifier -> latest version (by validFrom then version string)
    latest: dict[str, dict] = {}
    for r in codelists:
        ident = r["identifier"]
        key = (r.get("validFrom") or "", str(r.get("version") or ""))
        if ident not in latest or key > latest[ident]["_key"]:
            latest[ident] = {**r, "_key": key}
    chosen = list(latest.values())
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        list(ex.map(lambda r: codelist_csv(r["id"]), chosen))
    return [
        r
        for r in chosen
        if _csv_row_count(codelist_csv(r["id"])) >= CLASSIFICATION_MIN_ENTRIES
    ]


def write_classification_csv(src: Path, dest: Path) -> None:
    """Copy a code list CSV keeping the nomenclature columns (Code, ParentCode,
    multilingual Name_* / Description_*) and dropping the verbose Annotation_*
    columns and any column empty for every row."""
    with src.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        cols = list(reader.fieldnames or [])
        rows = list(reader)
    kept = [
        c
        for c in cols
        if (c in ("Code", "ParentCode") or c.startswith(("Name_", "Description_")))
        and any(r.get(c) for r in rows)
    ]
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=kept, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in kept})


def build(out: Path, limit: int | None, publisher: str | None, download: bool) -> None:
    data_dir = out / "data" / "i14y"
    meta_dir = out / "metadata"
    data_dir.mkdir(parents=True, exist_ok=True)

    datasets = native_datasets()
    if publisher:
        datasets = [
            d
            for d in datasets
            if sid((d.get("publisher") or {}).get("identifier", "")) == sid(publisher)
        ]
    if limit:
        datasets = datasets[:limit]
    print(f"i14y: {len(datasets)} native dataset(s) selected")

    # Warm the per-dataset caches in parallel (records + structures); the main
    # loop below then reads them from disk. Sequential fetching of a few hundred
    # small JSON docs is the slow part on a cold cache.
    ids = [d["id"] for d in datasets]
    print("i14y: prefetching records + structures...")
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        list(ex.map(dataset_record, ids))
        list(ex.map(dataset_structure, ids))

    cidx = concept_index()

    orgs: dict[str, dict] = {
        NATIONAL_ROOT: {
            "id": NATIONAL_ROOT,
            "name": "Switzerland",
            "name:de": "Schweiz",
            "name:fr": "Suisse",
            "name:it": "Svizzera",
        },
    }
    tags: dict[str, dict] = {
        THEME_ROOT: {
            "id": THEME_ROOT,
            "name": "Themes",
            "name:de": "Themen",
            "name:fr": "Thèmes",
            "name:it": "Temi",
        },
        KEYWORD_ROOT: {
            "id": KEYWORD_ROOT,
            "name": "Keywords",
            "name:de": "Schlüsselwörter",
            "name:fr": "Mots-clés",
            "name:it": "Parole chiave",
        },
    }
    folders: dict[str, dict] = {
        "i14y": {"id": "i14y", "name": "i14y", "name:de": "i14y", "name:fr": "i14y"},
        CODELIST_FOLDER: {
            "id": CODELIST_FOLDER,
            "parent_id": "i14y",
            "name": "Code lists",
            "name:de": "Codelisten",
            "name:fr": "Listes de codes",
            "name:it": "Liste di codici",
        },
        NOMEN_FOLDER: {
            "id": NOMEN_FOLDER,
            "parent_id": "i14y",
            "name": "Nomenclatures",
            "name:de": "Nomenklaturen",
            "name:fr": "Nomenclatures",
            "name:it": "Nomenclature",
        },
    }
    ds_rows: list[dict] = []
    var_rows: list[dict] = []
    docs: dict[str, dict] = {}
    needed_codelists: set[str] = set()
    concepts_used: dict[str, dict] = {}
    structure_only = 0
    unmatched = 0

    for i, summary in enumerate(datasets, 1):
        did = summary["id"]
        rec = dataset_record(did)
        variables = parse_variables(dataset_structure(did))
        if not variables:
            continue

        # organization (publisher), grouped under its classification below a
        # single national root -> a real 3-level tree
        pub = rec.get("publisher") or {}
        org_id = ensure_org(pub, orgs)

        # thematic tags (i14y controlled theme vocabulary)
        dataset_tag_ids: list[str] = []
        for th in rec.get("themes") or []:
            code = th.get("code") if isinstance(th, dict) else th
            if not code:
                continue
            tid = f"theme{ID_SEP}{sid(code)}"
            if tid not in dataset_tag_ids:
                dataset_tag_ids.append(tid)
            if tid not in tags:
                tags[tid] = {
                    "id": tid,
                    "parent_id": THEME_ROOT,
                    **loc_cols("name", name_map(th.get("name"))),
                }

        # free keyword tags (i14y controlled keywords, deduped by termdat uri)
        for kw in rec.get("keywords") or []:
            label = name_map(kw.get("label")) if isinstance(kw, dict) else {}
            if not label:
                continue
            key = kw.get("uri") or "|".join(sorted(label.values()))
            tid = f"keyword{ID_SEP}{sid(key)}"
            if tid not in dataset_tag_ids:
                dataset_tag_ids.append(tid)
            if tid not in tags:
                tags[tid] = {
                    "id": tid,
                    "parent_id": KEYWORD_ROOT,
                    **loc_cols("name", label),
                }

        # folder per publisher
        folder_id = "i14y" + ID_SEP + org_id
        if folder_id not in folders:
            folders[folder_id] = {
                "id": folder_id,
                "parent_id": "i14y",
                **loc_cols("name", name_map(pub.get("name")) or {"en": org_id}),
            }

        dataset_id = folder_id + ID_SEP + sid(summary.get("identifier") or did)

        # file: download inline so we can read the real header for column
        # matching. Try each distribution in turn and keep the first non-empty
        # one (some datasets publish a 0-byte CSV alongside a full XLSX).
        data_path = ""
        match_path = ""
        colmap: dict[str, str] = {}
        base_name = sid(summary.get("identifier") or did)
        for url, ext in candidate_distributions(rec):
            local = data_dir / f"{base_name}{ext}"
            if download:
                download_file(url, local)
            if local.exists() and local.stat().st_size > 0:
                data_path = url
                # datannurpy resolves _match_path relative to the metadata dir,
                # so write the local path relative to it (walk_up -> ../data/...).
                match_path = local.relative_to(meta_dir, walk_up=True).as_posix()
                colmap = {col_key(c): c for c in real_columns(local)}
                break
            if local.exists():  # empty/broken download: drop it and try the next
                local.unlink()
        else:
            structure_only += 1
        has_file = bool(match_path)

        ds_rows.append(
            {
                "id": dataset_id,
                "folder_id": folder_id,
                "owner_organization_id": org_id,
                "manager_organization_id": ensure_manager(rec, orgs, org_id),
                "tag_ids": ", ".join(dataset_tag_ids),
                "doc_ids": ", ".join(collect_docs(rec, docs)),
                "data_path": data_path,
                "_match_path": match_path,
                **loc_cols(
                    "license",
                    ACCESS_LABELS.get(
                        str((rec.get("accessRights") or {}).get("code")), {}
                    ),
                ),
                "last_update_date": rec.get("modified") or rec.get("issued"),
                **loc_cols("name", name_map(rec.get("title"))),
                **loc_cols("description", name_map(rec.get("description"))),
                **dataset_extra(rec),
            }
        )

        # variable overlays. Resolve the i14y (lowercased) column to the real
        # header so the overlay id equals datannurpy's scanned variable id. Skip
        # when a file column can't be matched, rather than create an orphan.
        for v in variables:
            if has_file:
                # strict: attach only when the real header is found (skip when
                # unmatched instead of creating a phantom twin of the scanned var)
                column = colmap.get(col_key(v["column"]))
            else:
                # structure-only dataset (no data file): emit the documented
                # variable on its own
                column = v["column"]
            if column is None:
                unmatched += 1
                continue
            # datannurpy sanitizes the column for the id (PM2.5 -> PM2_5) but
            # keeps the raw name; mirror both so the overlay merges onto the
            # scanned variable instead of creating a phantom twin.
            vid = dataset_id + ID_SEP + sanitize_id(column)
            row: dict[str, Any] = {"id": vid, "dataset_id": dataset_id, "name": column}
            # only override the display name when i14y gives a real label
            if v["label"] and set(v["label"].values()) != {v["column"]}:
                row.update(loc_cols("name", v["label"]))
            row.update(loc_cols("description", v["description"]))
            meta = cidx.get(v["conforms"]) if v["conforms"] else None
            if meta and meta["type"] == "CodeList":
                # controlled code list -> datannur enumeration (code -> label)
                needed_codelists.add(v["conforms"])
                row["enumeration_ids"] = "codelist" + ID_SEP + sid(v["conforms"])
            elif meta:
                # Numeric / String / Date concept -> datannur concept (a semantic
                # type with no enumerated values)
                cid = "concept" + ID_SEP + sid(v["conforms"])
                row["concept_id"] = cid
                if cid not in concepts_used:
                    concepts_used[cid] = {
                        "id": cid,
                        **loc_cols("name", meta["title"]),
                        **loc_cols("description", meta["description"]),
                    }
            var_rows.append(row)

        if i % 25 == 0:
            print(f"  ...{i}/{len(datasets)}")

    # code lists -> enumeration + value. Prefetch every referenced code list in
    # parallel first (this was the sequential bottleneck), then read from cache.
    print(f"i14y: prefetching {len(needed_codelists)} code list(s)...")
    uuids = {cidx[i]["uuid"] for i in needed_codelists if i in cidx}
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        list(ex.map(codelist_entries, uuids))

    enum_rows: list[dict] = []
    val_rows: list[dict] = []
    for ident in sorted(needed_codelists):
        meta = cidx.get(ident)
        if not meta:
            continue
        entries = codelist_entries(meta["uuid"])
        if not entries:
            continue  # empty code list: skip rather than emit a valueless enum
        enum_id = "codelist" + ID_SEP + sid(ident)
        enum_rows.append(
            {
                "id": enum_id,
                "folder_id": CODELIST_FOLDER,
                **(loc_cols("name", meta["title"]) or {"name": ident}),
            }
        )
        for entry in entries:
            code = entry.get("code")
            if code is None:
                continue
            val_rows.append(
                {
                    "enumeration_id": enum_id,
                    "value": str(code),
                    **loc_cols("description", name_map(entry.get("name"))),
                }
            )

    # big code lists -> browsable nomenclature datasets (NOGA, ISCO, CHOP, ...).
    # Cleaned CSV goes to data/classifications, scanned by a dedicated catalog.yml
    # entry at depth: stat with a high preview_rows (full table, loaded on demand).
    class_dir = out / "data" / "classifications"
    classifications = select_classifications()
    print(f"i14y: {len(classifications)} classification(s) as nomenclature datasets")
    for concept in classifications:
        ident = concept["identifier"]
        dest = class_dir / f"{sid(ident)}.csv"
        write_classification_csv(codelist_csv(concept["id"]), dest)
        org_id = ensure_org(concept.get("publisher") or {}, orgs)
        cls_ds_id = NOMEN_FOLDER + ID_SEP + sid(ident)
        ds_rows.append(
            {
                "id": cls_ds_id,
                "folder_id": NOMEN_FOLDER,
                "owner_organization_id": org_id,
                "data_path": f"{API}/concepts/{concept['id']}/codelist-entries/exports/csv",
                "_match_path": dest.relative_to(meta_dir, walk_up=True).as_posix(),
                "delivery_format": "csv",
                "last_update_date": concept.get("validFrom"),
                **loc_cols("name", name_map(concept.get("title"))),
                **loc_cols("description", name_map(concept.get("description"))),
            }
        )
        # clean multilingual labels for the technical columns (Code, Name_de, ...)
        for column in real_columns(dest):
            labels = NOMEN_COLUMN_LABELS.get(column)
            if not labels:
                continue
            var_rows.append(
                {
                    "id": cls_ds_id + ID_SEP + sanitize_id(column),
                    "dataset_id": cls_ds_id,
                    **loc_cols("name", labels),
                }
            )

    # documentation files -> download into staging/docs (copy_assets moves them
    # to data/doc at build time, matching the existing doc convention)
    doc_rows = list(docs.values())
    if download and doc_rows:
        docs_dir = ROOT / "staging" / "docs"
        got = 0
        for d in doc_rows:
            local = docs_dir / f"{d['id']}.{d['type']}"
            if download_file(d["_url"], local):
                got += 1
        print(f"i14y: {got}/{len(doc_rows)} documentation file(s) into staging/docs")

    # drop enumeration_ids that point at a skipped (empty) code list
    emitted_enums = {e["id"] for e in enum_rows}
    for row in var_rows:
        eid = row.get("enumeration_ids")
        if eid and eid not in emitted_enums:
            del row["enumeration_ids"]

    # write metadata
    write_csv(meta_dir / "organization.csv", ORG_COLS, list(orgs.values()))
    write_csv(meta_dir / "tag.csv", TAG_COLS, list(tags.values()))
    write_csv(meta_dir / "folder.csv", FOLDER_COLS, list(folders.values()))
    write_csv(meta_dir / "dataset.csv", DATASET_COLS, ds_rows)
    write_csv(meta_dir / "variable.csv", VARIABLE_COLS, var_rows)
    write_csv(meta_dir / "enumeration.csv", ENUM_COLS, enum_rows)
    write_csv(meta_dir / "value.csv", VALUE_COLS, val_rows)
    write_csv(meta_dir / "concept.csv", CONCEPT_COLS, list(concepts_used.values()))
    write_csv(meta_dir / "doc.csv", DOC_COLS, doc_rows)

    # manually maintained app config (glossary text, global filters) lives in
    # public/ and is read by datannurpy from the metadata dir
    for name in ("config.json", "configFilter.json"):
        src_cfg = ROOT / "public" / name
        if src_cfg.exists():
            shutil.copy(src_cfg, meta_dir / name)

    print(
        f"i14y: {len(ds_rows)} datasets ({structure_only} structure-only), "
        f"{len(var_rows)} variable overlays ({unmatched} unmatched, skipped), "
        f"{len(enum_rows)} code lists, {len(val_rows)} code values, "
        f"{len(concepts_used)} concepts, {len(doc_rows)} docs, "
        f"{len(tags)} tags, {len(orgs)} orgs"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a datannur catalog from i14y.")
    ap.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help="output catalog dir (metadata/ + data/); default staging/i14y_catalog",
    )
    ap.add_argument("--limit", type=int, default=None, help="only the first N datasets")
    ap.add_argument(
        "--publisher", default=None, help="keep only this publisher identifier"
    )
    ap.add_argument("--no-download", action="store_true", help="emit metadata only")
    args = ap.parse_args()
    build(args.out, args.limit, args.publisher, download=not args.no_download)
    return 0


if __name__ == "__main__":
    sys.exit(main())
