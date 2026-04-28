"""Build datannur metadata CSVs from CKAN crawl + manifest outputs.

Reads:
    staging/packages.jsonl   (CKAN package payloads)
    staging/download_state.jsonl   (download results)

Writes (in ./metadata/):
  institution.csv   — publishers + reconstructed hierarchy (Suisse → …)
  folder.csv        — 14 thematic roots + one folder per CKAN package
  dataset.csv       — one row per successfully downloaded resource
  tag.csv           — thematic tags + free CKAN keywords
    doc.csv           — PDF documentation URLs attached to folders/datasets

Decisions documented in MAPPING.md.

CSV handling:
  Values may contain commas, quotes, and newlines (rich descriptions, …).
  The stdlib ``csv`` module with QUOTE_MINIMAL + ``newline=""`` at open time
  handles all of it correctly — no manual escaping.
"""

from __future__ import annotations

import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

from config import doc_manifest_path
from doc_utils import extract_pdf_urls, pdf_doc_id, pdf_doc_name

ROOT = Path(__file__).parent
STAGING_DIR = ROOT / "staging"
META_DIR = ROOT / "metadata"
META_DIR.mkdir(exist_ok=True)


# =============================================================================
# Static tables (see MAPPING.md §3.2 / §3.4 / §3.5)
# =============================================================================

# DCAT-AP-CH thematic groups observed in the corpus. Labels are official
# (taken from the CKAN group display_name.fr when available).
DCAT_GROUPS_FR: dict[str, str] = {
    "econ": "Économie et finances",
    "soci": "Population et société",
    "envi": "Environnement",
    "regi": "Régions et villes",
    "just": "Justice, système juridique et sécurité publique",
    "educ": "Éducation, culture et sport",
    "heal": "Santé",
    "agri": "Agriculture, pêche, sylviculture et alimentation",
    "tran": "Transports",
    "gove": "Administration publique",
    "ener": "Énergie",
    "tech": "Science et technologie",
}
# Extra thematic roots (not DCAT groups)
EXTRA_ROOTS_FR: dict[str, str] = {
    "multi": "Multi-thématiques",
    "other": "Hors thématique",
}

# Institutions the script creates itself (not publishing orgs in CKAN).
# id → (parent_id, name_fr)
VIRTUAL_INSTITUTIONS: dict[str, tuple[str | None, str]] = {
    "suisse": (None, "Suisse"),
    "confederation": ("suisse", "Confédération"),
    "cantons": ("suisse", "Cantons"),
    "autres": ("suisse", "Autres institutions"),
    # Cantons without a "Canton of X" publishing org
    "kanton_freiburg": ("cantons", "Canton de Fribourg"),
    "kanton-vaud": ("cantons", "Canton de Vaud"),
    # "Communes" container under each canton that has communes
    "communes-kanton-bern-2": ("kanton-bern-2", "Communes"),
    "communes-kanton-vaud": ("kanton-vaud", "Communes"),
    # Virtual city grouping 4 publishing services
    "biel-bienne": ("communes-kanton-bern-2", "Ville de Bienne"),
    # Non-publishing parents referenced by CKAN organisation.groups[]
    "eth-zuerich": ("autres", "ETH Zürich"),
    "schweizerische-bundesbahnen-sbb": ("autres", "Chemins de fer fédéraux CFF"),
}

# Commune-level publishing orgs (with no CKAN parent group) → their canton
COMMUNE_TO_COMMUNES_CONTAINER: dict[str, str] = {
    "lausanne": "communes-kanton-vaud",
}

# CKAN accrual_periodicity URIs → FR labels
ACCRUAL_FR: dict[str, str] = {
    "http://publications.europa.eu/resource/authority/frequency/CONT": "continu",
    "http://publications.europa.eu/resource/authority/frequency/DAILY": "quotidien",
    "http://publications.europa.eu/resource/authority/frequency/WEEKLY": "hebdomadaire",
    "http://publications.europa.eu/resource/authority/frequency/BIWEEKLY": "bi-hebdomadaire",
    "http://publications.europa.eu/resource/authority/frequency/MONTHLY": "mensuel",
    "http://publications.europa.eu/resource/authority/frequency/BIMONTHLY": "bimestriel",
    "http://publications.europa.eu/resource/authority/frequency/QUARTERLY": "trimestriel",
    "http://publications.europa.eu/resource/authority/frequency/ANNUAL": "annuel",
    "http://publications.europa.eu/resource/authority/frequency/ANNUAL_2": "semestriel",
    "http://publications.europa.eu/resource/authority/frequency/ANNUAL_3": "tous les 4 mois",
    "http://publications.europa.eu/resource/authority/frequency/BIENNIAL": "biennal",
    "http://publications.europa.eu/resource/authority/frequency/TRIENNIAL": "triennal",
    "http://publications.europa.eu/resource/authority/frequency/IRREG": "irrégulier",
    "http://publications.europa.eu/resource/authority/frequency/UNKNOWN": "inconnue",
    "http://publications.europa.eu/resource/authority/frequency/OTHER": "autre",
    "http://publications.europa.eu/resource/authority/frequency/NEVER": "pas de mise à jour",
    "http://publications.europa.eu/resource/authority/frequency/OP_DATPRO": "provisoire",
}

POLITICAL_LEVEL_FR: dict[str, str] = {
    "confederation": "Confédération",
    "canton": "Canton",
    "commune": "Commune",
    "other": "Autre",
}


# =============================================================================
# Helpers
# =============================================================================

_SLUG_RE = re.compile(r"[^a-z0-9]+")
def slugify(s: str) -> str:
    """Produce a safe tag id: lowercase, [a-z0-9_-] only."""
    if not s:
        return ""
    s = s.lower().strip()
    # Keep letters/digits, replace everything else with '-'
    s = _SLUG_RE.sub("-", s)
    return s.strip("-")


def pick_fr(ml: dict | str | None, fallback_order=("fr", "en", "de", "it")) -> str:
    """Return the French value from a multilingual CKAN dict, falling back
    to other languages, then to the raw string, then to empty."""
    if ml is None:
        return ""
    if isinstance(ml, str):
        return ml
    if isinstance(ml, dict):
        for lang in fallback_order:
            v = ml.get(lang)
            if v:
                return v
    return ""


def join_ids(ids) -> str:
    """Format a list of entity ids for a *_ids CSV cell (comma separated)."""
    seen, out = set(), []
    for i in ids:
        if not i:
            continue
        if i in seen:
            continue
        seen.add(i)
        out.append(i)
    return ", ".join(out)


def normalize_unix_timestamp(value: str | int | float | None) -> int | None:
    """Return a Unix timestamp in seconds when the input is parseable."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value)

    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)

    try:
        dt = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError, OverflowError):
        dt = None

    if dt is None:
        iso_text = text.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(iso_text)
        except ValueError:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp())


def build_pdf_docs(
    doc_registry: dict[str, dict],
    doc_downloads: dict[str, dict],
    owner_name: str,
    updated_at: str | None,
    *texts: Any,
) -> tuple[str | None, set[str]]:
    """Register PDFs globally and return joined doc_ids plus referenced paths."""

    def build_pdf_doc_description(
        source_url: str,
        localized_path: str | None,
    ) -> str | None:
        if not localized_path:
            return None

        return f"Source originale: {source_url}"

    urls = extract_pdf_urls(*texts)
    if not urls:
        return None, set()

    doc_ids: list[str] = []
    doc_paths: set[str] = set()
    for url in urls:
        doc_paths.add(url)
        download = doc_downloads.get(url, {})
        export_path = download.get("export_path")
        source_last_update = normalize_unix_timestamp(
            download.get("last_modified") or updated_at
        )
        row = doc_registry.get(url)
        if row is None:
            row = {
                "id": pdf_doc_id(url),
                "name": pdf_doc_name(url, owner_name),
                "description": build_pdf_doc_description(
                    url,
                    export_path,
                ),
                "path": export_path or url,
                "type": "pdf",
                "last_update": source_last_update,
            }
            doc_registry[url] = row
        else:
            if export_path:
                row["path"] = export_path
                row["description"] = build_pdf_doc_description(
                    url,
                    export_path,
                )
            if not row.get("last_update") and source_last_update:
                row["last_update"] = source_last_update
        doc_ids.append(row["id"])
    return join_ids(doc_ids) or None, doc_paths


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    """Write a CSV with UTF-8 + LF line endings. Multiline / comma / quote
    safe via the stdlib ``csv`` module (QUOTE_MINIMAL, automatic quoting).

    Drops columns that are empty for all rows: otherwise pandas infers the
    column as float64 (NaN-filled), which breaks datannurpy's polars-based
    ingestion for fields typed str|None or int|None.
    """
    # Keep only columns with at least one non-empty value
    kept = [
        c
        for c in columns
        if c == "id" or any(row.get(c) not in (None, "") for row in rows)
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
            # Normalize None → "" so CSV stays clean
            w.writerow({c: ("" if row.get(c) is None else row.get(c)) for c in kept})
    dropped = [c for c in columns if c not in kept]
    suffix = f"  (dropped empty: {', '.join(dropped)})" if dropped else ""
    print(f"  wrote {path.relative_to(ROOT)}  ({len(rows)} rows){suffix}")


# =============================================================================
# Load inputs
# =============================================================================


def load_packages() -> dict[str, dict]:
    """Return {package_id: ckan_payload} from the shared staging file."""
    pkgs: dict[str, dict] = {}
    fp = STAGING_DIR / "packages.jsonl"
    if not fp.exists():
        return pkgs
    for line in fp.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        p = json.loads(line)
        pid = p.get("id")
        if pid:
            pkgs[pid] = p
    return pkgs


def load_organizations() -> dict[str, dict]:
    """Return {organization_name: organization_payload} from staging."""
    out: dict[str, dict] = {}
    fp = STAGING_DIR / "organizations.jsonl"
    if not fp.exists():
        return out
    for line in fp.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        org = json.loads(line)
        name = org.get("name")
        if name:
            out[name] = org
    return out


def load_resources() -> dict[str, dict]:
    """Return {resource_id: resource_payload} from the shared staging file."""
    out: dict[str, dict] = {}
    fp = STAGING_DIR / "resources.jsonl"
    if not fp.exists():
        return out
    for line in fp.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        resource = json.loads(line)
        rid = resource.get("resource_id")
        if rid:
            out[rid] = resource
    return out


def load_manifests() -> dict[str, dict]:
    """Return {resource_id: manifest_entry} for successfully downloaded files only."""
    out: dict[str, dict] = {}
    fp = STAGING_DIR / "download_state.jsonl"
    if not fp.exists():
        return out
    for line in fp.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        m = json.loads(line)
        # "ok" = just downloaded ; "skipped" = already present (idempotent rerun).
        # Both have a valid local_path and are successes.
        if m.get("download_status") not in {"ok", "skipped"}:
            continue
        lp = m.get("local_path")
        if not lp:
            continue
        # Defensive: ensure the file actually exists (manifest may be stale)
        if not (ROOT / lp).exists() and not Path(lp).exists():
            continue
        out[m["resource_id"]] = m
    return out


def load_doc_downloads() -> dict[str, dict]:
    """Return {source_url: manifest_entry} for successfully cached PDFs."""
    out: dict[str, dict] = {}
    fp = doc_manifest_path()
    if not fp.exists():
        return out
    for line in fp.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        if entry.get("download_status") not in {"ok", "skipped"}:
            continue
        source_url = entry.get("source_url")
        local_path = entry.get("local_path")
        if not source_url or not local_path:
            continue
        if not (ROOT / local_path).exists() and not Path(local_path).exists():
            continue
        out[source_url] = entry
    return out


def load_excluded_ids() -> set[str]:
    """Read ids flagged in excluded_datasets.csv (drop from dataset metadata)."""
    fp = ROOT / "excluded_datasets.csv"
    if not fp.exists():
        return set()
    ids: set[str] = set()
    with fp.open(newline="") as f:
        for row in csv.DictReader(f):
            rid = (row.get("id") or "").strip()
            if rid:
                ids.add(rid)
    return ids


def package_organization(package: dict, organizations: dict[str, dict]) -> dict:
    org_name = package.get("organization_name")
    return organizations.get(org_name or "", {})


# =============================================================================
# Build entities
# =============================================================================


def build_institutions(organizations: dict[str, dict]) -> list[dict]:
    """Collect unique CKAN orgs + add virtual conteneurs. Return institution rows."""

    # Resolve parent for each real org
    def resolve_parent(org_name: str, org: dict) -> str | None:
        parents = [g["name"] for g in (org.get("groups") or [])]
        if parents:
            # first CKAN group is the parent (virtual or real org)
            return parents[0]
        level = org.get("political_level")
        if level == "confederation":
            return "confederation"
        if level == "canton":
            # canton-bern-2 etc. — root publishing canton
            return "cantons"
        if level == "commune":
            return COMMUNE_TO_COMMUNES_CONTAINER.get(org_name, "autres")
        return "autres"

    rows: list[dict] = []
    # 1. Virtual institutions first (stable order)
    virtual_order = [
        "suisse",
        "confederation",
        "cantons",
        "autres",
        "kanton_freiburg",
        "kanton-vaud",
        "communes-kanton-bern-2",
        "communes-kanton-vaud",
        "biel-bienne",
        "eth-zuerich",
        "schweizerische-bundesbahnen-sbb",
    ]
    for vid in virtual_order:
        parent, name = VIRTUAL_INSTITUTIONS[vid]
        rows.append(
            {
                "id": vid,
                "parent_id": parent,
                "name": name,
                "description": None,
                "email": None,
                "phone": None,
                "start_date": None,
                "end_date": None,
                "tag_ids": None,
                "doc_ids": None,
            }
        )

    # 2. Real CKAN organisations
    for name in sorted(organizations):
        o = organizations[name]
        rows.append(
            {
                "id": name,
                "parent_id": resolve_parent(name, o),
                "name": pick_fr(o.get("display_name")) or o.get("title") or name,
                "description": pick_fr(o.get("description")),
                "email": None,
                "phone": None,
                "start_date": None,
                "end_date": None,
                "tag_ids": None,
                "doc_ids": None,
            }
        )
    return rows


def pick_thematic_root(groups: list[dict]) -> str:
    """1 group → that group ; ≥ 2 groups → "multi" ; 0 group → "other"."""
    n = len(groups)
    if n == 0:
        return "other"
    if n == 1:
        g = groups[0]["name"]
        return g if g in DCAT_GROUPS_FR else "other"
    return "multi"


def build_folders_and_docs(
    packages: dict[str, dict],
    organizations: dict[str, dict],
    doc_registry: dict[str, dict],
    doc_downloads: dict[str, dict],
) -> list[dict]:
    """Return folder rows while registering shared PDF docs globally.

    Package URLs are exposed directly on folder.link. PDF documents visible on
    the package itself, in its description, or in staged documentation/
    relations fields feed doc rows when present.
    """
    folder_rows: list[dict] = []

    # 1. Thematic roots
    for tid, tname in DCAT_GROUPS_FR.items():
        folder_rows.append(
            {
                "id": tid,
                "parent_id": None,
                "name": tname,
                "description": None,
                "type": "thematique",
                "owner_id": None,
                "manager_id": None,
                "tag_ids": None,
                "doc_ids": None,
                "link": None,
                "data_path": None,
                "delivery_format": None,
                "last_update_date": None,
                "localisation": None,
                "start_date": None,
                "end_date": None,
                "updating_each": None,
            }
        )
    for tid, tname in EXTRA_ROOTS_FR.items():
        folder_rows.append(
            {
                "id": tid,
                "parent_id": None,
                "name": tname,
                "description": None,
                "type": "thematique",
                "link": None,
            }
        )

    # 2. Package folders
    for pid, p in sorted(packages.items()):
        groups = p.get("groups") or []
        parent = pick_thematic_root(groups)
        org = package_organization(p, organizations)
        org_name = org.get("name")
        level = org.get("political_level")
        title = pick_fr(p.get("title")) or p.get("name") or pid
        description = pick_fr(p.get("description"))

        # Tags: one per group + free keywords (FR)
        thematic_tags = [f"thematique---{g['name']}" for g in groups if g.get("name")]
        kw_tags = [slugify(kw) for kw in ((p.get("keywords") or {}).get("fr") or [])]
        tag_ids = join_ids(thematic_tags + kw_tags)

        # Temporals
        temporals = p.get("temporals") or []
        start_date = temporals[0].get("start_date") if temporals else None
        end_date = temporals[0].get("end_date") if temporals else None
        folder_doc_ids, _ = build_pdf_docs(
            doc_registry,
            doc_downloads,
            title,
            p.get("modified") or p.get("metadata_modified"),
            p.get("url"),
            description,
            p.get("documentation_urls"),
            p.get("relation_urls"),
        )

        accrual_uri = p.get("accrual_periodicity") or ""
        folder_rows.append(
            {
                "id": pid,
                "parent_id": parent,
                "name": title,
                "description": description,
                "type": "package",
                "owner_id": org_name,
                "manager_id": None,
                "tag_ids": tag_ids or None,
                "doc_ids": folder_doc_ids,
                "link": p.get("url"),
                "data_path": None,
                "delivery_format": None,
                "last_update_date": p.get("modified") or p.get("metadata_modified"),
                "localisation": POLITICAL_LEVEL_FR.get(level) if level else None,
                "start_date": start_date,
                "end_date": end_date,
                "updating_each": ACCRUAL_FR.get(
                    accrual_uri, accrual_uri if accrual_uri else None
                ),
            }
        )

    return folder_rows


def build_datasets(
    packages: dict[str, dict],
    resources: dict[str, dict],
    manifests: dict[str, dict],
    organizations: dict[str, dict],
    doc_registry: dict[str, dict],
    doc_downloads: dict[str, dict],
) -> list[dict]:
    """Return dataset rows while registering shared PDF docs globally."""
    rows: list[dict] = []
    for rid, m in sorted(manifests.items()):
        res = resources.get(rid)
        if res is None:
            continue

        pid = res.get("package_id") or ""
        p = packages.get(pid)
        if p is None:
            continue  # resource belonged to a package we don't have (shouldn't happen)

        org = package_organization(p, organizations)
        level = org.get("political_level")
        temporals = p.get("temporals") or []
        start_date = temporals[0].get("start_date") if temporals else None
        end_date = temporals[0].get("end_date") if temporals else None
        accrual_uri = p.get("accrual_periodicity") or ""

        fmt_value = (res.get("format") or "").upper()
        name_fr = (
            pick_fr(res.get("title")) or pick_fr(res.get("name")) or fmt_value or rid
        )
        # Avoid names that are just "csv" / "parquet" — prefix with package title
        pkg_title = pick_fr(p.get("title")) or p.get("name") or pid
        if name_fr.strip().lower() in {"csv", "parquet", "xls", "xlsx", "excel", ""}:
            name_fr = f"{pkg_title} — {fmt_value}"
        resource_description = pick_fr(res.get("description"))
        description = resource_description or pick_fr(p.get("description"))
        dataset_doc_ids, _ = build_pdf_docs(
            doc_registry,
            doc_downloads,
            name_fr,
            res.get("modified") or res.get("last_modified") or m.get("modified"),
            res.get("url"),
            resource_description,
            res.get("documentation_urls"),
            res.get("relation_urls"),
        )

        local_path = m.get("local_path") or ""
        # Derive delivery_format from the actual local extension when available
        # (CKAN labels both .xls and .xlsx as "XLS"); fall back to CKAN value.
        if local_path:
            ext = Path(local_path).suffix.lstrip(".").lower()
            fmt_lower = ext or fmt_value.lower()
        else:
            fmt_lower = fmt_value.lower()
        # data_path is resolved by datannurpy relative to the metadata file's
        # directory (METADATA_DIR), so write the path relative to that.
        if local_path:
            try:
                local_path = str(
                    Path(local_path)
                    .resolve()
                    .relative_to(META_DIR.resolve(), walk_up=True)
                )
            except ValueError, OSError:
                pass

        rows.append(
            {
                "id": rid,
                "folder_id": pid,
                "owner_id": org.get("name"),
                "manager_id": None,
                "tag_ids": None,
                "doc_ids": dataset_doc_ids,
                "name": name_fr,
                "description": description,
                "data_path": local_path,
                "link": res.get("url"),
                "delivery_format": fmt_lower or None,
                "type": "resource",
                "localisation": POLITICAL_LEVEL_FR.get(level) if level else None,
                "start_date": start_date,
                "end_date": end_date,
                "last_update_date": res.get("modified")
                or res.get("last_modified")
                or m.get("modified"),
                "updating_each": ACCRUAL_FR.get(
                    accrual_uri, accrual_uri if accrual_uri else None
                ),
            }
        )
    return rows


def build_tags(packages: dict[str, dict]) -> list[dict]:
    rows: list[dict] = []
    # Root thematic tag
    rows.append(
        {
            "id": "thematique",
            "parent_id": None,
            "name": "Thématique",
            "description": None,
            "doc_ids": None,
        }
    )
    # One child per DCAT group
    for tid, tname in DCAT_GROUPS_FR.items():
        rows.append(
            {
                "id": f"thematique---{tid}",
                "parent_id": "thematique",
                "name": tname,
                "description": None,
                "doc_ids": None,
            }
        )

    # Free keywords (FR), deduped. Keep original text as display name.
    seen: dict[str, str] = {}
    for p in packages.values():
        for kw in (p.get("keywords") or {}).get("fr") or []:
            tid = slugify(kw)
            if not tid:
                continue
            seen.setdefault(tid, kw)

    for tid, name in sorted(seen.items()):
        rows.append(
            {
                "id": tid,
                "parent_id": None,
                "name": name,
                "description": None,
                "doc_ids": None,
            }
        )
    return rows


# =============================================================================
# Main
# =============================================================================


INSTITUTION_COLS = [
    "id",
    "parent_id",
    "tag_ids",
    "doc_ids",
    "name",
    "description",
    "email",
    "phone",
    "start_date",
    "end_date",
]
FOLDER_COLS = [
    "id",
    "parent_id",
    "manager_id",
    "owner_id",
    "tag_ids",
    "doc_ids",
    "name",
    "description",
    "link",
    "data_path",
    "delivery_format",
    "type",
    "last_update_date",
    "localisation",
    "start_date",
    "end_date",
    "updating_each",
]
DATASET_COLS = [
    "id",
    "folder_id",
    "manager_id",
    "owner_id",
    "tag_ids",
    "doc_ids",
    "name",
    "description",
    "data_path",
    "link",
    "delivery_format",
    "type",
    "localisation",
    "start_date",
    "end_date",
    "last_update_date",
    "updating_each",
]
TAG_COLS = ["id", "parent_id", "doc_ids", "name", "description"]
DOC_COLS = ["id", "name", "description", "path", "type", "last_update"]


def cascade_purge(
    folders: list[dict],
    datasets: list[dict],
    docs: list[dict],
    tags: list[dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    """Drop folders with no surviving descendants, then orphan docs/tags.

    A folder is kept iff at least one dataset lives under it (directly or via
    a kept descendant folder). Tags/docs referenced only by purged folders
    are dropped too.
    """
    children_of: dict[str, list[str]] = defaultdict(list)
    for f in folders:
        pid = f.get("parent_id")
        if pid:
            children_of[pid].append(f["id"])

    # Folders that directly host a dataset
    has_dataset: set[str] = {d["folder_id"] for d in datasets}

    # Bottom-up: a folder is kept if it has datasets or a kept descendant.
    keep: set[str] = set()

    def visit(fid: str) -> bool:
        kept_child = False
        for cid in children_of.get(fid, []):
            if visit(cid):
                kept_child = True
        if fid in has_dataset or kept_child:
            keep.add(fid)
            return True
        return False

    for f in folders:
        if not f.get("parent_id"):
            visit(f["id"])

    kept_folders = [f for f in folders if f["id"] in keep]

    # Collect referenced tag/doc ids from surviving folders + datasets
    used_tags: set[str] = set()
    used_docs: set[str] = set()

    def collect(row: dict) -> None:
        for col, sink in (("tag_ids", used_tags), ("doc_ids", used_docs)):
            v = row.get(col)
            if not v:
                continue
            for tok in str(v).split(","):
                tok = tok.strip()
                if tok:
                    sink.add(tok)

    for r in kept_folders:
        collect(r)
    for r in datasets:
        collect(r)

    # Keep tag tree: a tag stays if it (or any descendant) is used.
    tag_children: dict[str, list[str]] = defaultdict(list)
    for t in tags:
        if t.get("parent_id"):
            tag_children[t["parent_id"]].append(t["id"])

    keep_tag: set[str] = set()

    def visit_tag(tid: str) -> bool:
        kept_child = False
        for cid in tag_children.get(tid, []):
            if visit_tag(cid):
                kept_child = True
        if tid in used_tags or kept_child:
            keep_tag.add(tid)
            return True
        return False

    for t in tags:
        if not t.get("parent_id"):
            visit_tag(t["id"])

    kept_tags = [t for t in tags if t["id"] in keep_tag]
    kept_docs = [d for d in docs if d["id"] in used_docs]
    return kept_folders, kept_docs, kept_tags


def main() -> int:
    print("Loading CKAN packages…")
    packages = load_packages()
    print(f"  {len(packages)} unique packages")
    print("Loading CKAN resources…")
    resources = load_resources()
    print(f"  {len(resources)} unique resources")
    print("Loading CKAN organizations…")
    organizations = load_organizations()
    print(f"  {len(organizations)} unique organizations")
    print("Loading manifests…")
    manifests = load_manifests()
    print(f"  {len(manifests)} successfully downloaded resources")
    print("Loading doc downloads…")
    doc_downloads = load_doc_downloads()
    print(f"  {len(doc_downloads)} successfully downloaded docs")

    excluded = load_excluded_ids()
    if excluded:
        before = len(manifests)
        manifests = {rid: m for rid, m in manifests.items() if rid not in excluded}
        print(
            f"  excluded_datasets.csv: -{before - len(manifests)} dropped "
            f"({len(excluded)} ids in list)"
        )

    print("\nBuilding entities…")
    doc_registry: dict[str, dict] = {}
    institutions = build_institutions(organizations)
    folders = build_folders_and_docs(
        packages,
        organizations,
        doc_registry,
        doc_downloads,
    )
    datasets = build_datasets(
        packages,
        resources,
        manifests,
        organizations,
        doc_registry,
        doc_downloads,
    )
    docs = list(doc_registry.values())
    tags = build_tags(packages)

    # Cascade purge: drop folders without surviving descendants, then orphan
    # docs and tags. Idempotent — runs every build, no state to maintain.
    if excluded:
        n_f0, n_d0, n_t0 = len(folders), len(docs), len(tags)
        folders, docs, tags = cascade_purge(folders, datasets, docs, tags)
        print(
            f"  cascade purge: -{n_f0 - len(folders)} folders, "
            f"-{n_d0 - len(docs)} docs, -{n_t0 - len(tags)} tags"
        )

    print(f"\nWriting CSVs to {META_DIR.relative_to(ROOT)}/")
    write_csv(META_DIR / "institution.csv", INSTITUTION_COLS, institutions)
    write_csv(META_DIR / "folder.csv", FOLDER_COLS, folders)
    write_csv(META_DIR / "dataset.csv", DATASET_COLS, datasets)
    write_csv(META_DIR / "tag.csv", TAG_COLS, tags)
    write_csv(META_DIR / "doc.csv", DOC_COLS, docs)

    # Sanity: detect FK issues
    print("\nSanity checks…")
    inst_ids = {r["id"] for r in institutions}
    folder_ids = {r["id"] for r in folders}

    errors = 0
    for r in folders:
        if r.get("parent_id") and r["parent_id"] not in folder_ids:
            print(f"  ERR folder {r['id']}: parent_id {r['parent_id']!r} not found")
            errors += 1
        if r.get("owner_id") and r["owner_id"] not in inst_ids:
            print(f"  ERR folder {r['id']}: owner_id {r['owner_id']!r} not found")
            errors += 1
    for r in datasets:
        if r["folder_id"] not in folder_ids:
            print(f"  ERR dataset {r['id']}: folder_id {r['folder_id']!r} not found")
            errors += 1
        if r.get("owner_id") and r["owner_id"] not in inst_ids:
            print(f"  ERR dataset {r['id']}: owner_id {r['owner_id']!r} not found")
            errors += 1
    for r in institutions:
        if r.get("parent_id") and r["parent_id"] not in inst_ids:
            print(
                f"  ERR institution {r['id']}: parent_id {r['parent_id']!r} not found"
            )
            errors += 1
    print(f"  {errors} FK error(s)")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
