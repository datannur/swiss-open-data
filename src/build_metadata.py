"""Build datannur metadata CSVs from CKAN crawl + manifest outputs.

Reads:
    staging/packages.jsonl   (CKAN package payloads)
    staging/download_state.jsonl   (download results)

Writes (in ./metadata/):
    organization.csv  — publishers + reconstructed hierarchy (Suisse → …)
  folder.csv        — 14 thematic roots + one folder per CKAN package
  dataset.csv       — one row per successfully downloaded resource
  tag.csv           — thematic tags + free CKAN keywords
    doc.csv           — PDF documentation URLs attached to folders/datasets

Mapping decisions are summarized in README.md.

CSV handling:
  Values may contain commas, quotes, and newlines (rich descriptions, …).
  The stdlib ``csv`` module with QUOTE_MINIMAL + ``newline=""`` at open time
  handles all of it correctly — no manual escaping.
"""

from __future__ import annotations

import csv
import difflib
import json
import re
import shutil
import sys
import unicodedata
from collections import defaultdict
from collections import Counter
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from config import doc_manifest_path, log_dir
from doc_utils import extract_pdf_urls, pdf_doc_id, pdf_doc_name

ROOT = Path(__file__).resolve().parent.parent
STAGING_DIR = ROOT / "staging"
META_DIR = ROOT / "metadata"
PUBLIC_DIR = ROOT / "public"
KEYWORD_TRANSLATIONS_FILE = ROOT / "src" / "keyword_translations.json"
META_DIR.mkdir(exist_ok=True)


# =============================================================================
# Static tables used by the CKAN-to-datannur mapping summarized in README.md.
# =============================================================================

# DCAT-AP-CH thematic groups observed in the corpus. Labels are official
# (taken from the CKAN group display_name.fr when available).
DCAT_GROUPS_EN: dict[str, str] = {
    "econ": "Economy and finance",
    "soci": "Population and society",
    "envi": "Environment",
    "regi": "Regions and cities",
    "just": "Justice, legal system and public safety",
    "educ": "Education, culture and sport",
    "heal": "Health",
    "agri": "Agriculture, fisheries, forestry and food",
    "tran": "Transport",
    "gove": "Public administration",
    "ener": "Energy",
    "tech": "Science and technology",
}
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
EXTRA_ROOTS_EN: dict[str, str] = {
    "multi": "Multiple themes",
    "other": "Other topics",
}
EXTRA_ROOTS_FR: dict[str, str] = {
    "multi": "Multi-thématiques",
    "other": "Hors thématique",
}

# Institutions the script creates itself (not publishing orgs in CKAN).
# id → (parent_id, name_en, name_fr)
VIRTUAL_INSTITUTIONS: dict[str, tuple[str | None, str, str]] = {
    "suisse": (None, "Switzerland", "Suisse"),
    "confederation": ("suisse", "Swiss Confederation", "Confédération"),
    "cantons": ("suisse", "Cantons", "Cantons"),
    "autres": ("suisse", "Other institutions", "Autres institutions"),
    # Cantons without a "Canton of X" publishing org
    "kanton_freiburg": ("cantons", "Canton of Fribourg", "Canton de Fribourg"),
    "kanton-vaud": ("cantons", "Canton of Vaud", "Canton de Vaud"),
    # "Communes" container under each canton that has communes
    "communes-kanton-bern-2": ("kanton-bern-2", "Municipalities", "Communes"),
    "communes-kanton-vaud": ("kanton-vaud", "Municipalities", "Communes"),
    # Virtual city grouping 4 publishing services
    "biel-bienne": ("communes-kanton-bern-2", "City of Biel/Bienne", "Ville de Bienne"),
    # Non-publishing parents referenced by CKAN organisation.groups[]
    "eth-zuerich": ("autres", "ETH Zurich", "ETH Zürich"),
    "wsl": (
        "autres",
        "Swiss Federal Institute for Forest, Snow and Landscape Research WSL",
        "Institut fédéral de recherches sur la forêt, la neige et le paysage WSL",
    ),
    "schweizerische-bundesbahnen-sbb": (
        "autres",
        "Swiss Federal Railways SBB",
        "Chemins de fer fédéraux CFF",
    ),
}

# Commune-level publishing orgs (with no CKAN parent group) → their canton
COMMUNE_TO_COMMUNES_CONTAINER: dict[str, str] = {
    "lausanne": "communes-kanton-vaud",
}

# CKAN accrual_periodicity URIs → EN labels
ACCRUAL_EN: dict[str, str] = {
    "http://publications.europa.eu/resource/authority/frequency/CONT": "continuous",
    "http://publications.europa.eu/resource/authority/frequency/DAILY": "daily",
    "http://publications.europa.eu/resource/authority/frequency/WEEKLY": "weekly",
    "http://publications.europa.eu/resource/authority/frequency/BIWEEKLY": "biweekly",
    "http://publications.europa.eu/resource/authority/frequency/MONTHLY": "monthly",
    "http://publications.europa.eu/resource/authority/frequency/BIMONTHLY": "bimonthly",
    "http://publications.europa.eu/resource/authority/frequency/QUARTERLY": "quarterly",
    "http://publications.europa.eu/resource/authority/frequency/ANNUAL": "annual",
    "http://publications.europa.eu/resource/authority/frequency/ANNUAL_2": "semiannual",
    "http://publications.europa.eu/resource/authority/frequency/ANNUAL_3": "every 4 months",
    "http://publications.europa.eu/resource/authority/frequency/BIENNIAL": "biennial",
    "http://publications.europa.eu/resource/authority/frequency/TRIENNIAL": "triennial",
    "http://publications.europa.eu/resource/authority/frequency/IRREG": "irregular",
    "http://publications.europa.eu/resource/authority/frequency/UNKNOWN": "unknown",
    "http://publications.europa.eu/resource/authority/frequency/OTHER": "other",
    "http://publications.europa.eu/resource/authority/frequency/NEVER": "no updates",
    "http://publications.europa.eu/resource/authority/frequency/OP_DATPRO": "provisional",
}

POLITICAL_LEVEL_EN: dict[str, str] = {
    "confederation": "Confederation",
    "canton": "Canton",
    "commune": "Commune",
    "other": "Other",
}


# =============================================================================
# Helpers
# =============================================================================

_SLUG_RE = re.compile(r"[^a-z0-9]+")
_WHITESPACE_RE = re.compile(r"\s+")
_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
SPATIAL_PLACEHOLDERS = {
    "spatial",
    "allgemeine infos",
    "general information",
    "informations generales",
}
SPATIAL_CANONICAL = {
    "switzerland": "Switzerland",
    "schweiz": "Switzerland",
    "suisse": "Switzerland",
}
OPENDATA_SWISS_LICENSE_LABELS = {
    "terms_by": "Opendata.swiss BY",
    "terms_ask": "Opendata.swiss ASK",
    "terms_by_ask": "Opendata.swiss BY ASK",
}


def slugify(s: str) -> str:
    """Produce a safe tag id: lowercase, [a-z0-9_-] only."""
    if not s:
        return ""
    s = s.lower().strip()
    # Keep letters/digits, replace everything else with '-'
    s = _SLUG_RE.sub("-", s)
    return s.strip("-")


def pick_text(
    ml: dict | str | None,
    preferred_lang: str | None = None,
    fallback_order: tuple[str, ...] = (),
) -> str:
    """Return a preferred localized value, falling back to other languages.

    Raw strings are treated as already-resolved default text.
    """
    if ml is None:
        return ""
    if isinstance(ml, str):
        return ml
    if isinstance(ml, dict):
        if preferred_lang:
            value = ml.get(preferred_lang)
            if value:
                return value
        for lang in fallback_order:
            v = ml.get(lang)
            if v:
                return v
    return ""


def pick_default_text(ml: dict | str | None) -> str:
    return pick_text(ml, preferred_lang="en", fallback_order=("fr",))


def pick_fr(ml: dict | str | None) -> str:
    return pick_text(ml, preferred_lang="fr", fallback_order=("en",))


def pick_exact_lang(ml: dict | str | None, lang: str) -> str:
    if not isinstance(ml, dict):
        return ""
    value = ml.get(lang)
    return value if isinstance(value, str) and value else ""


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


def normalize_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = _WHITESPACE_RE.sub(" ", str(value)).strip()
    return text or None


def fold_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def keyword_lexical_similarity(name_en: str, name_fr: str) -> float:
    left = fold_text(slugify(name_en)).replace("-", "")
    right = fold_text(slugify(name_fr)).replace("-", "")
    if not left or not right:
        return 0.0
    return difflib.SequenceMatcher(a=left, b=right).ratio()


def classify_keyword_translation_candidate(
    name_en: str,
    name_fr: str,
    *,
    cooccurrence: int,
    positional: int,
    ratio_en: float,
    ratio_fr: float,
    second: int,
) -> tuple[str, str]:
    lexical_similarity = keyword_lexical_similarity(name_en, name_fr)
    en_folded = fold_text(slugify(name_en))
    fr_folded = fold_text(slugify(name_fr))

    if en_folded == fr_folded:
        return "freeze", "identity"
    if (
        lexical_similarity >= 0.82
        and ratio_en >= 0.9
        and ratio_fr >= 0.75
        and second <= cooccurrence * 0.8
    ):
        return "freeze", "lexical-cognate"
    if (
        positional >= 2
        and ratio_en >= 0.9
        and ratio_fr >= 0.9
        and second == 0
        and lexical_similarity >= 0.6
    ):
        return "freeze", "isolated-alignment"
    if positional >= 1 and ratio_en >= 0.9 and ratio_fr >= 0.9:
        return "consider", "strong-alignment"
    if second >= cooccurrence * 0.8:
        return "review", "competitive-match"
    if lexical_similarity >= 0.7 and ratio_en >= 0.75 and ratio_fr >= 0.6:
        return "consider", "possible-cognate"
    return "review", "contextual-match"


def normalize_keyword_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [text for keyword in value if (text := normalize_text(keyword))]


def pick_best_keyword_match(
    counts: dict[str, int],
) -> tuple[str, int, str | None, int] | None:
    if not counts:
        return None

    best_name: str | None = None
    best_count = -1
    second_name: str | None = None
    second_count = 0
    for name, count in counts.items():
        if count > best_count or (
            count == best_count and (best_name is None or name < best_name)
        ):
            second_name, second_count = best_name, best_count if best_count >= 0 else 0
            best_name, best_count = name, count
        elif count > second_count or (
            count == second_count and (second_name is None or name < second_name)
        ):
            second_name, second_count = name, count

    if best_name is None:
        return None
    return best_name, best_count, second_name, second_count


def iter_text_values(value: Any):
    if value is None:
        return
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, dict):
        for child in value.values():
            yield from iter_text_values(child)
        return
    if isinstance(value, list):
        for child in value:
            yield from iter_text_values(child)


def pick_clean_spatial(value: Any) -> str | None:
    seen: set[str] = set()
    for candidate in iter_text_values(value):
        text = normalize_text(candidate)
        if not text:
            continue
        lowered = text.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        if lowered in SPATIAL_PLACEHOLDERS:
            continue
        if "://" in text:
            continue
        if len(text) < 3 or len(text) > 120:
            continue
        if not any(char.isalpha() for char in text):
            continue
        return SPATIAL_CANONICAL.get(lowered, text)
    return None


def normalize_email(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    if text.lower().startswith("mailto:"):
        text = text[7:].strip()
    text = text.lower()
    return text if _EMAIL_RE.match(text) else None


def normalize_contact_points(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, str]] = []
    seen: set[tuple[str | None, str | None]] = set()
    for entry in value:
        if not isinstance(entry, dict):
            continue
        name = normalize_text(pick_default_text(entry.get("name")) or entry.get("name"))
        email = normalize_email(entry.get("email"))
        if not name and not email:
            continue
        key = (name, email)
        if key in seen:
            continue
        seen.add(key)
        item: dict[str, str] = {}
        if name:
            item["name"] = name
        if email:
            item["email"] = email
        out.append(item)
    return out


def pick_manager_contact(value: Any) -> dict[str, str] | None:
    for entry in normalize_contact_points(value):
        if entry.get("email"):
            return entry
    return None


def manager_contact_id(value: Any) -> str | None:
    contact = pick_manager_contact(value)
    if not contact:
        return None
    email = contact.get("email")
    if not email:
        return None
    return f"contact---{slugify(email)}"


def pick_localisation(package: dict, organization: dict) -> str | None:
    return pick_clean_spatial(package.get("spatial")) or POLITICAL_LEVEL_EN.get(
        organization.get("political_level") or ""
    )


def normalize_license_label(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    parsed = urlparse(text)
    if parsed.scheme and parsed.netloc:
        token = (parsed.fragment or Path(parsed.path).name or "").casefold()
        if parsed.netloc.casefold().endswith("opendata.swiss"):
            return OPENDATA_SWISS_LICENSE_LABELS.get(token, f"opendata.swiss:{token}")
        return text
    return text


def dataset_license_label(resource: dict) -> str | None:
    return normalize_license_label(resource.get("license")) or normalize_license_label(
        resource.get("rights")
    )


def dataset_type_label(license_label: str | None) -> str:
    if license_label == "Opendata.swiss BY ASK":
        return "On request"
    return "Open"


def folder_license_label(package: dict, package_resources: list[dict]) -> str | None:
    package_label = normalize_license_label(
        package.get("license_title")
    ) or normalize_license_label(package.get("license_id"))
    resource_labels = sorted(
        {
            label
            for resource in package_resources
            if (label := dataset_license_label(resource))
        }
    )
    if len(resource_labels) == 1:
        return resource_labels[0]
    if not resource_labels:
        return package_label
    return None


def build_keyword_translation_map(
    packages: dict[str, dict],
    base_translations: dict[str, str] | None = None,
    min_count: int = 3,
) -> dict[str, str]:
    return analyze_keyword_translations(packages, base_translations, min_count)[
        "translations"
    ]


def analyze_keyword_translations(
    packages: dict[str, dict],
    base_translations: dict[str, str] | None = None,
    min_count: int = 3,
) -> dict[str, Any]:
    pair_counts: dict[tuple[str, str], int] = defaultdict(int)
    en_totals: dict[str, int] = defaultdict(int)
    fr_totals: dict[str, int] = defaultdict(int)
    cooccurrence_counts: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    positional_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for package in packages.values():
        keywords = package.get("keywords")
        if not isinstance(keywords, dict):
            continue
        normalized_en = normalize_keyword_list(keywords.get("en"))
        normalized_fr = normalize_keyword_list(keywords.get("fr"))

        for name_en in {name for name in normalized_en if name}:
            en_totals[name_en] += 1
            for name_fr in {name for name in normalized_fr if name}:
                cooccurrence_counts[name_en][name_fr] += 1
        for name_fr in {name for name in normalized_fr if name}:
            fr_totals[name_fr] += 1
        if len(normalized_en) == len(normalized_fr):
            for name_en, name_fr in zip(normalized_en, normalized_fr):
                if name_en and name_fr:
                    positional_counts[name_en][name_fr] += 1

        if len(normalized_en) != 1 or len(normalized_fr) != 1:
            continue

        name_en = normalized_en[0]
        name_fr = normalized_fr[0]
        if not name_en or not name_fr:
            continue

        pair_counts[(name_en, name_fr)] += 1

    translations: dict[str, str] = dict(base_translations or {})
    translation_sources: dict[str, str] = {
        name_en: "frozen" for name_en in translations
    }
    fr_to_en: dict[str, str] = {
        name_fr: name_en for name_en, name_fr in translations.items()
    }
    for (name_en, name_fr), count in pair_counts.items():
        if name_en in translations or name_fr in fr_to_en:
            continue
        if count < min_count:
            continue
        if count != en_totals[name_en] or count != fr_totals[name_fr]:
            continue
        translations[name_en] = name_fr
        translation_sources[name_en] = "singleton"
        fr_to_en[name_fr] = name_en

    candidates: list[tuple[int, int, int, str, str]] = []
    for name_en, counts in cooccurrence_counts.items():
        if name_en in translations:
            continue
        match = pick_best_keyword_match(counts)
        if match is None:
            continue
        name_fr, cooccurrence, _, second = match
        if name_fr in fr_to_en:
            continue
        positional = positional_counts[name_en].get(name_fr, 0)
        total_en = en_totals[name_en]
        total_fr = fr_totals[name_fr]

        if cooccurrence < min_count:
            continue
        if positional < 2:
            continue
        if cooccurrence / total_en < 0.8:
            continue
        if cooccurrence / total_fr < 0.8:
            continue
        if second and cooccurrence / second < 2:
            continue

        candidates.append((cooccurrence, positional, total_en, name_en, name_fr))

    for _, _, _, name_en, name_fr in sorted(
        candidates, key=lambda item: (-item[0], -item[1], -item[2], item[3])
    ):
        if name_en in translations or name_fr in fr_to_en:
            continue
        translations[name_en] = name_fr
        translation_sources[name_en] = "cooccurrence"
        fr_to_en[name_fr] = name_en

    review_candidates: list[dict[str, Any]] = []
    for name_en, counts in cooccurrence_counts.items():
        if name_en in translations:
            continue
        match = pick_best_keyword_match(counts)
        if match is None:
            continue
        name_fr, cooccurrence, second_name_fr, second = match
        if name_fr in fr_to_en:
            continue
        positional = positional_counts[name_en].get(name_fr, 0)
        total_en = en_totals[name_en]
        total_fr = fr_totals[name_fr]
        ratio_en = cooccurrence / total_en if total_en else 0.0
        ratio_fr = cooccurrence / total_fr if total_fr else 0.0
        lexical_similarity = keyword_lexical_similarity(name_en, name_fr)
        recommendation, rationale = classify_keyword_translation_candidate(
            name_en,
            name_fr,
            cooccurrence=cooccurrence,
            positional=positional,
            ratio_en=ratio_en,
            ratio_fr=ratio_fr,
            second=second,
        )

        if cooccurrence < 2 and positional < 1:
            continue
        if ratio_en < 0.5 or ratio_fr < 0.5:
            continue

        review_candidates.append(
            {
                "keyword_en": name_en,
                "keyword_fr": name_fr,
                "cooccurrence": cooccurrence,
                "positional": positional,
                "en_occurrences": total_en,
                "fr_occurrences": total_fr,
                "cooccurrence_ratio_en": round(ratio_en, 4),
                "cooccurrence_ratio_fr": round(ratio_fr, 4),
                "second_best_keyword_fr": second_name_fr,
                "second_best_cooccurrence": second,
                "lexical_similarity": round(lexical_similarity, 4),
                "recommendation": recommendation,
                "rationale": rationale,
            }
        )

    review_candidates.sort(
        key=lambda row: (
            str(row["recommendation"]) != "freeze",
            str(row["recommendation"]) != "consider",
            -int(row["cooccurrence"]),
            -int(row["positional"]),
            -float(row["cooccurrence_ratio_en"]),
            -float(row["cooccurrence_ratio_fr"]),
            str(row["keyword_en"]),
        )
    )

    return {
        "translations": translations,
        "translation_sources": translation_sources,
        "review_candidates": review_candidates,
    }


def load_keyword_translation_overrides() -> dict[str, str]:
    if not KEYWORD_TRANSLATIONS_FILE.exists():
        return {}

    payload = json.loads(KEYWORD_TRANSLATIONS_FILE.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("keyword_translations.json must contain an object")

    translations: dict[str, str] = {}
    seen_fr: set[str] = set()
    for raw_en, raw_fr in sorted(payload.items()):
        name_en = normalize_text(raw_en)
        name_fr = normalize_text(raw_fr)
        if not name_en or not name_fr:
            continue
        if name_fr in seen_fr:
            raise ValueError(
                f"Duplicate French keyword translation target: {name_fr!r}"
            )
        translations[name_en] = name_fr
        seen_fr.add(name_fr)

    return translations


def write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"  wrote {path.relative_to(ROOT)}")


def iter_keyword_entries(
    package: dict, keyword_translations: dict[str, str] | None = None
) -> list[tuple[str, str, str | None]]:
    keywords = package.get("keywords")
    if not isinstance(keywords, dict):
        return []

    entries: dict[str, tuple[str, str | None]] = {}
    keyword_translations = keyword_translations or {}

    for keyword in keywords.get("en") or []:
        name_en = normalize_text(keyword)
        if not name_en:
            continue
        tag_id = slugify(name_en)
        if not tag_id or tag_id in entries:
            continue
        entries[tag_id] = (name_en, keyword_translations.get(name_en))

    for keyword in keywords.get("fr") or []:
        name_fr = normalize_text(keyword)
        if not name_fr:
            continue
        tag_id = slugify(name_fr)
        if not tag_id:
            continue
        current = entries.get(tag_id)
        if current is None:
            entries[tag_id] = (name_fr, None)
            continue
        if current[1] is None:
            entries[tag_id] = (current[0], name_fr)

    return [
        (tag_id, name, name_fr) for tag_id, (name, name_fr) in sorted(entries.items())
    ]


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
    except TypeError, ValueError, IndexError, OverflowError:
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
    ) -> tuple[str | None, str | None]:
        if not localized_path:
            return None, None

        return f"Original source: {source_url}", f"Source originale: {source_url}"

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
            description, description_fr = build_pdf_doc_description(
                url,
                export_path,
            )
            row = {
                "id": pdf_doc_id(url),
                "name": pdf_doc_name(url, owner_name),
                "name:fr": None,
                "description": description,
                "description:fr": description_fr,
                "path": export_path or url,
                "type": "pdf",
                "last_update": source_last_update,
            }
            doc_registry[url] = row
        else:
            if export_path:
                row["path"] = export_path
                description, description_fr = build_pdf_doc_description(
                    url,
                    export_path,
                )
                row["description"] = description
                row["description:fr"] = description_fr
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
    with fp.open(encoding="utf-8") as fh:
        for line in fh:
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
    with fp.open(encoding="utf-8") as fh:
        for line in fh:
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
    with fp.open(encoding="utf-8") as fh:
        for line in fh:
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
    with fp.open(encoding="utf-8") as fh:
        for line in fh:
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
    with fp.open(encoding="utf-8") as fh:
        for line in fh:
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
    """Read ids flagged in staging/excluded_datasets.csv (drop from dataset metadata)."""
    fp = ROOT / "staging" / "excluded_datasets.csv"
    if not fp.exists():
        return set()
    ids: set[str] = set()
    with fp.open(newline="") as f:
        for row in csv.DictReader(f):
            rid = (row.get("id") or "").strip()
            if rid:
                ids.add(rid)
    return ids


def load_excluded_package_ids() -> set[str]:
    """Read package ids flagged in staging/excluded_packages.csv."""
    fp = ROOT / "staging" / "excluded_packages.csv"
    if not fp.exists():
        return set()
    ids: set[str] = set()
    with fp.open(newline="") as f:
        for row in csv.DictReader(f):
            package_id = (row.get("id") or "").strip()
            if package_id:
                ids.add(package_id)
    return ids


def package_organization(package: dict, organizations: dict[str, dict]) -> dict:
    org_name = package.get("organization_name")
    return organizations.get(org_name or "", {})


# =============================================================================
# Build entities
# =============================================================================


def build_institutions(
    packages: dict[str, dict], organizations: dict[str, dict]
) -> list[dict]:
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
        "wsl",
        "schweizerische-bundesbahnen-sbb",
    ]
    for vid in virtual_order:
        parent, name_en, name_fr = VIRTUAL_INSTITUTIONS[vid]
        rows.append(
            {
                "id": vid,
                "parent_id": parent,
                "name": name_en,
                "name:fr": name_fr,
                "description": None,
                "description:fr": None,
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
        display_name = o.get("display_name")
        title = o.get("title")
        description = o.get("description")
        rows.append(
            {
                "id": name,
                "parent_id": resolve_parent(name, o),
                "name": pick_default_text(display_name)
                or pick_default_text(title)
                or name,
                "name:fr": pick_exact_lang(display_name, "fr")
                or pick_exact_lang(title, "fr")
                or None,
                "description": pick_default_text(description) or None,
                "description:fr": pick_exact_lang(description, "fr") or None,
                "email": None,
                "phone": None,
                "start_date": None,
                "end_date": None,
                "tag_ids": None,
                "doc_ids": None,
            }
        )

    manager_contacts: dict[str, dict[str, Any]] = {}
    for _, package in sorted(packages.items()):
        contact = pick_manager_contact(package.get("contact_points"))
        if not contact:
            continue
        manager_organization_id = manager_contact_id(package.get("contact_points"))
        email = contact.get("email")
        if not manager_organization_id or not email:
            continue
        row = manager_contacts.get(manager_organization_id)
        if row is None:
            row = {
                "parent_candidates": set(),
                "name": None,
                "email": email,
            }
            manager_contacts[manager_organization_id] = row
        if package.get("organization_name"):
            row["parent_candidates"].add(package["organization_name"])
        display_name = contact.get("name")
        if (
            display_name
            and display_name.casefold() != email.casefold()
            and not row["name"]
        ):
            row["name"] = display_name

    for manager_organization_id in sorted(manager_contacts):
        manager = manager_contacts[manager_organization_id]
        parent_candidates = manager["parent_candidates"]
        rows.append(
            {
                "id": manager_organization_id,
                "parent_id": next(iter(parent_candidates))
                if len(parent_candidates) == 1
                else None,
                "name": manager.get("name") or manager.get("email"),
                "description": None,
                "email": manager.get("email"),
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
    resources: dict[str, dict],
    organizations: dict[str, dict],
    doc_registry: dict[str, dict],
    doc_downloads: dict[str, dict],
    keyword_translations: dict[str, str],
) -> list[dict]:
    """Return folder rows while registering shared PDF docs globally.

    Package URLs are exposed directly on folder.link. PDF documents visible on
    the package itself, in its description, or in staged documentation/
    relations fields feed doc rows when present.
    """
    folder_rows: list[dict] = []
    resources_by_package: dict[str, list[dict]] = defaultdict(list)
    for resource in resources.values():
        package_id = resource.get("package_id")
        if package_id:
            resources_by_package[package_id].append(resource)

    # 1. Thematic roots
    for tid, tname_fr in DCAT_GROUPS_FR.items():
        folder_rows.append(
            {
                "id": tid,
                "parent_id": None,
                "name": DCAT_GROUPS_EN.get(tid) or tname_fr,
                "name:fr": tname_fr,
                "description": None,
                "description:fr": None,
                "type": "thematique",
                "owner_organization_id": None,
                "manager_organization_id": None,
                "tag_ids": None,
                "doc_ids": None,
                "link": None,
                "data_path": None,
                "delivery_format": None,
                "last_update_date": None,
                "localisation": None,
                "license": None,
                "start_date": None,
                "end_date": None,
                "updating_each": None,
            }
        )
    for tid, tname_fr in EXTRA_ROOTS_FR.items():
        folder_rows.append(
            {
                "id": tid,
                "parent_id": None,
                "name": EXTRA_ROOTS_EN.get(tid) or tname_fr,
                "name:fr": tname_fr,
                "description": None,
                "description:fr": None,
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
        title = pick_default_text(p.get("title")) or p.get("name") or pid
        title_fr = pick_exact_lang(p.get("title"), "fr") or None
        description = pick_default_text(p.get("description"))
        description_fr = pick_exact_lang(p.get("description"), "fr") or None
        manager_organization_id = manager_contact_id(p.get("contact_points"))

        # Tags: one per group + free keywords (EN default, FR secondary)
        thematic_tags = [f"thematique---{g['name']}" for g in groups if g.get("name")]
        kw_tags = [
            tag_id for tag_id, _, _ in iter_keyword_entries(p, keyword_translations)
        ]
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
                "name:fr": title_fr,
                "description": description or None,
                "description:fr": description_fr,
                "type": "package",
                "owner_organization_id": org_name,
                "manager_organization_id": manager_organization_id,
                "tag_ids": tag_ids or None,
                "doc_ids": folder_doc_ids,
                "link": p.get("url"),
                "data_path": None,
                "delivery_format": None,
                "last_update_date": p.get("modified") or p.get("metadata_modified"),
                "localisation": pick_localisation(p, org),
                "license": folder_license_label(p, resources_by_package.get(pid, [])),
                "start_date": start_date,
                "end_date": end_date,
                "updating_each": ACCRUAL_EN.get(
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
        temporals = p.get("temporals") or []
        start_date = temporals[0].get("start_date") if temporals else None
        end_date = temporals[0].get("end_date") if temporals else None
        accrual_uri = p.get("accrual_periodicity") or ""
        manager_organization_id = manager_contact_id(p.get("contact_points"))

        fmt_value = (res.get("format") or "").upper()
        name_default = (
            pick_default_text(res.get("title"))
            or pick_default_text(res.get("name"))
            or fmt_value
            or rid
        )
        name_fr = pick_exact_lang(res.get("title"), "fr") or pick_exact_lang(
            res.get("name"), "fr"
        )
        # Avoid names that are just "csv" / "parquet" — prefix with package title
        pkg_title = pick_default_text(p.get("title")) or p.get("name") or pid
        pkg_title_fr = pick_exact_lang(p.get("title"), "fr")
        if name_default.strip().lower() in {
            "csv",
            "parquet",
            "xls",
            "xlsx",
            "excel",
            "",
        }:
            name_default = f"{pkg_title} — {fmt_value}"
            if pkg_title_fr:
                name_fr = f"{pkg_title_fr} — {fmt_value}"
        resource_description = pick_default_text(res.get("description"))
        description = resource_description or pick_default_text(p.get("description"))
        description_fr = pick_exact_lang(
            res.get("description"), "fr"
        ) or pick_exact_lang(p.get("description"), "fr")
        dataset_doc_ids, _ = build_pdf_docs(
            doc_registry,
            doc_downloads,
            name_default,
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
        # _match_path is resolved by datannurpy relative to the metadata file's
        # directory (METADATA_DIR), so write the local path relative to that.
        if local_path:
            try:
                local_path = str(
                    Path(local_path)
                    .resolve()
                    .relative_to(META_DIR.resolve(), walk_up=True)
                )
            except ValueError, OSError:
                pass

        license_label = dataset_license_label(res)
        rows.append(
            {
                "id": rid,
                "folder_id": pid,
                "owner_organization_id": org.get("name"),
                "manager_organization_id": manager_organization_id,
                "tag_ids": None,
                "doc_ids": dataset_doc_ids,
                "name": name_default,
                "name:fr": name_fr or None,
                "description": description or None,
                "description:fr": description_fr or None,
                "data_path": res.get("url"),
                "_match_path": local_path,
                "link": None,
                "license": license_label,
                "delivery_format": fmt_lower or None,
                "type": dataset_type_label(license_label),
                "localisation": pick_localisation(p, org),
                "start_date": start_date,
                "end_date": end_date,
                "last_update_date": res.get("modified")
                or res.get("last_modified")
                or m.get("modified"),
                "updating_each": ACCRUAL_EN.get(
                    accrual_uri, accrual_uri if accrual_uri else None
                ),
            }
        )
    return rows


def build_tags(
    packages: dict[str, dict], keyword_translations: dict[str, str]
) -> list[dict]:
    rows: list[dict] = []
    # Root thematic tag
    rows.append(
        {
            "id": "thematique",
            "parent_id": None,
            "name": "Theme",
            "name:fr": "Thématique",
            "description": None,
            "description:fr": None,
            "doc_ids": None,
        }
    )
    rows.append(
        {
            "id": "mot-cle---root",
            "parent_id": None,
            "name": "Keywords",
            "name:fr": "Mots-cles",
            "description": None,
            "description:fr": None,
            "doc_ids": None,
        }
    )
    # One child per DCAT group
    for tid, tname_fr in DCAT_GROUPS_FR.items():
        rows.append(
            {
                "id": f"thematique---{tid}",
                "parent_id": "thematique",
                "name": DCAT_GROUPS_EN.get(tid) or tname_fr,
                "name:fr": tname_fr,
                "description": None,
                "description:fr": None,
                "doc_ids": None,
            }
        )

    # Free keywords, deduped. Keep English by default and French as name:fr.
    seen: dict[str, tuple[str, str | None]] = {}
    for p in packages.values():
        for tid, name, name_fr in iter_keyword_entries(p, keyword_translations):
            current = seen.get(tid)
            if current is None:
                seen[tid] = (name, name_fr)
            elif current[1] is None and name_fr:
                seen[tid] = (current[0], name_fr)

    for tid, (name, name_fr) in sorted(seen.items()):
        rows.append(
            {
                "id": tid,
                "parent_id": "mot-cle---root",
                "name": name,
                "name:fr": name_fr,
                "description": None,
                "description:fr": None,
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
    "name:fr",
    "description",
    "description:fr",
    "email",
    "phone",
    "start_date",
    "end_date",
]
FOLDER_COLS = [
    "id",
    "parent_id",
    "manager_organization_id",
    "owner_organization_id",
    "tag_ids",
    "doc_ids",
    "name",
    "name:fr",
    "description",
    "description:fr",
    "link",
    "license",
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
    "manager_organization_id",
    "owner_organization_id",
    "tag_ids",
    "doc_ids",
    "name",
    "name:fr",
    "description",
    "description:fr",
    "data_path",
    "_match_path",
    "link",
    "license",
    "delivery_format",
    "type",
    "localisation",
    "start_date",
    "end_date",
    "last_update_date",
    "updating_each",
]
TAG_COLS = [
    "id",
    "parent_id",
    "doc_ids",
    "name",
    "name:fr",
    "description",
    "description:fr",
]
DOC_COLS = [
    "id",
    "name",
    "name:fr",
    "description",
    "description:fr",
    "path",
    "type",
    "last_update",
]


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


def purge_organizations(
    organizations: list[dict], folders: list[dict], datasets: list[dict]
) -> list[dict]:
    """Keep only organizations referenced by surviving rows, plus their ancestors."""
    org_by_id = {row["id"]: row for row in organizations}
    keep: set[str] = set()

    def mark(org_id: str | None) -> None:
        current = org_id
        while current and current in org_by_id and current not in keep:
            keep.add(current)
            current = org_by_id[current].get("parent_id")

    for row in folders:
        mark(row.get("owner_organization_id"))
        mark(row.get("manager_organization_id"))
    for row in datasets:
        mark(row.get("owner_organization_id"))
        mark(row.get("manager_organization_id"))

    return [row for row in organizations if row["id"] in keep]


def main() -> int:
    log_dir()
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
    excluded_package_ids = load_excluded_package_ids()
    if excluded:
        before = len(manifests)
        manifests = {rid: m for rid, m in manifests.items() if rid not in excluded}
        print(
            f"  excluded_datasets.csv: -{before - len(manifests)} dropped "
            f"({len(excluded)} ids in list)"
        )
    if excluded_package_ids:
        resource_package_ids = {
            resource_id: resource.get("package_id")
            for resource_id, resource in resources.items()
        }
        before = len(manifests)
        manifests = {
            rid: m
            for rid, m in manifests.items()
            if resource_package_ids.get(rid) not in excluded_package_ids
        }
        print(
            f"  excluded_packages.csv: -{before - len(manifests)} dropped "
            f"({len(excluded_package_ids)} ids in list)"
        )

    print("\nBuilding entities…")
    doc_registry: dict[str, dict] = {}
    keyword_analysis = analyze_keyword_translations(
        packages, load_keyword_translation_overrides()
    )
    keyword_translations = keyword_analysis["translations"]
    institutions = build_institutions(packages, organizations)
    folders = build_folders_and_docs(
        packages,
        resources,
        organizations,
        doc_registry,
        doc_downloads,
        keyword_translations,
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
    tags = build_tags(packages, keyword_translations)

    # Cascade purge: drop folders without surviving descendants, then orphan
    # docs and tags. Idempotent — runs every build, no state to maintain.
    if excluded or excluded_package_ids:
        n_o0, n_f0, n_d0, n_t0 = len(institutions), len(folders), len(docs), len(tags)
        folders, docs, tags = cascade_purge(folders, datasets, docs, tags)
        institutions = purge_organizations(institutions, folders, datasets)
        print(
            f"  cascade purge: -{n_o0 - len(institutions)} organizations, "
            f"-{n_f0 - len(folders)} folders, "
            f"-{n_d0 - len(docs)} docs, -{n_t0 - len(tags)} tags"
        )

    print(f"\nWriting CSVs to {META_DIR.relative_to(ROOT)}/")
    legacy_institution = META_DIR / "institution.csv"
    if legacy_institution.exists():
        legacy_institution.unlink()
    write_csv(META_DIR / "organization.csv", INSTITUTION_COLS, institutions)
    write_csv(META_DIR / "folder.csv", FOLDER_COLS, folders)
    write_csv(META_DIR / "dataset.csv", DATASET_COLS, datasets)
    write_csv(META_DIR / "tag.csv", TAG_COLS, tags)
    write_csv(META_DIR / "doc.csv", DOC_COLS, docs)

    for config_name in ("config.json", "configFilter.json"):
        config_source = PUBLIC_DIR / config_name
        config_target = META_DIR / config_name
        shutil.copy2(config_source, config_target)
        print(
            f"  copied {config_source.relative_to(ROOT)} -> {config_target.relative_to(ROOT)}"
        )

    # Sanity: detect FK issues
    print("\nSanity checks…")
    inst_ids = {r["id"] for r in institutions}
    folder_ids = {r["id"] for r in folders}

    errors = 0
    for r in folders:
        if r.get("parent_id") and r["parent_id"] not in folder_ids:
            print(f"  ERR folder {r['id']}: parent_id {r['parent_id']!r} not found")
            errors += 1
        if (
            r.get("owner_organization_id")
            and r["owner_organization_id"] not in inst_ids
        ):
            print(
                f"  ERR folder {r['id']}: owner_organization_id {r['owner_organization_id']!r} not found"
            )
            errors += 1
        if (
            r.get("manager_organization_id")
            and r["manager_organization_id"] not in inst_ids
        ):
            print(
                f"  ERR folder {r['id']}: manager_organization_id {r['manager_organization_id']!r} not found"
            )
            errors += 1
    for r in datasets:
        if r["folder_id"] not in folder_ids:
            print(f"  ERR dataset {r['id']}: folder_id {r['folder_id']!r} not found")
            errors += 1
        if (
            r.get("owner_organization_id")
            and r["owner_organization_id"] not in inst_ids
        ):
            print(
                f"  ERR dataset {r['id']}: owner_organization_id {r['owner_organization_id']!r} not found"
            )
            errors += 1
        if (
            r.get("manager_organization_id")
            and r["manager_organization_id"] not in inst_ids
        ):
            print(
                f"  ERR dataset {r['id']}: manager_organization_id {r['manager_organization_id']!r} not found"
            )
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
