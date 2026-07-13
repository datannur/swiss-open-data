"""Shared config for format-specific pipelines (parquet, csv, ...)."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class FormatConfig:
    key: str  # format key: "parquet", "csv", "excel"
    ckan_res_format: str  # Solr value: "PARQUET", "CSV", "XLS"
    file_ext: str  # default extension; may be overridden per-file
    # For download-time validation. Lowercased substrings.
    accepted_content_types: tuple[str, ...] = ()
    # Magic-byte prefixes we EXPECT (bytes). Empty = skip check.
    accepted_magic: tuple[bytes, ...] = ()
    # Magic-byte prefixes we must REJECT (bytes).
    rejected_magic: tuple[bytes, ...] = ()
    # Max size per resource (bytes). None = unlimited.
    max_bytes: int | None = None
    # Optional: return the real extension for a given 16-byte head.
    # If None, file_ext is used.
    ext_from_magic: tuple[tuple[bytes, str], ...] = ()


FORMATS: dict[str, FormatConfig] = {
    "parquet": FormatConfig(
        key="parquet",
        ckan_res_format="PARQUET",
        file_ext=".parquet",
        accepted_content_types=("parquet", "octet-stream"),
        accepted_magic=(b"PAR1",),
        rejected_magic=(),
        max_bytes=10 * 1024 * 1024,  # 10 MB
    ),
    "csv": FormatConfig(
        key="csv",
        ckan_res_format="CSV",
        file_ext=".csv",
        # text/csv, application/csv, text/plain, text/tab-separated-values; some servers mislabel.
        # Zipped/gzipped CSVs are kept with their container extension:
        # datannurpy scans .csv.gz and single-data-file .zip archives.
        accepted_content_types=(
            "csv",
            "text/plain",
            "octet-stream",
            "tab-separated",
            "zip",
            "gzip",
        ),
        accepted_magic=(),  # CSV has no magic bytes
        # Reject obvious non-CSV: HTML, PDF, Excel.
        rejected_magic=(
            b"PK\x05\x06",  # empty zip
            b"%PDF",
            b"<!DOC",
            b"<!doc",
            b"<html",
            b"<HTML",
            b"\xd0\xcf\x11\xe0",  # legacy Office (xls, doc)
        ),
        max_bytes=10 * 1024 * 1024,  # 10 MB
        ext_from_magic=(
            (b"PK\x03\x04", ".zip"),
            (b"\x1f\x8b", ".csv.gz"),
        ),
    ),
    "excel": FormatConfig(
        key="excel",
        # opendata.swiss only indexes 'XLS'; in practice this covers both
        # legacy .xls (BIFF / CFBF) and modern .xlsx (ZIP). We accept both
        # and pick the extension based on the magic bytes of the payload.
        ckan_res_format="XLS",
        file_ext=".xlsx",  # default when magic is unknown
        accepted_content_types=(
            "spreadsheet",
            "excel",
            "xls",
            "octet-stream",
        ),
        # Accept either OLE Compound File (legacy .xls) or ZIP (.xlsx).
        accepted_magic=(
            b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1",  # .xls (BIFF/CFBF)
            b"PK\x03\x04",  # .xlsx (ZIP)
        ),
        rejected_magic=(
            b"%PDF",
            b"<!DOC",
            b"<!doc",
            b"<html",
            b"<HTML",
        ),
        max_bytes=10 * 1024 * 1024,  # 10 MB
        ext_from_magic=(
            (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1", ".xls"),
            (b"PK\x03\x04", ".xlsx"),
        ),
    ),
    "ods": FormatConfig(
        key="ods",
        ckan_res_format="ODS",
        file_ext=".ods",
        accepted_content_types=("opendocument", "ods", "zip", "octet-stream"),
        accepted_magic=(b"PK\x03\x04",),  # ODS is a ZIP container
        rejected_magic=(b"%PDF", b"<!DOC", b"<!doc", b"<html", b"<HTML"),
        max_bytes=10 * 1024 * 1024,  # 10 MB
    ),
    "zip": FormatConfig(
        # datannurpy scans .zip archives holding exactly one data file
        # (CSV, Excel, ODS, Parquet, …) and zipped Shapefiles.
        key="zip",
        ckan_res_format="ZIP",
        file_ext=".zip",
        # Some servers label zip downloads as Excel; the PK magic check below
        # still guarantees the payload is an archive.
        accepted_content_types=("zip", "compressed", "octet-stream", "excel"),
        accepted_magic=(b"PK\x03\x04",),
        rejected_magic=(b"%PDF", b"<!DOC", b"<!doc", b"<html", b"<HTML"),
        max_bytes=10 * 1024 * 1024,  # 10 MB
    ),
    "geojson": FormatConfig(
        key="geojson",
        ckan_res_format="GEOJSON",
        file_ext=".geojson",
        accepted_content_types=("geo+json", "json", "octet-stream"),
        accepted_magic=(),  # JSON text, no magic bytes
        rejected_magic=(
            b"PK\x03\x04",
            b"%PDF",
            b"<!DOC",
            b"<!doc",
            b"<html",
            b"<HTML",
        ),
        max_bytes=10 * 1024 * 1024,  # 10 MB
    ),
    "shp": FormatConfig(
        # CKAN "SHP" downloads are usually zipped Shapefiles; a bare .shp
        # main file also exists. Extension follows the actual payload.
        key="shp",
        ckan_res_format="SHP",
        file_ext=".zip",
        accepted_content_types=("zip", "shape", "octet-stream"),
        accepted_magic=(
            b"PK\x03\x04",  # zipped Shapefile
            b"\x00\x00\x27\x0a",  # bare .shp main file header
        ),
        rejected_magic=(b"%PDF", b"<!DOC", b"<!doc", b"<html", b"<HTML"),
        max_bytes=10 * 1024 * 1024,  # 10 MB
        ext_from_magic=(
            (b"PK\x03\x04", ".zip"),
            (b"\x00\x00\x27\x0a", ".shp"),
        ),
    ),
    "kml": FormatConfig(
        # Zipped payloads (KMZ-style) are stored as .zip for datannurpy's
        # single-data-file archive scanning.
        key="kml",
        ckan_res_format="KML",
        file_ext=".kml",
        accepted_content_types=("kml", "xml", "octet-stream", "zip", "compressed"),
        accepted_magic=(),  # XML text
        rejected_magic=(b"%PDF", b"<!DOC", b"<!doc", b"<html", b"<HTML"),
        max_bytes=10 * 1024 * 1024,  # 10 MB
        ext_from_magic=((b"PK\x03\x04", ".zip"),),
    ),
    "gml": FormatConfig(
        key="gml",
        ckan_res_format="GML",
        file_ext=".gml",
        accepted_content_types=("gml", "xml", "octet-stream", "zip", "compressed"),
        accepted_magic=(),  # XML text
        rejected_magic=(b"%PDF", b"<!DOC", b"<!doc", b"<html", b"<HTML"),
        max_bytes=10 * 1024 * 1024,  # 10 MB
        ext_from_magic=((b"PK\x03\x04", ".zip"),),
    ),
    "gpkg": FormatConfig(
        # Discovered by datannurpy folder scans since 0.31.0 — one dataset
        # per layer, like an explicit database: entry.
        key="gpkg",
        ckan_res_format="GPKG",
        file_ext=".gpkg",
        # Many geoportals serve GPKG zipped; keep the archive extension so
        # datannurpy's zip discovery can handle it.
        accepted_content_types=(
            "geopackage",
            "gpkg",
            "sqlite",
            "octet-stream",
            "zip",
            "compressed",
        ),
        accepted_magic=(b"SQLite format 3\x00", b"PK\x03\x04"),
        rejected_magic=(b"%PDF", b"<!DOC", b"<!doc", b"<html", b"<HTML"),
        max_bytes=10 * 1024 * 1024,  # 10 MB
        ext_from_magic=(
            (b"SQLite format 3\x00", ".gpkg"),
            (b"PK\x03\x04", ".zip"),
        ),
    ),
    "gpx": FormatConfig(
        key="gpx",
        ckan_res_format="GPX",
        file_ext=".gpx",
        accepted_content_types=("gpx", "xml", "octet-stream"),
        accepted_magic=(),  # XML text
        rejected_magic=(b"%PDF", b"<!DOC", b"<!doc", b"<html", b"<HTML"),
        max_bytes=10 * 1024 * 1024,  # 10 MB
    ),
}


def iter_formats() -> tuple[FormatConfig, ...]:
    return tuple(FORMATS[key] for key in sorted(FORMATS))


def parse_noop_args(description: str) -> None:
    ap = argparse.ArgumentParser(description=description)
    ap.parse_args()


def staging_dir() -> Path:
    p = ROOT / "staging"
    p.mkdir(parents=True, exist_ok=True)
    return p


def doc_cache_dir() -> Path:
    p = staging_dir() / "docs"
    p.mkdir(parents=True, exist_ok=True)
    return p


def doc_manifest_path() -> Path:
    return staging_dir() / "doc_download_state.jsonl"


def log_dir() -> Path:
    p = staging_dir() / "logs"
    p.mkdir(parents=True, exist_ok=True)
    return p


def exported_doc_rel_path(doc_id: str) -> Path:
    return Path("data") / "doc" / f"{doc_id}.pdf"


def data_dir(fmt: FormatConfig) -> Path:
    p = ROOT / "data"
    p.mkdir(parents=True, exist_ok=True)
    return p
