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
        max_bytes=None,
    ),
    "csv": FormatConfig(
        key="csv",
        ckan_res_format="CSV",
        file_ext=".csv",
        # text/csv, application/csv, text/plain, text/tab-separated-values; some servers mislabel.
        accepted_content_types=("csv", "text/plain", "octet-stream", "tab-separated"),
        accepted_magic=(),  # CSV has no magic bytes
        # Reject obvious non-CSV: ZIP, HTML, PDF, Excel.
        rejected_magic=(
            b"PK\x03\x04",  # zip / xlsx
            b"PK\x05\x06",  # empty zip
            b"%PDF",
            b"<!DOC",
            b"<!doc",
            b"<html",
            b"<HTML",
            b"\xd0\xcf\x11\xe0",  # legacy Office (xls, doc)
        ),
        max_bytes=300 * 1024 * 1024,  # 300 MB
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
        max_bytes=300 * 1024 * 1024,
        ext_from_magic=(
            (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1", ".xls"),
            (b"PK\x03\x04", ".xlsx"),
        ),
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


def exported_doc_rel_path(doc_id: str) -> Path:
    return Path("data") / "doc" / f"{doc_id}.pdf"


def data_dir(fmt: FormatConfig) -> Path:
    p = ROOT / "data"
    p.mkdir(parents=True, exist_ok=True)
    return p
