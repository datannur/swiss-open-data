from __future__ import annotations

import hashlib
import re
from typing import Any

PDF_URL_RE = re.compile(r'https?://[^\s<>"\']+')


def extract_pdf_urls(*values: Any) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []

    def visit(value: Any) -> None:
        if not value:
            return
        if isinstance(value, str):
            for match in PDF_URL_RE.findall(value):
                url = match.rstrip(").,;]>")
                base = url.lower().split("?", 1)[0].split("#", 1)[0]
                if not base.endswith(".pdf"):
                    continue
                if url in seen:
                    continue
                seen.add(url)
                out.append(url)
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                visit(item)

    for value in values:
        visit(value)
    return out


def pdf_doc_name(url: str, fallback: str) -> str:
    base = url.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    filename = base.rsplit("/", 1)[-1] if "/" in base else base
    return filename or fallback


def pdf_doc_id(url: str) -> str:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    return f"doc---pdf---{digest}"