"""
Crawl opendata.swiss for datasets exposing the configured resource formats.

Outputs are appended to the shared staging directory. Packages are deduped by
package id; resources stay one line per CKAN resource and carry their format in
the payload.

Usage:
    uv run python src/crawl.py
"""

from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Iterator

from ckanapi import RemoteCKAN
from ckanapi.errors import CKANAPIError

from config import iter_formats, parse_noop_args, staging_dir

CKAN_URL = "https://ckan.opendata.swiss"
USER_AGENT = (
    "swiss-open-data-crawler/0.1 (+https://github.com/datannur/swiss-open-data)"
)
PAGE_SIZE = 500
LANGUAGES = ("en", "fr", "de", "it")
URL_RE = re.compile(r'https?://[^\s<>"\']+')


def project_localized_text(
    value: dict[str, Any] | str | None,
    keep_languages: tuple[str, ...] = LANGUAGES,
) -> dict[str, str] | str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    if isinstance(value, dict):
        projected = {
            lang: text
            for lang in keep_languages
            if isinstance((text := value.get(lang)), str) and text
        }
        return projected or None
    return None


def project_contact_points(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, Any]] = []
    for entry in value:
        if not isinstance(entry, dict):
            continue
        projected: dict[str, Any] = {}
        name = project_localized_text(entry.get("name"))
        email = entry.get("email")
        if name:
            projected["name"] = name
        if email:
            projected["email"] = email
        if projected:
            out.append(projected)
    return out


def project_keywords(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, list[str]] = {}
    for lang in LANGUAGES:
        items = value.get(lang)
        if not isinstance(items, list):
            continue
        filtered = [item for item in items if isinstance(item, str) and item]
        if filtered:
            out[lang] = filtered
    return out


def extract_urls(value: Any) -> list[str]:
    """Collect URLs found anywhere in a nested CKAN field."""
    seen: set[str] = set()
    out: list[str] = []

    def visit(node: Any) -> None:
        if node is None:
            return
        if isinstance(node, str):
            for match in URL_RE.findall(node):
                url = match.rstrip(").,;]>")
                if url in seen:
                    continue
                seen.add(url)
                out.append(url)
            return
        if isinstance(node, dict):
            for child in node.values():
                visit(child)
            return
        if isinstance(node, list):
            for child in node:
                visit(child)

    visit(value)
    return out


def iter_packages(ckan: RemoteCKAN) -> Iterator[dict[str, Any]]:
    start = 0
    total: int | None = None
    resp: dict[str, Any] = {}
    while True:
        for attempt in range(1, 4):
            try:
                resp = ckan.action.package_search(
                    rows=PAGE_SIZE, start=start, sort="id asc"
                )
                break
            except (CKANAPIError, Exception) as exc:  # noqa: BLE001
                if attempt == 3:
                    raise
                wait = 2**attempt
                print(f"  ! error ({exc}), retry in {wait}s", file=sys.stderr)
                time.sleep(wait)

        if total is None:
            total = resp["count"]
            print(f"  total datasets: {total}")

        results = resp.get("results", [])
        if not results:
            return
        for pkg in results:
            yield pkg
        start += len(results)
        assert total is not None
        if start >= total:
            return


def extract_resources(
    pkg: dict[str, Any], format_map: dict[str, str]
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for res in pkg.get("resources", []) or []:
        res_format = (res.get("format") or "").strip().upper()
        format_key = format_map.get(res_format)
        if format_key is None:
            continue
        url = res.get("download_url") or res.get("url") or ""
        out.append(
            {
                "format_key": format_key,
                "package_id": pkg.get("id"),
                "resource_id": res.get("id"),
                "name": project_localized_text(res.get("name")),
                "title": project_localized_text(res.get("title")),
                "description": project_localized_text(res.get("description")),
                "language": res.get("language"),
                "format": res.get("format"),
                "url": url,
                "documentation_urls": extract_urls(res.get("documentation")),
                "relation_urls": extract_urls(res.get("relations")),
                "license": res.get("license"),
                "rights": res.get("rights"),
                "modified": res.get("modified"),
            }
        )
    return out


def project_organization(org: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": org.get("name"),
        "title": project_localized_text(org.get("title")),
        "display_name": project_localized_text(org.get("display_name")),
        "description": project_localized_text(org.get("description")),
        "political_level": org.get("political_level"),
        "groups": [
            {"name": group.get("name")}
            for group in (org.get("groups") or [])
            if group.get("name")
        ],
    }


def project_package(pkg: dict[str, Any]) -> dict[str, Any]:
    org = pkg.get("organization") or {}

    return {
        "id": pkg.get("id"),
        "name": pkg.get("name"),
        "title": project_localized_text(pkg.get("title")),
        "description": project_localized_text(pkg.get("description")),
        "url": pkg.get("url"),
        "documentation_urls": extract_urls(pkg.get("documentation")),
        "relation_urls": extract_urls(pkg.get("relations")),
        "spatial": project_localized_text(pkg.get("spatial")),
        "contact_points": project_contact_points(pkg.get("contact_points")),
        "license_id": pkg.get("license_id"),
        "license_title": pkg.get("license_title"),
        "modified": pkg.get("modified"),
        "metadata_modified": pkg.get("metadata_modified"),
        "accrual_periodicity": pkg.get("accrual_periodicity"),
        "keywords": project_keywords(pkg.get("keywords")),
        "temporals": [
            {
                "start_date": temporal.get("start_date"),
                "end_date": temporal.get("end_date"),
            }
            for temporal in (pkg.get("temporals") or [])
        ],
        "groups": [
            {"name": group.get("name")}
            for group in (pkg.get("groups") or [])
            if group.get("name")
        ],
        "organization_name": org.get("name"),
    }


def load_jsonl_by_key(path: Path, key: str) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    rows: dict[str, dict[str, Any]] = {}
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            row = json.loads(line)
            row_key = row.get(key)
            if row_key:
                rows[row_key] = row
    return rows


def load_packages_by_id(
    primary_path: Path, legacy_path: Path
) -> dict[str, dict[str, Any]]:
    if primary_path.exists():
        return load_jsonl_by_key(primary_path, "id")
    return load_jsonl_by_key(legacy_path, "id")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_summary(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return {}
    by_format = (
        data.get("by_format") if isinstance(data.get("by_format"), dict) else data
    )
    if not isinstance(by_format, dict):
        return {}
    return {
        str(key): value for key, value in by_format.items() if isinstance(value, dict)
    }


def main() -> int:
    parse_noop_args("Crawl opendata.swiss for all configured resource formats.")
    o = staging_dir()
    organizations_file = o / "organizations.jsonl"
    packages_file = o / "packages.jsonl"
    resources_file = o / "resources.jsonl"
    summary_file = o / "crawl_summary.json"

    formats = iter_formats()
    format_map = {fmt.ckan_res_format: fmt.key for fmt in formats}
    counts_by_format = {
        fmt.key: {"packages_with_resources": 0, "resources": 0} for fmt in formats
    }

    ckan = RemoteCKAN(CKAN_URL, user_agent=USER_AGENT)

    n_packages = 0
    n_packages_matching = 0
    n_resources = 0

    print(f"Crawling {CKAN_URL} ...")
    t0 = time.monotonic()

    staged_organizations: dict[str, dict[str, Any]] = {}
    staged_packages: dict[str, dict[str, Any]] = {}
    staged_resources: dict[str, dict[str, Any]] = {}

    for pkg in iter_packages(ckan):
        n_packages += 1
        resources = extract_resources(pkg, format_map)
        if not resources:
            continue
        n_packages_matching += 1
        n_resources += len(resources)

        seen_formats = {resource["format_key"] for resource in resources}
        for format_key in seen_formats:
            counts_by_format[format_key]["packages_with_resources"] += 1
        for resource in resources:
            counts_by_format[resource["format_key"]]["resources"] += 1

        org = pkg.get("organization") or {}
        org_name = org.get("name")
        if org_name:
            staged_organizations[org_name] = project_organization(org)
        staged_packages[pkg["id"]] = project_package(pkg)
        for resource in resources:
            staged_resources[resource["resource_id"]] = resource

        if n_packages_matching % 25 == 0:
            print(
                f"  scanned={n_packages} matching={n_packages_matching} "
                f"resources={n_resources}"
            )

    write_jsonl(
        organizations_file,
        sorted(staged_organizations.values(), key=lambda row: row["name"]),
    )
    write_jsonl(
        packages_file, sorted(staged_packages.values(), key=lambda row: row["id"])
    )
    write_jsonl(
        resources_file,
        sorted(staged_resources.values(), key=lambda row: row["resource_id"]),
    )

    elapsed = time.monotonic() - t0
    by_format = load_summary(summary_file)
    for fmt in formats:
        by_format[fmt.key] = {
            "format_key": fmt.key,
            "res_format": fmt.ckan_res_format,
            "packages_with_resources": counts_by_format[fmt.key][
                "packages_with_resources"
            ],
            "resources": counts_by_format[fmt.key]["resources"],
        }
    summary = {
        "ckan_url": CKAN_URL,
        "packages_scanned": n_packages,
        "packages_with_resources": n_packages_matching,
        "resources": n_resources,
        "elapsed_seconds": round(elapsed, 1),
        "by_format": by_format,
    }
    summary_file.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("\n=== Done ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"\nOutputs in: {o}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
