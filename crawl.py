"""
Crawl opendata.swiss for French-language datasets exposing resources of a
chosen format.

Usage:
    uv run python crawl.py --format parquet
    uv run python crawl.py --format csv
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any, Iterator

from ckanapi import RemoteCKAN
from ckanapi.errors import CKANAPIError

from config import out_dir, parse_format_arg

CKAN_URL = "https://ckan.opendata.swiss"
USER_AGENT = "datannur-opench-crawler/0.1 (+https://github.com/datannur)"
PAGE_SIZE = 500
LANGUAGE = "fr"


def iter_packages(ckan: RemoteCKAN, fq: str) -> Iterator[dict[str, Any]]:
    start = 0
    total: int | None = None
    resp: dict[str, Any] = {}
    while True:
        for attempt in range(1, 4):
            try:
                resp = ckan.action.package_search(
                    fq=fq, rows=PAGE_SIZE, start=start, sort="id asc"
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
            print(f"  total datasets matching fq: {total}")

        results = resp.get("results", [])
        if not results:
            return
        for pkg in results:
            yield pkg
        start += len(results)
        assert total is not None
        if start >= total:
            return


def extract_resources(pkg: dict[str, Any], res_format: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for res in pkg.get("resources", []) or []:
        fmt = (res.get("format") or "").strip().upper()
        if fmt != res_format:
            continue
        url = res.get("download_url") or res.get("url") or ""
        out.append(
            {
                "dataset_id": pkg.get("id"),
                "dataset_name": pkg.get("name"),
                "organization": (pkg.get("organization") or {}).get("name"),
                "dataset_title": pkg.get("title"),
                "resource_id": res.get("id"),
                "resource_name": res.get("name"),
                "format": res.get("format"),
                "media_type": res.get("media_type") or res.get("mimetype"),
                "url": url,
                "rights": res.get("rights"),
                "byte_size": res.get("byte_size") or res.get("size"),
                "issued": res.get("issued"),
                "modified": res.get("modified"),
            }
        )
    return out


def main() -> int:
    fmt = parse_format_arg()
    o = out_dir(fmt)
    datasets_file = o / "datasets.jsonl"
    resources_file = o / "resources.jsonl"
    summary_file = o / "crawl_summary.json"

    fq = f"language:{LANGUAGE} AND res_format:{fmt.ckan_res_format}"
    ckan = RemoteCKAN(CKAN_URL, user_agent=USER_AGENT)

    n_datasets = 0
    n_datasets_matching = 0
    n_resources = 0

    print(f"Crawling {CKAN_URL} with fq='{fq}' ...")
    t0 = time.monotonic()

    with (
        datasets_file.open("w", encoding="utf-8") as fds,
        resources_file.open("w", encoding="utf-8") as frs,
    ):
        for pkg in iter_packages(ckan, fq):
            n_datasets += 1
            resources = extract_resources(pkg, fmt.ckan_res_format)
            if not resources:
                continue
            n_datasets_matching += 1
            n_resources += len(resources)

            fds.write(json.dumps(pkg, ensure_ascii=False) + "\n")
            for r in resources:
                frs.write(json.dumps(r, ensure_ascii=False) + "\n")

            if n_datasets_matching % 25 == 0:
                print(
                    f"  scanned={n_datasets} matching={n_datasets_matching} "
                    f"resources={n_resources}"
                )

    elapsed = time.monotonic() - t0
    summary = {
        "ckan_url": CKAN_URL,
        "fq": fq,
        "res_format": fmt.ckan_res_format,
        "datasets_scanned": n_datasets,
        "datasets_with_resources": n_datasets_matching,
        "resources": n_resources,
        "elapsed_seconds": round(elapsed, 1),
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
