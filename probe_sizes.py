"""
Probe each resource URL with HEAD (fallback to ranged GET) to collect
content-length, content-type, etag, last-modified, resolved URL.

Usage:
    uv run python probe_sizes.py --format csv
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import out_dir, parse_format_arg

USER_AGENT = "datannur-opench-crawler/0.1 (+https://github.com/datannur)"
TIMEOUT = (10, 30)
WORKERS = 8
MAX_RETRIES = 3


def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=0.5,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(("HEAD", "GET")),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry, pool_connections=WORKERS, pool_maxsize=WORKERS
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "identity",
            "Connection": "close",
        }
    )
    return s


def probe(session: requests.Session, url: str) -> dict:
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = session.head(url, allow_redirects=True, timeout=TIMEOUT)
            if r.status_code in (403, 405, 501) or (
                r.ok and not r.headers.get("Content-Length")
            ):
                r = session.get(
                    url,
                    allow_redirects=True,
                    timeout=TIMEOUT,
                    headers={"Range": "bytes=0-0"},
                    stream=True,
                )
                r.close()
            h = r.headers
            content_length: int | None = None
            cr = h.get("Content-Range", "")
            if "/" in cr:
                try:
                    content_length = int(cr.rsplit("/", 1)[1])
                except ValueError:
                    pass
            if content_length is None and h.get("Content-Length"):
                try:
                    content_length = int(h["Content-Length"])
                except ValueError:
                    pass
            return {
                "status_code": r.status_code,
                "resolved_url": r.url,
                "content_length": content_length,
                "content_type": h.get("Content-Type"),
                "etag": h.get("ETag"),
                "last_modified": h.get("Last-Modified"),
                "accept_ranges": h.get("Accept-Ranges"),
                "error": None,
            }
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                time.sleep(1 + attempt)
    return {
        "status_code": None,
        "resolved_url": None,
        "content_length": None,
        "content_type": None,
        "etag": None,
        "last_modified": None,
        "accept_ranges": None,
        "error": f"{type(last_exc).__name__}: {last_exc}",
    }


def main() -> int:
    fmt = parse_format_arg()
    o = out_dir(fmt)
    in_file: Path = o / "resources.jsonl"
    out_file: Path = o / "resources_probed.jsonl"

    resources = [
        json.loads(line) for line in in_file.read_text().splitlines() if line.strip()
    ]
    print(f"Probing {len(resources)} {fmt.key} resources with {WORKERS} workers...")
    t0 = time.monotonic()
    results: list[dict] = [None] * len(resources)  # type: ignore[list-item]

    session = _make_session()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        future_to_idx = {
            pool.submit(probe, session, res["url"]): i
            for i, res in enumerate(resources)
            if res.get("url")
        }
        done = 0
        for fut in as_completed(future_to_idx):
            i = future_to_idx[fut]
            results[i] = fut.result()
            done += 1
            if done % 100 == 0 or done == len(future_to_idx):
                print(f"  {done}/{len(future_to_idx)}")

    with out_file.open("w", encoding="utf-8") as f:
        for res, probed in zip(resources, results):
            res["probe"] = probed or {"error": "no url"}
            f.write(json.dumps(res, ensure_ascii=False) + "\n")

    ok = sum(
        1
        for r in results
        if r and r.get("status_code") and 200 <= r["status_code"] < 400
    )
    with_size = sum(1 for r in results if r and r.get("content_length"))
    total_bytes = sum(
        r["content_length"] for r in results if r and r.get("content_length")
    )
    errors = sum(1 for r in results if r and r.get("error"))
    bad_status = sum(
        1 for r in results if r and r.get("status_code") and r["status_code"] >= 400
    )

    print("\n=== HEAD probe summary ===")
    print(f"total              : {len(resources)}")
    print(f"reachable (2xx/3xx): {ok}")
    print(f"HTTP >= 400        : {bad_status}")
    print(f"network errors     : {errors}")
    print(f"with content_length: {with_size}")
    if total_bytes:
        print(f"total declared size: {total_bytes / 1e9:.2f} GB")
    if fmt.max_bytes:
        over = sum(
            1
            for r in results
            if r and r.get("content_length") and r["content_length"] > fmt.max_bytes
        )
        print(f"over {fmt.max_bytes / 1e6:.0f} MB limit : {over}")
    print(f"elapsed            : {time.monotonic() - t0:.1f} s")
    print(f"\nOutput: {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
