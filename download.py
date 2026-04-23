"""
Download resources referenced in out/<format>/resources_probed.jsonl
into ./data/<format>/<resource_id><ext> and maintain
out/<format>/manifest.jsonl linking every local file back to its CKAN metadata.

Features:
    - Idempotent: skip files already present with expected size.
    - Size cap per format (e.g. CSV <= 100 MB). Aborts mid-stream if exceeded
      when size is unknown upfront.
    - Content validation: Content-Type check + magic-byte check against a
      per-format accept/reject list (rejects ZIP/HTML/PDF for CSV, etc.).
    - Safe to interrupt: manifest is rewritten atomically after each batch.

Usage:
    uv run python download.py --format parquet
    uv run python download.py --format csv
"""

from __future__ import annotations

import hashlib
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import FormatConfig, data_dir, out_dir, parse_format_arg

USER_AGENT = "datannur-opench-crawler/0.1 (+https://github.com/datannur)"
TIMEOUT = (15, 120)
WORKERS = 6
CHUNK = 1 << 20  # 1 MiB
MAGIC_SNIFF_BYTES = 16


def target_path(fmt: FormatConfig, res: dict[str, Any]) -> Path:
    rid = res.get("resource_id") or ""
    if not rid:
        raise ValueError("resource has no resource_id")
    return data_dir(fmt) / f"{rid}{fmt.file_ext}"


def existing_file(fmt: FormatConfig, res: dict[str, Any]) -> Path | None:
    """Return a pre-existing download for this resource, whatever its extension."""
    rid = res["resource_id"]
    dd = data_dir(fmt)
    exts = {fmt.file_ext, *(e for _, e in fmt.ext_from_magic)}
    for ext in exts:
        p = dd / f"{rid}{ext}"
        if p.exists():
            return p
    return None


def ext_for_payload(fmt: FormatConfig, head: bytes) -> str:
    for magic, ext in fmt.ext_from_magic:
        if head.startswith(magic):
            return ext
    return fmt.file_ext


def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(("GET",)),
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


@dataclass
class DlResult:
    status: str
    # "ok" | "skipped" | "http_error" | "network_error" | "size_mismatch"
    # | "too_large" | "content_rejected"
    downloaded_bytes: int
    sha256: str | None
    http_status: int | None
    error: str | None
    content_type: str | None = None


def _content_type_ok(fmt: FormatConfig, ct: str | None) -> bool:
    if not fmt.accepted_content_types:
        return True
    if not ct:
        # Unknown Content-Type: accept, magic-byte check will catch bad payloads.
        return True
    ct = ct.lower()
    return any(tok in ct for tok in fmt.accepted_content_types)


def _magic_ok(fmt: FormatConfig, head: bytes) -> tuple[bool, str | None]:
    for bad in fmt.rejected_magic:
        if head.startswith(bad):
            return False, f"rejected magic bytes: {head[: len(bad)]!r}"
    if fmt.accepted_magic and not any(
        head.startswith(good) for good in fmt.accepted_magic
    ):
        return False, f"unexpected magic bytes: {head[:8]!r}"
    return True, None


def download_one(
    session: requests.Session, fmt: FormatConfig, res: dict[str, Any]
) -> tuple[Path, DlResult]:
    dest = target_path(fmt, res)
    probe = res.get("probe") or {}
    expected = (
        probe.get("content_length")
        if isinstance(probe.get("content_length"), int)
        else None
    )

    # Pre-filter on declared size
    if fmt.max_bytes and expected is not None and expected > fmt.max_bytes:
        return dest, DlResult(
            "too_large",
            0,
            None,
            None,
            f"declared size {expected} > {fmt.max_bytes}",
            probe.get("content_type"),
        )

    # Idempotency
    existing = existing_file(fmt, res)
    if existing is not None:
        size = existing.stat().st_size
        if expected is not None and size == expected and size > 0:
            return existing, DlResult("skipped", size, None, None, None)
        if expected is None and size > 0:
            return existing, DlResult("skipped", size, None, None, None)

    # Staging path: use a generic .part name, rename with correct extension at the end.
    rid = res["resource_id"]
    tmp = data_dir(fmt) / f"{rid}.part"
    tmp.parent.mkdir(parents=True, exist_ok=True)

    url = res["url"]
    try:
        with session.get(
            url, stream=True, timeout=TIMEOUT, allow_redirects=True
        ) as r:
            ct = r.headers.get("Content-Type")
            if r.status_code >= 400:
                return (
                    data_dir(fmt) / f"{rid}{fmt.file_ext}",
                    DlResult(
                        "http_error",
                        0,
                        None,
                        r.status_code,
                        f"HTTP {r.status_code}",
                        ct,
                    ),
                )

            if not _content_type_ok(fmt, ct):
                return (
                    data_dir(fmt) / f"{rid}{fmt.file_ext}",
                    DlResult(
                        "content_rejected",
                        0,
                        None,
                        r.status_code,
                        f"Content-Type: {ct}",
                        ct,
                    ),
                )

            h = hashlib.sha256()
            written = 0
            magic_checked = False
            chosen_ext = fmt.file_ext
            limit = fmt.max_bytes
            with tmp.open("wb") as f:
                for chunk in r.iter_content(CHUNK):
                    if not chunk:
                        continue
                    if not magic_checked and len(chunk) >= MAGIC_SNIFF_BYTES:
                        head = chunk[:MAGIC_SNIFF_BYTES]
                        ok, why = _magic_ok(fmt, head)
                        magic_checked = True
                        if not ok:
                            tmp.unlink(missing_ok=True)
                            return (
                                data_dir(fmt) / f"{rid}{fmt.file_ext}",
                                DlResult(
                                    "content_rejected",
                                    0,
                                    None,
                                    r.status_code,
                                    why,
                                    ct,
                                ),
                            )
                        chosen_ext = ext_for_payload(fmt, head)
                    f.write(chunk)
                    h.update(chunk)
                    written += len(chunk)
                    if limit is not None and written > limit:
                        tmp.unlink(missing_ok=True)
                        return (
                            data_dir(fmt) / f"{rid}{fmt.file_ext}",
                            DlResult(
                                "too_large",
                                written,
                                None,
                                r.status_code,
                                f"streamed {written} > {limit}",
                                ct,
                            ),
                        )

        dest = data_dir(fmt) / f"{rid}{chosen_ext}"
        if expected is not None and written != expected:
            tmp.unlink(missing_ok=True)
            return dest, DlResult(
                "size_mismatch",
                written,
                None,
                r.status_code,
                f"expected {expected}, got {written}",
                ct,
            )
        tmp.replace(dest)
        return dest, DlResult(
            "ok", written, h.hexdigest(), r.status_code, None, ct
        )
    except requests.RequestException as exc:
        tmp.unlink(missing_ok=True)
        return (
            data_dir(fmt) / f"{rid}{fmt.file_ext}",
            DlResult(
                "network_error", 0, None, None, f"{type(exc).__name__}: {exc}"
            ),
        )


def load_existing_manifest(manifest_file: Path) -> dict[str, dict[str, Any]]:
    if not manifest_file.exists():
        return {}
    entries: dict[str, dict[str, Any]] = {}
    for line in manifest_file.read_text().splitlines():
        if not line.strip():
            continue
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        rid = e.get("resource_id")
        if rid:
            entries[rid] = e
    return entries


def write_manifest(manifest_file: Path, entries: list[dict[str, Any]]) -> None:
    tmp = manifest_file.with_suffix(".jsonl.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    tmp.replace(manifest_file)


def human(n: float | None) -> str:
    if n is None:
        return "?"
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def main() -> int:
    fmt = parse_format_arg()
    o = out_dir(fmt)
    in_file = o / "resources_probed.jsonl"
    manifest_file = o / "manifest.jsonl"

    resources = [
        json.loads(line) for line in in_file.read_text().splitlines() if line.strip()
    ]
    previous = load_existing_manifest(manifest_file)
    print(
        f"[{fmt.key}] input={len(resources)} resources, "
        f"previous manifest entries={len(previous)}"
    )
    if fmt.max_bytes:
        print(f"[{fmt.key}] max_bytes per resource: {fmt.max_bytes / 1e6:.0f} MB")

    session = _make_session()
    manifest: dict[str, dict[str, Any]] = dict(previous)
    counts: dict[str, int] = {}
    bytes_total = 0

    # Filter up-front the unreachable ones from probe stage
    to_do = []
    for res in resources:
        rid = res.get("resource_id")
        if not rid or not res.get("url"):
            continue
        probe = res.get("probe") or {}
        if probe.get("error") or (
            probe.get("status_code") and probe["status_code"] >= 400
        ):
            manifest[rid] = {
                **res,
                "local_path": None,
                "download_status": "skipped_unreachable",
                "downloaded_bytes": 0,
                "sha256": None,
                "http_status": probe.get("status_code"),
                "error": probe.get("error") or f"HTTP {probe.get('status_code')}",
                "downloaded_at": None,
            }
            continue
        to_do.append(res)

    print(f"[{fmt.key}] scheduling {len(to_do)} downloads (workers={WORKERS})")
    t0 = time.monotonic()

    def record(res: dict[str, Any], dest: Path, r: DlResult) -> None:
        nonlocal bytes_total
        rid = res["resource_id"]
        counts[r.status] = counts.get(r.status, 0) + 1
        if r.status in ("ok", "skipped"):
            bytes_total += r.downloaded_bytes
        manifest[rid] = {
            **res,
            "local_path": (
                str(dest.relative_to(Path(__file__).parent))
                if r.status in ("ok", "skipped")
                else None
            ),
            "download_status": r.status,
            "downloaded_bytes": r.downloaded_bytes,
            "sha256": r.sha256,
            "http_status": r.http_status,
            "error": r.error,
            "response_content_type": r.content_type,
            "downloaded_at": (
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                if r.status == "ok"
                else (
                    previous.get(rid, {}).get("downloaded_at")
                    if r.status == "skipped"
                    else None
                )
            ),
        }

    done = 0
    flush_every = 10
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(download_one, session, fmt, res): res for res in to_do}
        for fut in as_completed(futures):
            res = futures[fut]
            try:
                dest, result = fut.result()
            except Exception as exc:  # noqa: BLE001
                dest = target_path(fmt, res)
                result = DlResult(
                    "network_error", 0, None, None, f"{type(exc).__name__}: {exc}"
                )
            record(res, dest, result)
            done += 1
            if done % flush_every == 0:
                write_manifest(manifest_file, list(manifest.values()))
                err_keys = [
                    "http_error",
                    "network_error",
                    "size_mismatch",
                    "too_large",
                    "content_rejected",
                ]
                err = sum(counts.get(k, 0) for k in err_keys)
                print(
                    f"  {done}/{len(to_do)}  ok={counts.get('ok', 0)} "
                    f"skipped={counts.get('skipped', 0)} err={err} "
                    f"total={human(bytes_total)}",
                    flush=True,
                )

    write_manifest(manifest_file, list(manifest.values()))

    elapsed = time.monotonic() - t0
    print(f"\n=== [{fmt.key}] Download summary ===")
    for k in sorted(counts):
        print(f"  {k:20s} {counts[k]}")
    print(f"  bytes on disk         {human(bytes_total)}")
    print(f"  elapsed               {elapsed:.1f} s")
    print(f"\nManifest: {manifest_file}")
    print(f"Data dir: {data_dir(fmt)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
