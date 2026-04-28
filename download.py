"""
Download resources referenced in staging/resources.jsonl into
./data/<resource_id><ext> and maintain staging/download_state.jsonl linking
every local file back to its CKAN metadata.

Features:
        - Idempotent: skip files already present with expected size.
        - Size cap per format (e.g. CSV <= 100 MB). Uses GET headers when present,
            otherwise aborts mid-stream when the size becomes too large.
    - Content validation: Content-Type check + magic-byte check against a
      per-format accept/reject list (rejects ZIP/HTML/PDF for CSV, etc.).
    - Safe to interrupt: manifest is rewritten atomically after each batch.

Usage:
    uv run python download.py
"""

from __future__ import annotations

import csv
import hashlib
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests

from config import FormatConfig, data_dir, iter_formats, parse_noop_args, staging_dir
from download_common import (
    CHUNK,
    MAGIC_SNIFF_BYTES,
    TIMEOUT,
    WORKERS,
    DlResult,
    human,
    make_session,
    utc_now,
)

EXCLUDED_CSV = Path(__file__).parent / "excluded_datasets.csv"

DOWNLOAD_STATE_KEYS = (
    "resource_id",
    "format_key",
    "local_path",
    "download_status",
    "downloaded_bytes",
    "sha256",
    "http_status",
    "error",
    "response_content_type",
    "downloaded_at",
)

FORMAT_BY_KEY = {fmt.key: fmt for fmt in iter_formats()}


def download_state_entry(
    res: dict[str, Any],
    *,
    local_path: str | None,
    download_status: str,
    downloaded_bytes: int,
    sha256: str | None,
    http_status: int | None,
    error: str | None,
    response_content_type: str | None,
    downloaded_at: str | None,
) -> dict[str, Any]:
    return {
        "resource_id": res.get("resource_id"),
        "format_key": res.get("format_key"),
        "local_path": local_path,
        "download_status": download_status,
        "downloaded_bytes": downloaded_bytes,
        "sha256": sha256,
        "http_status": http_status,
        "error": error,
        "response_content_type": response_content_type,
        "downloaded_at": downloaded_at,
    }


def normalize_download_state_entry(entry: dict[str, Any]) -> dict[str, Any]:
    normalized = {key: entry.get(key) for key in DOWNLOAD_STATE_KEYS}
    normalized["local_path"] = normalize_local_path(
        normalized.get("format_key"),
        normalized.get("local_path"),
    )
    local_path = normalized.get("local_path")
    if local_path and not (Path(__file__).parent / local_path).exists():
        normalized["local_path"] = None
        if not normalized.get("error"):
            normalized["error"] = "missing local file"
    return normalized


def load_excluded_ids() -> set[str]:
    """Read ids flagged in excluded_datasets.csv (skipped before download)."""
    if not EXCLUDED_CSV.exists():
        return set()
    ids: set[str] = set()
    with EXCLUDED_CSV.open(newline="") as f:
        for row in csv.DictReader(f):
            rid = (row.get("id") or "").strip()
            if rid:
                ids.add(rid)
    return ids


def target_path(fmt: FormatConfig, res: dict[str, Any]) -> Path:
    rid = res.get("resource_id") or ""
    if not rid:
        raise ValueError("resource has no resource_id")
    return data_dir(fmt) / f"{rid}{fmt.file_ext}"


def legacy_data_dir(fmt: FormatConfig) -> Path:
    return Path(__file__).parent / "data" / fmt.key


def migrate_legacy_file(fmt: FormatConfig, path: Path) -> Path:
    target = data_dir(fmt) / path.name
    if path == target:
        return path
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        path.unlink(missing_ok=True)
        return target
    path.replace(target)
    legacy_dir = path.parent
    if legacy_dir != target.parent:
        try:
            legacy_dir.rmdir()
        except OSError:
            pass
    return target


def normalize_local_path(format_key: str | None, local_path: str | None) -> str | None:
    if not local_path or not format_key:
        return local_path
    fmt = FORMAT_BY_KEY.get(format_key)
    if fmt is None:
        return local_path

    path = Path(local_path)
    flat_path = Path("data") / path.name
    if path == flat_path:
        return local_path

    legacy_path = legacy_data_dir(fmt) / path.name
    if path.parts[:2] == ("data", fmt.key):
        if legacy_path.exists():
            return str(migrate_legacy_file(fmt, legacy_path).relative_to(Path(__file__).parent))
        if (data_dir(fmt) / path.name).exists():
            return str(flat_path)

    return local_path


def existing_file(fmt: FormatConfig, res: dict[str, Any]) -> Path | None:
    """Return a pre-existing download for this resource, whatever its extension."""
    rid = res["resource_id"]
    dd = data_dir(fmt)
    legacy_dd = legacy_data_dir(fmt)
    exts = {fmt.file_ext, *(e for _, e in fmt.ext_from_magic)}
    for ext in exts:
        p = dd / f"{rid}{ext}"
        if p.exists():
            return p
        legacy = legacy_dd / f"{rid}{ext}"
        if legacy.exists():
            return migrate_legacy_file(fmt, legacy)
    return None


def ext_for_payload(fmt: FormatConfig, head: bytes) -> str:
    for magic, ext in fmt.ext_from_magic:
        if head.startswith(magic):
            return ext
    return fmt.file_ext


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
    expected = None

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
        with session.get(url, stream=True, timeout=TIMEOUT, allow_redirects=True) as r:
            ct = r.headers.get("Content-Type")
            content_length = r.headers.get("Content-Length")
            if content_length and content_length.isdigit():
                expected = int(content_length)
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

            if fmt.max_bytes and expected is not None and expected > fmt.max_bytes:
                return (
                    data_dir(fmt) / f"{rid}{fmt.file_ext}",
                    DlResult(
                        "too_large",
                        0,
                        None,
                        r.status_code,
                        f"declared size {expected} > {fmt.max_bytes}",
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
        return dest, DlResult("ok", written, h.hexdigest(), r.status_code, None, ct)
    except requests.RequestException as exc:
        tmp.unlink(missing_ok=True)
        return (
            data_dir(fmt) / f"{rid}{fmt.file_ext}",
            DlResult("network_error", 0, None, None, f"{type(exc).__name__}: {exc}"),
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
            entries[rid] = normalize_download_state_entry(e)
    return entries


def write_manifest(manifest_file: Path, entries: list[dict[str, Any]]) -> None:
    tmp = manifest_file.with_suffix(".jsonl.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for e in entries:
            f.write(
                json.dumps(normalize_download_state_entry(e), ensure_ascii=False)
                + "\n"
            )
    tmp.replace(manifest_file)


def process_format(
    session: requests.Session,
    fmt: FormatConfig,
    resources: list[dict[str, Any]],
    manifest_file: Path,
    all_manifest: dict[str, dict[str, Any]],
    excluded_ids: set[str],
) -> None:
    previous = {
        rid: entry
        for rid, entry in all_manifest.items()
        if entry.get("format_key") == fmt.key
    }
    print(
        f"[{fmt.key}] input={len(resources)} resources, "
        f"previous manifest entries={len(previous)}"
    )
    if fmt.max_bytes:
        print(f"[{fmt.key}] max_bytes per resource: {fmt.max_bytes / 1e6:.0f} MB")
    if excluded_ids:
        print(f"[{fmt.key}] excluded_datasets.csv: {len(excluded_ids)} ids")

    manifest: dict[str, dict[str, Any]] = dict(previous)
    counts: dict[str, int] = {}
    bytes_total = 0

    to_do = []
    for res in resources:
        rid = res.get("resource_id")
        if not rid or not res.get("url"):
            continue
        if rid in excluded_ids:
            manifest[rid] = download_state_entry(
                res,
                local_path=None,
                download_status="skipped_excluded",
                downloaded_bytes=0,
                sha256=None,
                http_status=None,
                error="id in excluded_datasets.csv",
                response_content_type=None,
                downloaded_at=None,
            )
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
        manifest[rid] = download_state_entry(
            res,
            local_path=(
                str(dest.relative_to(Path(__file__).parent))
                if r.status in ("ok", "skipped")
                else None
            ),
            download_status=r.status,
            downloaded_bytes=r.downloaded_bytes,
            sha256=r.sha256,
            http_status=r.http_status,
            error=r.error,
            response_content_type=r.content_type,
            downloaded_at=(
                utc_now()
                if r.status == "ok"
                else (
                    previous.get(rid, {}).get("downloaded_at")
                    if r.status == "skipped"
                    else None
                )
            ),
        )

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
                all_manifest.update(manifest)
                write_manifest(manifest_file, list(all_manifest.values()))
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

    all_manifest.update(manifest)
    write_manifest(manifest_file, list(all_manifest.values()))

    elapsed = time.monotonic() - t0
    print(f"\n=== [{fmt.key}] Download summary ===")
    for k in sorted(counts):
        print(f"  {k:20s} {counts[k]}")
    print(f"  bytes on disk         {human(bytes_total)}")
    print(f"  elapsed               {elapsed:.1f} s")
    print(f"\nManifest: {manifest_file}")
    print(f"Data dir: {data_dir(fmt)}")


def main() -> int:
    parse_noop_args("Download all staged resources.")
    o = staging_dir()
    in_file = o / "resources.jsonl"
    manifest_file = o / "download_state.jsonl"

    resources_by_format: dict[str, list[dict[str, Any]]] = {
        fmt.key: [] for fmt in iter_formats()
    }
    for line in in_file.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        resource = json.loads(line)
        format_key = resource.get("format_key")
        if format_key not in resources_by_format:
            continue
        resources_by_format[format_key].append(resource)

    session = make_session()
    all_manifest = load_existing_manifest(manifest_file)
    excluded_ids = load_excluded_ids()
    for fmt in iter_formats():
        process_format(
            session,
            fmt,
            resources_by_format[fmt.key],
            manifest_file,
            all_manifest,
            excluded_ids,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
