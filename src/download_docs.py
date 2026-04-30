"""Download staged PDF documents into a dedicated local cache.

Usage:
    uv run python src/download_docs.py
"""

from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests

from config import (
    doc_cache_dir,
    doc_manifest_path,
    exported_doc_rel_path,
    parse_noop_args,
    staging_dir,
)
from doc_utils import extract_pdf_urls, pdf_doc_id
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

ROOT = Path(__file__).resolve().parent.parent

DOC_DOWNLOAD_STATE_KEYS = (
    "doc_id",
    "source_url",
    "local_path",
    "export_path",
    "download_status",
    "downloaded_bytes",
    "sha256",
    "http_status",
    "error",
    "response_content_type",
    "etag",
    "last_modified",
    "downloaded_at",
)


def doc_download_state_entry(
    *,
    doc_id: str,
    source_url: str,
    local_path: str | None,
    export_path: str,
    download_status: str,
    downloaded_bytes: int,
    sha256: str | None,
    http_status: int | None,
    error: str | None,
    response_content_type: str | None,
    etag: str | None,
    last_modified: str | None,
    downloaded_at: str | None,
) -> dict[str, Any]:
    return {
        "doc_id": doc_id,
        "source_url": source_url,
        "local_path": local_path,
        "export_path": export_path,
        "download_status": download_status,
        "downloaded_bytes": downloaded_bytes,
        "sha256": sha256,
        "http_status": http_status,
        "error": error,
        "response_content_type": response_content_type,
        "etag": etag,
        "last_modified": last_modified,
        "downloaded_at": downloaded_at,
    }


def normalize_doc_download_state_entry(entry: dict[str, Any]) -> dict[str, Any]:
    normalized = {key: entry.get(key) for key in DOC_DOWNLOAD_STATE_KEYS}
    local_path = normalized.get("local_path")
    if local_path and not (ROOT / local_path).exists():
        normalized["local_path"] = None
        if not normalized.get("error"):
            normalized["error"] = "missing local file"
    return normalized


def doc_target_path(doc_id: str) -> Path:
    return doc_cache_dir() / f"{doc_id}.pdf"


def _doc_magic_ok(head: bytes) -> tuple[bool, str | None]:
    if head.startswith(b"%PDF"):
        return True, None
    return False, f"unexpected magic bytes: {head[:8]!r}"


def load_existing_doc_manifest(manifest_file: Path) -> dict[str, dict[str, Any]]:
    if not manifest_file.exists():
        return {}
    entries: dict[str, dict[str, Any]] = {}
    for line in manifest_file.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        source_url = entry.get("source_url")
        if source_url:
            entries[source_url] = normalize_doc_download_state_entry(entry)
    return entries


def write_doc_manifest(manifest_file: Path, entries: list[dict[str, Any]]) -> None:
    tmp = manifest_file.with_suffix(".jsonl.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for entry in entries:
            f.write(
                json.dumps(normalize_doc_download_state_entry(entry), ensure_ascii=False)
                + "\n"
            )
    tmp.replace(manifest_file)


def iter_staged_doc_candidates(staging: Path) -> list[dict[str, str]]:
    candidates: dict[str, dict[str, str]] = {}
    for name in ("packages.jsonl", "resources.jsonl"):
        fp = staging / name
        if not fp.exists():
            continue
        for line in fp.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            for url in extract_pdf_urls(
                payload.get("url"),
                payload.get("description"),
                payload.get("documentation_urls"),
                payload.get("relation_urls"),
            ):
                doc_id = pdf_doc_id(url)
                candidates.setdefault(
                    url,
                    {
                        "doc_id": doc_id,
                        "source_url": url,
                        "export_path": str(exported_doc_rel_path(doc_id)),
                    },
                )
    return [candidates[url] for url in sorted(candidates)]


def download_one_doc(
    session: requests.Session,
    source_url: str,
    doc_id: str,
) -> tuple[Path, DlResult, str | None, str | None]:
    dest = doc_target_path(doc_id)
    if dest.exists() and dest.stat().st_size > 0:
        return (
            dest,
            DlResult("skipped", dest.stat().st_size, None, None, None),
            None,
            None,
        )

    tmp = dest.with_suffix(".part")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    try:
        with session.get(
            source_url,
            stream=True,
            timeout=TIMEOUT,
            allow_redirects=True,
        ) as response:
            content_type = response.headers.get("Content-Type")
            etag = response.headers.get("ETag")
            last_modified = response.headers.get("Last-Modified")
            if response.status_code >= 400:
                return (
                    dest,
                    DlResult(
                        "http_error",
                        0,
                        None,
                        response.status_code,
                        f"HTTP {response.status_code}",
                        content_type,
                    ),
                    etag,
                    last_modified,
                )

            hasher = hashlib.sha256()
            written = 0
            magic_checked = False
            with tmp.open("wb") as fh:
                for chunk in response.iter_content(CHUNK):
                    if not chunk:
                        continue
                    if not magic_checked and len(chunk) >= MAGIC_SNIFF_BYTES:
                        ok, why = _doc_magic_ok(chunk[:MAGIC_SNIFF_BYTES])
                        magic_checked = True
                        if not ok:
                            tmp.unlink(missing_ok=True)
                            return (
                                dest,
                                DlResult(
                                    "content_rejected",
                                    0,
                                    None,
                                    response.status_code,
                                    why,
                                    content_type,
                                ),
                                etag,
                                last_modified,
                            )
                    fh.write(chunk)
                    hasher.update(chunk)
                    written += len(chunk)

            if written == 0:
                tmp.unlink(missing_ok=True)
                return (
                    dest,
                    DlResult(
                        "content_rejected",
                        0,
                        None,
                        response.status_code,
                        "empty response body",
                        content_type,
                    ),
                    etag,
                    last_modified,
                )

            tmp.replace(dest)
            return (
                dest,
                DlResult(
                    "ok",
                    written,
                    hasher.hexdigest(),
                    response.status_code,
                    None,
                    content_type,
                ),
                etag,
                last_modified,
            )
    except requests.RequestException as exc:
        tmp.unlink(missing_ok=True)
        return (
            dest,
            DlResult(
                "network_error",
                0,
                None,
                None,
                f"{type(exc).__name__}: {exc}",
            ),
            None,
            None,
        )


def process_docs(session: requests.Session, staging: Path) -> None:
    manifest_file = doc_manifest_path()
    previous = load_existing_doc_manifest(manifest_file)
    candidates = iter_staged_doc_candidates(staging)
    print(
        f"[docs] input={len(candidates)} pdf urls, previous manifest entries={len(previous)}"
    )
    manifest: dict[str, dict[str, Any]] = dict(previous)
    counts: dict[str, int] = {}
    bytes_total = 0

    def record(
        candidate: dict[str, str],
        dest: Path,
        result: DlResult,
        etag: str | None,
        last_modified: str | None,
    ) -> None:
        nonlocal bytes_total
        source_url = candidate["source_url"]
        counts[result.status] = counts.get(result.status, 0) + 1
        if result.status in {"ok", "skipped"}:
            bytes_total += result.downloaded_bytes
        previous_entry = previous.get(source_url, {})
        manifest[source_url] = doc_download_state_entry(
            doc_id=candidate["doc_id"],
            source_url=source_url,
            local_path=(
                str(dest.relative_to(ROOT))
                if result.status in {"ok", "skipped"}
                else None
            ),
            export_path=candidate["export_path"],
            download_status=result.status,
            downloaded_bytes=result.downloaded_bytes,
            sha256=result.sha256,
            http_status=result.http_status,
            error=result.error,
            response_content_type=result.content_type,
            etag=etag if result.status == "ok" else previous_entry.get("etag"),
            last_modified=(
                last_modified
                if result.status == "ok"
                else previous_entry.get("last_modified")
            ),
            downloaded_at=(
                utc_now() if result.status == "ok" else previous_entry.get("downloaded_at")
            ),
        )

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {
            pool.submit(
                download_one_doc,
                session,
                candidate["source_url"],
                candidate["doc_id"],
            ): candidate
            for candidate in candidates
        }
        done = 0
        for future in as_completed(futures):
            candidate = futures[future]
            try:
                dest, result, etag, last_modified = future.result()
            except Exception as exc:  # noqa: BLE001
                dest = doc_target_path(candidate["doc_id"])
                result = DlResult(
                    "network_error",
                    0,
                    None,
                    None,
                    f"{type(exc).__name__}: {exc}",
                )
                etag = None
                last_modified = None
            record(candidate, dest, result, etag, last_modified)
            done += 1
            if done % 10 == 0:
                write_doc_manifest(manifest_file, list(manifest.values()))
                err = sum(
                    counts.get(key, 0)
                    for key in ("http_error", "network_error", "content_rejected")
                )
                print(
                    f"  {done}/{len(candidates)}  ok={counts.get('ok', 0)} skipped={counts.get('skipped', 0)} err={err} total={human(bytes_total)}",
                    flush=True,
                )

    write_doc_manifest(manifest_file, list(manifest.values()))
    print("\n=== [docs] Download summary ===")
    for key in sorted(counts):
        print(f"  {key:20s} {counts[key]}")
    print(f"  bytes on disk         {human(bytes_total)}")
    print(f"\nManifest: {manifest_file}")
    print(f"Cache dir: {doc_cache_dir()}")


def main() -> int:
    parse_noop_args("Download staged PDF docs.")
    session = make_session()
    process_docs(session, staging_dir())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())