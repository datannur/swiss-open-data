from __future__ import annotations

import time
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USER_AGENT = "datannur-opench-crawler/0.1 (+https://github.com/datannur)"
TIMEOUT = (15, 120)
WORKERS = 6
CHUNK = 1 << 20  # 1 MiB
MAGIC_SNIFF_BYTES = 16


@dataclass
class DlResult:
    status: str
    downloaded_bytes: int
    sha256: str | None
    http_status: int | None
    error: str | None
    content_type: str | None = None


def make_session() -> requests.Session:
    session = requests.Session()
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
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "identity",
            "Connection": "close",
        }
    )
    return session


def human(n: float | None) -> str:
    if n is None:
        return "?"
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
