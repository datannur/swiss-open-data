"""
Maintain ./staging/excluded_datasets.csv (id,reason).

Scans datannurpy logs and adds any dataset with 0 extracted variables as
`reason=non-tabular`. The CSV is the source of truth: existing rows are
preserved (manual edits / additional reasons survive).

Usage:
    uv run python src/mark_excluded.py [datannurpy*.log ...]
    # default: all logs matching ./staging/logs/datannurpy*.log
    # and legacy locations ./datannurpy*.log, ./staging/datannurpy*.log
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STAGING_DIR = ROOT / "staging"
LOGS_DIR = STAGING_DIR / "logs"
EXCLUDED_CSV = STAGING_DIR / "excluded_datasets.csv"

# Match: "  ✓ <uuid>.<ext> (0 vars) in <time>"
LINE_RE = re.compile(r"^\s*✓\s+(?P<id>[0-9a-f-]{36})\.[A-Za-z0-9]+\s+\(0 vars\)")


def load_existing() -> dict[str, str]:
    if not EXCLUDED_CSV.exists():
        return {}
    out: dict[str, str] = {}
    with EXCLUDED_CSV.open(newline="") as f:
        for row in csv.DictReader(f):
            rid = (row.get("id") or "").strip()
            if rid:
                out[rid] = (row.get("reason") or "").strip()
    return out


def write(rows: dict[str, str]) -> None:
    with EXCLUDED_CSV.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "reason"])
        for rid in sorted(rows):
            w.writerow([rid, rows[rid]])


def scan_log(path: Path) -> list[str]:
    ids = []
    for line in path.read_text(errors="replace").splitlines():
        m = LINE_RE.match(line)
        if m:
            ids.append(m.group("id"))
    return ids


def main(argv: list[str]) -> int:
    if argv:
        logs = [Path(a) for a in argv]
    else:
        logs = sorted(
            {
                *LOGS_DIR.glob("datannurpy*.log"),
                *ROOT.glob("datannurpy*.log"),
                *STAGING_DIR.glob("datannurpy*.log"),
            }
        )

    if not logs:
        print("No log files to scan.", file=sys.stderr)
        return 1

    existing = load_existing()
    added = 0

    for log in logs:
        if not log.exists():
            print(f"  skip (missing): {log}")
            continue
        ids = scan_log(log)
        new_in_log = 0
        for rid in ids:
            if rid not in existing:
                existing[rid] = "non-tabular"
                added += 1
                new_in_log += 1
        print(f"  {log.name}: {len(ids)} 0-var rows, +{new_in_log} new")

    write(existing)
    print(f"\n{EXCLUDED_CSV.name}: {len(existing)} ids total (+{added} added)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
