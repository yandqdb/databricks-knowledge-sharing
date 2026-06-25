#!/usr/bin/env python3
"""Detect ANSI-mode-affected constructs in a Synapse-origin Spark notebook.

Source notebooks ran on Synapse Spark with `spark.sql.ansi.enabled=false`,
where an invalid cast silently resolves to NULL. Target DBR runs ANSI on by
default, so the same construct raises at runtime. This script flags the sites
that change behavior; it does NOT edit anything (see apply_remediation.py).

Usage:
    python detect_ansi_sites.py NOTEBOOK [NOTEBOOK ...]
    python detect_ansi_sites.py NOTEBOOK --json report.json

Output: a human-readable table on stdout, and (with --json) a machine-readable
report consumed by apply_remediation.py and by the post-conversion validation
skill. Exit code is 0 always; the count is the signal, not the status.

Confidence levels:
    high   - the construct definitely changes behavior under ANSI on
    medium - usually changes behavior; review the surrounding types
    low    - heuristic; may be a false positive, always eyeball it

See ../references/ansi-constructs.md for what each construct does and why.
"""
from __future__ import annotations

import argparse
import json
import re
import sys

from nb_io import load_cells, logical

# Ordered most-specific first. Each entry: (construct, confidence, regex).
# Patterns run against the *logical* content of a line (magic prefix stripped).
PATTERNS = [
    # Explicit cast. `\bcast` will not match `try_cast` (preceded by `_`, a word
    # char, so no boundary) — already-remediated sites are correctly skipped.
    ("EXPLICIT_CAST", "high", re.compile(r"(?<![\w])cast\s*\(", re.I)),
    # Date/timestamp parsing: a malformed string raises instead of -> NULL.
    ("DATE_PARSE", "high",
     re.compile(r"\b(to_date|to_timestamp|to_unix_timestamp|unix_timestamp)\s*\(", re.I)),
    # Integer division operator; ANSI changes divide-by-zero to an error.
    ("INT_DIVISION", "medium", re.compile(r"\bdiv\b", re.I)),
    # Insert into typed columns: out-of-range / bad-type values now raise.
    ("INSERT_TYPED", "medium", re.compile(r"\bINSERT\s+(INTO|OVERWRITE)\b", re.I)),
    # Implicit numeric coercion: a quoted number in arithmetic. Heuristic.
    ("IMPLICIT_NUMERIC", "low",
     re.compile(r"""['"]\d+(?:\.\d+)?['"]\s*[-+*/]|[-+*/]\s*['"]\d+(?:\.\d+)?['"]""")),
]

# Lines that are pure comments in their logical form are skipped to cut noise.
_PY_COMMENT = re.compile(r"^\s*#")
_SQL_COMMENT = re.compile(r"^\s*--")


def _is_comment(content: str, language: str) -> bool:
    if language == "sql":
        return bool(_SQL_COMMENT.match(content))
    return bool(_PY_COMMENT.match(content))


def scan_notebook(path: str) -> list[dict]:
    findings = []
    for cell in load_cells(path):
        if cell["language"] == "md":
            continue
        for li, raw_line in enumerate(cell["lines"]):
            content = logical(raw_line)
            stripped = content.strip()
            if not stripped or stripped.startswith("%"):
                continue
            if _is_comment(content, cell["language"]):
                continue
            for construct, confidence, pat in PATTERNS:
                for m in pat.finditer(content):
                    findings.append({
                        "file": path,
                        "cell_index": cell["index"],
                        "language": cell["language"],
                        "line_in_cell": li,
                        "column": m.start(),
                        "construct": construct,
                        "confidence": confidence,
                        "snippet": stripped[:160],
                    })
    return findings


def _print_table(findings: list[dict]) -> None:
    if not findings:
        print("No ANSI-affected constructs detected.")
        return
    by_construct: dict[str, int] = {}
    for f in findings:
        by_construct[f["construct"]] = by_construct.get(f["construct"], 0) + 1
    print(f"{len(findings)} site(s) found:\n")
    print(f"{'CELL':>4}  {'LINE':>4}  {'CONF':<6}  {'CONSTRUCT':<17}  SNIPPET")
    print("-" * 100)
    for f in findings:
        print(f"{f['cell_index']:>4}  {f['line_in_cell']:>4}  {f['confidence']:<6}  "
              f"{f['construct']:<17}  {f['snippet']}")
    print("\nby construct: " + ", ".join(f"{k}={v}" for k, v in sorted(by_construct.items())))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("notebooks", nargs="+", help="notebook file(s): .py, .sql, or .ipynb")
    ap.add_argument("--json", metavar="PATH", help="write the full report as JSON")
    args = ap.parse_args(argv)

    all_findings = []
    for nb in args.notebooks:
        f = scan_notebook(nb)
        all_findings.extend(f)
        if len(args.notebooks) > 1:
            print(f"\n=== {nb} ===")
        _print_table(f)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(all_findings, fh, indent=2)
        print(f"\nReport written to {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
