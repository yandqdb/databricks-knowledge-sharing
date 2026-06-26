#!/usr/bin/env python3
"""Change coverage: of every migration-needed site in the ORIGINAL notebook,
what fraction did the migration skills actually resolve in the MIGRATED copy?

Two categories of required site, both found on the ORIGINAL:
  * ansi      - cast / parse / integer-divide / typed-insert / implicit-coercion
                sites, detected with the sibling ANSI detector's `scan_notebook`.
  * metastore - every `hive_metastore.<db>.<table>` reference (qualified,
                back-quoted, and `USE hive_metastore` forms), detected by a regex
                pass over the parsed cells with the detector's logical-line /
                comment / markdown handling.

A site is resolved when it no longer appears in the MIGRATED notebook (rewritten
to try_cast, remapped to the UC name, or — for ansi — covered by a notebook-scope
`spark.sql.ansi.enabled=false` flag cell, which resolves ALL ansi sites at once).

    change_coverage = resolved / required        (1.0 when required == 0)

Stdlib only. The ANSI detector is reused by adding the sibling skill's scripts
dir to sys.path and importing its `scan_notebook` directly. This is preferred
over `subprocess detect_ansi_sites.py --json` here because it avoids a process
launch and JSON round-trip, and the bundled `nb_io.py` is a verbatim copy of the
detector's, so there is no module-collision risk when both import `nb_io`.

Usage:
    python change_coverage.py --original ORIG --migrated MIGRATED [--json PATH]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter

from nb_io import load_cells, logical

# Reuse the ANSI detector's scanner from the sibling skill (see module docstring).
_ANSI_SCRIPTS = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)),
                 "..", "..", "databricks-ansi-remediation", "scripts"))
if _ANSI_SCRIPTS not in sys.path:
    sys.path.insert(0, _ANSI_SCRIPTS)
try:
    from detect_ansi_sites import scan_notebook as _scan_ansi
except Exception as exc:  # pragma: no cover - environment-specific
    _scan_ansi = None
    _ANSI_IMPORT_ERR = exc


# ---- metastore detection ---------------------------------------------------

# Qualified (plain or quoted) and back-quoted three-part names. Backticks around
# each part are optional, so this matches both `hive_metastore.db.tbl` and
# `` `hive_metastore`.`db`.`tbl` ``.
_META_QUALIFIED = re.compile(
    r"(?<![\w.`])`?hive_metastore`?\s*\.\s*`?[a-z_]\w*`?\s*\.\s*`?[a-z_]\w*`?",
    re.I)
# `USE hive_metastore` (optionally back-quoted) switches the default catalog.
_META_USE = re.compile(r"(?<![\w.`])USE\s+`?hive_metastore`?\b", re.I)
_META_PATTERNS = [_META_QUALIFIED, _META_USE]

# Same logical-line comment handling as the ANSI detector.
_PY_COMMENT = re.compile(r"^\s*#")
_SQL_COMMENT = re.compile(r"^\s*--")

# A notebook-scope ANSI-off flag cell resolves every ansi site. Matches the
# PySpark form `spark.conf.set("spark.sql.ansi.enabled", "false")` and the SQL
# form `SET spark.sql.ansi.enabled = false`.
_ANSI_FLAG = re.compile(r"spark\.sql\.ansi\.enabled", re.I)
_FALSE = re.compile(r"\bfalse\b", re.I)


def _is_comment(content: str, language: str) -> bool:
    if language == "sql":
        return bool(_SQL_COMMENT.match(content))
    return bool(_PY_COMMENT.match(content))


def scan_metastore(path: str) -> list:
    """Every hive_metastore reference in runnable (non-md, non-comment) lines."""
    sites = []
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
            for pat in _META_PATTERNS:
                for m in pat.finditer(content):
                    sites.append({
                        "category": "metastore",
                        "cell_index": cell["index"],
                        "line_in_cell": li,
                        "snippet": stripped[:160],
                        "text": m.group(0),
                    })
    return sites


def has_ansi_flag(path: str) -> bool:
    for cell in load_cells(path):
        if cell["language"] == "md":
            continue
        for raw_line in cell["lines"]:
            content = logical(raw_line)
            stripped = content.strip()
            if not stripped or stripped.startswith("%"):
                continue
            if _is_comment(content, cell["language"]):
                continue
            if _ANSI_FLAG.search(content) and _FALSE.search(content):
                return True
    return False


def scan_ansi(path: str) -> list:
    if _scan_ansi is None:  # pragma: no cover - environment-specific
        raise RuntimeError(
            f"could not import the ANSI detector from {_ANSI_SCRIPTS}: "
            f"{_ANSI_IMPORT_ERR}")
    sites = []
    for f in _scan_ansi(path):
        sites.append({
            "category": "ansi",
            "cell_index": f["cell_index"],
            "line_in_cell": f["line_in_cell"],
            "snippet": f["snippet"],
        })
    return sites


# ---- metastore resolution --------------------------------------------------

def _norm(text: str) -> str:
    """Normalize a hive reference for text comparison: drop backticks and
    whitespace, lowercase. So `` `hive_metastore`.`db`.`t` `` == hive_metastore.db.t."""
    return re.sub(r"[`\s]", "", text).lower()


def _metastore_residual(required_sites: list, migrated: str) -> list:
    """The ORIGINAL required metastore sites still unresolved in the migrated copy.

    Per metrics-and-method.md, a required metastore site is resolved when its
    specific `hive_metastore...` text no longer appears in the migrated notebook.
    Residuals are matched by that text and capped, per distinct text, at the
    number originally required. So residual <= required always, and a different
    or newly-introduced hive_metastore reference in the migrated copy can never
    push residual above required or be mis-counted as one of the required sites.
    Each residual is reported at its migrated location so the engineer can see
    exactly what to hand-fix.
    """
    req_counts = Counter(_norm(s["text"]) for s in required_sites)
    residual, used = [], {}
    for s in scan_metastore(migrated):
        key = _norm(s["text"])
        cap = req_counts.get(key, 0)
        if cap and used.get(key, 0) < cap:
            residual.append(s)
            used[key] = used.get(key, 0) + 1
    return residual


# ---- coverage --------------------------------------------------------------

def _cov(resolved: int, required: int) -> float:
    return 1.0 if required == 0 else round(resolved / required, 4)


def compute(original: str, migrated: str) -> dict:
    # ansi
    ansi_required = scan_ansi(original)
    if has_ansi_flag(migrated):
        ansi_residual = []  # flag cell resolves all ansi sites at once
    else:
        ansi_residual = scan_ansi(migrated)
    ansi_req, ansi_res = len(ansi_required), len(ansi_residual)
    ansi_resolved = max(0, ansi_req - ansi_res)

    # metastore: residuals are ORIGINAL required sites still present in the
    # migrated notebook, matched by their specific hive_metastore text and capped
    # per text at the required count (see _metastore_residual).
    meta_required = scan_metastore(original)
    meta_residual = _metastore_residual(meta_required, migrated)
    meta_req, meta_res = len(meta_required), len(meta_residual)
    meta_resolved = meta_req - meta_res

    residual_sites = ansi_residual + meta_residual
    # Drop the internal `text` key from the emitted residual list.
    residual_sites = [{k: s[k] for k in
                       ("category", "cell_index", "line_in_cell", "snippet")}
                      for s in residual_sites]

    overall_req = ansi_req + meta_req
    overall_res = ansi_res + meta_res
    overall_resolved = max(0, overall_req - overall_res)

    return {
        "original": original,
        "migrated": migrated,
        "overall": {
            "required": overall_req,
            "resolved": overall_resolved,
            "residual": overall_res,
            "coverage": _cov(overall_resolved, overall_req),
        },
        "by_category": {
            "ansi": {
                "required": ansi_req, "resolved": ansi_resolved,
                "residual": ansi_res, "coverage": _cov(ansi_resolved, ansi_req),
            },
            "metastore": {
                "required": meta_req, "resolved": meta_resolved,
                "residual": meta_res, "coverage": _cov(meta_resolved, meta_req),
            },
        },
        "residual_sites": residual_sites,
    }


def _print_table(rep: dict) -> None:
    o = rep["overall"]
    print(f"change coverage: {o['resolved']}/{o['required']} resolved "
          f"= {o['coverage'] * 100:.1f}%")
    print(f"\n{'CATEGORY':<10}  {'REQUIRED':>8}  {'RESOLVED':>8}  "
          f"{'RESIDUAL':>8}  {'COVERAGE':>8}")
    print("-" * 52)
    for cat in ("ansi", "metastore"):
        c = rep["by_category"][cat]
        print(f"{cat:<10}  {c['required']:>8}  {c['resolved']:>8}  "
              f"{c['residual']:>8}  {c['coverage'] * 100:>7.1f}%")
    print(f"{'overall':<10}  {o['required']:>8}  {o['resolved']:>8}  "
          f"{o['residual']:>8}  {o['coverage'] * 100:>7.1f}%")
    if rep["residual_sites"]:
        print(f"\n{len(rep['residual_sites'])} residual site(s) to hand-fix:")
        print(f"{'CAT':<10}  {'CELL':>4}  {'LINE':>4}  SNIPPET")
        print("-" * 80)
        for s in rep["residual_sites"]:
            print(f"{s['category']:<10}  {s['cell_index']:>4}  "
                  f"{s['line_in_cell']:>4}  {s['snippet']}")
    else:
        print("\nNo residual sites: every required edit was resolved.")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--original", required=True, help="pre-migration notebook")
    ap.add_argument("--migrated", required=True, help="post-migration notebook")
    ap.add_argument("--json", metavar="PATH", help="write the report as JSON")
    args = ap.parse_args(argv)

    for label, p in (("original", args.original), ("migrated", args.migrated)):
        if not os.path.isfile(os.path.expanduser(p)):
            print(f"error: {label} notebook not found: {p}", file=sys.stderr)
            return 2

    rep = compute(os.path.expanduser(args.original),
                  os.path.expanduser(args.migrated))
    # Preserve the user-supplied paths in the report for readability.
    rep["original"], rep["migrated"] = args.original, args.migrated
    _print_table(rep)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(rep, fh, indent=2)
        print(f"\nReport written to {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
