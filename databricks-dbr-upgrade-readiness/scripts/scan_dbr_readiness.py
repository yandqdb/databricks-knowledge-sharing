#!/usr/bin/env python3
"""Static DBR-upgrade readiness scanner.

Walks a notebook / source tree and flags patterns that break when a Databricks
workload moves from an older DBR to a newer one. Findings are filtered to the
specific source -> target jump: a change introduced at or before the source
version is already absorbed and is not reported.

This is a FIRST PASS. Every hit must be confirmed by reading the surrounding
code — many matches are benign (an ISO-8601 date format, a numpy alias already
removed, a cast that is already safe). See references/behavior-changes.md for
the authoritative catalog and the fix/bridge for each pattern ID.

Usage:
    python scan_dbr_readiness.py --source-dbr 10.4 --target-dbr 15.4 PATH [PATH ...]
    python scan_dbr_readiness.py --source-dbr 12.2 --target-dbr 16.4 --json report.json repo/

Exit code is 0 always; the JSON/stdout report carries the findings.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

try:
    from nb_io import load_cells, logical
except ImportError:  # allow running from another cwd
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from nb_io import load_cells, logical

# DBR major -> (spark_version, python_version) as (major, minor) tuples.
# Tuples, NOT floats: as floats 3.8 > 3.12 and 3.10 == 3.1, which silently
# corrupts every version comparison. Headline versions only; confirm exact
# maintenance versions against the release notes.
DBR_TABLE = {
    "9.1": ((3, 1), (3, 8)),
    "10.4": ((3, 2), (3, 8)),
    "11.3": ((3, 3), (3, 9)),
    "12.2": ((3, 3), (3, 9)),
    "13.3": ((3, 4), (3, 10)),
    "14.3": ((3, 5), (3, 10)),
    "15.4": ((3, 5), (3, 11)),
    "16.4": ((3, 5), (3, 12)),
}

# Each rule: id, regex, category (1/2/3), severity, the dimension it gates on
# ("spark" | "python" | "always"), and the (major, minor) version at which it
# becomes a risk. A rule fires only if the jump crosses that version on that
# dimension (src < ver <= tgt).
RULES = [
    # --- Spark SQL semantics (gate on spark version when ANSI/parse changed) ---
    ("ansi-cast", r"\bcast\s*\(|\bCAST\s*\(.*\bAS\b", 2, "Blocker", "spark", (3, 3)),
    ("ansi-divide-zero", r"/\s*0\b|\bmod\s*\([^,]+,\s*0\s*\)", 2, "Blocker", "spark", (3, 3)),
    ("time-parser-legacy", r"\b(to_date|to_timestamp|from_unixtime|unix_timestamp)\s*\(", 1, "Blocker", "spark", (3, 0)),
    ("negative-scale-decimal", r"DECIMAL\s*\(\s*\d+\s*,\s*-\d+\s*\)", 1, "Warning", "spark", (3, 0)),
    ("interval-literal", r"\bINTERVAL\b\s+['\"]?\d", 2, "Warning", "spark", (3, 2)),
    ("map-key-dup", r"\bmap_from_arrays\s*\(|\bmap\s*\(", 2, "Info", "spark", (3, 0)),
    # --- Python interpreter / stdlib (gate on python version) ---
    ("py-distutils", r"^\s*(import\s+distutils|from\s+distutils)", 2, "Blocker", "python", (3, 12)),
    ("py-imp", r"^\s*import\s+imp\b", 2, "Blocker", "python", (3, 12)),
    ("py-collections-abc", r"collections\.(Mapping|Iterable|Sequence|MutableMapping|Callable)\b", 2, "Warning", "python", (3, 10)),
    ("py-asyncio-coroutine", r"@asyncio\.coroutine", 2, "Warning", "python", (3, 11)),
    # --- Preinstalled libraries (gate on spark version as a proxy for DBR gen) ---
    ("pandas-append", r"\.append\s*\(", 2, "Warning", "spark", (3, 4)),
    ("pandas-iteritems", r"\.iteritems\s*\(", 2, "Blocker", "spark", (3, 4)),
    ("pandas-pd-np", r"\bpd\.np\.", 2, "Blocker", "spark", (3, 4)),
    ("numpy-aliases", r"\bnp\.(float|int|bool|object)\b(?!\w)", 2, "Blocker", "spark", (3, 4)),
    # --- Platform / cluster config ---
    ("init-script-dbfs", r"dbfs:/.*\.(sh|bash)|init_scripts", 2, "Blocker", "always", (0, 0)),
    ("log4j1-config", r"log4j\.properties|log4j\.appender", 2, "Warning", "always", (0, 0)),
    ("dbfs-mount", r"dbutils\.fs\.mount\s*\(|['\"]/mnt/", 2, "Info", "always", (0, 0)),
    # --- Delta protocol coordination ---
    ("delta-protocol-upgrade", r"enableDeletionVectors|delta\.columnMapping|\.saveAsTable\s*\(|\.write\b", 2, "Blocker", "always", (0, 0)),
    # --- Deprecated / removed APIs ---
    ("sqlcontext", r"\bsqlContext\b|\bSQLContext\s*\(", 2, "Warning", "always", (0, 0)),
    ("hive-metastore-ref", r"hive_metastore\.", 2, "Info", "always", (0, 0)),
    ("scala-cell", r"^\s*%scala\b", 1, "Info", "always", (0, 0)),
    ("r-workload", r"^\s*%r\b|\blibrary\(SparkR\)|\bsparklyr\b", 1, "Warning", "always", (0, 0)),
]

FIX_HINTS = {
    "ansi-cast": "Confirm the cast can fail; route rewrite to databricks-ansi-remediation (try_cast) or bridge spark.sql.ansi.enabled=false.",
    "ansi-divide-zero": "Guard denominator or use try_divide; bridge spark.sql.ansi.enabled=false.",
    "time-parser-legacy": "Verify the datetime pattern against Java-8 DateTimeFormatter; bridge spark.sql.legacy.timeParserPolicy=LEGACY.",
    "negative-scale-decimal": "Remove negative scale or bridge spark.sql.legacy.allowNegativeScaleOfDecimal=true.",
    "interval-literal": "Rewrite to typed year-month / day-time interval literals (Spark 3.2+).",
    "map-key-dup": "Duplicate map keys now error instead of last-wins; dedupe keys.",
    "py-distutils": "distutils removed in Python 3.12; use setuptools/packaging/sysconfig.",
    "py-imp": "imp removed; use importlib.",
    "py-collections-abc": "Import ABCs from collections.abc, not collections.",
    "py-asyncio-coroutine": "Use async def / await.",
    "pandas-append": "If this is a pandas DataFrame, .append was removed in pandas 2.x; use pd.concat. (Spark DataFrame .append is fine.)",
    "pandas-iteritems": "pandas .iteritems() removed; use .items().",
    "pandas-pd-np": "pd.np removed; import numpy directly.",
    "numpy-aliases": "np.float/int/bool/object aliases removed; use builtins or np.float64 etc.",
    "init-script-dbfs": "Relocate init scripts off DBFS to workspace files / UC volumes / cloud storage; update the spec.",
    "log4j1-config": "Migrate Log4j 1 config to Log4j 2 (log4j2.xml).",
    "dbfs-mount": "Works on classic DBR; plan UC external locations if also moving to Unity Catalog.",
    "delta-protocol-upgrade": "Writing from a newer DBR can raise the table's reader/writer protocol; check that downstream readers are upgraded first. A/B test against TEST tables only.",
    "sqlcontext": "Use spark.sql(...) / SparkSession.",
    "hive-metastore-ref": "Orthogonal to DBR; route HMS->UC rewrites to databricks-metastore-remap.",
    "scala-cell": "Scala stays 2.12 on classic; usually no recompile, but re-test against the target Spark.",
    "r-workload": "Confirm R/CRAN package builds exist for the target DBR.",
}


def vstr(t) -> str:
    """(3, 2) -> '3.2'."""
    return ".".join(str(x) for x in t)


def parse_dbr(v: str) -> str:
    """Normalize '10.4.x-scala2.12' / '10.4 LTS' / '10.4' -> '10.4'."""
    m = re.match(r"(\d+\.\d+)", v.strip())
    if not m:
        raise SystemExit(f"Could not parse DBR version: {v!r}")
    return m.group(1)


def rule_applies(rule, src_spark, tgt_spark, src_py, tgt_py) -> bool:
    _id, _re, _cat, _sev, dim, ver = rule
    if dim == "always":
        return True
    if dim == "spark":
        return src_spark < ver <= tgt_spark
    if dim == "python":
        return src_py < ver <= tgt_py
    return True


def scan_file(path, active_rules):
    findings = []
    try:
        cells = load_cells(path)
    except Exception as e:  # noqa: BLE001
        return [{"file": path, "error": str(e)}]
    for cell in cells:
        for lineno, raw in enumerate(cell["lines"], start=1):
            line = logical(raw)
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                # still scan magics like %scala / %r which arrive via logical()
                if not (stripped.startswith("%scala") or stripped.startswith("%r")):
                    continue
            for rid, pat, cat, sev, _dim, _ver in active_rules:
                if re.search(pat, line):
                    findings.append({
                        "file": path,
                        "cell": cell["index"],
                        "line": lineno,
                        "pattern_id": rid,
                        "category": cat,
                        "severity": sev,
                        "language": cell["language"],
                        "snippet": stripped[:160],
                        "fix": FIX_HINTS.get(rid, ""),
                    })
    return findings


def iter_source_files(paths):
    exts = {".py", ".ipynb", ".sql", ".scala", ".r"}
    for p in paths:
        if os.path.isfile(p):
            yield p
        else:
            for root, _dirs, files in os.walk(p):
                for f in files:
                    if os.path.splitext(f)[1].lower() in exts:
                        yield os.path.join(root, f)


def main():
    ap = argparse.ArgumentParser(description="Static DBR upgrade readiness scanner")
    ap.add_argument("--source-dbr", required=True, help="e.g. 10.4 or 10.4.x-scala2.12")
    ap.add_argument("--target-dbr", required=True, help="e.g. 15.4")
    ap.add_argument("--json", help="write findings to this JSON file")
    ap.add_argument("paths", nargs="+", help="notebook files or directories")
    args = ap.parse_args()

    src = parse_dbr(args.source_dbr)
    tgt = parse_dbr(args.target_dbr)
    if src not in DBR_TABLE or tgt not in DBR_TABLE:
        sys.stderr.write(
            f"Unknown DBR. Known: {', '.join(DBR_TABLE)}. "
            "Add the version to DBR_TABLE or use the nearest LTS.\n")
    src_spark, src_py = DBR_TABLE.get(src, ((0, 0), (0, 0)))
    tgt_spark, tgt_py = DBR_TABLE.get(tgt, ((99, 99), (99, 99)))

    active = [r for r in RULES if rule_applies(r, src_spark, tgt_spark, src_py, tgt_py)]

    all_findings = []
    for f in iter_source_files(args.paths):
        all_findings.extend(scan_file(f, active))

    by_cat = {1: 0, 2: 0, 3: 0}
    for fnd in all_findings:
        if "category" in fnd:
            by_cat[fnd["category"]] = by_cat.get(fnd["category"], 0) + 1

    report = {
        "source_dbr": src,
        "target_dbr": tgt,
        "spark_jump": f"{vstr(src_spark)} -> {vstr(tgt_spark)}",
        "python_jump": f"{vstr(src_py)} -> {vstr(tgt_py)}",
        "rules_active": [r[0] for r in active],
        "finding_count": len([f for f in all_findings if "pattern_id" in f]),
        "by_category": by_cat,
        "findings": all_findings,
    }

    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2)
        print(f"Wrote {report['finding_count']} findings to {args.json}")
    else:
        print(json.dumps(report, indent=2))

    print(
        f"\nDBR {src} -> {tgt}  |  Spark {vstr(src_spark)}->{vstr(tgt_spark)}, "
        f"Python {vstr(src_py)}->{vstr(tgt_py)}\n"
        f"{report['finding_count']} findings  "
        f"(cat1={by_cat.get(1,0)} bridgeable, cat2={by_cat.get(2,0)} code-change, cat3={by_cat.get(3,0)} blocker)\n"
        "Confirm each hit by reading the code. See references/behavior-changes.md for fixes.",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
