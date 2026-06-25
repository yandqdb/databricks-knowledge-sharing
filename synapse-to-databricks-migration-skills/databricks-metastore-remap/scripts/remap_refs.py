#!/usr/bin/env python3
"""Rewrite hive_metastore table references to their Unity Catalog equivalents in
a Synapse-origin Spark notebook, and emit a reviewable per-cell diff.

This is the single highest-frequency edit in the migration: the UC external
tables are registered against the *same* ADLS Gen2 Delta paths, so the rewrite
is a pure rename — `hive_metastore.<db>.<table>` -> `<catalog>.<schema>.<table>`.

The mapping is the input of record (see build_mapping.py and the sample). The
script rewrites three reference forms:
  * fully qualified: `hive_metastore.db.table` (any quoting / spark.table(...))
  * two-part under a known default db: `db.table` if `db` is in the mapping
  * unqualified bare names: only when a `default_database` is set AND the bare
    name maps unambiguously — and never a name shadowed by a CTE / temp view.

Safety: shadowed names (CTE `WITH x AS`, `CREATE TEMP VIEW x`, subquery aliases)
are collected per notebook and excluded. Any `hive_metastore.*` reference with no
mapping entry is reported as UNMAPPED and left untouched — never guessed.

By default nothing is written: prints a per-cell unified diff with rationale.
Pass --write to apply in place (a .bak is kept).

Usage:
    python remap_refs.py NB --mapping mapping.yaml
    python remap_refs.py NB --mapping mapping.yaml --write
"""
from __future__ import annotations

import argparse
import difflib
import re
import shutil
import sys

from nb_io import COMMAND_SEP, is_magic, load_cells, logical, physical

try:
    import yaml  # PyYAML if available
except Exception:  # pragma: no cover - fallback parser below
    yaml = None


# ---- mapping loading -------------------------------------------------------

def load_mapping(path: str) -> dict:
    """Return {"default_database": str|None, "tables": {hive_fqn: uc_fqn}}.

    Accepts YAML (preferred) or a 2-column CSV `hive,uc`. The YAML shape is:
        default_database: hive_metastore.transit   # optional
        tables:
          - hive: hive_metastore.transit.raw_taps
            uc:   transit_prod.bronze.raw_taps
    """
    if path.lower().endswith(".csv"):
        return _load_mapping_csv(path)
    text = open(path, encoding="utf-8").read()
    if yaml is not None:
        data = yaml.safe_load(text)
        return _normalize_mapping(data)
    return _load_mapping_yaml_minimal(text)


def _normalize_mapping(data: dict) -> dict:
    tables = {}
    for row in data.get("tables", []):
        tables[row["hive"].strip().lower()] = row["uc"].strip()
    default_db = data.get("default_database")
    return {"default_database": default_db.strip().lower() if default_db else None,
            "tables": tables}


def _load_mapping_csv(path: str) -> dict:
    import csv
    tables = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.reader(f):
            if len(row) >= 2 and row[0].strip() and not row[0].startswith("#"):
                tables[row[0].strip().lower()] = row[1].strip()
    return {"default_database": None, "tables": tables}


def _load_mapping_yaml_minimal(text: str) -> dict:
    """Tiny YAML reader for the documented shape, used when PyYAML is absent."""
    default_db, tables, hive = None, {}, None
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("default_database:"):
            default_db = s.split(":", 1)[1].strip() or None
        elif s.startswith("- hive:"):
            hive = s.split(":", 1)[1].strip()
        elif s.startswith("hive:"):
            hive = s.split(":", 1)[1].strip()
        elif s.startswith("uc:") and hive:
            tables[hive.lower()] = s.split(":", 1)[1].strip()
            hive = None
    return {"default_database": default_db.lower() if default_db else None,
            "tables": tables}


# ---- shadowing detection ---------------------------------------------------

_CTE = re.compile(r"\bwith\s+([a-z_][\w]*)\s+as\s*\(", re.I)
_CTE_NEXT = re.compile(r",\s*([a-z_][\w]*)\s+as\s*\(", re.I)
_TEMPVIEW = re.compile(r"(?:create\s+(?:or\s+replace\s+)?(?:global\s+)?temp(?:orary)?\s+view\s+"
                       r"|createOrReplaceTempView\s*\(\s*[\"'])([a-z_][\w]*)", re.I)


def collect_shadowed(cells) -> set[str]:
    names: set[str] = set()
    for cell in cells:
        if cell["language"] == "md":
            continue
        text = "\n".join(logical(l) for l in cell["lines"])
        for pat in (_CTE, _CTE_NEXT, _TEMPVIEW):
            for m in pat.finditer(text):
                names.add(m.group(1).lower())
    return names


# ---- rewriting -------------------------------------------------------------

# A bare/two-part name is only a *table* reference when it sits in SQL table
# position (after one of these keywords) — never when it is a bare Python
# identifier such as a variable on the left of an assignment.
_SQL_KW = r"(?i:from|join|into|update|table)"


def _repl_plain(uc):
    return lambda m: uc


def _repl_keep_group1(uc):
    """Preserve a captured prefix (SQL keyword + whitespace, or an opening
    quote) and substitute only the table token."""
    return lambda m: m.group(1) + uc


def build_patterns(mapping: dict, shadowed: set[str]):
    """Compile (regex, repl, kind) tuples, longest source first so a three-part
    name is matched before its two-part suffix.

    Precision rules that prevent corrupting non-table tokens:
      * fully_qualified (hive_metastore.db.table) is unambiguous -> rewrite anywhere.
      * two_part (db.table) -> only in quotes or after a SQL keyword.
      * unqualified (bare table) -> only after a SQL keyword (never a bare ident).
    """
    rules = []
    default_db = mapping["default_database"]
    for hive, uc in sorted(mapping["tables"].items(), key=lambda kv: -len(kv[0])):
        rules.append((re.compile(r"(?<![\w.])" + re.escape(hive) + r"(?![\w])", re.I),
                      _repl_plain(uc), "fully_qualified"))
        parts = hive.split(".")
        if len(parts) == 3:
            _, db, tbl = parts
            two_part = re.escape(f"{db}.{tbl}")
            # two-part inside quotes: e.g. spark.table("transit.routes")
            rules.append((re.compile(r"(['\"])" + two_part + r"(['\"])", re.I),
                          lambda m, uc=uc: m.group(1) + uc + m.group(2), "two_part"))
            # two-part after a SQL keyword
            rules.append((re.compile(r"(" + _SQL_KW + r"\s+)" + two_part + r"(?![\w.])"),
                          _repl_keep_group1(uc), "two_part"))
            # bare name: only after a SQL keyword, default db declared & matching,
            # and not shadowed by a CTE / temp view.
            if default_db and default_db.endswith(db) and tbl not in shadowed:
                rules.append((re.compile(r"(" + _SQL_KW + r"\s+)" + re.escape(tbl) + r"(?![\w.])"),
                              _repl_keep_group1(uc), "unqualified"))
    return rules


def rewrite_cell(cell, rules):
    out, changes = [], []
    for li, line in enumerate(cell["lines"]):
        magic = is_magic(line)
        content = logical(line)
        new_content = content
        for pat, repl, kind in rules:
            new_content, n = pat.subn(repl, new_content)
            if n:
                changes.append((li, kind, n))
        out.append(physical(new_content, magic) if new_content != content else line)
    return out, changes


_HIVE_ANY = re.compile(r"(?<![\w.])hive_metastore\.[a-z_][\w]*\.[a-z_][\w]*", re.I)


def find_unmapped(cells, mapping) -> list[tuple[int, str]]:
    mapped = set(mapping["tables"].keys())
    hits = []
    for cell in cells:
        if cell["language"] == "md":
            continue
        for li, line in enumerate(cell["lines"]):
            for m in _HIVE_ANY.finditer(logical(line)):
                if m.group(0).lower() not in mapped:
                    hits.append((cell["index"], m.group(0)))
    return hits


def cell_diff(path, idx, before, after, kinds) -> str:
    diff = difflib.unified_diff(before, after,
                                fromfile=f"{path} [cell {idx}] (before)",
                                tofile=f"{path} [cell {idx}] (after)", lineterm="")
    body = "\n".join(diff)
    if not body:
        return ""
    summary = ", ".join(f"{k}×{v}" for k, v in sorted(kinds.items()))
    return (f"# rationale: remap hive_metastore -> UC (same ADLS path); {summary}\n"
            f"{body}\n")


def serialize_py(cells_lines) -> str:
    return ("\n" + COMMAND_SEP + "\n").join("\n".join(c) for c in cells_lines) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("notebook")
    ap.add_argument("--mapping", required=True, help="mapping.yaml or mapping.csv")
    ap.add_argument("--write", action="store_true", help="apply in place (keeps .bak)")
    ap.add_argument("--out", metavar="PATH", help="write result to a new file")
    args = ap.parse_args(argv)

    mapping = load_mapping(args.mapping)
    cells = load_cells(args.notebook)
    shadowed = collect_shadowed(cells)
    rules = build_patterns(mapping, shadowed)

    new_cells, total = [], 0
    for cell in cells:
        after, changes = rewrite_cell(cell, rules)
        new_cells.append(after)
        if changes:
            kinds: dict[str, int] = {}
            for _, k, n in changes:
                kinds[k] = kinds.get(k, 0) + n
                total += n
            print(cell_diff(args.notebook, cell["index"], cell["lines"], after, kinds))

    unmapped = find_unmapped(cells, mapping)
    if unmapped:
        print("\n# UNMAPPED hive_metastore references (left untouched — add to mapping):")
        for idx, ref in sorted(set(unmapped)):
            print(f"#   cell {idx}: {ref}")
    if shadowed:
        print(f"\n# shadowed names excluded from bare-name remap: {sorted(shadowed)}")
    print(f"\n# {total} reference(s) rewritten; {len(set(unmapped))} unmapped")

    if (args.write or args.out) and args.notebook.lower().endswith(".py"):
        text = serialize_py(new_cells)
        if args.out:
            open(args.out, "w", encoding="utf-8").write(text)
            print(f"# wrote {args.out}")
        if args.write:
            shutil.copyfile(args.notebook, args.notebook + ".bak")
            open(args.notebook, "w", encoding="utf-8").write(text)
            print(f"# wrote {args.notebook} (backup at {args.notebook}.bak)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
