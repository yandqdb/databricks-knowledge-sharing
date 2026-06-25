#!/usr/bin/env python3
"""Scaffold a hive_metastore -> Unity Catalog mapping file from the references
actually used in a set of notebooks.

The mapping is the input of record for remap_refs.py, but writing it by hand for
~200 notebooks is the chore. This script scans the notebooks, collects every
distinct `hive_metastore.<db>.<table>` reference, and emits a mapping.yaml stub
with a best-guess UC target the engineer then verifies/edits.

Two ways to fill the UC side of each row:

  --catalog NAME            naming-convention guess: hive_metastore.db.table ->
                            NAME.db.table. Fast, offline, ALWAYS review it.

  (recommended, manual)     derive the real target by matching storage paths:
                            the UC external table registered against the same
                            ADLS Gen2 Delta path. Run this SQL in the workspace
                            and paste the result into the `uc:` fields:

      SELECT table_catalog, table_schema, table_name, storage_path
      FROM system.information_schema.tables
      WHERE storage_path IS NOT NULL;

    then match each hive table's `DESCRIBE EXTENDED ... Location` to a row above.

The point of the stub is that NO target is trusted until a human confirms it maps
to the same physical path. The naming-convention guess is a starting draft only.

Usage:
    python build_mapping.py NB [NB ...] --catalog transit_prod > mapping.yaml
    python build_mapping.py NB [NB ...]            # emits TODO placeholders
"""
from __future__ import annotations

import argparse
import re
import sys

from nb_io import load_cells, logical

_HIVE = re.compile(r"(?<![\w.])hive_metastore\.([a-z_][\w]*)\.([a-z_][\w]*)", re.I)


def collect_refs(paths) -> list[tuple[str, str, str]]:
    found = {}
    for p in paths:
        for cell in load_cells(p):
            if cell["language"] == "md":
                continue
            for line in cell["lines"]:
                for m in _HIVE.finditer(logical(line)):
                    db, tbl = m.group(1), m.group(2)
                    fqn = f"hive_metastore.{db}.{tbl}".lower()
                    found.setdefault(fqn, (db.lower(), tbl.lower()))
    return [(fqn, db, tbl) for fqn, (db, tbl) in sorted(found.items())]


def emit(refs, catalog: str | None) -> str:
    lines = [
        "# hive_metastore -> Unity Catalog mapping (REVIEW EVERY uc: VALUE).",
        "# Each UC table MUST be the one registered against the SAME ADLS Gen2",
        "# Delta path as its hive source. The uc: values below are unverified.",
        "",
        "# default_database: hive_metastore.<db>   # set to enable bare-name remap",
        "",
        "tables:",
    ]
    for fqn, db, tbl in refs:
        if catalog:
            uc = f"{catalog}.{db}.{tbl}"
        else:
            uc = f"TODO.{db}.{tbl}   # <-- confirm catalog.schema.table by storage path"
        lines.append(f"  - hive: {fqn}")
        lines.append(f"    uc:   {uc}")
    if not refs:
        lines.append("  []  # no hive_metastore references found")
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("notebooks", nargs="+")
    ap.add_argument("--catalog", help="target catalog for naming-convention guess")
    args = ap.parse_args(argv)

    refs = collect_refs(args.notebooks)
    sys.stdout.write(emit(refs, args.catalog))
    print(f"# {len(refs)} distinct hive_metastore table(s) found", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
