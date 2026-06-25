#!/usr/bin/env python3
"""Apply one of two ANSI remediations to a Synapse-origin Spark notebook and
emit a reviewable per-cell diff.

Two remediation modes, per the migration brief:

  --mode try_cast       Rewrite explicit cast(...) / CAST(...) sites to
                        try_cast(...) / TRY_CAST(...). Invalid values resolve
                        to NULL again, matching the *intent* of ANSI-off code.
                        NOTE: try_cast is NOT a behavioral identity for ANSI-off
                        CAST in every edge case, and it only covers explicit
                        casts — date-parse / arithmetic / insert sites are left
                        for review. Use where NULL-on-bad-value is acceptable.

  --mode session-flag   Inject `spark.conf.set("spark.sql.ansi.enabled","false")`
                        as a new top cell, restoring exact legacy parity for the
                        whole notebook (all constructs at once). This is the
                        recommendation where bit-for-bit parity matters.

By default nothing is written: the script prints a unified diff per changed cell
with a rationale line. Pass --write to apply in place (a .bak is kept).

Usage:
    python apply_remediation.py NB --mode try_cast
    python apply_remediation.py NB --mode session-flag --write
    python apply_remediation.py NB --mode try_cast --out NB.remediated.py
"""
from __future__ import annotations

import argparse
import difflib
import re
import shutil
import sys

from nb_io import COMMAND_SEP, NB_HEADER, is_magic, load_cells, logical, physical

# Match an explicit cast that is not already try_cast. Preserves the original
# casing of the matched token so CAST -> TRY_CAST and cast -> try_cast.
_CAST = re.compile(r"(?<![\w])(cast)(\s*\()", re.I)


def _to_try(m: re.Match) -> str:
    tok = m.group(1)
    prefix = "TRY_" if tok.isupper() else "try_"
    return prefix + tok + m.group(2)


def _rewrite_line_try_cast(content: str) -> tuple[str, int]:
    new, n = _CAST.subn(_to_try, content)
    return new, n


def remediate_try_cast(cell: dict) -> tuple[list[str], int]:
    """Return (new physical lines, number of sites rewritten) for one cell."""
    out, count = [], 0
    for line in cell["lines"]:
        magic = is_magic(line)
        content = logical(line)
        new_content, n = _rewrite_line_try_cast(content)
        count += n
        out.append(physical(new_content, magic) if n else line)
    return out, count


def _cell_diff(path: str, idx: int, before: list[str], after: list[str], rationale: str) -> str:
    diff = difflib.unified_diff(
        before, after,
        fromfile=f"{path} [cell {idx}] (before)",
        tofile=f"{path} [cell {idx}] (after)",
        lineterm="",
    )
    body = "\n".join(diff)
    if not body:
        return ""
    return f"# rationale: {rationale}\n{body}\n"


def run_try_cast(path: str):
    cells = load_cells(path)
    diffs, total = [], 0
    new_cells = []
    for cell in cells:
        after, n = remediate_try_cast(cell)
        total += n
        if n:
            diffs.append(_cell_diff(
                path, cell["index"], cell["lines"], after,
                f"{n} explicit cast(...) -> try_cast(...): bad values resolve to NULL "
                f"instead of raising under ANSI on"))
        new_cells.append(after)
    return new_cells, diffs, total


SESSION_FLAG_PY = 'spark.conf.set("spark.sql.ansi.enabled", "false")'


def run_session_flag(path: str):
    """Build the new notebook with a parity cell injected right after the header
    cell, and return a diff that shows the inserted cell."""
    cells = load_cells(path)
    flag_cell = [SESSION_FLAG_PY]
    # Insert after cell 0 (the notebook-source header cell).
    new_cells = [cells[0]["lines"]] if cells else [[NB_HEADER]]
    new_cells.append(flag_cell)
    for cell in cells[1:]:
        new_cells.append(cell["lines"])
    rationale = ("inject session-scoped ANSI-off flag: restores exact legacy "
                 "behavior for ALL constructs (recommended where bit-for-bit "
                 "parity matters)")
    inserted = "\n".join([COMMAND_SEP, ""] + flag_cell)
    diff = (f"# rationale: {rationale}\n"
            f"--- {path} (before)\n+++ {path} (after)\n"
            f"@@ inserted new cell after the notebook header @@\n"
            + "\n".join("+" + l for l in [COMMAND_SEP, ""] + flag_cell) + "\n")
    return new_cells, [diff], 1


def _serialize_py(cells_lines: list[list[str]]) -> str:
    return ("\n" + COMMAND_SEP + "\n").join("\n".join(c) for c in cells_lines) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("notebook")
    ap.add_argument("--mode", required=True, choices=["try_cast", "session-flag"])
    ap.add_argument("--write", action="store_true", help="apply in place (keeps .bak)")
    ap.add_argument("--out", metavar="PATH", help="write the result to a new file")
    args = ap.parse_args(argv)

    if not args.notebook.lower().endswith(".py"):
        print("note: in-place write supports Databricks .py source; for .sql/.ipynb "
              "review the printed diff and apply manually.", file=sys.stderr)

    if args.mode == "try_cast":
        new_cells, diffs, total = run_try_cast(args.notebook)
        summary = f"try_cast mode: {total} explicit cast site(s) across {len(diffs)} cell(s)"
    else:
        new_cells, diffs, total = run_session_flag(args.notebook)
        summary = "session-flag mode: 1 parity cell injected"

    for d in diffs:
        print(d)
    print(f"\n# {summary}")

    if (args.write or args.out) and args.notebook.lower().endswith(".py"):
        text = _serialize_py(new_cells)
        if args.out:
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(text)
            print(f"# wrote {args.out}")
        if args.write:
            shutil.copyfile(args.notebook, args.notebook + ".bak")
            with open(args.notebook, "w", encoding="utf-8") as f:
                f.write(text)
            print(f"# wrote {args.notebook} (backup at {args.notebook}.bak)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
