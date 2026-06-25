#!/usr/bin/env python3
"""Notebook I/O helpers shared by the ANSI-remediation scripts.

Parses the three formats a Synapse-origin Spark notebook can arrive in and
exposes them through one cell model so the detector and the remediator never
have to know the on-disk format.

Cell model: a list of dicts, each
    {"index": int, "language": "python"|"sql"|"md"|"scala"|"r",
     "lines": [str, ...]}   # physical lines, exactly as on disk

Two formats carry magic-prefixed lines (Databricks `.py` source export):
a `%sql` cell stores its SQL as `# MAGIC <sql>` lines. `logical()` strips that
prefix so detection sees real SQL; `physical()` re-adds it so a rewrite round-
trips byte-for-byte except at the changed site. Keep these two in sync.
"""
from __future__ import annotations

import json
import os

COMMAND_SEP = "# COMMAND ----------"
NB_HEADER = "# Databricks notebook source"
MAGIC_PREFIX = "# MAGIC "
MAGIC_BARE = "# MAGIC"


def logical(line: str) -> str:
    """The analyzable content of a physical line (magic prefix removed)."""
    if line.startswith(MAGIC_PREFIX):
        return line[len(MAGIC_PREFIX):]
    if line.rstrip() == MAGIC_BARE:
        return ""
    return line


def physical(line: str, was_magic: bool) -> str:
    """Re-wrap edited logical content back into its physical form."""
    if not was_magic:
        return line
    return MAGIC_PREFIX + line if line != "" else MAGIC_BARE


def is_magic(line: str) -> bool:
    return line.startswith(MAGIC_PREFIX) or line.rstrip() == MAGIC_BARE


def _language_of(lines: list[str]) -> str:
    for ln in lines:
        s = logical(ln).strip()
        if not s:
            continue
        if s.startswith("%sql"):
            return "sql"
        if s.startswith("%md"):
            return "md"
        if s.startswith("%scala"):
            return "scala"
        if s.startswith("%r"):
            return "r"
        if s.startswith("%python"):
            return "python"
        break
    return "python"


def load_cells(path: str):
    ext = os.path.splitext(path)[1].lower()
    if ext == ".ipynb":
        return _load_ipynb(path)
    if ext == ".sql":
        with open(path, encoding="utf-8") as f:
            return [{"index": 0, "language": "sql", "lines": f.read().splitlines()}]
    return _load_py(path)


def _load_py(path: str):
    with open(path, encoding="utf-8") as f:
        raw = f.read().splitlines()
    chunks, cur = [], []
    for ln in raw:
        if ln.strip() == COMMAND_SEP:
            chunks.append(cur)
            cur = []
        else:
            cur.append(ln)
    chunks.append(cur)
    return [
        {"index": i, "language": _language_of(c), "lines": c}
        for i, c in enumerate(chunks)
    ]


def _load_ipynb(path: str):
    with open(path, encoding="utf-8") as f:
        nb = json.load(f)
    cells = []
    for i, cell in enumerate(nb.get("cells", [])):
        if cell.get("cell_type") != "code":
            continue
        src = cell.get("source", [])
        lines = "".join(src).splitlines() if isinstance(src, list) else src.splitlines()
        lang = "python"
        if lines and lines[0].strip().startswith("%sql"):
            lang = "sql"
        cells.append({"index": i, "language": lang, "lines": lines})
    return cells
