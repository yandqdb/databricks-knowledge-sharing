#!/usr/bin/env python3
"""Shared run-spec / baseline loader for run_notebook.py and compare_baseline.py.

Both inputs are plain YAML (JSON also accepted). They are read with PyYAML when
it is installed; otherwise the text is parsed as JSON, and if that also fails a
clear error tells the caller to install PyYAML or supply JSON. The documented
shapes live in references/baseline-schema.md.

Exports:
  load_runspec(path)            -> the run-spec document (a mapping)
  load_baseline(path)           -> the baseline document (a mapping)
  resolve_notebook(spec, name)  -> the named notebook entry merged with defaults
  executable_cell_count(path)   -> count of runnable (non-md, non-empty) cells
"""
from __future__ import annotations

import json
import os

from nb_io import load_cells, logical

try:
    import yaml  # PyYAML if available
except Exception:  # pragma: no cover - environment-specific
    yaml = None


def _strip_inline_comment(line: str) -> str:
    """Drop a trailing ` # ...` comment that sits outside quotes."""
    out, quote, i = [], None, 0
    while i < len(line):
        ch = line[i]
        if quote:
            out.append(ch)
            if ch == quote:
                quote = None
        elif ch in "\"'":
            quote = ch
            out.append(ch)
        elif ch == "#" and (i == 0 or line[i - 1] in " \t"):
            break
        else:
            out.append(ch)
        i += 1
    return "".join(out).rstrip()


def _parse_scalar(s: str):
    s = s.strip()
    if not s:
        return None
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        return s[1:-1]
    low = s.lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("null", "~"):
        return None
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    return s


def _is_kv(s: str) -> bool:
    """True if `s` is a `key: value` (or `key:`) entry, not a scalar that merely
    contains a colon (e.g. a path or URL)."""
    i = s.find(":")
    while i != -1:
        nxt = s[i + 1:i + 2]
        if nxt in ("", " "):
            return True
        if nxt == "/":            # part of a path/URL; keep scanning
            i = s.find(":", i + 1)
            continue
        return False
    return False


def _minimal_yaml(text: str):
    """Indentation-based reader for the documented run-spec / baseline shapes.

    Handles nested mappings, lists of scalars, and lists of mappings. This is not
    a general YAML parser; it is the minimal built-in reader baseline-schema.md
    calls for when PyYAML is unavailable (the same fallback approach the sibling
    skills use).
    """
    items = []
    for raw in text.splitlines():
        line = _strip_inline_comment(raw)
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        items.append((indent, line.strip()))
    if not items:
        return {}
    n = len(items)

    def parse(idx, indent):
        kind = parse_list if items[idx][1].startswith("-") else parse_map
        return kind(idx, indent)

    def parse_map(idx, indent):
        result = {}
        while idx < n:
            cur, content = items[idx]
            if cur != indent or content.startswith("-"):
                break
            key, _, rest = content.partition(":")
            key, rest = key.strip(), rest.strip()
            idx += 1
            if rest == "":
                if idx < n and items[idx][0] > indent:
                    value, idx = parse(idx, items[idx][0])
                else:
                    value = None
            else:
                value = _parse_scalar(rest)
            result[key] = value
        return result, idx

    def parse_list(idx, indent):
        result = []
        while idx < n:
            cur, content = items[idx]
            if cur != indent or not content.startswith("-"):
                break
            after = content[1:].lstrip()
            item_indent = cur + (len(content[1:]) - len(after)) + 1
            if after == "":
                idx += 1
                if idx < n and items[idx][0] > cur:
                    value, idx = parse(idx, items[idx][0])
                else:
                    value = None
                result.append(value)
            elif _is_kv(after):
                # A mapping element: rewrite this line as its first key at the
                # element indent, then parse the whole element as one map.
                items[idx] = (item_indent, after)
                value, idx = parse_map(idx, item_indent)
                result.append(value)
            else:
                result.append(_parse_scalar(after))
                idx += 1
        return result, idx

    value, _ = parse(0, items[0][0])
    return value


def _load_doc(path: str) -> dict:
    p = os.path.expanduser(path)
    with open(p, encoding="utf-8") as f:
        text = f.read()
    if yaml is not None:
        data = yaml.safe_load(text)
    else:
        # No PyYAML: accept JSON (a subset of YAML) directly, otherwise fall back
        # to the minimal built-in reader for the documented YAML shape, and only
        # error if neither can parse (baseline-schema.md).
        try:
            data = json.loads(text)
        except Exception:
            try:
                data = _minimal_yaml(text)
            except Exception as exc:  # pragma: no cover - malformed input
                raise RuntimeError(
                    f"cannot parse {path}: PyYAML is not installed and the file "
                    f"is neither valid JSON nor recognizable run-spec/baseline "
                    f"YAML ({exc}). Install PyYAML (pip install pyyaml).") from exc
    if not isinstance(data, dict):
        raise RuntimeError(
            f"{path}: expected a mapping at the top level, "
            f"got {type(data).__name__}")
    return data


def load_runspec(path: str) -> dict:
    """Load a run-spec document: {defaults?: {...}, notebooks: [ {...}, ... ]}."""
    return _load_doc(path)


def load_baseline(path: str) -> dict:
    """Load a baseline document: {tolerances?: {...}, tables: {fqn: {...}}}."""
    return _load_doc(path)


def resolve_notebook(spec: dict, name: str):
    """Return the notebook entry named `name`, merged over `defaults`, or None.

    Per-notebook keys win over defaults. `compute` is replaced wholesale when the
    notebook declares its own block (no key-level merge), so a per-notebook
    compute fully overrides the default; a notebook with no compute inherits the
    default compute.
    """
    defaults = spec.get("defaults") or {}
    for nb in spec.get("notebooks") or []:
        if nb.get("name") == name:
            merged = dict(defaults)
            merged.update(nb)  # per-notebook keys win; compute swapped wholesale
            return merged
    return None


def executable_cell_indices(path: str) -> list:
    """The nb_io cell indices of the runnable cells (not markdown, not empty).

    A cell qualifies when its language is not `md` and it has at least one real
    code line: a non-blank logical line that is not a bare `%magic` directive and
    not a comment (per-language: `--` for SQL, `#` otherwise). A `%md` anywhere
    makes the whole cell markdown even when nb_io labels it `python` (the file
    header `# Databricks notebook source` precedes the `%md`). Returning the
    indices lets execution coverage localize a failing cell by its nb_io index,
    using the same parser as every other script so indices line up.
    """
    indices = []
    for cell in load_cells(path):
        if cell["language"] == "md":
            continue
        logical_lines = [logical(line).strip() for line in cell["lines"]]
        if any(s.startswith("%md") for s in logical_lines):
            continue
        is_sql = cell["language"] == "sql"
        for s in logical_lines:
            if not s or s.startswith("%"):
                continue
            if s.startswith("--") if is_sql else s.startswith("#"):
                continue
            indices.append(cell["index"])
            break
    return indices


def executable_cell_count(path: str) -> int:
    """Number of runnable cells; the denominator of execution coverage."""
    return len(executable_cell_indices(path))
