---
name: databricks-ansi-remediation
description: Detect and remediate ANSI-mode data-type breakages when migrating Synapse Spark notebooks to Databricks (DBR). Source notebooks ran with spark.sql.ansi.enabled=false (invalid casts resolve to NULL); target DBR runs ANSI on by default (the same casts raise at runtime). The skill scans notebook cells for affected constructs (explicit CAST/cast(), date/timestamp parsing, integer division, inserts into typed columns, implicit numeric coercions) and applies one of two remediations — rewrite sites to try_cast, or set spark.sql.ansi.enabled=false at notebook scope for exact legacy parity — emitting a reviewable per-cell diff the engineer approves. Use for Synapse-to-Databricks notebook migration, "ANSI cast failures", "CAST_INVALID_INPUT", "fix ansi mode", "try_cast migration", or notebooks that worked on Synapse but raise on DBR.
---

# Databricks ANSI-Mode Remediation (Synapse → DBR notebooks)

Synapse Spark notebooks ran with `spark.sql.ansi.enabled=false`, where an invalid
cast silently resolves to `NULL`. Target DBR runs ANSI **on** by default, so the
same cast raises at runtime (`CAST_INVALID_INPUT`, `DIVIDE_BY_ZERO`,
`ARITHMETIC_OVERFLOW`, parse errors). This is one of the two highest-frequency,
most-mechanical edits in a Synapse migration.

This skill is the executable form of that fix. It detects the affected constructs,
applies one of two remediations, and emits a **per-cell diff with rationale** that
an engineer reviews and approves. It does not change workload logic, and it never
writes a file without an explicit `--write`.

This skill is also a **teaching reference**: it is a deliberately small, one-task
skill (SKILL.md + a detector + a remediator + a construct catalog + a before/after
demo). Copy its shape when authoring the team's own skills during innovation week.

## When to use

- Migrating Synapse Spark notebooks to DBR and hitting cast / parse / overflow
  errors that did not occur on the source.
- Someone asks to "fix ANSI mode", "remediate cast failures", or "set up try_cast".
- You want a repeatable, reviewable procedure across many notebooks rather than
  hand-fixing each one.

Pair with: `databricks-metastore-remap` (the other flagship — run it on the same
notebooks), `databricks-resource-deployment` (deploy the fixed notebook), and the
team-built post-conversion validation skill (confirm row counts / null rates match
the Synapse baseline after the fix).

## Prerequisites

1. **The notebook(s)** in Databricks `.py` source format (preferred; `# COMMAND
   ----------` cells), or `.sql` / `.ipynb`. Export from the workspace if needed.
2. **Python 3.9+** to run the scripts (standard library only — no install).
3. **A remediation decision per notebook** — `try_cast` vs session-flag (see the
   decision guide in `references/ansi-constructs.md`). When unsure, default to
   session-flag for exact parity.

## Workflow

### Phase 0 — Scope
Confirm with the user: which notebook(s), and the default remediation preference
(parity-exact → session-flag; ANSI-native → try_cast). Note that we validate
against the source's NULL/row-count behavior afterward.

### Phase 1 — Detect
Run the detector to find every affected site and triage by confidence:

```bash
cd ~/.claude/skills/databricks-ansi-remediation/scripts
python detect_ansi_sites.py <notebook.py> --json /tmp/ansi_report.json
```

Output is a table (cell, line, confidence, construct, snippet) plus a JSON report.
`high` = definitely changes behavior; `medium` = usually; `low` = heuristic, eyeball
it. See `references/ansi-constructs.md` for what each construct does.

### Phase 2 — Choose the remediation
Per the decision guide:
- **Many mixed constructs, or parity must be exact** → session-flag.
- **Only explicit casts and NULL-on-bad-value is acceptable** → try_cast.
- A mix of casts + date-parse + inserts is safest handled by the session-flag in
  one step, because `try_cast` only covers explicit casts.

### Phase 3 — Apply and review the diff
Run the remediator. By default it prints a per-cell unified diff with a rationale
line and writes nothing:

```bash
# Option A: rewrite explicit casts to try_cast
python apply_remediation.py <notebook.py> --mode try_cast

# Option B: inject the session-scoped parity flag (recommended for exact parity)
python apply_remediation.py <notebook.py> --mode session-flag
```

The engineer reads the diff and approves. Then apply:

```bash
python apply_remediation.py <notebook.py> --mode session-flag --write   # keeps .bak
# or write to a new file instead of in place:
python apply_remediation.py <notebook.py> --mode try_cast --out <notebook>.remediated.py
```

### Phase 4 — Validate
Run the affected cells on DBR before the fix (they should raise), apply the fix,
run again (they should succeed). Confirm row counts and null rates match the
Synapse baseline — exactly for session-flag, and wherever inputs were valid for
try_cast. Hand this to the post-conversion validation skill if it exists.

### Phase 5 — Output
Deliver: the detection report, the approved per-cell diff, the remediated notebook
(`.bak` retained), and a one-line note of which mode was chosen and why. Flag any
`medium`/`low` sites the engineer chose not to touch.

## Worked example — the bundled demo

`assets/sample_synapse_notebook.py` is a realistic before fixture (a transit-style
ridership rollup) with planted constructs: explicit casts in PySpark and SQL, a
`to_date` parse, `div` integer division, an `INSERT INTO` a typed table, an implicit
numeric coercion, and a **trap** — an existing `try_cast` that must not be flagged
or double-rewritten.

`assets/demo_before_after.py` is the runnable workshop notebook: it creates a tiny
table with one bad value, shows the `CAST` **raising** under ANSI on (the failure a
migrated notebook hits), then shows both remediations succeeding — so the team sees
the try_cast-vs-parity trade-off live. Run the detector and both remediation modes
against `sample_synapse_notebook.py` to read the two diffs side by side.

Expected detector result on the fixture: 8 sites — 5 high (4 cast + 1 date-parse),
2 medium (div + insert), 1 low (implicit numeric); the `try_cast` trap is not
flagged. `--mode try_cast` rewrites the 4 casts across 3 cells; `--mode
session-flag` injects one parity cell.

## Notes for the assistant

- **Never write without showing the diff first.** The reviewable diff is the
  product; the engineer's approval is the gate.
- `try_cast` is **not** a behavioral identity for ANSI-off `CAST` in every edge
  case and only covers explicit casts. When in doubt, recommend session-flag.
- The session-flag is a **migration aid, not an end state** — it pins the notebook
  to legacy semantics. Note it as tech debt to revisit toward ANSI-native.
- Don't flag or rewrite `try_cast` (already remediated). The regex uses a word
  boundary so `try_cast` is correctly skipped — keep that property if you edit it.
- Magic `%sql` cells in `.py` source store SQL as `# MAGIC` lines; the scripts
  strip and re-add that prefix so rewrites round-trip. Don't bypass `nb_io.py`.

## References
- Construct catalog + decision guide: `references/ansi-constructs.md`
- Demo walkthrough: `assets/demo_walkthrough.md`
- Spark ANSI compliance: https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html
- Databricks `try_cast`: https://docs.databricks.com/en/sql/language-manual/functions/try_cast.html
