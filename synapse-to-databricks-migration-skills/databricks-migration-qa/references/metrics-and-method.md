# Metrics and method

Three numbers, computed independently, then combined into the success rate.

## 1. Change coverage (skill effectiveness)

Question: of everything the migration skills *should* have changed in a
notebook, what fraction did they actually resolve?

```
change_coverage = resolved_sites / required_sites
resolved_sites  = required_sites - residual_sites
```

- **required_sites**: every migration-needed site found in the ORIGINAL
  notebook. Two categories:
  - `ansi`: cast / parse / integer-divide / typed-insert / implicit-coercion
    sites, detected with `databricks-ansi-remediation/scripts/detect_ansi_sites.py`.
  - `metastore`: every `hive_metastore.<db>.<table>` reference (qualified,
    quoted, and `USE`/SQL-position forms), detected by a regex pass over the
    parsed cells.
- **residual_sites**: of those, how many are STILL present in the MIGRATED
  notebook. A site is resolved when it no longer appears in the migrated cell
  (rewritten to `try_cast`, covered by a session-flag cell, or remapped to the
  UC name).

Report:
- overall coverage % (all categories pooled),
- per-category coverage % (ansi, metastore),
- the residual list: each residual site's category, cell index, line, snippet,
  so the engineer sees precisely what to hand-fix.

Notes:
- The session-flag remediation (`spark.sql.ansi.enabled=false`) resolves ALL
  ansi sites at once. When the migrated notebook contains that flag cell, count
  every ansi site as resolved regardless of whether the casts were also
  rewritten. `change_coverage.py` checks for the flag before counting ansi
  residuals.
- 100% does not prove correctness, only completeness of the mechanical edits.
  Correctness is Phase 2 (runs) + Phase 3 (parity).

## 2. Execution coverage

Question: when the migrated notebook runs on Databricks, how much of it actually
executes?

```
execution_coverage = executed_cells / executable_cells
```

- **executable_cells**: cells that are runnable code, i.e. not markdown
  (`%md`) and not empty/whitespace-only, counted with the same `nb_io.py` parser
  used everywhere else.
- **executed_cells**: cells that ran during the validation run. A notebook job
  run stops at the first error, so this is the count of cells up to and
  including the last one that produced output, derived from the run's task
  output / cell results.

Interpretation:
- 100% = ran end to end.
- < 100% = the run stopped early; the report names the first failing cell and
  its error message. This is the single most useful diagnostic line.
- Cell granularity is intentional. Statement/line coverage inside cells would
  require instrumenting the notebook with coverage.py and running it as a
  script, which is out of scope for a jobs notebook run and not how these
  notebooks are deployed.

## 3. Run outcome and parity

Per notebook:
- **runs** (bool): the job run reached a terminal SUCCESS state with no error.
- **parity** (pass / fail / not_evaluated): the migrated notebook's declared
  output tables match the Synapse baseline within tolerance:
  - row_count: exact match required (tolerance configurable, default 0),
  - null_rate per column: within `null_rate_tol` (default 0.0),
  - checksum (optional): exact match when provided.
  - `not_evaluated` when no baseline was supplied for that table.

## 4. Success and success rate

Per notebook:

```
pass = runs AND (parity == pass) AND (change_coverage >= change_gate)
```

- `change_gate` default = 1.0 (require 100% change coverage). Configurable via
  `qa_report.py --change-gate 0.95` etc. If relaxed, the report states the gate
  used so the number is never silently inflated.
- If parity is `not_evaluated` (no baseline), `pass` falls back to
  `runs AND change_coverage >= change_gate` and the scorecard flags parity as
  not evaluated. Do not report such a run as full success without saying so.

Batch:

```
success_rate = (# notebooks where pass == true) / (total notebooks)   # as a %
```

Also report the batch averages of change coverage and execution coverage, and a
table of every notebook with its three numbers and pass/fail, sorted with
failures first.
