# Script contract

The exact CLI and JSON contracts for the bundled scripts. Implementations must
match these so the four scripts compose and the report can read each other's
JSON. All scripts: `python <script>.py --help` works, exit 0 on success, exit
non-zero on usage/IO error, and `--json PATH` writes the documented object.

Style to match the sibling skills: argparse, `def main(argv=None) -> int`, a
human-readable table on stdout, machine JSON via `--json`. `change_coverage.py`,
`qa_report.py`, and `nb_io.py` are standard-library only. `run_notebook.py` and
`compare_baseline.py` may import `databricks-sdk` (preferred) or shell out to the
`databricks` CLI.

## nb_io.py
Bundled copy of the sibling skills' notebook reader (keep behavior identical so
cell indices line up). Must expose `load_cells(path) -> list[dict]` where each
cell is `{index, language, lines: list[str]}` and markdown cells have
`language == "md"`. Reuse the existing implementation from
`databricks-ansi-remediation/scripts/nb_io.py` verbatim.

## change_coverage.py
```
python change_coverage.py --original ORIG --migrated MIGRATED [--json PATH]
```
- Detect `required_sites` on ORIG: ansi sites (reuse the ansi detector's
  `scan_notebook`, imported or via subprocess `--json`) + metastore sites
  (regex over parsed cells for `hive_metastore.<db>.<table>` in qualified,
  back-quoted, and `USE hive_metastore` forms; skip comments/markdown using the
  same logical-line handling as the detector).
- Detect residuals on MIGRATED the same way. If MIGRATED contains a
  `spark.sql.ansi.enabled=false` setting cell, treat all ansi sites as resolved
  (ansi residual = 0).
- A metastore site is resolved when its specific `hive_metastore...` text no
  longer appears in the migrated notebook.
- JSON object:
```json
{
  "original": "ORIG", "migrated": "MIGRATED",
  "overall": {"required": N, "resolved": N, "residual": N, "coverage": 0.0-1.0},
  "by_category": {
    "ansi":      {"required": N, "resolved": N, "residual": N, "coverage": 0.0-1.0},
    "metastore": {"required": N, "resolved": N, "residual": N, "coverage": 0.0-1.0}
  },
  "residual_sites": [
    {"category":"ansi|metastore","cell_index":N,"line_in_cell":N,"snippet":"..."}
  ]
}
```
- `coverage` is `resolved/required`, or `1.0` when `required == 0`.

## run_notebook.py
```
python run_notebook.py --run-spec SPEC --notebook NAME [--profile P] [--json PATH]
```
- Resolve the notebook entry + merged defaults from SPEC.
- Import the `migrated` source to `workspace_path`, submit a one-time notebook
  job run with the configured compute and `notebook_params` from `parameters`,
  poll to terminal state, pull run output.
- `executable_cells` = count of non-md, non-empty cells via `nb_io.load_cells`.
  `executed_cells` = cells that ran (up to and including the last cell that
  produced output; on success this equals `executable_cells`).
- JSON object:
```json
{
  "name":"NAME","run_id":123,"run_url":"https://...","state":"SUCCESS|FAILED",
  "runs": true,
  "first_failing_cell": null,
  "error": null,
  "executable_cells": N, "executed_cells": N, "execution_coverage": 0.0-1.0
}
```
- Must not write tokens anywhere. Profile only.

## compare_baseline.py
```
python compare_baseline.py --baseline BASELINE --run-spec SPEC --notebook NAME [--profile P] [--json PATH]
```
- For each table in the notebook's `outputs`, run the queries in
  `run-on-databricks.md` (row count; per-column null rate; optional checksum)
  against the workspace, compare to BASELINE within tolerances.
- A table in `outputs` but missing from BASELINE => `not_evaluated`.
- JSON object:
```json
{
  "name":"NAME",
  "parity":"pass|fail|not_evaluated",
  "tables":[
    {"table":"c.s.t","row_count":{"expected":N,"actual":N,"ok":true},
     "null_rates":[{"column":"x","expected":0.0,"actual":0.0,"ok":true}],
     "checksum":{"expected":"..","actual":"..","ok":true},
     "result":"pass|fail|not_evaluated"}
  ]
}
```

## qa_report.py
```
# single notebook
python qa_report.py --change CHG.json --run RUN.json [--parity PAR.json] \
  [--change-gate 1.0] --out report.md [--html report.html]
# batch: a directory containing <name>.change.json / <name>.run.json / <name>.parity.json
python qa_report.py --batch DIR [--change-gate 1.0] --out batch.md [--html batch.html]
```
- Compute per-notebook `pass` per `metrics-and-method.md` section 4. Honor
  `--change-gate` and state it in the output. Parity `not_evaluated` falls back
  to runs + change-gate and is flagged.
- Batch: emit `success_rate` (% pass), batch averages of change and execution
  coverage, and a per-notebook table sorted failures-first.
- Markdown always; HTML optional. Also write a `*.json` summary next to the
  Markdown for machine use.

## Tests
Ship a small `scripts/test_qa.py` (stdlib `unittest`, no network) covering
`change_coverage.py` and `qa_report.py` against tiny inline fixtures:
- a fully-migrated notebook (coverage 1.0),
- one with a leftover `hive_metastore` ref (metastore residual, coverage < 1.0),
- one with a leftover cast and no session flag (ansi residual),
- report math: pass/fail and batch success_rate, including the
  parity-not-evaluated fallback and a relaxed change-gate.
The two Databricks-touching scripts are excluded from unit tests (no network in
CI); structure them so the SDK/CLI call is isolated behind one function for easy
manual or mocked checking.
