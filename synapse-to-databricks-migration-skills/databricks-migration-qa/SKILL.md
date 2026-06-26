---
name: databricks-migration-qa
description: Measure how well the Synapse-to-Databricks migration skills actually did their job, and whether a migrated notebook is correct. Given an original notebook, its migrated version, the parameters needed to run it, where to run it, and a captured Synapse baseline, this skill reports three numbers: change coverage (what percentage of the sites that needed migrating were resolved by the skills vs left for a human), execution coverage (what percentage of the migrated notebook's cells actually ran end to end on Databricks), and success rate (across a batch, the percentage of notebooks that run clean and match the Synapse baseline). Use as the post-conversion QA / validation step after databricks-ansi-remediation and databricks-metastore-remap, for "validate the migration", "did the skills catch everything", "code coverage of the migration", "post-conversion validation", "does the migrated notebook match Synapse", or "migration success rate".
---

# Databricks Migration QA (post-conversion validation + skill scoring)

The two flagship migration skills (`databricks-ansi-remediation`,
`databricks-metastore-remap`) change Synapse Spark notebooks so they run on
Databricks. This skill answers the two questions you ask once the changes are
made:

1. **Did the skills change everything they should have?** (change coverage)
2. **Does the migrated notebook actually run and produce the right answers?**
   (execution coverage + parity against the Synapse baseline)

It rolls those into a **success rate** across a batch of notebooks, so you can
quote a single percentage for how far the skill-driven migration got on its own.

Like its two siblings, this is a deliberately small, one-task teaching skill
(SKILL.md + a handful of scripts + a sample run-spec, baseline, and demo). Copy
its shape when the TransLink team authors their own skills.

## What it measures

Three independent numbers, defined precisely in
`references/metrics-and-method.md`. Short version:

- **Change coverage** = `resolved_sites / required_sites` per notebook.
  `required_sites` is every migration-needed site found in the **original**
  notebook (ANSI cast/parse/divide sites + `hive_metastore` references).
  `resolved_sites` is that count minus the sites still present in the
  **migrated** notebook. 100% means the skills caught everything and no hand
  fixing was needed. Anything below 100% lists exactly which sites a human had
  to take (or still needs to take). Reported overall and split by category
  (ansi, metastore).
- **Execution coverage** = `executed_cells / executable_cells` for one run of
  the migrated notebook on Databricks. Of the runnable (non-markdown, non-empty)
  cells, how many actually executed before the run finished or failed. 100%
  means the whole notebook ran end to end. Below 100% pinpoints the first cell
  that failed. (Cell-level is the unit; statement/line coverage needs a
  coverage.py wrapper and is out of scope for notebook job runs.)
- **Success rate** (batch) = `passing_notebooks / total_notebooks` as a percent.
  A notebook **passes** when all three hold: it **runs** to completion with no
  error, its outputs **match** the Synapse baseline within tolerance, and its
  **change coverage is 100%** (no residual unmigrated sites). The
  change-coverage gate is configurable; default requires 100%.

## When to use

- After running the ANSI and metastore-remap skills on a notebook, to confirm
  the conversion is complete and correct before deploying.
- When someone asks for the "code coverage" of the migration, "how well did the
  skills do", "the success rate", or "post-conversion validation".
- To produce a batch rollup over many notebooks (a folder export) so the
  engagement can report a single migration success percentage.

Pair with: `databricks-ansi-remediation` and `databricks-metastore-remap` (run
both first; this scores their output) and `databricks-resource-deployment`
(deploy only the notebooks that pass).

## Prerequisites

1. **Both versions of each notebook**: the `original` (pre-migration, still has
   `hive_metastore` refs and ANSI-unsafe casts) and the `migrated` version, in
   Databricks `.py` source format (preferred), `.sql`, or `.ipynb`.
2. **A run-spec** (`run-spec.yaml`) describing, per notebook: original path,
   migrated path, the workspace path where it should run, the run parameters
   (notebook widget key/values), and the compute target. Schema and a sample are
   in `references/baseline-schema.md` and `assets/sample_run_spec.yaml`.
3. **A Synapse baseline** (`baseline.yaml`): per output table, the row count and
   (optional) per-column null rates / checksum captured from the source system,
   plus tolerances. Schema and sample in `references/baseline-schema.md` and
   `assets/sample_baseline.yaml`.
4. **Python 3.9+.** Change coverage and reporting are standard library only. The
   run and parity steps need `databricks-sdk` (or the Databricks CLI) and a
   configured auth profile for the target workspace. See
   `references/run-on-databricks.md`.

## Workflow

### Phase 0 - Scope
Confirm the unit of work: a single notebook, or a batch (a folder / list).
Collect the run-spec and the baseline. For a single notebook you can pass paths
on the command line instead of a run-spec.

### Phase 1 - Change coverage (static, no compute)
Compare original vs migrated. This needs no cluster:

```bash
cd ~/.claude/skills/databricks-migration-qa/scripts
python change_coverage.py --original <orig.py> --migrated <migrated.py> \
  --json /tmp/change.json
```

Output is a table (category, required, resolved, residual, coverage %) plus a
JSON report and a list of any residual sites with cell/line/snippet so the
engineer can see exactly what the skills missed.

### Phase 2 - Run on Databricks (execution coverage + runs)
Submit the migrated notebook as a one-time job run on the target workspace with
its parameters, poll to completion, and capture per-cell execution:

```bash
python run_notebook.py --run-spec run-spec.yaml --notebook <name> \
  --profile <db_profile> --json /tmp/run.json
```

Returns: run state (succeeded / failed), the first failing cell if any, the run
URL, and `execution_coverage`. See `references/run-on-databricks.md` for the
demo workspace settings (serverless, the migration folder) and auth.

### Phase 3 - Parity vs the Synapse baseline
Compare the migrated notebook's output tables to the captured baseline:

```bash
python compare_baseline.py --baseline baseline.yaml --notebook <name> \
  --profile <db_profile> --json /tmp/parity.json
```

Returns per-table: row-count match (exact), null-rate match (within tolerance),
optional checksum match, and an overall parity pass/fail.

### Phase 4 - Score and report
Assemble the three reports into a per-notebook scorecard and, with `--batch`,
a rollup with the headline success rate:

```bash
python qa_report.py --change /tmp/change.json --run /tmp/run.json \
  --parity /tmp/parity.json --out /tmp/qa_report.md
# batch: pass a directory of per-notebook json triples
python qa_report.py --batch /tmp/qa_runs/ --out /tmp/qa_batch.md --html /tmp/qa_batch.html
```

### Phase 5 - Output
Deliver: the per-notebook scorecards (change %, execution %, runs, parity), the
batch rollup with the success rate, and the residual-site list for any notebook
under 100% change coverage so it is obvious what is left to hand-fix.

## Worked example - the bundled demo

The two sibling skills already ship a before/after notebook pair. Reuse them as
the end-to-end fixture:

- original  = `databricks-metastore-remap/assets/sample_synapse_notebook.py`
- migrated  = run both siblings on it (remap + ANSI) to get a fully migrated copy

`assets/sample_run_spec.yaml` and `assets/sample_baseline.yaml` are filled in
for that fixture. `assets/demo_walkthrough.md` walks the four phases and shows
the expected numbers: change coverage 100% when both siblings ran cleanly, and a
deliberately under-migrated copy (skip one cell) to show coverage dropping below
100% with the residual site named.

## Notes for the assistant

- **Static before dynamic.** Always run Phase 1 first. It needs no compute, it
  is instant, and a change coverage below 100% usually explains a later run
  failure (a missed `hive_metastore` ref or unsafe cast).
- **Never invent a baseline.** Parity is only meaningful against a real captured
  Synapse baseline. If the user has no baseline, run change + execution coverage
  and report parity as "not evaluated", do not fake an expected value.
- **Read the cells with `nb_io.py`.** The bundled copy matches the siblings so
  cell indexing lines up across all three skills' reports. Do not re-implement
  cell parsing.
- **Runs that fail partway are signal, not noise.** Report the first failing
  cell and its error; that is the most useful single line of the run report.
- **The success gate is configurable but honest.** Default success requires
  100% change coverage. If you relax it (for example to count notebooks that ran
  and matched even with one hand fix), say so explicitly in the report.

## References
- Metrics, formulas, and pass criteria: `references/metrics-and-method.md`
- Run-spec and baseline schema: `references/baseline-schema.md`
- Running on the demo workspace + auth: `references/run-on-databricks.md`
- Demo walkthrough: `assets/demo_walkthrough.md`
- Databricks Jobs run-now / one-time runs: https://docs.databricks.com/api/workspace/jobs/submit
- Databricks SDK for Python: https://databricks-sdk-py.readthedocs.io/
