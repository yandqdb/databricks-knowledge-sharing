# Demo walkthrough

Runs the four phases end to end on the bundled fixture so the team sees the
three numbers move. Phase 1 and the report need no Databricks; Phases 2-3 need
the `translink-demo` profile (see `references/run-on-databricks.md`).

## 0. Build the migrated copy from the original

```bash
# original fixture lives in the metastore-remap skill
ORIG=~/.claude/skills/databricks-metastore-remap/assets/sample_synapse_notebook.py

# remap hive_metastore -> UC
cd ~/.claude/skills/databricks-metastore-remap/scripts
python remap_refs.py "$ORIG" --mapping ../assets/sample_mapping.yaml --out /tmp/demo_migrated.py

# fix ANSI (session-flag = exact parity)
cd ~/.claude/skills/databricks-ansi-remediation/scripts
python apply_remediation.py /tmp/demo_migrated.py --mode session-flag --write
```

## 1. Change coverage (no compute)

```bash
cd ~/.claude/skills/databricks-migration-qa/scripts
python change_coverage.py --original "$ORIG" --migrated /tmp/demo_migrated.py --json /tmp/change.json
```

Expected: overall coverage 100% (both siblings ran cleanly, so no residual ansi
or metastore sites). To show coverage dropping, make an under-migrated copy that
skips one cell and re-run:

```bash
# leave one hive_metastore ref un-remapped on purpose
python change_coverage.py --original "$ORIG" --migrated /tmp/demo_partial.py --json /tmp/change_partial.json
```

Expected: overall coverage below 100% with the leftover `hive_metastore`
reference named in `residual_sites` (category metastore, with cell/line/snippet).

## 2. Run on Databricks

```bash
python run_notebook.py --run-spec ../assets/sample_run_spec.yaml \
  --notebook ridership_rollup --profile translink-demo --json /tmp/run.json
```

Expected on a clean migration: `state SUCCESS`, `runs true`, execution coverage
100%. If a residual site survived (the partial copy), the run fails at that cell
and execution coverage drops, with `first_failing_cell` and the error reported.

## 3. Parity vs baseline

```bash
python compare_baseline.py --baseline ../assets/sample_baseline.yaml \
  --run-spec ../assets/sample_run_spec.yaml --notebook ridership_rollup \
  --profile translink-demo --json /tmp/parity.json
```

Expected: `parity pass` when the output table row count and null rates match the
baseline within tolerance. (The sample baseline numbers are placeholders; use a
real Synapse capture for a real check.)

## 4. Report

```bash
python qa_report.py --change /tmp/change.json --run /tmp/run.json \
  --parity /tmp/parity.json --out /tmp/qa_report.md

# batch rollup over a folder of per-notebook json triples
python qa_report.py --batch /tmp/qa_runs/ --out /tmp/qa_batch.md --html /tmp/qa_batch.html
```

The single-notebook scorecard shows change %, execution %, runs, parity, and
pass/fail. The batch rollup shows the headline success rate and the per-notebook
table sorted failures-first. The talking point for the team: a clean migration
reads 100% / 100% / pass; the partial copy shows exactly which number breaks and
which cell to fix.
