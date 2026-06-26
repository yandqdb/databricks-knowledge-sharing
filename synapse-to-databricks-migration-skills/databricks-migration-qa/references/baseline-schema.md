# Run-spec and baseline schema

Two input files. Both are plain YAML (JSON also accepted). The scripts read them
with PyYAML if present, otherwise a minimal built-in reader handles the
documented shape (same approach as the sibling skills).

## run-spec.yaml

Describes what to run, where, and with which parameters. One entry per notebook.

```yaml
# Optional global defaults applied to every notebook unless overridden.
defaults:
  workspace_profile: translink-demo        # Databricks CLI/SDK auth profile
  compute:
    type: serverless                       # serverless | existing_cluster | job_cluster
    # existing_cluster_id: 0612-...        # when type == existing_cluster
  catalog: translink                       # used by compare_baseline for table refs
  schema: compass

notebooks:
  - name: ridership_rollup                 # logical id, used on the CLI and in reports
    original: ./original/ridership_rollup.py
    migrated: ./migrated/ridership_rollup.py
    # where it runs in the workspace; the migrated source is deployed here for the run
    workspace_path: /Workspace/translink/migration/ridership_rollup
    parameters:                            # notebook widgets (dbutils.widgets.get)
      run_date: "2026-06-01"
      region: "metro"
    # output tables this notebook writes, used for parity (Phase 3)
    outputs:
      - translink.compass.ridership_daily
    # optional per-notebook overrides
    compute:
      type: serverless
```

Field notes:
- `name` is the key used by `--notebook <name>` across run_notebook,
  compare_baseline, and the report.
- `parameters` map directly to notebook widget values passed to the job run.
- `workspace_path` is where the migrated notebook is imported/run. For the demo
  this lives under the migration folder (see `run-on-databricks.md`).
- `outputs` lists the fully-qualified tables to check for parity. Omit to skip
  parity for that notebook (it will report `not_evaluated`).

## baseline.yaml

The captured Synapse truth each output table is compared against. Capture this
from the source system BEFORE cutover.

```yaml
tolerances:
  row_count_tol: 0          # absolute row-count delta allowed (default 0 = exact)
  null_rate_tol: 0.0        # allowed abs difference in per-column null fraction

tables:
  translink.compass.ridership_daily:
    row_count: 1840293
    # optional, per column null fraction in [0,1]
    null_rates:
      rider_id: 0.0
      tap_ts: 0.0
      fare_amount: 0.012
    # optional content checksum (see run-on-databricks.md for how to compute the
    # same checksum on both sides so they are comparable)
    checksum: "a3f9c1..."   # omit if not captured
    # optional override of global tolerances for this table
    row_count_tol: 0
```

Rules:
- Only fields present are checked. A table with just `row_count` is checked on
  row count only.
- `checksum` must be computed the same way on Synapse and Databricks to be
  meaningful. The reference query is in `run-on-databricks.md`. When in doubt,
  rely on row_count + null_rates and leave checksum out.
- Tables listed in a notebook's `outputs` but absent from `baseline.yaml` are
  reported as `not_evaluated`, not as failures.
