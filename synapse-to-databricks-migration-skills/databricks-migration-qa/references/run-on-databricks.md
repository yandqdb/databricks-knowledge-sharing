# Running on Databricks (demo workspace + auth)

Phase 2 (run) and Phase 3 (parity) are the only steps that touch a cluster.
Phase 1 (change coverage) and the report are local and need no auth.

## Demo workspace

- Workspace: `https://fevm-serverless-stable-l26d62.cloud.databricks.com`
  (`?o=7474646739115164`).
- Migration folder (where the demo notebooks live and run):
  `folders/1922333013000467` in that workspace. Point `workspace_path` in the
  run-spec under this folder.
- Compute: serverless by default for the demo (no cluster to start, fastest path
  for a live walkthrough).

## Auth

Use a Databricks CLI / SDK profile. Configure once:

```bash
databricks auth login --host https://fevm-serverless-stable-l26d62.cloud.databricks.com \
  --profile translink-demo
```

Reference the profile with `--profile translink-demo` (the scripts pass it to
`databricks-sdk` `WorkspaceClient(profile=...)`, or to the CLI). Never hardcode
tokens in the run-spec or scripts.

## How the run step works

`run_notebook.py` should:
1. Import / update the migrated notebook source to `workspace_path`.
2. Submit a one-time job run (Jobs `submit`, a.k.a. "runs submit") with a single
   notebook task, the configured compute, and `notebook_params` set from the
   run-spec `parameters`.
3. Poll the run to a terminal state.
4. Pull the run output to determine, per cell/task, what executed and where it
   stopped, then compute `execution_coverage = executed_cells / executable_cells`
   using the local `nb_io.py` cell count for the denominator.
5. Emit JSON: `{name, run_id, run_url, state, runs (bool), first_failing_cell,
   error, executable_cells, executed_cells, execution_coverage}`.

Notes:
- A notebook job run halts at the first uncaught error; that cell index is
  `first_failing_cell`. Cells after it did not execute.
- Prefer serverless for the demo; allow `existing_cluster_id` for repeat runs on
  a warm cluster.

## How the parity step works

`compare_baseline.py` should, per output table in the notebook's `outputs`:
1. `SELECT COUNT(*)` for row count.
2. For each column in the baseline `null_rates`, compute
   `SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END) / COUNT(*)`.
3. Optional checksum: use a deterministic, order-independent table digest so it
   matches the Synapse-side capture. Reference form:

   ```sql
   SELECT md5(string(sum(crc32(to_json(struct(*)))))) AS checksum
   FROM <catalog>.<schema>.<table>
   ```

   (Capture the equivalent on Synapse. If you cannot guarantee identical
   serialization on both engines, drop checksum and rely on row_count +
   null_rates.)
4. Compare to baseline within tolerances; emit JSON per table plus an overall
   `parity` of pass / fail / not_evaluated.

Run these queries through the SDK SQL execution API or the CLI against a SQL
warehouse / serverless on the same workspace.
